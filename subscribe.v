module nats

import json2
import time

// subscribe creates a subscription to a subject.
// Returns a Subscription which you can pass to next_msg() or unsubscribe().
// Subject patterns: 'order.create' (exact), 'order.>' (all subjects under order), 'order.*' (one level).
pub fn (mut nc Client) subscribe(subject string) !Subscription {
	return nc.queue_subscribe(subject, '')
}

// queue_subscribe creates a subscription in a queue group.
// All subscribers in the same queue group will receive messages in a load-balanced fashion
// (each message goes to one random subscriber). Use this for worker pools or load distribution.
// Pass empty string for queue to create a regular (non-queue) subscription.
pub fn (mut nc Client) queue_subscribe(subject string, queue string) !Subscription {
	validate_subject(subject)!
	if queue.contains(' ') {
		return error('queue group must not contain spaces')
	}
	sid := nc.next_sid.str()
	nc.next_sid++
	if queue == '' {
		nc.write('SUB ${subject} ${sid}${crlf}')!
	} else {
		nc.write('SUB ${subject} ${queue} ${sid}${crlf}')!
	}
	sub := Subscription{
		subject: subject
		queue:   queue
		sid:     sid
	}
	nc.subs[sid] = sub
	return sub
}

// unsubscribe stops listening on a subscription.
// After unsubscribe, no new messages for this subject will be received.
// Messages already buffered for this subscription are discarded.
pub fn (mut nc Client) unsubscribe(sub Subscription) ! {
	nc.subs.delete(sub.sid)
	nc.pending_msgs = nc.pending_msgs.filter(it.sid != sub.sid)
	nc.write('UNSUB ${sub.sid}${crlf}')!
	return
}

// next_msg waits for and returns the next message on any active subscription.
// This call blocks until a message arrives or the read timeout is exceeded.
// Also processes async JetStream publish acknowledgments (routes to callbacks).
// Returns error if no messages available within the timeout, or if connection is closed.
pub fn (mut nc Client) next_msg() !Msg {
	if nc.pending_msgs.len > 0 {
		msg := nc.pending_msgs[0]
		nc.pending_msgs.delete(0)
		return msg
	}
	return nc.read_next_msg()
}

// next_msg_for_sid returns the next message for one subscription only.
// Messages for other subscriptions received meanwhile are buffered in
// pending_msgs (in order) so a later next_msg() still returns them.
fn (mut nc Client) next_msg_for_sid(sid string) !Msg {
	for i, msg in nc.pending_msgs {
		if msg.sid == sid {
			nc.pending_msgs.delete(i)
			return msg
		}
	}
	for {
		msg := nc.read_next_msg()!
		if msg.sid == sid {
			return msg
		}
		nc.pending_msgs << msg
	}
	return error(err_connection_closed)
}

// read_next_msg reads protocol frames from the socket until a message for an
// active subscription arrives. It ignores pending_msgs.
fn (mut nc Client) read_next_msg() !Msg {
	for {
		msg, ok := nc.read_frame()!
		if ok {
			return msg
		}
	}
	return error(err_connection_closed)
}

// read_frame reads and handles one protocol frame. It returns (msg, true) when
// the frame is a message to hand to the caller, (Msg{}, false) otherwise
// (PING/PONG/INFO, async publish ACK, stale or unsubscribed message).
fn (mut nc Client) read_frame() !(Msg, bool) {
	line := nc.read_line()!
	if line == '' {
		return Msg{}, false
	}
	frame := parse_protocol_line(line)
	match frame.op {
		.ping {
			nc.write('PONG${crlf}')!
		}
		.pong, .ok {}
		.err {
			return error(frame.raw)
		}
		.info {
			nc.parse_info(frame.payload) or {}
		}
		.msg, .hmsg {
			return nc.route_msg_frame(frame)!
		}
		.unknown {
			return error('unexpected NATS protocol line: ${frame.raw}')
		}
	}
	return Msg{}, false
}

// route_msg_frame parses a MSG/HMSG frame and decides where it goes. Returns
// (msg, true) if the message must be delivered to the caller.
fn (mut nc Client) route_msg_frame(frame ProtocolLine) !(Msg, bool) {
	msg := if frame.op == .msg {
		nc.parse_msg(frame.raw)!
	} else {
		nc.parse_hmsg(frame.raw)!
	}
	if nc.resp_sid != '' && msg.sid == nc.resp_sid {
		// Shared response inbox: either an async publish ACK, the reply
		// request() is waiting for, or a stale reply (timed out) to drop.
		if nc.handle_async_publish_ack(msg) {
			return Msg{}, false
		}
		return msg, msg.subject == nc.resp_waiting
	}
	if nc.pending_publishes.len > 0 {
		nc.cleanup_expired_publishes()
	}
	// Drop messages still in flight for a subscription that was
	// already removed (e.g. a late reply after a request timeout).
	return msg, msg.sid in nc.subs
}

// handle_async_publish_ack routes a reply on the shared inbox to its pending
// async publish. Returns true if the message was an ACK (callback invoked),
// false otherwise.
fn (mut nc Client) handle_async_publish_ack(msg Msg) bool {
	pending := nc.pending_publishes[msg.subject] or { return false }
	nc.pending_publishes.delete(msg.subject)

	callback := pending.callback or { return true }
	if msg.is_no_responders() {
		callback(mut nc, pending.subject, PublishResult{
			error_msg: 'JetStream publish error: no stream matches subject ${pending.subject}'
		})
		return true
	}
	decoded := json2.decode[PubAck](msg.text()) or {
		callback(mut nc, pending.subject, PublishResult{
			error_msg: 'JetStream publish error: invalid ACK: ${err}'
		})
		return true
	}
	if decoded.error.code != 0 || decoded.error.err_code != 0 || decoded.error.description != '' {
		callback(mut nc, pending.subject, PublishResult{
			error_msg: 'JetStream publish error: ${decoded.error.description}'
		})
	} else {
		callback(mut nc, pending.subject, PublishResult{
			ack: decoded
		})
	}
	return true
}

// cleanup_expired_publishes removes pending publishes that have exceeded their timeout.
// Called from read_next_msg() whenever publishes are pending, to prevent leaks.
fn (mut nc Client) cleanup_expired_publishes() {
	mut expired := []string{}

	for reply, pending in nc.pending_publishes {
		elapsed := time.since(pending.created_at)
		timeout_duration := pending.timeout_ms * time.millisecond
		if elapsed > timeout_duration {
			expired << reply
		}
	}

	for reply in expired {
		if pending := nc.pending_publishes[reply] {
			if callback := pending.callback {
				result := PublishResult{
					ack:       PubAck{}
					error_msg: 'JetStream publish timeout: no ACK received within ${pending.timeout_ms}ms'
				}
				callback(mut nc, pending.subject, result)
			}
		}
		nc.pending_publishes.delete(reply)
	}
}
