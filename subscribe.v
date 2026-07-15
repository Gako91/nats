module nats

import json
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
pub fn (mut nc Client) unsubscribe(sub Subscription) ! {
	nc.write('UNSUB ${sub.sid}${crlf}')!
	nc.subs.delete(sub.sid)
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

	for {
		line := nc.read_line()!
		if line == '' {
			continue
		}
		frame := parse_protocol_line(line)
		match frame.op {
			.ping {
				nc.write('PONG${crlf}')!
				continue
			}
			.pong, .ok {
				continue
			}
			.err {
				return error(frame.raw)
			}
			.info {
				nc.parse_info(frame.payload) or {}
				continue
			}
			.msg {
				msg := nc.parse_msg(frame.raw)!
				// Check if this is an async publish ACK response
				if nc.handle_async_publish_ack(msg) {
					continue
				}
				return msg
			}
			.hmsg {
				msg := nc.parse_hmsg(frame.raw)!
				// Check if this is an async publish ACK response
				if nc.handle_async_publish_ack(msg) {
					continue
				}
				return msg
			}
			.unknown {
				return error('unexpected NATS protocol line: ${frame.raw}')
			}
		}
	}
	return error(err_connection_closed)
}

// handle_async_publish_ack checks if a message is an async publish ACK response.
// If it is, invokes the callback and returns true (message was handled).
// Returns false if message is not an ACK and should be returned to caller.
// Also performs cleanup of expired pending publishes.
fn (mut nc Client) handle_async_publish_ack(msg Msg) bool {
	// Check if this message's SID corresponds to a pending publish inbox
	if msg.sid !in nc.pending_publishes {
		// Clean expired publishes periodically (every 100 messages or so)
		if nc.pending_publishes.len > 0 {
			nc.cleanup_expired_publishes()
		}
		return false
	}

	// Try to decode as PubAck JSON
	decoded := json.decode(PubAck, msg.text()) or {
		// Not a valid PubAck JSON, probably a regular message, don't handle
		return false
	}

	// Get the pending publish info
	pending := nc.pending_publishes[msg.sid] or { return false }

	// Invoke the callback with the decoded ACK
	if callback := pending.callback {
		if decoded.error.code != 0 || decoded.error.err_code != 0 || decoded.error.description != '' {
			// Error case
			result := PublishResult{
				ack:       PubAck{}
				error_msg: 'JetStream publish error: ${decoded.error.description}'
			}
			callback(mut nc, pending.subject, result)
		} else {
			// Success case
			result := PublishResult{
				ack:       decoded
				error_msg: ''
			}
			callback(mut nc, pending.subject, result)
		}
	}

	// Remove from pending publishes
	nc.pending_publishes.delete(msg.sid)

	// Message was handled by callback
	return true
}

// cleanup_expired_publishes removes pending publishes that have exceeded their timeout.
// Called periodically from handle_async_publish_ack to prevent memory leak.
fn (mut nc Client) cleanup_expired_publishes() {
	mut expired_sids := []string{}

	for sid, pending in nc.pending_publishes {
		elapsed := time.since(pending.created_at)
		timeout_duration := pending.timeout_ms * time.millisecond
		if elapsed > timeout_duration {
			expired_sids << sid
		}
	}

	for sid in expired_sids {
		if pending := nc.pending_publishes[sid] {
			if callback := pending.callback {
				result := PublishResult{
					ack:       PubAck{}
					error_msg: 'JetStream publish timeout: no ACK received within ${pending.timeout_ms}ms'
				}
				callback(mut nc, pending.subject, result)
			}
		}
		nc.pending_publishes.delete(sid)
	}
}
