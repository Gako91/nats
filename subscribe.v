module nats

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
				return nc.parse_msg(frame.raw)!
			}
			.hmsg {
				return nc.parse_hmsg(frame.raw)!
			}
			.unknown {
				return error('unexpected NATS protocol line: ${frame.raw}')
			}
		}
	}
	return error(err_connection_closed)
}
