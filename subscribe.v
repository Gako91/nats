module nats

pub fn (mut nc Client) subscribe(subject string) !Subscription {
	return nc.queue_subscribe(subject, '')
}

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

pub fn (mut nc Client) unsubscribe(sub Subscription) ! {
	nc.write('UNSUB ${sub.sid}${crlf}')!
	nc.subs.delete(sub.sid)
	return
}

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
