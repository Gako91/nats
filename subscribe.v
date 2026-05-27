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
	for {
		line := nc.read_line()!
		if line == '' {
			continue
		}
		if line == 'PING' {
			nc.write('PONG${crlf}')!
			continue
		}
		if line == 'PONG' || line.starts_with('+OK') {
			continue
		}
		if line.starts_with('-ERR') {
			return error(line)
		}
		if line.starts_with('INFO ') {
			nc.parse_info(line[5..]) or {}
			continue
		}
		if line.starts_with('MSG ') {
			return nc.parse_msg(line)!
		}
		return error('unexpected NATS protocol line: ${line}')
	}
	return error('connection closed')
}
