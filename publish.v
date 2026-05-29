module nats

pub fn (mut nc Client) publish(subject string, data []u8) ! {
	nc.publish_with_reply(subject, '', data)!
	return
}

pub fn (mut nc Client) publish_string(subject string, data string) ! {
	nc.publish(subject, data.bytes())!
	return
}

pub fn (mut nc Client) publish_with_reply(subject string, reply string, data []u8) ! {
	validate_subject(subject)!
	if nc.info.max_payload > 0 && data.len > nc.info.max_payload {
		return error(err_max_payload(data.len, nc.info.max_payload))
	}
	if reply == '' {
		nc.write('PUB ${subject} ${data.len}${crlf}')!
	} else {
		validate_subject(reply)!
		nc.write('PUB ${subject} ${reply} ${data.len}${crlf}')!
	}
	nc.conn.write(data)!
	nc.write(crlf)!
	return
}
