module nats

// publish sends a message to the given subject.
// Returns error if subject is invalid, connection is closed, or message exceeds max_payload.
// Use publish_string() if you have a string message.
pub fn (mut nc Client) publish(subject string, data []u8) ! {
	nc.publish_with_reply(subject, '', data)!
	return
}

// publish_string sends a string message to the given subject.
// Convenience wrapper around publish() that converts string to bytes.
pub fn (mut nc Client) publish_string(subject string, data string) ! {
	nc.publish(subject, data.bytes())!
	return
}

// publish_with_reply sends a message with a reply subject.
// Used for request-reply patterns: the reply subject is where responses will be sent.
// Set reply='' for normal pub/sub (same as publish()).
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
