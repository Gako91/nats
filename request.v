module nats

import rand
import time

pub fn (mut nc Client) request(subject string, data []u8, timeout time.Duration) !Msg {
	inbox := new_inbox()
	sub := nc.subscribe(inbox)!
	nc.flush()!
	nc.publish_with_reply(subject, inbox, data)!
	nc.conn.set_read_timeout(timeout)
	defer {
		nc.conn.set_read_timeout(nc.opts.connect_timeout)
		nc.unsubscribe(sub) or {}
	}
	for {
		msg := nc.next_msg()!
		if msg.sid == sub.sid || msg.subject == inbox {
			return msg
		}
	}
	return error('request timed out')
}

pub fn (mut nc Client) request_string(subject string, data string, timeout time.Duration) !Msg {
	return nc.request(subject, data.bytes(), timeout)
}

pub fn new_inbox() string {
	return '_INBOX.${rand.ulid()}'
}
