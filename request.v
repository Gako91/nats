module nats

import rand
import time

// request sends a message and waits for a response (request-reply pattern).
// Automatically creates a unique reply inbox, publishes the request, and waits for response.
// Returns error if no responder answers within the timeout or if no responder exists.
// Common timeout values: 1*time.second for quick responses, 5*time.second for slower services.
pub fn (mut nc Client) request(subject string, data []u8, timeout time.Duration) !Msg {
	inbox := new_inbox()
	sub := nc.subscribe(inbox)!
	nc.flush()!
	nc.publish_with_reply(subject, inbox, data)!
	nc.set_read_timeout(timeout)
	defer {
		nc.set_read_timeout(nc.opts.connect_timeout)
		nc.unsubscribe(sub) or {}
	}
	for {
		msg := nc.next_msg() or {
			if is_timeout_error(err) {
				return error(err_request_timeout)
			}
			return err
		}
		if msg.sid == sub.sid || msg.subject == inbox {
			if msg.is_no_responders() {
				return error(err_no_responders)
			}
			return msg
		}
	}
	return error(err_request_timeout)
}

// request_string sends a string request and waits for a response.
// Convenience wrapper around request() that converts string to bytes.
pub fn (mut nc Client) request_string(subject string, data string, timeout time.Duration) !Msg {
	return nc.request(subject, data.bytes(), timeout)
}

pub fn new_inbox() string {
	return '_INBOX.${rand.ulid()}'
}
