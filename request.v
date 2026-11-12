module nats

import rand
import time

// request sends a message and waits for a response (request-reply pattern).
// Replies come back on a shared response inbox, so a request costs a single
// round trip (no per-request SUB/flush/UNSUB).
// Returns error if no responder answers within the timeout or if no responder exists.
// Common timeout values: 1*time.second for quick responses, 5*time.second for slower services.
pub fn (mut nc Client) request(subject string, data []u8, timeout time.Duration) !Msg {
	reply := nc.new_resp_subject()!
	nc.publish_with_reply(subject, reply, data)!
	nc.set_read_timeout(timeout)
	defer {
		nc.set_read_timeout(nc.opts.connect_timeout)
	}
	// Messages for other subscriptions arriving meanwhile are kept for next_msg().
	msg := nc.next_resp_msg(reply) or {
		if is_timeout_error(err) {
			return error(err_request_timeout)
		}
		return err
	}
	if msg.is_no_responders() {
		return error(err_no_responders)
	}
	return msg
}

// request_string sends a string request and waits for a response.
// Convenience wrapper around request() that converts string to bytes.
pub fn (mut nc Client) request_string(subject string, data string, timeout time.Duration) !Msg {
	return nc.request(subject, data.bytes(), timeout)
}

pub fn new_inbox() string {
	return '_INBOX.${rand.ulid()}'
}

// new_resp_subject returns a fresh reply subject on the shared response inbox,
// subscribing to `<prefix>*` the first time. The subscription is part of
// nc.subs, so it is restored automatically after a reconnect.
fn (mut nc Client) new_resp_subject() !string {
	if nc.resp_prefix == '' {
		nc.resp_prefix = '${new_inbox()}.'
	}
	if nc.resp_sid == '' || nc.resp_sid !in nc.subs {
		sub := nc.subscribe('${nc.resp_prefix}*')!
		nc.resp_sid = sub.sid
	}
	nc.resp_seq++
	return '${nc.resp_prefix}${nc.resp_seq}'
}

// next_resp_msg waits for the reply sent to `reply` on the shared inbox.
// Messages for other subscriptions received meanwhile are buffered in
// pending_msgs; stale replies on the shared inbox are dropped.
fn (mut nc Client) next_resp_msg(reply string) !Msg {
	nc.resp_waiting = reply
	defer {
		nc.resp_waiting = ''
	}
	for {
		msg := nc.read_next_msg()!
		if msg.sid == nc.resp_sid && msg.subject == reply {
			return msg
		}
		nc.pending_msgs << msg
	}
	return error(err_connection_closed)
}
