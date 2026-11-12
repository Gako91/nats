module nats

import json2
import time

// PubAck is the acknowledgment returned when publishing to a JetStream stream.
pub struct PubAck {
pub:
	// stream: name of the stream that stored the message
	stream string
	// seq: sequence number of the message in the stream (for replay/ordering)
	seq u64
	// duplicate: true if this message was previously published (idempotent re-delivery)
	duplicate bool
	// error: if set, an error occurred during publish
	error ApiError
}

// PublishResult encapsulates the result of an async JetStream publish.
// Either ack is populated (success) or error_msg is non-empty (failure).
pub struct PublishResult {
pub:
	ack       PubAck
	error_msg string
}

// publish sends a message to a JetStream stream with persistent acknowledgment.
// Unlike plain publish(), this waits for the server to confirm message storage.
// Returns a PubAck with the stream name and sequence number, or error if publish fails.
pub fn (mut js JetStream) publish(subject string, data []u8) !PubAck {
	ack_msg := js.nc.request(subject, data, 5 * time.second)!
	ack := json2.decode[PubAck](ack_msg.text())!
	if ack.error.code != 0 || ack.error.err_code != 0 || ack.error.description != '' {
		return error('JetStream publish failed: ${ack.error.description}')
	}
	return ack
}

// publish_string sends a string message to a JetStream stream with persistent acknowledgment.
// Convenience wrapper around publish() that converts string to bytes.
pub fn (mut js JetStream) publish_string(subject string, data string) !PubAck {
	return js.publish(subject, data.bytes())
}

// publish_async sends a message to a JetStream stream without waiting for the ACK.
// The ACK comes back on the client's shared response inbox and `callback` is
// invoked from next_msg(), flush() or publish_async_complete() when it arrives,
// or with a timeout error after Options.publish_async_timeout.
// No SUB/flush per message: the call only writes the PUB and returns.
pub fn (mut js JetStream) publish_async(subject string, data []u8, callback PublishAckCallback) ! {
	reply := js.nc.new_resp_subject()!
	// Register before publishing so a fast ACK always finds its entry.
	js.nc.pending_publishes[reply] = PendingPublish{
		inbox:      reply
		subject:    subject
		callback:   callback
		created_at: time.now()
		timeout_ms: js.nc.opts.publish_async_timeout.milliseconds()
	}
	js.nc.publish_with_reply(subject, reply, data) or {
		js.nc.pending_publishes.delete(reply)
		return err
	}
}

// publish_async_complete processes incoming frames until every pending async
// publish has completed (callback invoked) or `timeout` elapses. Messages for
// regular subscriptions received meanwhile are kept for next_msg().
pub fn (mut js JetStream) publish_async_complete(timeout time.Duration) ! {
	deadline := time.now().add(timeout)
	defer {
		js.nc.set_read_timeout(js.nc.opts.connect_timeout)
	}
	for js.nc.pending_publishes.len > 0 {
		remaining := deadline - time.now()
		if remaining <= 0 {
			return error('nats: ${js.nc.pending_publishes.len} async publish(es) still pending after ${timeout}')
		}
		js.nc.set_read_timeout(remaining)
		msg, deliver := js.nc.read_frame() or {
			if is_timeout_error(err) {
				continue
			}
			return err
		}
		if deliver {
			js.nc.pending_msgs << msg
		}
	}
}

// publish_async_pending returns the number of async publishes awaiting an ACK.
pub fn (js &JetStream) publish_async_pending() int {
	return js.nc.pending_publishes.len
}

// publish_string_async is a convenience wrapper around publish_async that converts string to bytes.
pub fn (mut js JetStream) publish_string_async(subject string, data string, callback PublishAckCallback) ! {
	return js.publish_async(subject, data.bytes(), callback)
}
