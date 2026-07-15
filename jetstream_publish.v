module nats

import json
import rand
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
	ack := json.decode(PubAck, ack_msg.text())!
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

// publish_async sends a message to a JetStream stream without blocking.
// The acknowledgment will be received asynchronously and the callback will be invoked.
// Useful for high-throughput publishing where you don't want to wait for each ACK.
// The callback is invoked on the reader thread, so keep it fast. Returns immediately.
pub fn (mut js JetStream) publish_async(subject string, data []u8, callback PublishAckCallback) ! {
	// Create a unique reply inbox
	inbox := new_inbox()

	// Subscribe to the reply inbox to receive the ACK
	sub := js.nc.subscribe(inbox)!
	js.nc.flush()!

	// Register the pending publish with callback before publishing
	// Use configurable timeout from Options (default 5s, convert to milliseconds)
	timeout_ms := js.nc.opts.publish_async_timeout.milliseconds()
	pending := PendingPublish{
		inbox:      inbox
		subject:    subject
		callback:   callback
		created_at: time.now()
		timeout_ms: timeout_ms
	}
	js.nc.pending_publishes[sub.sid] = pending

	// Publish the message with the reply subject
	// The ACK will come back on the inbox subscription and be handled by next_msg()'s routing
	js.nc.publish_with_reply(subject, inbox, data)!

	return
}

// publish_string_async is a convenience wrapper around publish_async that converts string to bytes.
pub fn (mut js JetStream) publish_string_async(subject string, data string, callback PublishAckCallback) ! {
	return js.publish_async(subject, data.bytes(), callback)
}

// new_inbox creates a unique reply inbox for internal use (re-exported from request module).
fn new_inbox() string {
	return '_INBOX.${rand.ulid()}'
}
