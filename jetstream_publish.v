module nats

import json
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
