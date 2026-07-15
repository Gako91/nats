module nats

// Msg represents a message received from NATS.
// `subject`: the topic the message was sent to
// `data`: the message payload (binary data)
// `reply`: if this message is a response, contains the reply inbox address
// `headers`: optional key-value metadata (if server supports headers)
// `status`: HTTP-like status code for special messages (e.g., 503 = no responders)
pub struct Msg {
pub:
	subject     string
	sid         string
	reply       string
	data        []u8
	headers     map[string]string
	status      int
	description string
}

// text converts the message data to a string. Returns empty string if data cannot be converted.
pub fn (m Msg) text() string {
	return m.data.bytestr()
}

// is_no_responders returns true if this message is a 503 "no responders" error.
// This happens when you call `request()` but no service is listening on that subject.
pub fn (m Msg) is_no_responders() bool {
	return m.status == 503
}

// is_jetstream returns true if this message was delivered by a JetStream consumer.
// JetStream messages always have a reply subject starting with '$JS.ACK'.
pub fn (m Msg) is_jetstream() bool {
	return m.reply.starts_with('\$JS.ACK')
}

// ack acknowledges successful processing of this JetStream message.
// The server will not redeliver it. Returns error if the message has no reply subject
// or if the publish fails.
pub fn (m Msg) ack(mut nc Client) ! {
	if m.reply == '' {
		return error('message has no reply subject — not a JetStream message')
	}
	nc.publish(m.reply, '+ACK'.bytes())!
}

// nak signals that processing failed and the message should be redelivered.
// The server will redeliver according to the consumer's ack_wait and max_deliver settings.
pub fn (m Msg) nak(mut nc Client) ! {
	if m.reply == '' {
		return error('message has no reply subject — not a JetStream message')
	}
	nc.publish(m.reply, '-NAK'.bytes())!
}

// nak_with_delay signals failure and requests redelivery after a specific delay.
// delay is in nanoseconds. Use time.second, time.minute, etc. for convenience.
pub fn (m Msg) nak_with_delay(mut nc Client, delay i64) ! {
	if m.reply == '' {
		return error('message has no reply subject — not a JetStream message')
	}
	nc.publish(m.reply, '-NAK {"delay":${delay}}'.bytes())!
}

// in_progress resets the ack_wait timer for this message, signaling that processing
// is still ongoing. Call periodically for long-running jobs to prevent redelivery.
pub fn (m Msg) in_progress(mut nc Client) ! {
	if m.reply == '' {
		return error('message has no reply subject — not a JetStream message')
	}
	nc.publish(m.reply, '+WPI'.bytes())!
}

// term instructs the server to never redeliver this message, even if unacknowledged.
// Use when a message is permanently invalid (e.g. unparseable) and retrying is pointless.
pub fn (m Msg) term(mut nc Client) ! {
	if m.reply == '' {
		return error('message has no reply subject — not a JetStream message')
	}
	nc.publish(m.reply, '+TERM'.bytes())!
}
