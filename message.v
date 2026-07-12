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
