module nats

const js_api_prefix = '$JS.API'

// JetStream provides access to NATS JetStream features for durable streaming.
// JetStream is built on top of NATS and offers persistent streams, consumer groups,
// and acknowledgment-based message delivery.
// Create with `client.jetstream()` after connecting.
pub struct JetStream {
mut:
	nc &Client = unsafe { nil }
pub:
	prefix string = js_api_prefix
}

// ApiError represents an error response from JetStream API.
// Returned when stream or consumer operations fail.
pub struct ApiError {
pub:
	// code: HTTP-like error code
	code int
	// err_code: JetStream-specific error code
	err_code int @[json: 'err_code']
	// description: human-readable error message
	description string
}

// jetstream returns a JetStream context for this client.
// Use this to create and manage streams, and publish/subscribe with persistence.
pub fn (mut nc Client) jetstream() JetStream {
	return JetStream{
		nc:     unsafe { &nc }
		prefix: js_api_prefix
	}
}

// jetstream_with_prefix returns a JetStream context with a custom API prefix.
// This is useful when connecting to a non-default JetStream domain.
pub fn (mut nc Client) jetstream_with_prefix(prefix string) JetStream {
	return JetStream{
		nc:     unsafe { &nc }
		prefix: prefix
	}
}
