module nats

const js_api_prefix = '$JS.API'

pub struct JetStream {
mut:
	nc &Client = unsafe { nil }
pub:
	prefix string = js_api_prefix
}

pub struct ApiError {
pub:
	code        int
	err_code    int @[json: err_code]
	description string
}

pub fn (mut nc Client) jetstream() JetStream {
	return JetStream{
		nc:     unsafe { &nc }
		prefix: js_api_prefix
	}
}

pub fn (mut nc Client) jetstream_with_prefix(prefix string) JetStream {
	return JetStream{
		nc:     unsafe { &nc }
		prefix: prefix
	}
}
