module nats

pub const err_connection_closed = 'nats: connection closed'
pub const err_no_responders = 'nats: no responders available for request'
pub const err_request_timeout = 'nats: request timed out'

fn err_max_payload(payload_size int, max_payload int) string {
	return 'nats: payload size ${payload_size} exceeds server max_payload ${max_payload}'
}

fn is_timeout_error(err IError) bool {
	msg := err.msg()
	return msg.contains('timed out') || msg.contains('timeout')
}

fn address_from_url(raw string) !string {
	mut url := raw
	if url == '' {
		url = default_url
	}
	if url.starts_with('nats://') {
		url = url[7..]
	}
	if url.starts_with('tls://') {
		return error('TLS is not implemented yet; use nats://')
	}
	if url.contains('@') {
		url = url.all_after('@')
	}
	url = url.trim_right('/')
	if url == '' {
		return error('empty NATS URL')
	}
	if !url.contains(':') {
		return '${url}:4222'
	}
	return url
}

fn validate_subject(subject string) ! {
	if subject == '' {
		return error('subject must not be empty')
	}
	if subject.contains(' ') || subject.contains('\t') || subject.contains('\r')
		|| subject.contains('\n') {
		return error('subject must not contain whitespace')
	}
	return
}
