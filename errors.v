module nats

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
