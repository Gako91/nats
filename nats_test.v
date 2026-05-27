module nats

fn test_address_from_url_defaults() {
	assert address_from_url('')! == '127.0.0.1:4222'
	assert address_from_url('nats://demo.local')! == 'demo.local:4222'
	assert address_from_url('nats://demo.local:4223')! == 'demo.local:4223'
}

fn test_validate_subject() {
	validate_subject('foo.bar')!
	validate_subject('foo bar') or { return }
	assert false
}

fn test_new_inbox() {
	inbox := new_inbox()
	assert inbox.starts_with('_INBOX.')
}
