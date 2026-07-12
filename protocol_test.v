module nats

fn test_parse_protocol_line() {
	assert parse_protocol_line('PING').op == .ping
	assert parse_protocol_line('PONG').op == .pong
	assert parse_protocol_line('+OK').op == .ok
	assert parse_protocol_line('-ERR something').op == .err
	info := parse_protocol_line('INFO {"server_id":"x"}')
	assert info.op == .info
	assert info.payload == '{"server_id":"x"}'
	assert parse_protocol_line('MSG foo 1 3').op == .msg
	assert parse_protocol_line('HMSG foo 1 10 10').op == .hmsg
}

fn test_parse_header_block() {
	parsed := parse_header_block('NATS/1.0 503 No Responders\r\nNats-Request-Info: test\r\n\r\n')
	assert parsed.status == 503
	assert parsed.description == 'No Responders'
	assert parsed.headers['Nats-Request-Info'] == 'test'
}

fn test_max_payload_error_before_socket_write() {
	mut nc := Client{}
	nc.info.max_payload = 4
	nc.publish_string('foo', 'hello') or {
		assert err.msg().contains('max_payload')
		return
	}
	assert false
}

fn test_parse_url_credentials() {
	u1, p1, t1 := parse_url_credentials('nats://user:pass@localhost:4222')
	assert u1 == 'user'
	assert p1 == 'pass'
	assert t1 == ''

	u2, p2, t2 := parse_url_credentials('nats://mytoken@localhost:4222')
	assert u2 == ''
	assert p2 == ''
	assert t2 == 'mytoken'

	u3, p3, t3 := parse_url_credentials('tls://user:pass@localhost:4222')
	assert u3 == 'user'
	assert p3 == 'pass'
	assert t3 == ''

	u4, p4, t4 := parse_url_credentials('nats://localhost:4222')
	assert u4 == ''
	assert p4 == ''
	assert t4 == ''
}

fn test_hostname_and_address_from_url() {
	assert address_from_url('nats://user:pass@localhost:4222')! == 'localhost:4222'
	assert address_from_url('tls://localhost:4222')! == 'localhost:4222'
	assert address_from_url('nats://localhost')! == 'localhost:4222'
	assert hostname_from_url('nats://user:pass@myhost:4222')! == 'myhost'
}

