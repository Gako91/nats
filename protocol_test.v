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
