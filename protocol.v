module nats

const default_url = 'nats://127.0.0.1:4222'
const crlf = '\r\n'
const client_lang = 'v'
const client_version = '0.1.0'

struct ConnectPayload {
	verbose       bool
	pedantic      bool
	name          string
	lang          string
	version       string
	protocol      int
	echo          bool
	headers       bool
	no_responders bool
	auth_token    string @[json: auth_token; omitempty]
	user          string @[json: user; omitempty]
	pass          string @[json: pass; omitempty]
}

enum ProtocolOp {
	unknown
	info
	msg
	hmsg
	ping
	pong
	ok
	err
}

struct ProtocolLine {
	op      ProtocolOp
	raw     string
	payload string
}

fn parse_protocol_line(line string) ProtocolLine {
	op_text := line.all_before(' ')
	payload := if line.contains(' ') { line.all_after(' ') } else { '' }
	op := match op_text {
		'INFO' { ProtocolOp.info }
		'MSG' { ProtocolOp.msg }
		'HMSG' { ProtocolOp.hmsg }
		'PING' { ProtocolOp.ping }
		'PONG' { ProtocolOp.pong }
		'+OK' { ProtocolOp.ok }
		'-ERR' { ProtocolOp.err }
		else { ProtocolOp.unknown }
	}

	return ProtocolLine{
		op:      op
		raw:     line
		payload: payload
	}
}
