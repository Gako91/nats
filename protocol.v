module nats

const default_url = 'nats://127.0.0.1:4222'
const crlf = '\r\n'

struct ConnectPayload {
	verbose       bool
	pedantic      bool
	name          string
	lang          string
	version       string
	protocol      int
	echo          bool
	no_responders bool
}
