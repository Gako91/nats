module nats

pub struct Msg {
pub:
	subject string
	sid     string
	reply   string
	data    []u8
}

pub fn (m Msg) text() string {
	return m.data.bytestr()
}
