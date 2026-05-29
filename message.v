module nats

pub struct Msg {
pub:
	subject     string
	sid         string
	reply       string
	data        []u8
	headers     map[string]string
	status      int
	description string
}

pub fn (m Msg) text() string {
	return m.data.bytestr()
}

pub fn (m Msg) is_no_responders() bool {
	return m.status == 503
}
