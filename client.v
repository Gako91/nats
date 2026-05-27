module nats

import net

@[heap]
pub struct Client {
mut:
	conn      &net.TcpConn = unsafe { nil }
	next_sid  int          = 1
	subs      map[string]Subscription
	connected bool
pub mut:
	info ServerInfo
	opts Options
}
