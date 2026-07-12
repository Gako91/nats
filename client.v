module nats

import net

@[heap]
pub struct Client {
mut:
	conn         net.Connection = &net.TcpConn(unsafe { nil })
	next_sid     int          = 1
	subs         map[string]Subscription
	pending_msgs []Msg
	connected    bool
	// Buffer fields for socket reading
	rx_buf       []u8
	rx_offset    int
	rx_len       int
pub mut:
	info ServerInfo
	opts Options
}
