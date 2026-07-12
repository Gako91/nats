module nats

import net

// Client represents a connection to a NATS server.
// Use `connect()` or `connect_with_options()` to create a new Client.
// After use, call `close()` to clean up resources.
@[heap]
pub struct Client {
mut:
	conn         net.Connection = &net.TcpConn(unsafe { nil })
	next_sid     int            = 1
	subs         map[string]Subscription
	pending_msgs []Msg
	connected    bool
	// Buffer fields for socket reading
	rx_buf    []u8
	rx_offset int
	rx_len    int
pub mut:
	info ServerInfo
	opts Options
}
