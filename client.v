module nats

import net
import time

// PendingPublish tracks an in-flight async JetStream publish request.
// Stores callback, inbox, and metadata needed to route ACK responses.
struct PendingPublish {
	inbox      string
	subject    string
	callback   ?PublishAckCallback
	created_at time.Time // Time when the publish was sent
	timeout_ms i64       // milliseconds until timeout
}

// Client represents a connection to a NATS server.
// Use `connect()` or `connect_with_options()` to create a new Client.
// After use, call `close()` to clean up resources.
@[heap]
pub struct Client {
mut:
	conn              net.Connection = &net.TcpConn(unsafe { nil })
	next_sid          int            = 1
	subs              map[string]Subscription
	pending_msgs      []Msg
	pending_publishes map[string]PendingPublish // Maps reply subject to pending async publish
	connected         bool
	connecting        bool // true while connect_to() runs: no nested reconnect
	conn_open         bool // conn holds a socket that has not been closed yet
	// Shared response inbox: one `<resp_prefix>*` subscription serves every
	// request() and publish_async() reply, each on its own `<resp_prefix><n>`.
	resp_prefix  string
	resp_sid     string
	resp_seq     u64
	resp_waiting string // reply subject request() is currently waiting for
	// Buffer fields for socket reading
	rx_buf    []u8
	rx_offset int
	rx_len    int
pub mut:
	info ServerInfo
	opts Options
}
