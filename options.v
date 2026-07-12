module nats

import time
import net.ssl

// ConnCallback is called when connection state changes (on_reconnect, on_disconnect, on_close).
// Called with mutable access to the Client.
pub type ConnCallback = fn (mut Client)

// ErrorCallback is called when an error occurs.
// Called with mutable access to the Client and an error message.
pub type ErrorCallback = fn (mut Client, string)

// Options configures a NATS Client connection.
// Use default values with `Options{}` for quick start, or customize fields as needed.
pub struct Options {
pub mut:
	// url: NATS server URL (default: 'nats://localhost:4222')
	url string = default_url
	// servers: list of NATS server URLs for failover
	servers []string
	// name: client name sent to server (for identification)
	name string
	// verbose: request verbose logging from server
	verbose bool
	// pedantic: request pedantic protocol checking from server
	pedantic bool
	// no_echo: if true, subscriptions won't receive messages this client publishes
	no_echo bool
	// headers: if true, enable HMSG protocol support for message headers
	headers bool = true
	// no_responders: if true, server will send 503 responses for un-answered requests
	no_responders bool = true
	// connect_timeout: how long to wait for server connection to establish (default: 2s)
	connect_timeout time.Duration = 2 * time.second
	// Authentication: use ONE of: auth_token, or (user + password)
	// auth_token: bearer token for authentication
	auth_token string
	// user: username for authentication
	user string
	// password: password for authentication
	password string
	// TLS configuration for secure connections
	tls_config ?ssl.SSLConnectConfig
	// allow_reconnect: if true, client will attempt to reconnect after disconnect
	allow_reconnect bool = true
	// max_reconnects: maximum number of reconnect attempts (default: 60)
	max_reconnects int = 60
	// reconnect_time_wait: time to wait between reconnect attempts (default: 2s)
	reconnect_time_wait time.Duration = 2 * time.second
	// Callbacks for connection state changes
	// on_disconnect: called when connection is lost
	on_disconnect ?ConnCallback
	// on_reconnect: called when connection is re-established
	on_reconnect ?ConnCallback
	// on_close: called when connection is closed by user
	on_close ?ConnCallback
	// on_error: called when an error occurs
	on_error ?ErrorCallback
}
