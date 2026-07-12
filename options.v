module nats

import time
import net.ssl

pub type ConnCallback = fn (mut Client)
pub type ErrorCallback = fn (mut Client, string)

pub struct Options {
pub mut:
	url             string = default_url
	servers         []string
	name            string
	verbose         bool
	pedantic        bool
	no_echo         bool
	headers         bool          = true
	no_responders   bool          = true
	connect_timeout time.Duration = 2 * time.second
	// Authentication
	auth_token      string
	user            string
	password        string
	// TLS
	tls_config      ?ssl.SSLConnectConfig
	// Reconnect
	allow_reconnect   bool          = true
	max_reconnects    int           = 60
	reconnect_time_wait time.Duration = 2 * time.second
	// Callbacks
	on_disconnect   ?ConnCallback
	on_reconnect    ?ConnCallback
	on_close        ?ConnCallback
	on_error        ?ErrorCallback
}

