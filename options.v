module nats

import time

pub struct Options {
pub mut:
	url             string = default_url
	name            string
	verbose         bool
	pedantic        bool
	no_echo         bool
	headers         bool          = true
	no_responders   bool          = true
	connect_timeout time.Duration = 2 * time.second
}
