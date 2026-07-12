module main

import nats
import time

fn main() {
	// Connect to NATS server, handling connection errors gracefully
	mut nc := nats.connect('nats://127.0.0.1:4222') or {
		eprintln('connect failed: ${err}')
		return
	}
	defer { nc.close() }

	// request_string() implements the request-reply pattern:
	// 1. Automatically creates a unique reply inbox
	// 2. Publishes the request to 'service.echo'
	// 3. Waits for a response (max 2 seconds)
	// This example sends 'ping' and expects a response
	reply := nc.request_string('service.echo', 'ping', 2 * time.second) or {
		eprintln('request failed: ${err}')
		eprintln('start examples/echo_service.v in another terminal, then retry')
		return
	}
	// Print the response text
	println(reply.text())
}
