module main

import nats
import time

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222') or {
		eprintln('connect failed: ${err}')
		return
	}
	defer { nc.close() }

	reply := nc.request_string('service.echo', 'ping', 2 * time.second) or {
		eprintln('request failed: ${err}')
		eprintln('start examples/echo_service.v in another terminal, then retry')
		return
	}
	println(reply.text())
}
