module main

import nats
import time

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	reply := nc.request_string('service.echo', 'ping', 2 * time.second)!
	println(reply.text())
}
