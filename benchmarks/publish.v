module main

import nats
import time

const total_messages = 10_000

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	start := time.now()
	for i := 0; i < total_messages; i++ {
		nc.publish_string('bench.publish', 'hello')!
	}
	nc.flush()!
	elapsed := time.since(start)
	println('published ${total_messages} messages in ${elapsed}')
}
