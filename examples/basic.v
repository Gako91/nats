module main

import nats

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	sub := nc.subscribe('demo.hello')!
	nc.flush()!
	nc.publish_string('demo.hello', 'hello from V')!

	msg := nc.next_msg()!
	assert msg.sid == sub.sid
	println('${msg.subject}: ${msg.text()}')
}
