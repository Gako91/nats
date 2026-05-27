module main

import nats

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	nc.subscribe('service.echo')!
	nc.flush()!
	println('listening on service.echo')

	for {
		msg := nc.next_msg()!
		println('received: ${msg.text()}')
		if msg.reply != '' {
			nc.publish_string(msg.reply, msg.text())!
			nc.flush()!
		}
	}
}
