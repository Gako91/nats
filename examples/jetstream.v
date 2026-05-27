module main

import nats

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	mut js := nc.jetstream()

	js.add_stream(nats.StreamConfig{
		name:     'EVENTS'
		subjects: ['events.>']
	})!

	ack := js.publish_string('events.created', '{"id":1}')!
	println('stored in ${ack.stream} at sequence ${ack.seq}')
}
