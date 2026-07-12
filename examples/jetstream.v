module main

import nats

fn main() {
	// Connect to NATS server
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	// Create a JetStream context for persistent streaming
	// JetStream is NATS's durability layer: messages are stored and can be replayed
	mut js := nc.jetstream()

	// Create a stream named 'EVENTS' that listens to all 'events.>' subjects
	// StreamConfig options:
	//   name: unique stream identifier
	//   subjects: which subjects this stream receives messages from (supports patterns like '>')
	//   retention: 'limits' (default) - automatically delete old messages
	//   storage: 'file' (default) or 'memory' for ephemeral storage
	//   max_msgs, max_bytes, max_age: retention limits (e.g., keep last 10000 messages)
	js.add_stream(nats.StreamConfig{
		name:     'EVENTS'
		subjects: ['events.>']
	})!

	// Publish a message to the stream with acknowledgment
	// Unlike regular publish(), this waits for the server to confirm message is stored
	ack := js.publish_string('events.created', '{"id":1}')!
	// PubAck contains:
	//   stream: name of the stream that stored the message
	//   seq: sequence number of this message (for replay/ordering)
	println('stored in ${ack.stream} at sequence ${ack.seq}')
}
