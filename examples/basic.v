module main

import nats

fn main() {
	// Connect to NATS server (default: localhost:4222)
	// The '!' operator propagates errors up (will panic if connection fails)
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	// 'defer' ensures close() is called even if an error occurs
	defer { nc.close() }

	// subscribe() starts listening for messages on the 'demo.hello' subject
	// Multiple subscribers can listen on the same subject; they all receive the same messages
	sub := nc.subscribe('demo.hello')!
	// flush() waits for the subscription to be registered with the server
	nc.flush()!

	// publish_string() sends a message to 'demo.hello'
	// This message will be received by the subscriber above
	nc.publish_string('demo.hello', 'hello from V')!

	// next_msg() blocks and waits for the next message on any active subscription
	msg := nc.next_msg()!
	// Verify the message came from our subscription
	assert msg.sid == sub.sid
	// Print the message: print subject and message text
	println('${msg.subject}: ${msg.text()}')
}
