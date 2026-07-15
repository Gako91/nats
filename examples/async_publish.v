module main

import nats
import time

fn main() {
	// Connect to NATS server
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	// Create a JetStream context
	mut js := nc.jetstream()

	// Create a test stream
	stream_name := 'async_example'
	js.add_stream(nats.StreamConfig{
		name:     stream_name
		subjects: ['events.>']
	}) or {
		// Stream might already exist, that's OK
		println('Stream creation info: ${err}')
	}

	// Track async publishes
	mut completed := 0
	mut failed := 0

	// Define a callback for when ACKs arrive
	ack_callback := fn [mut completed, mut failed] (mut c nats.Client, subject string, result nats.PublishResult) {
		if result.error_msg != '' {
			failed++
			println('[ERROR] Publish to ${subject} failed: ${result.error_msg}')
		} else {
			completed++
			println('[ACK] Message published to ${subject}')
			println('  - Stream: ${result.ack.stream}')
			println('  - Sequence: ${result.ack.seq}')
			println('  - Duplicate: ${result.ack.duplicate}')
		}
	}

	// Publish several messages asynchronously
	println('Publishing 5 messages asynchronously...')
	for i in 1 .. 6 {
		subject := 'events.order.created'
		message := 'Order #${i} created'

		// Publish async - returns immediately, callback will be called when ACK arrives
		js.publish_string_async(subject, message, ack_callback) or {
			println('Failed to queue async publish: ${err}')
		}
	}

	// Main thread can do other work while waiting for ACKs...
	println('Waiting for ACKs...')
	time.sleep(1 * time.second)

	// Print summary
	println('')
	println('=== Summary ===')
	println('Completed: ${completed}')
	println('Failed: ${failed}')

	// Compare with sync publish for contrast
	println('')
	println('Publishing 3 synchronous messages (for comparison)...')
	for i in 1 .. 4 {
		subject := 'events.order.processed'
		message := 'Order #${i} processed'

		ack := js.publish_string(subject, message) or {
			println('Sync publish failed: ${err}')
			continue
		}
		println('[SYNC ACK] ${subject} -> Seq: ${ack.seq}')
	}
}
