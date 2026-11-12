module main

import nats
import time

struct AckStats {
mut:
	completed int
	failed    int
}

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
		subjects: ['async_events.>']
	}) or {
		// Stream might already exist, that's OK
		println('Stream creation info: ${err}')
	}

	// Track async publishes. Closures capture copies of variables, so the
	// counters live in a heap struct shared with the callback.
	mut stats := &AckStats{}

	// Define a callback for when ACKs arrive
	ack_callback := fn [mut stats] (mut c nats.Client, subject string, result nats.PublishResult) {
		if result.error_msg != '' {
			stats.failed++
			println('[ERROR] Publish to ${subject} failed: ${result.error_msg}')
		} else {
			stats.completed++
			println('[ACK] Message published to ${subject}')
			println('  - Stream: ${result.ack.stream}')
			println('  - Sequence: ${result.ack.seq}')
			println('  - Duplicate: ${result.ack.duplicate}')
		}
	}

	// Publish several messages asynchronously
	println('Publishing 5 messages asynchronously...')
	for i in 1 .. 6 {
		subject := 'async_events.order.created'
		message := 'Order #${i} created'

		// Publish async - returns immediately, callback will be called when ACK arrives
		js.publish_string_async(subject, message, ack_callback) or {
			println('Failed to queue async publish: ${err}')
		}
	}

	// The client has no background reader: ACK callbacks run when the
	// connection is read (next_msg(), flush() or publish_async_complete()).
	println('Waiting for ACKs...')
	js.publish_async_complete(5 * time.second) or { println('Not all ACKs received: ${err}') }

	// Print summary
	println('')
	println('=== Summary ===')
	println('Completed: ${stats.completed}')
	println('Failed: ${stats.failed}')

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
