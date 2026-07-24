module main

import nats

fn main() {
	// Connect to NATS server
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	mut js := nc.jetstream()

	// Create a test stream
	stream_name := 'orders'
	js.delete_stream(stream_name) or {}
	js.add_stream(nats.StreamConfig{
		name:     stream_name
		subjects: ['orders.>']
	}) or { println('Stream creation info: ${err}') }

	println('=== Pull Consumer Example ===')
	example_pull_consumer(mut nc, mut js, stream_name)

	println('')
	println('=== Ordered Consumer Example ===')
	example_ordered_consumer(mut nc, mut js, stream_name)
}

fn example_pull_consumer(mut nc nats.Client, mut js nats.JetStream, stream_name string) {
	println('Publishing messages to stream...')
	for i := 1; i <= 5; i++ {
		js.publish_string('orders.created', 'Order #${i} created') or {
			println('Error publishing: ${err}')
		}
	}

	// Create a pull consumer (ephemeral)
	println('Creating pull consumer...')
	consumer_info := js.pull_consumer_create(stream_name, nats.PullConsumerOptions{
		filter_subject: 'orders.created'
	}) or {
		println('Error creating pull consumer: ${err}')
		return
	}
	println('Created pull consumer: ${consumer_info.name}')

	// Fetch messages in batches
	println('Fetching messages (batch of 5)...')
	messages, errors := js.fetch(stream_name, consumer_info.name, nats.PullFetchOptions{
		batch:           5
		idle_timeout_ms: 2000
	}) or {
		println('Error fetching: ${err}')
		return
	}

	if errors.len > 0 {
		println('Fetch errors: ${errors.len}')
		for err in errors {
			println('  - ${err}')
		}
	}

	// Process messages
	println('Received ${messages.len} messages:')
	for msg in messages {
		println('  [${msg.sid}] ${msg.subject}: ${msg.text()}')
		// In real code, you would acknowledge here
		// msg.ack(mut js.nc) or {}
	}
}

fn example_ordered_consumer(mut nc nats.Client, mut js nats.JetStream, stream_name string) {
	println('Creating ordered consumer...')

	// Create an ordered consumer
	// Ordered consumers guarantee strict ordering with no duplicates or gaps
	consumer_info := js.ordered_consumer_create(stream_name, nats.OrderedConsumerOptions{
		deliver_policy:  .all
		flow_control_ms: 5000
	}) or {
		println('Error creating ordered consumer: ${err}')
		return
	}
	println('Created ordered consumer: ${consumer_info.name}')
	println('Delivery subject: ${consumer_info.config.deliver_subject}')

	// Subscribe to receive ordered messages
	println('Subscribing to ordered consumer...')
	sub := js.subscribe_to_ordered_consumer(consumer_info.config.deliver_subject) or {
		println('Error subscribing: ${err}')
		return
	}
	defer { nc.unsubscribe(sub) or {} }

	// Receive ordered messages
	println('Receiving ordered messages...')
	for i := 0; i < 5; i++ {
		msg := nc.next_msg() or {
			if err.msg().contains('timeout') {
				println('Timeout waiting for message')
				break
			}
			println('Error receiving message: ${err}')
			break
		}

		println('  [${i}] ${msg.subject}: ${msg.text()}')

		// Acknowledge to advance the consumer
		msg.ack(mut nc) or { println('Error acknowledging: ${err}') }
	}

	println('Done with ordered consumer!')
}
