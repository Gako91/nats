module main

import nats
import os
import rand
import time

const integration_env = 'NATS_INTEGRATION'
const integration_url_env = 'NATS_URL'

fn integration_enabled() bool {
	return os.getenv(integration_env) == '1'
}

fn integration_url() string {
	url := os.getenv(integration_url_env)
	if url == '' {
		return 'nats://127.0.0.1:4222'
	}
	return url
}

fn test_integration_connect_flush() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }
	nc.flush()!
	assert nc.info.version != ''
}

fn test_integration_no_responders() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }

	subject := 'v.integration.no_responder.${rand.ulid()}'
	nc.request_string(subject, 'ping', 2 * time.second) or {
		assert err.msg() == nats.err_no_responders
		return
	}
	assert false
}

fn test_integration_request_timeout_without_no_responders() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect_with_options(nats.Options{
		url:             integration_url()
		headers:         false
		no_responders:   false
		connect_timeout: 250 * time.millisecond
	})!
	defer { nc.close() }

	subject := 'v.integration.timeout.${rand.ulid()}'
	nc.request_string(subject, 'ping', 50 * time.millisecond) or {
		assert err.msg() == nats.err_request_timeout
		return
	}
	assert false
}

fn test_integration_publish_subscribe() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }

	subject := 'v.integration.${rand.ulid()}'
	sub := nc.subscribe(subject)!
	nc.flush()!

	nc.publish_string(subject, 'hello integration')!

	msg := nc.next_msg()!
	assert msg.sid == sub.sid
	assert msg.subject == subject
	assert msg.text() == 'hello integration'
}

struct CallbackCounter {
mut:
	disconnects int
	reconnects  int
}

fn test_integration_reconnect() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut counter := &CallbackCounter{}

	mut nc := nats.connect_with_options(nats.Options{
		url:                 integration_url()
		allow_reconnect:     true
		reconnect_time_wait: 10 * time.millisecond
		max_reconnects:      5
		on_disconnect:       fn [mut counter] (mut client nats.Client) {
			counter.disconnects++
		}
		on_reconnect:        fn [mut counter] (mut client nats.Client) {
			counter.reconnects++
		}
	})!
	defer { nc.close() }

	subject := 'v.integration.reconnect.${rand.ulid()}'
	sub := nc.subscribe(subject)!
	nc.flush()!

	// Verify that the connection is active and we can publish/receive
	nc.publish_string(subject, 'first message')!
	msg := nc.next_msg()!
	assert msg.text() == 'first message'

	// Simulate unexpected connection loss by closing the socket
	nc.disconnect_for_testing()!

	// Publishing again should trigger reconnect automatically, restore subscriptions, and succeed
	nc.publish_string(subject, 'second message')!

	// Verify the reconnect callbacks were invoked
	assert counter.disconnects == 1
	assert counter.reconnects == 1

	// Verify we can receive the message after reconnect
	msg2 := nc.next_msg()!
	assert msg2.text() == 'second message'
	assert msg2.sid == sub.sid
}

fn test_integration_jetstream_async_publish() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }

	mut js := nc.jetstream()

	// Create a test stream
	stream_name := 'test_async_${rand.ulid()}'
	js.add_stream(nats.StreamConfig{
		name:     stream_name
		subjects: ['test.async.>${stream_name}']
	}) or { return }

	// Set up to capture callback results
	mut callback_invoked := false
	mut callback_ack := nats.PubAck{}
	mut callback_error := ''

	// Define the async publish callback
	async_callback := fn [mut callback_invoked, mut callback_ack, mut callback_error] (mut c nats.Client, subject string, result nats.PublishResult) {
		callback_invoked = true
		if result.error_msg != '' {
			callback_error = result.error_msg
		} else {
			callback_ack = result.ack
		}
	}

	// Publish async
	subject := 'test.async.${stream_name}'
	js.publish_string_async(subject, 'test message', async_callback) or {
		panic('failed to publish async: ${err}')
	}

	// Give the callback a moment to be invoked (it runs on the reader thread)
	time.sleep(100 * time.millisecond)

	// Check that the callback was invoked
	assert callback_invoked == true, 'callback was not invoked'
	assert callback_error == '', 'callback had an error: ${callback_error}'
	assert callback_ack.stream == stream_name, 'ack stream name mismatch'
	assert callback_ack.seq > 0, 'ack seq should be > 0'
}

fn test_integration_jetstream_async_publish_multiple() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }

	mut js := nc.jetstream()

	// Create a test stream
	stream_name := 'test_async_multi_${rand.ulid()}'
	js.add_stream(nats.StreamConfig{
		name:     stream_name
		subjects: ['test.multi.>${stream_name}']
	}) or { return }

	// Track all acks
	mut acks := []nats.PubAck{}
	mut errors := []string{}

	// Create callback
	async_callback := fn [mut acks, mut errors] (mut c nats.Client, subject string, result nats.PublishResult) {
		if result.error_msg != '' {
			errors << result.error_msg
		} else {
			acks << result.ack
		}
	}

	// Publish multiple messages async
	subject := 'test.multi.${stream_name}'
	for i := 0; i < 5; i++ {
		js.publish_string_async(subject, 'message ${i}', async_callback) or {
			panic('failed to publish: ${err}')
		}
	}

	// Wait for all acks to arrive
	time.sleep(500 * time.millisecond)

	// Verify all acks received
	assert acks.len == 5, 'expected 5 acks, got ${acks.len}'
	assert errors.len == 0, 'expected no errors, got ${errors.len}: ${errors}'

	// Verify sequences are in order
	for i in 1 .. acks.len {
		assert acks[i].seq > acks[i - 1].seq, 'sequences not in order'
	}
}

fn test_integration_jetstream_pull_consumer() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }

	mut js := nc.jetstream()

	// Create a test stream
	stream_name := 'test_pull_${rand.ulid()}'
	js.add_stream(nats.StreamConfig{
		name:     stream_name
		subjects: ['test.pull.>${stream_name}']
	}) or { return }

	// Publish some test messages
	subject := 'test.pull.${stream_name}'
	for i := 0; i < 5; i++ {
		js.publish_string(subject, 'message ${i}') or {}
	}

	// Create a pull consumer
	consumer_info := js.pull_consumer_create(stream_name, nats.PullConsumerOptions{
		filter_subject: subject
	}) or { panic('failed to create pull consumer: ${err}') }

	// Fetch messages using pull
	messages, errors := js.fetch(stream_name, consumer_info.name, nats.PullFetchOptions{
		batch:           5
		idle_timeout_ms: 2000
	}) or { panic('fetch failed: ${err}') }

	// Verify we got the messages
	assert messages.len == 5, 'expected 5 messages, got ${messages.len}'
	assert errors.len == 0, 'expected no errors, got ${errors.len}'

	// Verify message content
	for i := 0; i < 5; i++ {
		msg := messages[i]
		expected := 'message ${i}'
		actual := msg.text()
		assert actual == expected, 'expected "${expected}", got "${actual}"'
	}
}

fn test_integration_jetstream_ordered_consumer() {
	if !integration_enabled() {
		eprintln('skipping integration test; set ${integration_env}=1 to enable')
		return
	}

	mut nc := nats.connect(integration_url())!
	defer { nc.close() }

	mut js := nc.jetstream()

	// Create a test stream
	stream_name := 'test_ordered_${rand.ulid()}'
	js.add_stream(nats.StreamConfig{
		name:     stream_name
		subjects: ['test.ordered.>${stream_name}']
	}) or { return }

	// Publish messages in a specific order
	subject := 'test.ordered.${stream_name}'
	for i := 0; i < 3; i++ {
		js.publish_string(subject, 'ordered_${i}') or {}
	}

	// Create an ordered consumer
	consumer_info := js.ordered_consumer_create(stream_name, nats.OrderedConsumerOptions{
		deliver_policy: .all
	}) or { panic('failed to create ordered consumer: ${err}') }

	// Subscribe to the ordered consumer's delivery subject
	sub := js.subscribe_to_ordered_consumer(consumer_info.config.deliver_subject) or {
		panic('failed to subscribe: ${err}')
	}
	defer { nc.unsubscribe(sub) or {} }

	// Receive messages in order
	mut received_messages := []string{}
	for i := 0; i < 3; i++ {
		msg := nc.next_msg() or { break }
		received_messages << msg.text()
		// Acknowledge the message
		msg.ack(mut nc) or {}
	}

	// Verify we got all messages in order
	assert received_messages.len == 3, 'expected 3 messages, got ${received_messages.len}'
	for i in 0 .. received_messages.len {
		expected := 'ordered_${i}'
		actual := received_messages[i]
		assert actual == expected, 'expected "${expected}", got "${actual}" at index ${i}'
	}
}
