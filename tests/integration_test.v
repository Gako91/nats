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

