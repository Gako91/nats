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
