module main

import nats
import os
import rand

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
