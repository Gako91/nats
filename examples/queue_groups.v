module main

import nats
import time
import os

// This example demonstrates queue groups for load balancing.
// Queue groups distribute messages across multiple subscribers:
// - Each message is delivered to ONE random subscriber in the group
// - Great for worker pools, background jobs, or load distribution
//
// Run with one mode at a time in separate terminals:
//   Terminal 1: v run examples/queue_groups.v worker1
//   Terminal 2: v run examples/queue_groups.v worker2
//   Terminal 3: v run examples/queue_groups.v publisher

fn main() {
	args := os.args
	mode := if args.len > 1 { args[1] } else { 'worker' }

	if mode == 'publisher' {
		publisher()
	} else {
		worker(mode)
	}
}

fn publisher() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	println('[publisher] connected')

	// Publish 10 messages to the 'jobs.process' subject
	// These will be distributed among workers in the 'job_workers' queue group
	for i in 1 .. 11 {
		nc.publish_string('jobs.process', 'job_${i}')!
		println('[publisher] sent job_${i}')
		time.sleep(200 * time.millisecond)
	}
}

fn worker(name string) {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	// queue_subscribe() creates a queue group subscription
	// All subscribers in 'job_workers' group receive different messages (load balanced)
	// Compare this with subscribe() where all subscribers get all messages
	_ := nc.queue_subscribe('jobs.process', 'job_workers')!
	nc.flush()!
	println('[${name}] ready, listening on jobs.process in queue "job_workers"')

	// Wait for and process messages
	// Each message will be received by only one worker in the group
	for {
		msg := nc.next_msg()!
		println('[${name}] processing: ${msg.text()}')

		// Simulate work
		time.sleep(500 * time.millisecond)
		println('[${name}] done: ${msg.text()}')
	}
}
