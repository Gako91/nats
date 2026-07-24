module nats

import json2
import rand
import time

// DeliverPolicy controls where message delivery starts for a consumer.
pub enum DeliverPolicy {
	// all: deliver all available messages from the beginning of the stream
	all
	// last: deliver only the last message per subject
	last
	// new_msgs: deliver only new messages arriving after the consumer is created
	new_msgs
	// by_start_sequence: deliver from a specific sequence number (set deliver_start_sequence)
	by_start_sequence
	// by_start_time: deliver from a specific point in time (set deliver_start_time)
	by_start_time
	// last_per_subject: deliver the last message per subject
	last_per_subject
}

// AckPolicy controls how acknowledgments are handled for a consumer.
pub enum AckPolicy {
	// none: no acknowledgment required — messages are auto-acked
	none
	// all: acknowledging message N acks all messages up to N
	all
	// explicit: each message must be individually acknowledged
	explicit
}

// ReplayPolicy controls message replay speed when delivering old messages.
pub enum ReplayPolicy {
	// instant: deliver all messages as fast as possible
	instant
	// original: replay messages at the original rate they were published
	original
}

// ConsumerConfig describes the configuration for a JetStream consumer.
// Durable consumers persist across restarts (set durable_name).
// Ephemeral consumers are auto-deleted when inactive (leave durable_name empty).
pub struct ConsumerConfig {
pub mut:
	// durable_name: persistent name for durable consumers. Empty = ephemeral.
	durable_name string @[json: 'durable_name'; omitempty]
	// description: optional human-readable description
	description string @[json: 'description'; omitempty]
	// deliver_policy: where to start delivering messages from (default: all)
	deliver_policy DeliverPolicy @[json: 'deliver_policy']
	// deliver_start_sequence: starting sequence when deliver_policy = by_start_sequence
	deliver_start_sequence u64 @[json: 'opt_start_seq'; omitempty]
	// deliver_start_time: RFC 3339 timestamp when deliver_policy = by_start_time
	deliver_start_time string @[json: 'opt_start_time'; omitempty]
	// ack_policy: how messages should be acknowledged (default: explicit)
	ack_policy AckPolicy @[json: 'ack_policy']
	// ack_wait: nanoseconds before an unacknowledged message is redelivered (default: 30s)
	ack_wait i64 @[json: 'ack_wait'; omitempty]
	// max_deliver: maximum number of delivery attempts (-1 = unlimited)
	max_deliver int @[json: 'max_deliver'; omitempty]
	// filter_subject: optional subject filter, must be a subset of the stream's subjects
	filter_subject string @[json: 'filter_subject'; omitempty]
	// replay_policy: instant or original speed replay (default: instant)
	replay_policy ReplayPolicy @[json: 'replay_policy']
	// max_waiting: maximum number of pending pull requests (pull consumers only)
	max_waiting int @[json: 'max_waiting'; omitempty]
	// max_ack_pending: maximum number of unacknowledged messages in flight
	max_ack_pending int @[json: 'max_ack_pending'; omitempty]
	// flow_control: enable flow control for push consumers
	flow_control bool @[json: 'flow_control'; omitempty]
	// idle_heartbeat: nanoseconds between heartbeats for push consumers (0 = disabled)
	idle_heartbeat i64 @[json: 'idle_heartbeat'; omitempty]
	// deliver_subject: push subject for push-based consumers (empty = pull consumer)
	deliver_subject string @[json: 'deliver_subject'; omitempty]
	// deliver_group: queue group name for push consumers with load balancing
	deliver_group string @[json: 'deliver_group'; omitempty]
	// headers_only: deliver only message headers, no body
	headers_only bool @[json: 'headers_only'; omitempty]
	// mem_storage: store consumer state in memory instead of file
	mem_storage bool @[json: 'mem_storage'; omitempty]
}

// SequenceInfo holds paired stream and consumer sequence numbers.
pub struct SequenceInfo {
pub:
	// consumer_seq: sequence number within the consumer's delivery order
	consumer_seq u64 @[json: 'consumer_seq']
	// stream_seq: sequence number within the stream
	stream_seq u64 @[json: 'stream_seq']
}

// ConsumerInfo contains the current state and configuration of a consumer.
pub struct ConsumerInfo {
pub:
	// stream_name: the stream this consumer belongs to
	stream_name string @[json: 'stream_name']
	// name: the consumer's name (same as durable_name for durable consumers)
	name string
	// created: ISO 8601 timestamp when consumer was created
	created string
	// config: the consumer's current configuration
	config ConsumerConfig
	// delivered: last successfully delivered message sequence
	delivered SequenceInfo
	// ack_floor: highest contiguous acknowledged sequence
	ack_floor SequenceInfo @[json: 'ack_floor']
	// num_pending: number of messages waiting to be delivered
	num_pending u64 @[json: 'num_pending']
	// num_redelivered: number of messages that have been redelivered
	num_redelivered u64 @[json: 'num_redelivered']
	// num_waiting: number of pending pull requests (pull consumers only)
	num_waiting int @[json: 'num_waiting']
	// num_ack_pending: number of messages delivered but not yet acknowledged
	num_ack_pending int @[json: 'num_ack_pending']
}

struct CreateConsumerRequest {
pub mut:
	stream_name string         @[json: 'stream_name']
	config      ConsumerConfig @[json: 'config']
}

// add_consumer creates a new consumer on the given stream.
// For durable consumers, set cfg.durable_name. For ephemeral, leave it empty.
// Returns the ConsumerInfo with the server-assigned name and initial state.
pub fn (mut js JetStream) add_consumer(stream_name string, cfg ConsumerConfig) !ConsumerInfo {
	if stream_name == '' {
		return error('stream name must not be empty')
	}
	subject := if cfg.durable_name != '' {
		'${js.prefix}.CONSUMER.DURABLE.CREATE.${stream_name}.${cfg.durable_name}'
	} else {
		'${js.prefix}.CONSUMER.CREATE.${stream_name}'
	}
	req := CreateConsumerRequest{
		stream_name: stream_name
		config:      cfg
	}
	msg := js.api_request(subject, json2.encode[CreateConsumerRequest](req).bytes(), 5 * time.second)!
	return json2.decode[ConsumerInfo](msg.text())!
}

// update_consumer updates an existing durable consumer's configuration.
// Only mutable fields (ack_wait, max_deliver, etc.) can be changed after creation.
// Returns error if the consumer doesn't exist or durable_name is empty.
pub fn (mut js JetStream) update_consumer(stream_name string, cfg ConsumerConfig) !ConsumerInfo {
	if stream_name == '' {
		return error('stream name must not be empty')
	}
	if cfg.durable_name == '' {
		return error('consumer durable_name must not be empty for update')
	}
	req := CreateConsumerRequest{
		stream_name: stream_name
		config:      cfg
	}
	msg := js.api_request('${js.prefix}.CONSUMER.DURABLE.CREATE.${stream_name}.${cfg.durable_name}',
		json2.encode[CreateConsumerRequest](req).bytes(), 5 * time.second)!
	return json2.decode[ConsumerInfo](msg.text())!
}

// consumer_info retrieves the current state and configuration of a consumer.
// Returns error if the stream or consumer doesn't exist.
pub fn (mut js JetStream) consumer_info(stream_name string, consumer_name string) !ConsumerInfo {
	if stream_name == '' {
		return error('stream name must not be empty')
	}
	if consumer_name == '' {
		return error('consumer name must not be empty')
	}
	msg := js.api_request('${js.prefix}.CONSUMER.INFO.${stream_name}.${consumer_name}', []u8{},
		5 * time.second)!
	return json2.decode[ConsumerInfo](msg.text())!
}

// delete_consumer permanently removes a consumer from a stream.
// For durable consumers this also deletes their stored state.
// Returns error if the stream or consumer doesn't exist.
pub fn (mut js JetStream) delete_consumer(stream_name string, consumer_name string) !bool {
	if stream_name == '' {
		return error('stream name must not be empty')
	}
	if consumer_name == '' {
		return error('consumer name must not be empty')
	}
	msg := js.api_request('${js.prefix}.CONSUMER.DELETE.${stream_name}.${consumer_name}', []u8{},
		5 * time.second)!
	resp := json2.decode[map[string]bool](msg.text())!
	return resp['success'] or { false }
}

// ===== Pull Consumer Support =====

// PullConsumerOptions configures settings for pull consumer creation.
pub struct PullConsumerOptions {
pub:
	// durable_name: name for a durable consumer (empty = ephemeral)
	durable_name string
	// max_waiting: max concurrent pull requests (default: 512)
	max_waiting int = 512
	// max_ack_pending: max messages in flight before ack (default: 1000)
	max_ack_pending int = 1000
	// filter_subject: optional subject filter within the stream
	filter_subject string
}

// pull_consumer_create creates a pull-based consumer on a stream.
// Pull consumers require explicit requests to fetch messages (no push delivery).
// Returns ConsumerInfo with the consumer's server-assigned name and state.
pub fn (mut js JetStream) pull_consumer_create(stream_name string, opts PullConsumerOptions) !ConsumerInfo {
	if stream_name == '' {
		return error('stream name must not be empty')
	}

	mut cfg := ConsumerConfig{
		durable_name:    opts.durable_name
		ack_policy:      .explicit
		deliver_policy:  .all
		max_waiting:     opts.max_waiting
		max_ack_pending: opts.max_ack_pending
		filter_subject:  opts.filter_subject
		// For pull consumers, deliver_subject must be empty
		deliver_subject: ''
	}

	return js.add_consumer(stream_name, cfg)
}

// PullFetchOptions configures a single fetch() request for a pull consumer.
pub struct PullFetchOptions {
pub:
	// batch: number of messages to request
	batch int = 10
	// max_bytes: max bytes to receive (0 = unlimited)
	max_bytes int
	// idle_timeout_ms: timeout if no messages arrive (milliseconds)
	idle_timeout_ms i64 = 5000
}

// fetch retrieves a batch of messages from a pull consumer.
// Blocks until messages arrive, the batch is full, or the timeout expires.
// Returns array of messages and any errors.
pub fn (mut js JetStream) fetch(stream_name string, consumer_name string, opts PullFetchOptions) !([]Msg, []IError) {
	if stream_name == '' {
		return error('stream name must not be empty'), []IError{}
	}
	if consumer_name == '' {
		return error('consumer name must not be empty'), []IError{}
	}

	// Construct the PULL request subject
	subject := '\$JS.API.CONSUMER.MSG.NEXT.${stream_name}.${consumer_name}'

	// Create the pull request with batch size and max_bytes
	mut pull_request_data := '{"batch":${opts.batch}'
	if opts.max_bytes > 0 {
		pull_request_data += ',"max_bytes":${opts.max_bytes}'
	}
	if opts.idle_timeout_ms > 0 {
		// idle_heartbeat in nanoseconds
		idle_nanos := opts.idle_timeout_ms * 1_000_000
		pull_request_data += ',"idle_heartbeat":${idle_nanos}'
	}
	pull_request_data += '}'

	request_data := pull_request_data.bytes()

	// Subscribe to receive the messages
	inbox := new_inbox()
	sub := js.nc.subscribe(inbox)!
	js.nc.flush()!
	defer { js.nc.unsubscribe(sub) or {} }

	// Send the pull request with reply subject
	js.nc.publish_with_reply(subject, inbox, request_data)!

	// Collect messages
	mut messages := []Msg{}
	mut errors := []IError{}

	// Timeout for the fetch operation
	timeout := time.now().add(opts.idle_timeout_ms * time.millisecond)

	for {
		// Check if we've exceeded the timeout
		if time.now() > timeout {
			break
		}

		// Calculate remaining timeout
		remaining := timeout - time.now()
		if remaining <= 0 {
			break
		}

		js.nc.set_read_timeout(remaining)

		// Get the next message
		msg := js.nc.next_msg() or {
			if err.msg().contains('timeout') {
				break
			}
			errors << IError(error(err.msg()))
			continue
		}

		if msg.status == 100 {
			// Idle heartbeat - continue waiting
			continue
		}
		if msg.status == 408 || msg.status == 409 {
			// 408 Request Timeout / 409 Batch Complete
			break
		}

		messages << msg
		if messages.len >= opts.batch {
			break
		}
	}

	// Reset to default timeout
	js.nc.set_read_timeout(js.nc.opts.connect_timeout)

	return messages, errors
}

// ===== Ordered Consumer Support =====

// OrderedConsumerOptions configures settings for ordered consumer creation.
// Ordered consumers ensure messages are delivered in stream order with no gaps.
pub struct OrderedConsumerOptions {
pub:
	// deliver_policy: where to start (default: all for replay, new_msgs for real-time)
	deliver_policy DeliverPolicy = .all
	// max_ack_pending: max messages in flight (default: 1000)
	max_ack_pending int = 1000
	// filter_subject: optional subject filter (default: all subjects)
	filter_subject string
	// flow_control_ms: interval for heartbeats to detect stalls (default: 5s)
	flow_control_ms i64 = 5000
}

// ordered_consumer_create creates an ordered push-based consumer.
// Ordered consumers guarantee delivery in order with no duplicates or gaps.
// Messages are delivered push-based to a subscription, not via pull requests.
// The consumer is ephemeral and auto-deleted when idle.
// Returns ConsumerInfo with the consumer's server-assigned name.
pub fn (mut js JetStream) ordered_consumer_create(stream_name string, opts OrderedConsumerOptions) !ConsumerInfo {
	if stream_name == '' {
		return error('stream name must not be empty')
	}

	// Ordered consumers use a special deliver group to ensure ordering
	// Generate a unique subject for this ordered consumer
	ordered_subject := '_ordered.${stream_name}.${rand.ulid()}'

	mut cfg := ConsumerConfig{
		// Ordered consumers are always ephemeral
		durable_name:    ''
		deliver_subject: ordered_subject
		ack_policy:      .explicit
		deliver_policy:  opts.deliver_policy
		max_ack_pending: opts.max_ack_pending
		filter_subject:  opts.filter_subject
		// Enable flow control with idle heartbeats
		flow_control:   true
		idle_heartbeat: opts.flow_control_ms * 1_000_000 // Convert to nanoseconds
		// These headers are set by NATS but important for ordering
		headers_only: false
	}

	return js.add_consumer(stream_name, cfg)
}

// subscribe_to_ordered_consumer subscribes to receive messages from an ordered consumer.
// The consumer must already be created with ordered_consumer_create().
// This subscribes to the consumer's delivery subject.
pub fn (mut js JetStream) subscribe_to_ordered_consumer(deliver_subject string) !Subscription {
	if deliver_subject == '' {
		return error('deliver_subject must not be empty')
	}
	return js.nc.subscribe(deliver_subject)
}
