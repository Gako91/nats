module nats

import json
import time

// StreamConfig describes the configuration for a NATS JetStream stream.
// A stream is a persistent storage system for messages on a set of subjects.
// Common field defaults: retention='limits' (purge old messages), storage='file' (persistent),
// max_msgs=-1 (unlimited), max_age=0 (no age limit).
pub struct StreamConfig {
pub mut:
	// name: unique stream name
	name string
	// subjects: list of NATS subjects this stream receives (e.g., ['orders.>'])
	subjects []string
	// retention: 'limits' (default), 'interest', or 'workqueue' - determines cleanup policy
	retention string = 'limits'
	// max_msgs: maximum number of messages (per subject if interest/workqueue), -1 = unlimited
	max_msgs i64 = -1
	// max_bytes: maximum total storage size in bytes, -1 = unlimited
	max_bytes i64 = -1
	// max_age: maximum message age in nanoseconds before cleanup, 0 = no limit
	max_age i64
	// storage: 'file' (persistent) or 'memory' (ephemeral)
	storage string = 'file'
	// num_replicas: replication factor for cluster deployments (1 = single server)
	num_replicas int = 1
}

// StreamInfo contains information about a stream (configuration + current statistics).
pub struct StreamInfo {
pub:
	// config: the stream's current configuration
	config StreamConfig
	// created: ISO 8601 timestamp when stream was created
	created string
	// state: current message statistics (count, bytes, sequences, etc.)
	state StreamState
}

// StreamState contains statistics about messages in a stream.
pub struct StreamState {
pub:
	// messages: total number of messages currently stored
	messages u64
	// bytes: total bytes of message data currently stored
	bytes u64
	// first_seq: sequence number of first message
	first_seq u64 @[json: first_seq]
	// last_seq: sequence number of last message
	last_seq u64 @[json: last_seq]
	// consumer_count: number of active consumers on this stream
	consumer_count int @[json: consumer_count]
}

// add_stream creates a new stream with the given configuration.
// Stream name must be unique. Subjects determine which messages this stream receives.
// Returns error if stream already exists or configuration is invalid.
pub fn (mut js JetStream) add_stream(cfg StreamConfig) !StreamInfo {
	if cfg.name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.CREATE.${cfg.name}', json.encode(cfg).bytes(),
		5 * time.second)!
	info := json.decode(StreamInfo, msg.text())!
	return info
}

// update_stream updates an existing stream's configuration.
// Use this to change retention policy, max messages/bytes, storage type, etc.
// Returns error if stream doesn't exist.
pub fn (mut js JetStream) update_stream(cfg StreamConfig) !StreamInfo {
	if cfg.name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.UPDATE.${cfg.name}', json.encode(cfg).bytes(),
		5 * time.second)!
	return json.decode(StreamInfo, msg.text())!
}

// stream_info retrieves information about a stream: configuration and current statistics.
// Returns error if stream doesn't exist.
pub fn (mut js JetStream) stream_info(name string) !StreamInfo {
	if name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.INFO.${name}', []u8{}, 5 * time.second)!
	return json.decode(StreamInfo, msg.text())!
}

// delete_stream permanently deletes a stream and all its stored messages.
// Use with caution: this cannot be undone.
// Returns error if stream doesn't exist.
pub fn (mut js JetStream) delete_stream(name string) !bool {
	if name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.DELETE.${name}', []u8{}, 5 * time.second)!
	resp := json.decode(map[string]bool, msg.text())!
	return resp['success'] or { false }
}
