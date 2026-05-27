module nats

import json
import time

pub struct StreamConfig {
pub mut:
	name         string
	subjects     []string
	retention    string
	max_msgs     i64 = -1
	max_bytes    i64 = -1
	max_age      i64
	storage      string = 'file'
	num_replicas int    = 1
}

pub struct StreamInfo {
pub:
	config  StreamConfig
	created string
	state   StreamState
}

pub struct StreamState {
pub:
	messages       u64
	bytes          u64
	first_seq      u64 @[json: first_seq]
	last_seq       u64 @[json: last_seq]
	consumer_count int @[json: consumer_count]
}

pub fn (mut js JetStream) add_stream(cfg StreamConfig) !StreamInfo {
	if cfg.name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.CREATE.${cfg.name}', json.encode(cfg).bytes(),
		5 * time.second)!
	info := json.decode(StreamInfo, msg.text())!
	return info
}

pub fn (mut js JetStream) update_stream(cfg StreamConfig) !StreamInfo {
	if cfg.name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.UPDATE.${cfg.name}', json.encode(cfg).bytes(),
		5 * time.second)!
	return json.decode(StreamInfo, msg.text())!
}

pub fn (mut js JetStream) stream_info(name string) !StreamInfo {
	if name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.INFO.${name}', []u8{}, 5 * time.second)!
	return json.decode(StreamInfo, msg.text())!
}

pub fn (mut js JetStream) delete_stream(name string) !bool {
	if name == '' {
		return error('stream name must not be empty')
	}
	msg := js.api_request('${js.prefix}.STREAM.DELETE.${name}', []u8{}, 5 * time.second)!
	resp := json.decode(map[string]bool, msg.text())!
	return resp['success'] or { false }
}
