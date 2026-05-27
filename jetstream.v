module nats

import json
import time

const js_api_prefix = '$JS.API'

pub struct JetStream {
mut:
	nc &Client = unsafe { nil }
pub:
	prefix string = js_api_prefix
}

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

pub struct PubAck {
pub:
	stream    string
	seq       u64
	duplicate bool
	error     ApiError
}

pub struct ApiError {
pub:
	code        int
	err_code    int @[json: err_code]
	description string
}

pub fn (mut nc Client) jetstream() JetStream {
	return JetStream{
		nc:     unsafe { &nc }
		prefix: js_api_prefix
	}
}

pub fn (mut nc Client) jetstream_with_prefix(prefix string) JetStream {
	return JetStream{
		nc:     unsafe { &nc }
		prefix: prefix
	}
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

pub fn (mut js JetStream) publish(subject string, data []u8) !PubAck {
	ack_msg := js.nc.request(subject, data, 5 * time.second)!
	ack := json.decode(PubAck, ack_msg.text())!
	if ack.error.code != 0 || ack.error.err_code != 0 || ack.error.description != '' {
		return error('JetStream publish failed: ${ack.error.description}')
	}
	return ack
}

pub fn (mut js JetStream) publish_string(subject string, data string) !PubAck {
	return js.publish(subject, data.bytes())
}

fn (mut js JetStream) api_request(subject string, data []u8, timeout time.Duration) !Msg {
	msg := js.nc.request(subject, data, timeout)!
	if msg.text().contains('"error"') {
		ack := json.decode(PubAck, msg.text()) or { PubAck{} }
		if ack.error.description != '' {
			return error('JetStream API error: ${ack.error.description}')
		}
	}
	return msg
}
