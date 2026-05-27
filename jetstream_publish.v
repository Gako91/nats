module nats

import json
import time

pub struct PubAck {
pub:
	stream    string
	seq       u64
	duplicate bool
	error     ApiError
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
