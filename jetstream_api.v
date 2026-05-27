module nats

import json
import time

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
