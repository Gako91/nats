module nats

import json2
import time

// ApiSuccessResponse is the JetStream API response for delete operations.
struct ApiSuccessResponse {
	success bool
}

fn (mut js JetStream) api_request(subject string, data []u8, timeout time.Duration) !Msg {
	msg := js.nc.request(subject, data, timeout)!
	if msg.text().contains('"error"') {
		ack := json2.decode[PubAck](msg.text()) or { PubAck{} }
		if ack.error.description != '' {
			return error('JetStream API error: ${ack.error.description}')
		}
	}
	return msg
}
