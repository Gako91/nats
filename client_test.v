module nats

import net
import time

// MockConn is a scripted net.Connection: read() serves `rx`, then fails with
// a timeout error; write() records everything sent by the client.
struct MockConn {
mut:
	rx     []u8
	offset int
	tx     []u8
}

fn (c MockConn) addr() !net.Addr {
	return error('mock')
}

fn (c MockConn) peer_addr() !net.Addr {
	return error('mock')
}

fn (mut c MockConn) read(mut buf []u8) !int {
	if c.offset >= c.rx.len {
		return error('net: op timed out')
	}
	n := copy(mut buf, c.rx[c.offset..])
	c.offset += n
	return n
}

fn (mut c MockConn) write(data []u8) !int {
	c.tx << data
	return data.len
}

fn (mut c MockConn) close() ! {}

fn mock_client(rx string) &Client {
	mut nc := &Client{
		conn:   &MockConn{
			rx: rx.bytes()
		}
		rx_buf: []u8{len: 16384}
	}
	nc.opts.allow_reconnect = false
	nc.connected = true
	nc.resp_prefix = '_INBOX.t.'
	return nc
}

fn sent(nc &Client) string {
	conn := nc.conn
	if conn is MockConn {
		return conn.tx.bytestr()
	}
	return ''
}

fn test_request_keeps_messages_of_other_subscriptions() {
	// sid 1 = 'foo' subscription, sid 2 = shared response inbox
	mut nc := mock_client('MSG foo 1 5\r\nhello\r\nMSG _INBOX.t.1 2 3\r\nack\r\nMSG foo 1 5\r\nworld\r\n')
	sub := nc.subscribe('foo')!

	reply := nc.request_string('svc', 'ping', time.second)!
	assert reply.text() == 'ack'
	assert reply.sid == '2'

	first := nc.next_msg()!
	assert first.sid == sub.sid
	assert first.text() == 'hello'
	second := nc.next_msg()!
	assert second.text() == 'world'
	out := sent(nc)
	assert out.contains('SUB _INBOX.t.* 2\r\n')
	assert out.contains('PUB svc _INBOX.t.1 4\r\n')
	// single round trip: no PING/flush and no UNSUB per request
	assert !out.contains('PING')
	assert !out.contains('UNSUB')
}

fn test_request_drops_stale_reply_from_timed_out_request() {
	// _INBOX.t.1 belonged to an earlier request that timed out; its late reply
	// must be dropped, not returned to request 2 nor buffered for next_msg().
	mut nc := mock_client('MSG _INBOX.t.1 1 4\r\nlate\r\nMSG _INBOX.t.2 1 2\r\nr2\r\n')
	nc.resp_seq = 1
	reply := nc.request_string('svc', 'b', time.second)!
	assert reply.text() == 'r2'
	assert nc.pending_msgs.len == 0
}

fn test_requests_share_one_inbox() {
	mut nc := mock_client('MSG _INBOX.t.1 1 2\r\nr1\r\nMSG _INBOX.t.2 1 2\r\nr2\r\n')
	assert nc.request_string('svc', 'a', time.second)!.text() == 'r1'
	assert nc.request_string('svc', 'b', time.second)!.text() == 'r2'
	assert sent(nc).count('SUB ') == 1
}

fn test_request_timeout() {
	mut nc := mock_client('')
	nc.request_string('svc', 'ping', time.second) or {
		assert err.msg() == err_request_timeout
		return
	}
	assert false
}

fn test_request_no_responders() {
	hdr := 'NATS/1.0 503\r\n\r\n'
	mut nc := mock_client('HMSG _INBOX.t.1 1 ${hdr.len} ${hdr.len}\r\n${hdr}\r\n')
	nc.request_string('svc', 'ping', time.second) or {
		assert err.msg() == err_no_responders
		return
	}
	assert false
}

fn test_next_msg_drops_messages_for_unsubscribed_sid() {
	mut nc := mock_client('MSG late 9 4\r\nlate\r\nMSG foo 1 2\r\nok\r\n')
	nc.subscribe('foo')!
	msg := nc.next_msg()!
	assert msg.text() == 'ok'
}

fn test_unsubscribe_discards_buffered_messages() {
	mut nc := mock_client('')
	a := nc.subscribe('a')!
	nc.subscribe('b')!
	nc.pending_msgs << Msg{
		subject: 'a'
		sid:     a.sid
	}
	nc.pending_msgs << Msg{
		subject: 'b'
		sid:     '2'
	}
	nc.unsubscribe(a)!
	assert nc.pending_msgs.len == 1
	assert nc.pending_msgs[0].subject == 'b'
}

fn test_fetch_keeps_messages_of_other_subscriptions() {
	// sid 1 = 'foo' subscription, sid 2 = fetch inbox
	mut nc := mock_client('PONG\r\nMSG foo 1 5\r\nhello\r\nMSG orders.new 2 2\r\nm1\r\nMSG orders.new 2 2\r\nm2\r\n')
	nc.subscribe('foo')!
	mut js := nc.jetstream_with_prefix('\$JS.hub.API')

	msgs, errs := js.fetch('ORDERS', 'worker', PullFetchOptions{ batch: 2 })!
	assert errs.len == 0
	assert msgs.len == 2
	assert msgs[0].text() == 'm1'
	assert msgs[1].text() == 'm2'
	assert sent(nc).contains('PUB \$JS.hub.API.CONSUMER.MSG.NEXT.ORDERS.worker ')

	other := nc.next_msg()!
	assert other.text() == 'hello'
}

fn test_fetch_stops_on_no_messages_status() {
	hdr := 'NATS/1.0 404 No Messages\r\n\r\n'
	mut nc := mock_client('PONG\r\nHMSG _INBOX.x 1 ${hdr.len} ${hdr.len}\r\n${hdr}\r\n')
	mut js := nc.jetstream()
	msgs, errs := js.fetch('ORDERS', 'worker', PullFetchOptions{ batch: 5 })!
	assert msgs.len == 0
	assert errs.len == 0
}

struct AckLog {
mut:
	results []string
}

fn test_publish_async_uses_shared_inbox_and_routes_acks() {
	ack1 := '{"stream":"S","seq":1}'
	ack2 := '{"stream":"S","seq":2}'
	no_stream := 'NATS/1.0 503\r\n\r\n'
	mut nc := mock_client('MSG _INBOX.t.2 1 ${ack2.len}\r\n${ack2}\r\nMSG _INBOX.t.1 1 ${ack1.len}\r\n${ack1}\r\n' +
		'HMSG _INBOX.t.3 1 ${no_stream.len} ${no_stream.len}\r\n${no_stream}\r\nMSG foo 2 2\r\nok\r\n')
	mut js := nc.jetstream()
	mut log := &AckLog{}
	cb := fn [mut log] (mut nc Client, subject string, r PublishResult) {
		if r.error_msg != '' {
			log.results << '${subject}:err'
		} else {
			log.results << '${subject}:${r.ack.seq}'
		}
	}
	js.publish_string_async('a', 'x', cb)!
	js.publish_string_async('b', 'y', cb)!
	js.publish_string_async('nostream', 'z', cb)!
	assert nc.pending_publishes.len == 3
	nc.subscribe('foo')!

	// next_msg() processes the three ACKs (out of order) and returns 'ok'
	msg := nc.next_msg()!
	assert msg.text() == 'ok'
	assert log.results == ['b:2', 'a:1', 'nostream:err']
	assert nc.pending_publishes.len == 0

	out := sent(nc)
	assert out.count('SUB ') == 2 // shared inbox + foo
	assert !out.contains('PING')
}

fn test_publish_async_timeout() {
	mut nc := mock_client('MSG foo 2 2\r\nok\r\n')
	nc.opts.publish_async_timeout = 0
	mut js := nc.jetstream()
	mut log := &AckLog{}
	js.publish_string_async('a', 'x', fn [mut log] (mut nc Client, subject string, r PublishResult) {
		log.results << r.error_msg
	})!
	nc.subscribe('foo')!
	time.sleep(2 * time.millisecond)
	nc.next_msg()!
	assert log.results.len == 1
	assert log.results[0].contains('timeout')
	assert nc.pending_publishes.len == 0
}

fn test_publish_async_complete_waits_for_all_acks() {
	ack1 := '{"stream":"S","seq":1}'
	ack2 := '{"stream":"S","seq":2}'
	mut nc := mock_client('MSG _INBOX.t.1 1 ${ack1.len}\r\n${ack1}\r\nMSG foo 2 2\r\nok\r\nMSG _INBOX.t.2 1 ${ack2.len}\r\n${ack2}\r\n')
	mut js := nc.jetstream()
	mut log := &AckLog{}
	cb := fn [mut log] (mut nc Client, subject string, r PublishResult) {
		log.results << '${r.ack.seq}'
	}
	js.publish_string_async('a', 'x', cb)!
	nc.subscribe('foo')!
	js.publish_string_async('a', 'y', cb)!
	assert js.publish_async_pending() == 2

	// returns as soon as both ACKs are in, without waiting for the timeout
	js.publish_async_complete(time.second)!
	assert log.results == ['1', '2']
	assert js.publish_async_pending() == 0
	// the regular message received meanwhile is kept
	assert nc.next_msg()!.text() == 'ok'
}

fn test_publish_async_complete_timeout() {
	mut nc := mock_client('')
	mut js := nc.jetstream()
	js.publish_string_async('a', 'x', fn (mut nc Client, subject string, r PublishResult) {})!
	js.publish_async_complete(20 * time.millisecond) or {
		assert err.msg().contains('1 async publish(es) still pending')
		return
	}
	assert false
}

fn test_flush_routes_async_acks() {
	ack := '{"stream":"S","seq":7}'
	mut nc := mock_client('MSG _INBOX.t.1 1 ${ack.len}\r\n${ack}\r\nPONG\r\n')
	mut js := nc.jetstream()
	mut log := &AckLog{}
	js.publish_string_async('a', 'x', fn [mut log] (mut nc Client, subject string, r PublishResult) {
		log.results << '${r.ack.seq}'
	})!
	nc.flush()!
	assert log.results == ['7']
	assert nc.pending_msgs.len == 0
}

struct DisconnectCount {
mut:
	n int
}

fn test_request_timeout_does_not_reconnect() {
	// Reproduces the CI hang: with allow_reconnect, a read timeout used to be
	// treated as a lost connection and trigger reconnect() in a loop.
	mut nc := mock_client('')
	mut count := &DisconnectCount{}
	nc.opts.allow_reconnect = true
	nc.opts.on_disconnect = fn [mut count] (mut c Client) {
		count.n++
	}
	nc.request_string('svc', 'ping', 50 * time.millisecond) or {
		assert err.msg() == err_request_timeout
		assert count.n == 0
		return
	}
	assert false
}

fn test_should_reconnect() {
	mut nc := mock_client('')
	nc.opts.allow_reconnect = true
	assert !nc.should_reconnect(error('net: op timed out'))
	assert nc.should_reconnect(error('net: socket error: 104'))
	assert nc.should_reconnect(none)
	nc.connecting = true
	assert !nc.should_reconnect(error('net: socket error: 104'))
	nc.connecting = false
	nc.opts.allow_reconnect = false
	assert !nc.should_reconnect(none)
}
