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
	// sid 1 = 'foo' subscription, sid 2 = request inbox
	mut nc := mock_client('PONG\r\nMSG foo 1 5\r\nhello\r\nMSG _INBOX.x 2 3\r\nack\r\nMSG foo 1 5\r\nworld\r\n')
	sub := nc.subscribe('foo')!

	reply := nc.request_string('svc', 'ping', time.second)!
	assert reply.text() == 'ack'
	assert reply.sid == '2'

	first := nc.next_msg()!
	assert first.sid == sub.sid
	assert first.text() == 'hello'
	second := nc.next_msg()!
	assert second.text() == 'world'
	assert sent(nc).contains('UNSUB 2\r\n')
}

fn test_request_timeout() {
	mut nc := mock_client('PONG\r\n')
	nc.request_string('svc', 'ping', time.second) or {
		assert err.msg() == err_request_timeout
		return
	}
	assert false
}

fn test_request_no_responders() {
	hdr := 'NATS/1.0 503\r\n\r\n'
	mut nc := mock_client('PONG\r\nHMSG _INBOX.x 1 ${hdr.len} ${hdr.len}\r\n${hdr}\r\n')
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
