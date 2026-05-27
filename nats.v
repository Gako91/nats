module nats

import json
import net
import rand
import time

const default_url = 'nats://127.0.0.1:4222'
const crlf = '\r\n'

pub struct Options {
pub mut:
	url             string = default_url
	name            string
	verbose         bool
	pedantic        bool
	no_echo         bool
	connect_timeout time.Duration = 2 * time.second
}

@[heap]
pub struct Client {
mut:
	conn      &net.TcpConn = unsafe { nil }
	next_sid  int          = 1
	subs      map[string]Subscription
	connected bool
pub mut:
	info ServerInfo
	opts Options
}

pub struct ServerInfo {
pub mut:
	server_id     string @[json: server_id]
	server_name   string @[json: server_name]
	version       string
	proto         int
	go            string
	host          string
	port          int
	headers       bool
	max_payload   int    @[json: max_payload]
	client_id     u64    @[json: client_id]
	client_ip     string @[json: client_ip]
	auth_required bool   @[json: auth_required]
	tls_required  bool   @[json: tls_required]
}

struct ConnectPayload {
	verbose       bool
	pedantic      bool
	name          string
	lang          string
	version       string
	protocol      int
	echo          bool
	no_responders bool
}

pub struct Msg {
pub:
	subject string
	sid     string
	reply   string
	data    []u8
}

pub fn (m Msg) text() string {
	return m.data.bytestr()
}

pub struct Subscription {
pub:
	subject string
	queue   string
	sid     string
}

pub fn connect(url string) !Client {
	mut opts := Options{
		url: url
	}
	return connect_with_options(opts)
}

pub fn connect_with_options(opts Options) !Client {
	address := address_from_url(opts.url)!
	mut conn := net.dial_tcp(address)!
	conn.set_read_timeout(opts.connect_timeout)
	conn.set_write_timeout(opts.connect_timeout)

	mut nc := Client{
		conn: conn
		opts: opts
		subs: map[string]Subscription{}
	}
	nc.read_info()!
	nc.send_connect()!
	nc.flush()!
	nc.connected = true
	return nc
}

pub fn (mut nc Client) close() {
	if nc.connected {
		nc.connected = false
		nc.conn.close() or {}
	}
}

pub fn (mut nc Client) publish(subject string, data []u8) ! {
	nc.publish_with_reply(subject, '', data)!
	return
}

pub fn (mut nc Client) publish_string(subject string, data string) ! {
	nc.publish(subject, data.bytes())!
	return
}

pub fn (mut nc Client) publish_with_reply(subject string, reply string, data []u8) ! {
	validate_subject(subject)!
	if reply == '' {
		nc.write('PUB ${subject} ${data.len}${crlf}')!
	} else {
		validate_subject(reply)!
		nc.write('PUB ${subject} ${reply} ${data.len}${crlf}')!
	}
	nc.conn.write(data)!
	nc.write(crlf)!
	return
}

pub fn (mut nc Client) subscribe(subject string) !Subscription {
	return nc.queue_subscribe(subject, '')
}

pub fn (mut nc Client) queue_subscribe(subject string, queue string) !Subscription {
	validate_subject(subject)!
	if queue.contains(' ') {
		return error('queue group must not contain spaces')
	}
	sid := nc.next_sid.str()
	nc.next_sid++
	if queue == '' {
		nc.write('SUB ${subject} ${sid}${crlf}')!
	} else {
		nc.write('SUB ${subject} ${queue} ${sid}${crlf}')!
	}
	sub := Subscription{
		subject: subject
		queue:   queue
		sid:     sid
	}
	nc.subs[sid] = sub
	return sub
}

pub fn (mut nc Client) unsubscribe(sub Subscription) ! {
	nc.write('UNSUB ${sub.sid}${crlf}')!
	nc.subs.delete(sub.sid)
	return
}

pub fn (mut nc Client) next_msg() !Msg {
	for {
		line := nc.read_line()!
		if line == '' {
			continue
		}
		if line == 'PING' {
			nc.write('PONG${crlf}')!
			continue
		}
		if line == 'PONG' || line.starts_with('+OK') {
			continue
		}
		if line.starts_with('-ERR') {
			return error(line)
		}
		if line.starts_with('INFO ') {
			nc.parse_info(line[5..]) or {}
			continue
		}
		if line.starts_with('MSG ') {
			return nc.parse_msg(line)!
		}
		return error('unexpected NATS protocol line: ${line}')
	}
	return error('connection closed')
}

pub fn (mut nc Client) request(subject string, data []u8, timeout time.Duration) !Msg {
	inbox := new_inbox()
	sub := nc.subscribe(inbox)!
	nc.flush()!
	nc.publish_with_reply(subject, inbox, data)!
	nc.conn.set_read_timeout(timeout)
	defer {
		nc.conn.set_read_timeout(nc.opts.connect_timeout)
		nc.unsubscribe(sub) or {}
	}
	for {
		msg := nc.next_msg()!
		if msg.sid == sub.sid || msg.subject == inbox {
			return msg
		}
	}
	return error('request timed out')
}

pub fn (mut nc Client) request_string(subject string, data string, timeout time.Duration) !Msg {
	return nc.request(subject, data.bytes(), timeout)
}

pub fn (mut nc Client) flush() ! {
	nc.write('PING${crlf}')!
	for {
		line := nc.read_line()!
		if line == 'PONG' {
			return
		}
		if line == 'PING' {
			nc.write('PONG${crlf}')!
			continue
		}
		if line.starts_with('-ERR') {
			return error(line)
		}
		if line.starts_with('INFO ') {
			nc.parse_info(line[5..]) or {}
			continue
		}
		if line.starts_with('MSG ') {
			nc.parse_msg(line) or {}
			continue
		}
	}
}

fn (mut nc Client) read_info() ! {
	line := nc.read_line()!
	if !line.starts_with('INFO ') {
		return error('expected INFO from NATS server, got: ${line}')
	}
	nc.parse_info(line[5..])!
	return
}

fn (mut nc Client) parse_info(payload string) ! {
	nc.info = json.decode(ServerInfo, payload)!
	return
}

fn (mut nc Client) send_connect() ! {
	payload := ConnectPayload{
		verbose:       nc.opts.verbose
		pedantic:      nc.opts.pedantic
		name:          nc.opts.name
		lang:          'v'
		version:       '0.1.0'
		protocol:      1
		echo:          !nc.opts.no_echo
		no_responders: true
	}
	nc.write('CONNECT ${json.encode(payload)}${crlf}')!
	return
}

fn (mut nc Client) parse_msg(line string) !Msg {
	parts := line.split(' ')
	if parts.len != 4 && parts.len != 5 {
		return error('invalid MSG line: ${line}')
	}
	subject := parts[1]
	sid := parts[2]
	mut reply := ''
	mut size_txt := parts[3]
	if parts.len == 5 {
		reply = parts[3]
		size_txt = parts[4]
	}
	size := size_txt.int()
	if size < 0 {
		return error('invalid MSG payload size: ${size_txt}')
	}
	data := nc.read_exact(size)!
	terminator := nc.read_exact(2)!
	if terminator.bytestr() != crlf {
		return error('invalid MSG terminator')
	}
	return Msg{
		subject: subject
		sid:     sid
		reply:   reply
		data:    data
	}
}

fn (mut nc Client) read_line() !string {
	mut out := []u8{}
	mut one := []u8{len: 1}
	for {
		n := nc.conn.read(mut one)!
		if n == 0 {
			return error('connection closed')
		}
		out << one[0]
		if out.len >= 2 && out[out.len - 2] == `\r` && out[out.len - 1] == `\n` {
			return out[..out.len - 2].bytestr()
		}
	}
	return error('connection closed')
}

fn (mut nc Client) read_exact(size int) ![]u8 {
	mut data := []u8{len: size}
	mut read := 0
	for read < size {
		n := nc.conn.read(mut data[read..])!
		if n == 0 {
			return error('connection closed')
		}
		read += n
	}
	return data
}

fn (mut nc Client) write(s string) ! {
	nc.conn.write_string(s)!
	return
}

fn address_from_url(raw string) !string {
	mut url := raw
	if url == '' {
		url = default_url
	}
	if url.starts_with('nats://') {
		url = url[7..]
	}
	if url.starts_with('tls://') {
		return error('TLS is not implemented yet; use nats://')
	}
	if url.contains('@') {
		url = url.all_after('@')
	}
	url = url.trim_right('/')
	if url == '' {
		return error('empty NATS URL')
	}
	if !url.contains(':') {
		return '${url}:4222'
	}
	return url
}

fn validate_subject(subject string) ! {
	if subject == '' {
		return error('subject must not be empty')
	}
	if subject.contains(' ') || subject.contains('\t') || subject.contains('\r')
		|| subject.contains('\n') {
		return error('subject must not contain whitespace')
	}
	return
}

pub fn new_inbox() string {
	return '_INBOX.${rand.ulid()}'
}
