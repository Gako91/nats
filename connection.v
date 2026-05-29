module nats

import json
import net

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

fn (mut nc Client) flush_old() ! {
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

pub fn (mut nc Client) flush() ! {
	nc.write('PING${crlf}')!

	for {
		line := nc.read_line()!

		op := line.all_before(' ')

		match op {
			'PONG' {
				return
			}
			'PING' {
				nc.write('PONG${crlf}')!
			}
			'-ERR' {
				return error(line)
			}
			'INFO' {
				payload := line.all_after(' ')
				nc.parse_info(payload) or {}
			}
			'MSG' {
				nc.parse_msg(line) or {}
			}
			else {}
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

fn (mut nc Client) send_connect() ! {
	payload := ConnectPayload{
		verbose:       nc.opts.verbose
		pedantic:      nc.opts.pedantic
		name:          nc.opts.name
		lang:          'v'
		version:       '0.1.0'
		protocol:      1
		echo:          !nc.opts.no_echo
		no_responders: false
	}
	nc.write('CONNECT ${json.encode(payload)}${crlf}')!
	return
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
