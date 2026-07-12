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
		conn:      conn
		opts:      opts
		subs:      map[string]Subscription{}
		rx_buf:    []u8{len: 16384}
		rx_offset: 0
		rx_len:    0
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

pub fn (mut nc Client) flush() ! {
	nc.write('PING${crlf}')!

	for {
		line := nc.read_line()!
		frame := parse_protocol_line(line)

		match frame.op {
			.pong {
				return
			}
			.ping {
				nc.write('PONG${crlf}')!
			}
			.ok {
				continue
			}
			.err {
				return error(frame.raw)
			}
			.info {
				nc.parse_info(frame.payload) or {}
			}
			.msg {
				msg := nc.parse_msg(frame.raw)!
				nc.pending_msgs << msg
			}
			.hmsg {
				msg := nc.parse_hmsg(frame.raw)!
				nc.pending_msgs << msg
			}
			.unknown {
				continue
			}
		}
	}
}

fn (mut nc Client) read_info() ! {
	line := nc.read_line()!
	frame := parse_protocol_line(line)
	if frame.op != .info {
		return error('expected INFO from NATS server, got: ${line}')
	}
	nc.parse_info(frame.payload)!
	return
}

fn (mut nc Client) send_connect() ! {
	payload := ConnectPayload{
		verbose:       nc.opts.verbose
		pedantic:      nc.opts.pedantic
		name:          nc.opts.name
		lang:          client_lang
		version:       client_version
		protocol:      1
		echo:          !nc.opts.no_echo
		headers:       nc.opts.headers
		no_responders: nc.opts.headers && nc.opts.no_responders
	}
	nc.write('CONNECT ${json.encode(payload)}${crlf}')!
	return
}

fn (mut nc Client) read_line() !string {
	mut line := []u8{}
	for {
		if nc.rx_offset >= nc.rx_len {
			nc.rx_offset = 0
			nc.rx_len = 0
			n := nc.conn.read(mut nc.rx_buf)!
			if n == 0 {
				return error(err_connection_closed)
			}
			nc.rx_len = n
		}

		mut found := -1
		for i in nc.rx_offset .. nc.rx_len {
			if nc.rx_buf[i] == `\n` {
				found = i
				break
			}
		}

		if found != -1 {
			mut end := found
			if end > nc.rx_offset && nc.rx_buf[end - 1] == `\r` {
				end--
			} else if end == nc.rx_offset && line.len > 0 && line[line.len - 1] == `\r` {
				line.delete_last()
			}
			line << nc.rx_buf[nc.rx_offset..end]
			nc.rx_offset = found + 1
			return line.bytestr()
		} else {
			line << nc.rx_buf[nc.rx_offset..nc.rx_len]
			nc.rx_offset = nc.rx_len
		}
	}
	return error(err_connection_closed)
}

fn (mut nc Client) read_exact(size int) ![]u8 {
	mut data := []u8{len: size}
	mut read := 0

	if nc.rx_offset < nc.rx_len {
		available := nc.rx_len - nc.rx_offset
		to_copy := if available < size { available } else { size }
		copy(mut data[..to_copy], nc.rx_buf[nc.rx_offset .. nc.rx_offset + to_copy])
		nc.rx_offset += to_copy
		read += to_copy
	}

	for read < size {
		n := nc.conn.read(mut data[read..])!
		if n == 0 {
			return error(err_connection_closed)
		}
		read += n
	}

	return data
}

fn (mut nc Client) write(s string) ! {
	nc.conn.write_string(s)!
	return
}
