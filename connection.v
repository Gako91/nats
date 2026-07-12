module nats

import json
import net
import net.ssl
import time

pub fn connect(url string) !Client {
	mut opts := Options{
		url: url
	}
	return connect_with_options(opts)
}

pub fn connect_with_options(opts Options) !Client {
	mut nc := Client{
		subs:         map[string]Subscription{}
		rx_buf:       []u8{len: 16384}
		rx_offset:    0
		rx_len:       0
	}
	nc.connect_to(opts)!
	return nc
}

pub fn (mut nc Client) close() {
	if nc.connected {
		nc.connected = false
		nc.conn.close() or {}
		if cb := nc.opts.on_close {
			cb(mut nc)
		}
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
		auth_token:    nc.opts.auth_token
		user:          nc.opts.user
		pass:          nc.opts.password
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
			n := nc.conn.read(mut nc.rx_buf) or {
				if nc.opts.allow_reconnect {
					nc.reconnect()!
					continue
				}
				return err
			}
			if n == 0 {
				if nc.opts.allow_reconnect {
					nc.reconnect()!
					continue
				}
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
		n := nc.conn.read(mut data[read..]) or {
			if nc.opts.allow_reconnect {
				nc.reconnect()!
				return error(err_connection_closed)
			}
			return err
		}
		if n == 0 {
			if nc.opts.allow_reconnect {
				nc.reconnect()!
				return error(err_connection_closed)
			}
			return error(err_connection_closed)
		}
		read += n
	}

	return data
}

fn (mut nc Client) write(s string) ! {
	nc.write_bytes(s.bytes())!
}

fn (mut nc Client) write_bytes(data []u8) ! {
	nc.conn.write(data) or {
		if nc.opts.allow_reconnect {
			nc.reconnect()!
			nc.conn.write(data)!
			return
		}
		return err
	}
}

// Timeout helper methods

pub fn (mut nc Client) set_read_timeout(t time.Duration) {
	if mut nc.conn is net.TcpConn {
		nc.conn.set_read_timeout(t)
	} else if mut nc.conn is ssl.SSLConn {
		nc.conn.set_read_timeout(t)
	}
}

pub fn (mut nc Client) set_write_timeout(t time.Duration) {
	if mut nc.conn is net.TcpConn {
		nc.conn.set_write_timeout(t)
	}
}

// Internal connection helpers

fn (mut nc Client) connect_to(opts Options) ! {
	address := address_from_url(opts.url)!
	hostname := hostname_from_url(opts.url)!

	mut conn := net.dial_tcp(address)!
	conn.set_read_timeout(opts.connect_timeout)
	conn.set_write_timeout(opts.connect_timeout)

	nc.conn = conn
	
	// Reset the rx buffers for the new socket
	nc.rx_offset = 0
	nc.rx_len = 0

	// Parse credentials from URL if present and override options
	mut final_opts := opts
	url_user, url_pass, url_token := parse_url_credentials(opts.url)
	if url_token != '' {
		final_opts.auth_token = url_token
	}
	if url_user != '' {
		final_opts.user = url_user
		final_opts.password = url_pass
	}
	nc.opts = final_opts

	nc.read_info()!

	// Upgrade to TLS if required by the server or forced/requested by options
	if nc.info.tls_required || opts.tls_config != none || opts.url.starts_with('tls://') {
		nc.upgrade_to_tls(hostname)!
	}

	nc.send_connect()!
	nc.flush()!
	nc.connected = true
}

fn (mut nc Client) upgrade_to_tls(hostname string) ! {
	mut ssl_config := ssl.SSLConnectConfig{}
	if cfg := nc.opts.tls_config {
		ssl_config = cfg
	}

	mut tcp_conn := nc.conn as &net.TcpConn
	mut ssl_conn := ssl.new_ssl_conn(ssl_config)!
	ssl_conn.connect(mut *tcp_conn, hostname)!
	nc.conn = ssl_conn
}

pub fn (mut nc Client) reconnect() ! {
	if !nc.opts.allow_reconnect {
		return error(err_connection_closed)
	}

	nc.connected = false
	if cb := nc.opts.on_disconnect {
		cb(mut nc)
	}

	mut attempts := 0
	mut delay := nc.opts.reconnect_time_wait

	// Build the list of server URLs to try
	mut urls := []string{}
	if nc.opts.servers.len > 0 {
		urls << nc.opts.servers
	}
	if nc.opts.url !in urls {
		urls << nc.opts.url
	}

	for attempts < nc.opts.max_reconnects {
		attempts++
		for url in urls {
			mut new_opts := nc.opts
			new_opts.url = url

			nc.connect_to(new_opts) or {
				if cb := nc.opts.on_error {
					cb(mut nc, 'reconnect attempt failed for ${url}: ${err}')
				}
				continue
			}

			// Connection succeeded! Restore subscriptions.
			nc.restore_subscriptions() or {
				nc.close()
				if cb := nc.opts.on_error {
					cb(mut nc, 'failed to restore subscriptions on ${url}: ${err}')
				}
				continue
			}

			if cb := nc.opts.on_reconnect {
				cb(mut nc)
			}
			return
		}
		time.sleep(delay)
	}

	return error('nats: reconnect failed after ${attempts} attempts')
}

fn (mut nc Client) restore_subscriptions() ! {
	for _, sub in nc.subs {
		if sub.queue == '' {
			nc.write('SUB ${sub.subject} ${sub.sid}${crlf}')!
		} else {
			nc.write('SUB ${sub.subject} ${sub.queue} ${sub.sid}${crlf}')!
		}
	}
	nc.flush()!
}

pub fn (mut nc Client) disconnect_for_testing() ! {
	nc.conn.close()!
}

