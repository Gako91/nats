module nats

import json

fn (mut nc Client) parse_info(payload string) ! {
	nc.info = json.decode(ServerInfo, payload)!
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
