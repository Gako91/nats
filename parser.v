module nats

import json

struct ParsedHeaderBlock {
	headers     map[string]string
	status      int
	description string
}

fn (mut nc Client) parse_info(payload string) ! {
	nc.info = json.decode(ServerInfo, payload)!
	return
}

fn (mut nc Client) parse_msg(line string) !Msg {
	parts := line.split(' ')
	if parts.len != 4 && parts.len != 5 {
		return error('nats: invalid protocol line: ${line}')
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
	nc.read_msg_terminator()!
	return Msg{
		subject: subject
		sid:     sid
		reply:   reply
		data:    data
	}
}

fn (mut nc Client) parse_hmsg(line string) !Msg {
	parts := line.split(' ')
	if parts.len != 5 && parts.len != 6 {
		return error('nats: invalid protocol line: ${line}')
	}
	subject := parts[1]
	sid := parts[2]
	mut reply := ''
	mut header_size_txt := parts[3]
	mut total_size_txt := parts[4]
	if parts.len == 6 {
		reply = parts[3]
		header_size_txt = parts[4]
		total_size_txt = parts[5]
	}
	header_size := header_size_txt.int()
	total_size := total_size_txt.int()
	if header_size < 0 || total_size < 0 || header_size > total_size {
		return error('invalid HMSG sizes: ${line}')
	}
	wire_data := nc.read_exact(total_size)!
	nc.read_msg_terminator()!
	header_text := wire_data[..header_size].bytestr()
	body := wire_data[header_size..].clone()
	parsed := parse_header_block(header_text)
	return Msg{
		subject:     subject
		sid:         sid
		reply:       reply
		data:        body
		headers:     parsed.headers
		status:      parsed.status
		description: parsed.description
	}
}

fn (mut nc Client) read_msg_terminator() ! {
	terminator := nc.read_exact(2)!
	if terminator.bytestr() != crlf {
		return error('invalid message terminator')
	}
	return
}

fn parse_header_block(header_text string) ParsedHeaderBlock {
	mut headers := map[string]string{}
	mut status := 0
	mut description := ''
	for i, line in header_text.split(crlf) {
		clean := line.trim_space()
		if clean == '' {
			continue
		}
		if i == 0 && clean.starts_with('NATS/1.0') {
			parts := clean.split(' ')
			if parts.len >= 2 {
				status = parts[1].int()
			}
			if parts.len > 2 {
				description = parts[2..].join(' ')
			}
			continue
		}
		if clean.contains(':') {
			key := clean.all_before(':').trim_space()
			value := clean.all_after(':').trim_space()
			headers[key] = value
		}
	}
	return ParsedHeaderBlock{
		headers:     headers
		status:      status
		description: description
	}
}
