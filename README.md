# nats

`nats` is an open-source NATS and JetStream client for the V programming language.

The project focuses on simplicity, performance and idiomatic V APIs, while progressively building a production-ready messaging client for the V ecosystem.

## Status

Early development. The current implementation provides a small synchronous client:

- TCP connection to a `nats://` server
- NATS protocol handshake (`INFO`, `CONNECT`, `PING`/`PONG`)
- `publish`, `subscribe`, `unsubscribe`, `flush`
- request/reply helper using an auto-generated `_INBOX.*`
- basic JetStream context with stream create/update/info/delete and publish acknowledgements

Not implemented yet: TLS, authentication, reconnect logic, async dispatch callbacks, headers, consumer management, pull subscriptions and object/key-value helpers.

## Project layout

This repository follows the usual layout for a V library module:

```text
.
├── v.mod            # module metadata
├── nats.v           # core NATS client API, module nats
├── jetstream.v      # JetStream API, same module nats
├── nats_test.v      # unit tests for the module
└── examples/        # standalone example programs using import nats
```

There is intentionally no `src/` directory. In V, simple reusable modules commonly keep their `.v` files next to `v.mod`. Subdirectories are best reserved for examples, tests, documentation, or true V submodules.

## Example

See `examples/basic.v`:

```v
import nats

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	sub := nc.subscribe('demo.hello')!
	nc.publish_string('demo.hello', 'hello from V')!
	nc.flush()!

	msg := nc.next_msg()!
	assert msg.sid == sub.sid
	println('${msg.subject}: ${msg.text()}')
}
```

## Request/reply

See `examples/request_reply.v`:

```v
reply := nc.request_string('service.echo', 'ping', 2 * time.second)!
println(reply.text())
```

## JetStream

See `examples/jetstream.v`:

```v
mut js := nc.jetstream()

js.add_stream(nats.StreamConfig{
	name: 'EVENTS'
	subjects: ['events.>']
})!

ack := js.publish_string('events.created', '{"id":1}')!
println('stored in ${ack.stream} at sequence ${ack.seq}')
```

## Development

Format/check the module with:

```sh
v fmt -w .
v test .
```
