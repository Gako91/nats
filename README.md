# nats

`nats` is an open-source NATS and JetStream client for the V programming language.

The project focuses on simplicity, performance and idiomatic V APIs, while progressively building a production-ready messaging client for the V ecosystem.

## Status

Early development. The current implementation provides a small synchronous client:

- TCP connection to a `nats://` server
- NATS protocol handshake (`INFO`, `CONNECT`, `PING`/`PONG`)
- `publish`, `subscribe`, `unsubscribe`, `flush`
- request/reply helper using an auto-generated `_INBOX.*`
- headers / `HMSG` parsing and no-responders handling
- server `max_payload` checks before publish
- basic JetStream context with stream create/update/info/delete and publish acknowledgements

Not implemented yet: TLS, authentication, reconnect logic, async dispatch callbacks, consumer management, pull subscriptions and object/key-value helpers.

## Project layout

This repository follows the usual layout for a V library module:

```text
.
├── v.mod
├── nats.v                    # root module marker / public package entry
├── options.v                 # connection options
├── client.v                  # Client type and state
├── connection.v              # connect, close, flush, socket I/O
├── protocol.v                # NATS protocol constants and CONNECT payload
├── parser.v                  # INFO and MSG parsing
├── publish.v                 # publish helpers
├── subscribe.v               # subscribe, unsubscribe, next_msg
├── request.v                 # request/reply and inbox generation
├── message.v                 # Msg type
├── subscription.v            # Subscription type
├── server_info.v             # Server INFO model
├── errors.v                  # validation and URL helpers
├── jetstream.v               # JetStream context
├── jetstream_api.v           # JetStream API request helper
├── jetstream_stream.v        # stream operations
├── jetstream_publish.v       # JetStream publish acknowledgements
├── jetstream_consumer.v      # reserved for consumer management
├── jetstream_kv.v            # reserved for KV helpers
├── jetstream_objectstore.v   # reserved for object store helpers
├── nats_test.v
├── protocol_test.v
├── docker-compose.yml         # local NATS server for integration tests
├── tests/                     # optional integration tests
├── benchmarks/                # standalone benchmark programs
└── examples/
    ├── basic.v
    ├── echo_service.v
    ├── request_reply.v
    └── jetstream.v
```

There is intentionally no `src/` directory. In V, simple reusable modules commonly keep their `.v` files next to `v.mod`. Each file above uses `module nats`, so consumers still use a single import: `import nats`.

## Example

See `examples/basic.v`:

```v
import nats

fn main() {
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }

	sub := nc.subscribe('demo.hello')!
	nc.flush()!
	nc.publish_string('demo.hello', 'hello from V')!

	msg := nc.next_msg()!
	assert msg.sid == sub.sid
	println('${msg.subject}: ${msg.text()}')
}
```

## Request/reply

See `examples/request_reply.v`. It requires a responder; start `examples/echo_service.v` in another terminal first:

```sh
v run examples/echo_service.v
v run examples/request_reply.v
```

```v
reply := nc.request_string('service.echo', 'ping', 2 * time.second) or {
	eprintln('request failed: ${err}')
	return
}
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

Run optional integration tests against a local NATS server:

```sh
docker compose -f docker-compose.yml up -d
NATS_INTEGRATION=1 v test tests
```

Run the basic benchmark:

```sh
v run benchmarks/publish.v
```
