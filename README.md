# nats
<p align="center">
  <img src="logo.png" alt="nats.v logo" width="200">
</p>
`nats` is an open-source NATS and JetStream client for the V programming language.

The project focuses on simplicity, performance and idiomatic V APIs, while progressively building a production-ready messaging client for the V ecosystem.

## Status

Early development. The current implementation provides a synchronous client:

- TCP connection to a `nats://` server with high-performance custom input buffering (reducing syscalls)
- NATS protocol handshake (`INFO`, `CONNECT`, `PING`/`PONG`)
- `publish`, `subscribe`, `unsubscribe`, `flush`, `request`/`reply`
- headers / `HMSG` parsing and no-responders handling
- server `max_payload` checks before publish
- queue groups for load-balanced message distribution
- authentication (token and user/password)
- TLS support
- reconnect logic with configurable retries
- connection callbacks (on_disconnect, on_reconnect, on_error)
- basic JetStream context with stream create/update/info/delete and publish acknowledgements
- async publish with non-blocking callbacks
- consumer management (durable, pull, ordered)
- pull subscriptions with batch retrieval
- ordered consumers with delivery guarantee

Not implemented yet: object/key-value helpers, Atlas Stream Processing.

## Getting Started

### Prerequisites

- V installed ([download here](https://vlang.io))
- NATS server running locally (default: `nats://localhost:4222`)
  - Docker: `docker run -p 4222:4222 nats`
  - Or use the provided `docker-compose.yml`: `docker-compose up`

### Installation

Add this to your project's `v.mod`:

```
Module {
	name: 'my_app'
	dependencies: ['https://github.com/Gako91/nats.git']
}
```

Or use locally:

```v
import ../path/to/nats as nats
```

### Your First Program

```v
import nats

fn main() {
	// Connect to NATS server (will panic if connection fails)
	mut nc := nats.connect('nats://127.0.0.1:4222')!
	defer { nc.close() }  // Always close when done

	// Subscribe to a subject
	nc.subscribe('hello')!
	nc.flush()!

	// Publish a message
	nc.publish_string('hello', 'world')!

	// Receive the message
	msg := nc.next_msg()!
	println('Received: ${msg.text()}')
}
```

Run it:

```sh
v run your_program.v
```

## Concepts & Terminology

### Subject

A subject is the address where messages are sent. Think of it like a channel or topic.

- `hello` - a simple subject
- `orders.created` - hierarchical subject with dots
- `users.>` - wildcard: matches all subjects starting with `users.`
- `user.*` - wildcard: matches one level (e.g., `user.123`, `user.456`)

### Publish & Subscribe

- **Publish**: Send a message to a subject
- **Subscribe**: Listen for messages on a subject
- Multiple subscribers can listen to the same subject; they all receive the same message

### Request-Reply

A pattern where:

1. A client sends a request message with an auto-generated reply inbox
2. A service listens, receives the request, and publishes a response to the reply inbox
3. The client receives the response

- Useful for synchronous RPC-style communication

### Queue Groups

When multiple subscribers join the same queue group, each message is delivered to only ONE subscriber (load balanced).

- `subscribe('subject')` - all subscribers get all messages
- `queue_subscribe('subject', 'group_name')` - each message goes to one random subscriber in the group
- Great for worker pools and distributing load

### JetStream

NATS's durability layer:

- **Stream**: A persistent log of messages on a set of subjects
- **Consumer**: A stateful subscription to a stream (can replay messages, acknowledge delivery)
- Messages published to a stream are persisted and can be replayed or consumed in order
- Used for event sourcing, audit trails, and guaranteed delivery

## Common Patterns

### Basic Pub/Sub

See [examples/basic.v](examples/basic.v)

- Multiple subscribers all receive the same message
- Fire-and-forget: no guarantee message was processed

### Request-Reply (RPC Style)

See [examples/request_reply.v](examples/request_reply.v) and [examples/echo_service.v](examples/echo_service.v)

- Synchronous request from client to service
- Service responds with a result
- Built-in timeout handling

### Load Distribution with Queue Groups

See [examples/queue_groups.v](examples/queue_groups.v)

- Multiple workers listen on the same queue group
- Each job goes to exactly one worker
- Perfect for background jobs or worker pools

### Persistent Events with JetStream

See [examples/jetstream.v](examples/jetstream.v)

- Messages are stored persistently
- Can be replayed or consumed reliably
- Useful for event sourcing or audit logging

## Connection Options

The `Options` struct lets you customize connection behavior:

```v
mut opts := nats.Options{
	url: 'nats://user:pass@localhost:4222'
	name: 'my_client'              // Client name (for identification)
	auth_token: 'my_bearer_token'  // OR use token auth
	user: 'username'               // OR use username/password
	password: 'password'
	connect_timeout: 5 * time.second  // How long to wait for connection
	allow_reconnect: true          // Reconnect on disconnect
	max_reconnects: 60             // Max reconnect attempts
	reconnect_time_wait: 2 * time.second  // Wait between reconnects
	on_disconnect: fn (mut nc nats.Client) {
		println('Disconnected from NATS')
	}
	on_reconnect: fn (mut nc nats.Client) {
		println('Reconnected to NATS')
	}
	on_error: fn (mut nc nats.Client, err string) {
		println('Error: ${err}')
	}
}

mut nc := nats.connect_with_options(opts)!
```

## Troubleshooting

### "Connection refused"

**Problem**: Can't connect to NATS server
**Solution**: Make sure NATS is running:

```sh
# Using Docker
docker run -p 4222:4222 nats

# Or if installed locally
nats-server
```

### "No responders" error

**Problem**: `request()` returns an error saying no responders available
**Solution**: Make sure the service is running and listening:

```v
// Service must be running BEFORE client makes request
nc.subscribe('my.service')!
nc.flush()!
// Now client can safely call request()
```

### Request timeout

**Problem**: `request()` takes too long or times out
**Solution**: Increase the timeout value:

```v
// Default 2 second timeout might be too short
reply := nc.request_string('slow.service', 'data', 10 * time.second)!
```

### Message not received

**Problem**: Published messages aren't being received by subscriber
**Solution**: Remember to call `flush()` after subscribing:

```v
sub := nc.subscribe('hello')!
nc.flush()!  // IMPORTANT: Flush to register subscription
nc.publish_string('hello', 'world')!
```

### Error handling with `!`

V uses the `!` operator for error propagation:

```v
// This will panic if connection fails:
mut nc := nats.connect('nats://localhost:4222')!

// Or handle errors gracefully:
mut nc := nats.connect('nats://localhost:4222') or {
	eprintln('Failed to connect: ${err}')
	return
}
```

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

### Async Publish

See `examples/async_publish.v`:

```v
mut js := nc.jetstream()

async_callback := fn [mut received] (mut c nats.Client, subject string, result nats.PublishResult) {
	if result.error_msg != '' {
		eprintln('publish error: ${result.error_msg}')
	} else {
		println('ack: seq=${result.ack.seq}, stream=${result.ack.stream}')
		received++
	}
}

js.publish_string_async('events.user.created', 'user data', async_callback)!
```

### Pull Consumers

See `examples/pull_ordered_consumers.v`:

```v
consumer_info := js.pull_consumer_create('EVENTS', nats.PullConsumerOptions{
	durable_name: 'my_consumer'
	filter_subject: 'events.user.>'
})!

messages, errors := js.fetch('EVENTS', consumer_info.name, nats.PullFetchOptions{
	batch: 10
	idle_timeout_ms: 5000
})!
```

### Ordered Consumers

See `examples/pull_ordered_consumers.v`:

```v
consumer_info := js.ordered_consumer_create('EVENTS', nats.OrderedConsumerOptions{
	deliver_policy: .all
	filter_subject: 'events.>'
})!

sub := js.subscribe_to_ordered_consumer(consumer_info.config.deliver_subject)!
msg := nc.next_msg()!
msg.ack(mut nc)!
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
