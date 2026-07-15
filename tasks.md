# Roadmap

## Latest Updates (2026-07-15)

### Recently Implemented ✅

1. **Async Publish** — `publish_async()` with non-blocking ACK handling
   - Configurable timeout via `Options.publish_async_timeout` (default: 5s)
   - Automatic callback invocation when ACKs arrive
   - Cleanup of expired pending publishes
   - Full integration tests and examples

2. **Pull Consumers** — Batch message retrieval pattern
   - `pull_consumer_create()` for ephemeral/durable pull consumers
   - `fetch()` method with configurable batch sizes and timeouts
   - Flexible pull request customization (max_bytes, idle_heartbeat)

3. **Ordered Consumers** — Strict ordering guarantee
   - `ordered_consumer_create()` for guaranteed message ordering
   - No duplicates, no gaps, strict sequence delivery
   - Flow control with idle heartbeats
   - `subscribe_to_ordered_consumer()` integration

### Examples & Tests

- `examples/async_publish.v` — Async publish with callbacks
- `examples/pull_ordered_consumers.v` — Pull and ordered consumer usage
- Integration tests for all new features (async, pull, ordered)

---

## Phase 1 — MVP NATS core

- [x] connect
- [x] publish
- [x] subscribe
- [x] unsubscribe
- [x] queue groups
- [x] request/reply
- [x] ping/pong
- [x] flush
- [x] basic examples
- [x] integration test setup

## Phase 2 — protocol hardening

- [x] robust parser
- [x] max_payload checks
- [x] typed/consistent errors
- [x] headers / HMSG
- [x] no responders
- [x] request timeout behavior
- [x] benchmarks

## Phase 3 — connection quality

- [x] auth token
- [x] username/password
- [x] TLS
- [x] reconnect
- [x] multiple server URLs
- [x] async callbacks

## Phase 4 — JetStream core

- [x] stream management — partial: create/update/info/delete are implemented
- [x] sync publish — partial: publish with PubAck is implemented
- [x] async publish — ✅ NEW: publish_async() with configurable timeouts + callbacks
- [x] consumer management
- [x] durable consumers
- [x] ack/nak
- [x] pull consumers — ✅ NEW: pull_consumer_create() + fetch() batch retrieval
- [x] ordered consumers — ✅ NEW: ordered_consumer_create() + strict ordering guarantee

## Phase 5 — JetStream advanced

- [ ] KV store
- [ ] Object Store

## Phase 6 — transports / cluster

- [ ] WebSocket transport
- [ ] clustering awareness
- [ ] server discovery
