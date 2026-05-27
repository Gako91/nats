# Roadmap

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

- [ ] robust parser
- [ ] max_payload checks
- [ ] typed/consistent errors
- [ ] headers / HMSG
- [ ] no responders
- [ ] request timeout behavior
- [ ] benchmarks

## Phase 3 — connection quality

- [ ] auth token
- [ ] username/password
- [ ] TLS
- [ ] reconnect
- [ ] multiple server URLs
- [ ] async callbacks

## Phase 4 — JetStream core

- [x] stream management — partial: create/update/info/delete are implemented
- [x] sync publish — partial: publish with PubAck is implemented
- [ ] async publish
- [ ] consumer management
- [ ] durable consumers
- [ ] ack/nak
- [ ] pull consumers
- [ ] ordered consumers

## Phase 5 — JetStream advanced

- [ ] KV store
- [ ] Object Store

## Phase 6 — transports / cluster

- [ ] WebSocket transport
- [ ] clustering awareness
- [ ] server discovery
