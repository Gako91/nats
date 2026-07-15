# Integration tests

Integration tests require a running NATS server. They are disabled by default so `v test .` remains fast and does not depend on Docker or network services.

Start NATS with JetStream enabled:

```sh
docker compose -f docker-compose.yml up -d
```

Run integration tests:

```sh
NATS_INTEGRATION=1 v test tests
```

Use a custom server URL if needed:

```sh
NATS_INTEGRATION=1 NATS_URL=nats://127.0.0.1:4222 v test tests
```

Stop the server:

```sh
docker compose -f docker-compose.yml down
```

## Available Tests

### Core NATS
- `test_integration_connect_flush` - Connection and flush
- `test_integration_no_responders` - No responders error
- `test_integration_request_timeout_without_no_responders` - Request timeout
- `test_integration_publish_subscribe` - Pub/sub messaging
- `test_integration_reconnect` - Connection recovery

### JetStream
- `test_integration_jetstream_async_publish` - Async publish with callback
- `test_integration_jetstream_async_publish_multiple` - Multiple concurrent async publishes
- `test_integration_jetstream_pull_consumer` - Pull consumer batch retrieval
- `test_integration_jetstream_ordered_consumer` - Ordered consumer with delivery guarantee
