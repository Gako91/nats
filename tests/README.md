# Integration tests

Integration tests require a running NATS server. They are disabled by default so `v test .` remains fast and does not depend on Docker or network services.

Start NATS with JetStream enabled:

```sh
docker compose up -d
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
docker compose down
```
