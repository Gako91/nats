# Benchmarks

Benchmarks are standalone programs so they do not run as part of `v test .`.

Start a local NATS server first:

```sh
docker compose -f docker-compose.yml up -d
```

Run the basic publish benchmark:

```sh
v run benchmarks/publish.v
```
