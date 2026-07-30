# Testing

The default `go test ./...` suite uses `file://` replicas only and needs no
external services.

Both suites run in CI (test + integration) on every push to `master` and every
pull request — see [.github/workflows/ci.yml](.github/workflows/ci.yml). They also
gate the bot PR that moves the litestream pin
([.github/workflows/litestream-pin.yml](.github/workflows/litestream-pin.yml)); see
[LITESTREAM-FORK.md](LITESTREAM-FORK.md).

There is no third suite and no extra tag: client-side encryption
([INVARIANTS.md](INVARIANTS.md) #11) is covered in both. The default suite carries the
object format, the decorator against a real `file://` backend, the resume path, and a
whole chaos soak run with a key (`TestChaosSingleWriterDurabilityEncrypted`); the
integration suite adds what only a real object store can show — that the bucket holds
nothing but ciphertext, that the object metadata timestamp survives, and that two
encrypted instances hand the lease back and forth.

## Benchmarks

`BenchmarkSeal` / `BenchmarkOpen` cover the encryption streaming path. They exist to
guard *allocation* behaviour, not to chase throughput: both readers reuse a single
frame buffer, so `B/op` must stay a small constant rather than scaling with the
object. A regression shows up as `B/op` tracking the payload size.

```bash
go test -run '^$' -bench 'BenchmarkSeal|BenchmarkOpen' -benchmem
```

## Integration tests (MinIO via testcontainers)

Integration tests spin up a real MinIO container and exercise the `s3://`
replica path end-to-end. Gated behind a build tag so they don't run in the
default suite.

Requires a working Docker daemon (or Podman with the Docker socket exposed).

```bash
go test -tags=integration ./...
```

### Podman users

Testcontainers' Ryuk cleanup container fails on Podman's default networking.
Disable it:

```bash
TESTCONTAINERS_RYUK_DISABLED=true go test -tags=integration ./...
```

Containers are still terminated via `t.Cleanup`, so there's no leak.

### Colima users

Testcontainers looks for the Docker socket at `/var/run/docker.sock` and does not
read the Docker CLI's context, so point it at Colima's socket explicitly:

```bash
DOCKER_HOST=unix://$HOME/.colima/default/docker.sock \
TESTCONTAINERS_RYUK_DISABLED=true go test -tags=integration ./...
```

## Manual smoke test against an existing S3

If you want to validate against a real bucket (AWS, Scaleway, R2, etc.),
the integration test gives you the pattern — copy `integration_test.go`
into a scratch `main.go`, hard-code your endpoint and bucket, and run it.
