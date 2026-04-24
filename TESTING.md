# Testing

The default `go test ./...` suite uses `file://` replicas only and needs no
external services.

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

## Manual smoke test against an existing S3

If you want to validate against a real bucket (AWS, Scaleway, R2, etc.),
the integration test gives you the pattern — copy `integration_test.go`
into a scratch `main.go`, hard-code your endpoint and bucket, and run it.
