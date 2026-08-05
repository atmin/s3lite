.PHONY: fmt vet test test-integration bench

fmt:
	gofmt -l -w .

vet:
	go vet ./...

# The default suite: file:// replicas only, no external services.
test:
	go test -race -count=1 ./...

# A real MinIO container via testcontainers. Needs a Docker daemon — Podman and
# Colima need env this passes straight through (DOCKER_HOST,
# TESTCONTAINERS_RYUK_DISABLED); the incantations are in docs/testing.md.
test-integration:
	go test -tags=integration -race -count=1 ./...

# Allocation guards on the encryption streaming path, not throughput: B/op must
# stay a small constant rather than tracking payload size (docs/testing.md).
bench:
	go test -run '^$$' -bench 'BenchmarkSeal|BenchmarkOpen' -benchmem
