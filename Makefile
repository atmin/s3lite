.PHONY: fmt vet test test-integration bench

fmt:
	gofmt -l -w .

vet:
	go vet ./...

# The default suite: file:// replicas only, no external services.
test:
	go test -race -count=1 ./...

# A real MinIO container via testcontainers. Needs a Docker daemon. Testcontainers
# reads neither the Docker CLI's context nor a rootless socket path, so a Colima
# daemon has to be named twice: DOCKER_HOST for the client, and the socket's path
# *inside the VM* for the Ryuk reaper's bind mount — without the latter Ryuk mounts
# the host-side socket and the container fails to start. Default both when Colima's
# socket is there and nothing was exported; anything already in the environment
# wins. Podman still needs TESTCONTAINERS_RYUK_DISABLED — see docs/testing.md.
COLIMA_DOCKER_SOCKET := $(HOME)/.colima/default/docker.sock
ifeq ($(origin DOCKER_HOST),undefined)
ifneq ($(wildcard $(COLIMA_DOCKER_SOCKET)),)
export DOCKER_HOST := unix://$(COLIMA_DOCKER_SOCKET)
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock
endif
endif

test-integration:
	go test -tags=integration -race -count=1 ./...

# Allocation guards on the encryption streaming path, not throughput: B/op must
# stay a small constant rather than tracking payload size (docs/testing.md).
bench:
	go test -run '^$$' -bench 'BenchmarkSeal|BenchmarkOpen' -benchmem
