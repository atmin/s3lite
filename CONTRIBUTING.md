# Contributing

Entry point for anyone — human or agent — working on s3lite. Read it first.

s3lite is a **library**: `github.com/atmin/s3lite` is the import path and the
public API is the root package, so source files do not move into
subdirectories for tidiness. [README.md](README.md) is the founding document —
*what* this is and *why*. [INVARIANTS.md](INVARIANTS.md) is the correctness
contract: numbered claims, each pinned by a named test. If code and docs
disagree, fix one of them in the same change — never leave the divergence
silently.

## Language

Go, CGO-free — the SQLite driver is pure Go, and that is load-bearing:
consumers ship s3lite in scratch containers and cross-compile without a C
toolchain. No dependency may reintroduce cgo.

## Layout

```
README.md      Founding document — thesis, API, guarantees, limitations
INVARIANTS.md  The correctness contract — numbered claims, each pinned by a test
*.go           The library, one package: the public API and Open (s3lite.go),
               the CAS lease (lease.go), replication (replica.go), client-side
               encryption (encrypt.go + encryptclient.go, the ReplicaClient
               decorator), the generation-fenced database/sql connector
               (stableconn.go), logging (logging.go)
docs/          testing.md — the suites, their gates, local Docker quirks.
               litestream-fork.md — the patch ledger and its weekly sync.
               notes/ — the evidence base: litestream/ltx source reading and
               upstream findings the decisions here cite
tasks/         The frontier — active/upcoming work; deleted once landed.
               tasks/ideas/ holds candidates: worth doing, not yet scheduled
```

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/). Scope is optional
and reserved for what isn't the library itself — `fork`, `tasks`, `deps`.

```
type: short imperative description

Optional body explaining why, not what.
```

**Types:** `feat` `fix` `refactor` `perf` `test` `ci` `chore` `docs` `revert` —
breaking change appends `!`. Subject lowercase, imperative, no trailing period;
em dash for a subtitle when a bare type isn't enough context.

## Testing — correctness is the product

A database wrapper that loses a commit is worse than no wrapper:

- Every correctness change lands with the **numbered INVARIANTS.md claim** it
  upholds and the test that pins it. A claim with no test is not a claim.
- Cover the **golden path *and* the failure path**. Fencing, demotion
  mid-transaction, hard kill, and a wrong or absent key already have harnesses
  (`crash_test.go`, `chaos_test.go`, `encrypt_test.go`) — extend them rather
  than assert the happy case twice.
- The **chaos soak** is the end-to-end backstop and runs in the default suite,
  with a key and without.
- The **bucket format is the only compatibility surface.** Changing the
  encryption frame layout orphans every existing replica; corruption must
  always surface as a typed error, never as bytes.

Every change passes, in order — **format → vet → test**:

```
gofmt -l .                               # format
go vet ./...                             # vet
go test -race ./...                      # default suite — file:// replicas, no services
go test -tags=integration -race ./...    # + real MinIO via testcontainers (needs Docker)
```

`make fmt vet test test-integration` wraps all four. Both suites run in CI on
every push and pull request, and gate the bot PR that moves the litestream pin.
Podman/Colima socket quirks and the allocation benchmarks (`make bench`) are in
[docs/testing.md](docs/testing.md).

## Tasks

`tasks/` is one file per intent-to-implement unit of work, **deleted once it
lands** (git history is the record). Each file: `## Why`, `## What is true
today`, `## Sketch`, `## Verify`. Keep `tasks/README.md` a one-line-per-item
list of active work — never a changelog.

`tasks/ideas/` is the rung below the frontier: worth capturing, not committed
to. Lighter shape — `## The idea`, `## Why it's interesting`, `## Why it's
parked`, `## If we ever pick this up` — and the parked section is the point.
Promotion is a move up one level (`git mv tasks/ideas/x.md tasks/`); nothing in
`ideas/` is implemented without it.
