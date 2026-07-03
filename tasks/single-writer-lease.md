# Leased single-writer + read-only followers

Self-contained task. No prior context needed beyond this file and the s3lite
source (`s3lite.go`, `replica.go`). Companion: `tasks/durable-close.md` (land that
first — the lease lifecycle relies on a durable flush at demotion/release).

## Why

s3lite wraps litestream, which **requires exactly one writer** per replica — two
processes replicating to the same S3 target corrupt the replica (litestream
assumes single-writer and only *detects* conflicts, it doesn't prevent them).

Today s3lite has no way to enforce that: `Open` unconditionally starts replication
whenever `BackupTo` is set, so **every instance is a writer**. Consumers running on
serverless platforms can't guarantee single-writer from the outside:

- Rolling deploys briefly run old + new instances together (both writers).
- A cold-started instance can't tell "I'm the only one, safe to write" from "I'm a
  new deploy while the old one is still writing" — identical from inside.
- Scaling above one instance for read throughput is impossible without risking two
  writers.

The motivating consumer (gitmote, a self-hosted git remote) works around this by
pinning its platform to a single instance and doing a "stop-first" deploy drain —
which closes the two-writer window *in practice* but not *by proof*, and forbids
read replicas. The fix belongs here: **s3lite should enforce single-writer by
protocol (a lease) and support read-only followers**, so N instances run safely as
one writer + many readers. That yields, for free: safe rolling deploys (handoff by
lease), writer **failover** (a follower promotes when the leader's lease lapses),
and read scaling.

## Good news — the hard parts already exist

- **Conditional writes are now widely available**, including on Scaleway Object
  Storage (`If-Match` / `If-None-Match`) — plus R2 and AWS S3. So the lease can
  live on the *same* bucket as the WAL; no second provider.
- **litestream ships the lease primitive** (v0.5.8+, PR #1073). Reuse it — do
  **not** hand-roll conditional-write CAS or fencing:
  - `litestream.Leaser` interface: `AcquireLease(ctx) (*Lease, error)`,
    `RenewLease(ctx, *Lease) (*Lease, error)`, `ReleaseLease(ctx, *Lease) error`,
    `Type() string`.
  - `litestream.Lease{ Generation int64; ExpiresAt time.Time; Owner string; ETag string }`
    with `IsExpired()` and `TTL()`. `Generation` is a monotonic **fencing token**
    (bumped on each takeover).
  - `litestream.ErrLeaseNotHeld`, `litestream.LeaseExistsError{Owner, ExpiresAt}`.
  - `s3.NewLeaser()` → `*s3.Leaser` with fields `Bucket`, `Path`, `TTL`
    (`DefaultLeaseTTL = 30s`), `Owner` (defaults to a generated id), `SetClient(S3API)`,
    `SetLogger`. Lock object is `<Path>/lock.json` (`DefaultLeasePath`). Acquire
    uses `If-None-Match: *`; renew/release use `If-Match: <etag>`.

## The catch — litestream does NOT auto-gate

Verified against v0.5.11 (pinned) **and** v0.5.13 (latest): neither `Store` nor
`DB` has a `Leaser` field, and the replication monitor loop calls `Sync` with **no
lease precondition**. The leaser is a standalone building block. So **s3lite must
orchestrate the leader lifecycle** around it — that orchestration is the work here.
(Spike: check how litestream's own CLI `replicate`/"write mode" wires the leaser in
`cmd/litestream` + the `write`/lease config docs, and mirror that pattern.)

## Current code (`s3lite.go`, the part that changes)

`Open` (abridged): when `BackupTo != ""` it builds a replica client, a
`litestream.DB`, a `litestream.Store`, and calls `store.Open(ctx)` — starting
replication immediately and unconditionally. That's the single hook point: this
must become "start replication only while holding the lease."

## Design

### Config / API

Add a role/mode plus lease settings, reusing the existing `S3Config` + `BackupTo`
bucket for the lock:

```go
type Config struct {
    // ...existing...
    Role     Role          // Writer | Follower | Auto (default: current behaviour = always-writer, lease off)
    LeaseTTL time.Duration // default 30s (litestream DefaultLeaseTTL)
    Owner    string        // lease owner id for diagnostics; default generated
}
```

Behaviour:
- **Role off / unset:** today's behaviour exactly (always writer, no lease) — keep
  backward compatible.
- **Writer:** acquire the lease; if held by someone else, either block-until-free
  or return `LeaseExistsError` (caller's choice — expose both).
- **Follower:** open read-only, do **not** replicate; serve reads from the local
  file, kept fresh (see "follower freshness").
- **Auto:** try to acquire; become writer on success, follower on
  `LeaseExistsError`. This is the mode a serverless consumer wants.

Expose lease state so the consumer can react (e.g. flip "accept writes" on/off):
```go
func (db *DB) IsLeader() bool
func (db *DB) OnPromote(func())   // became writer
func (db *DB) OnDemote(func(err error)) // lost lease / stepped down
// or a single events channel — pick one, document it
```

### Leader lifecycle

1. **Acquire** via `s3.Leaser.AcquireLease`. On success, start the litestream
   `Store` (replication). On `LeaseExistsError`, go Follower.
2. **Renew loop:** renew at ~`TTL/3`. On **any** renew failure — especially
   `ErrLeaseNotHeld` — **stop replication immediately** (`store.Close`) and demote
   to Follower/error. This is the fencing discipline: renew interval ≪ TTL means a
   writer that can't renew halts *before* the TTL could let another instance
   acquire, so two writers never overlap. Do not keep writing on a stale lease.
3. **Release** on `Close`/demotion: flush (see `durable-close.md`), stop the store,
   then `ReleaseLease` (deletes `lock.json` via `If-Match`) so a successor can take
   over instantly rather than waiting out the TTL.

### Follower lifecycle

- Open the DB **read-only**; no `Store`/replication.
- **Freshness options (spike & choose):**
  - Periodic `Replica.Restore` (re-restore latest) on an interval — simple, works
    with the pure-Go `modernc` driver s3lite already uses.
  - litestream `restore -f` / continuous-restore follow mode (v0.5.9) — closer to
    real-time.
  - litestream **VFS read-replica** extension + `PRAGMA litestream_write_enabled`
    (v0.5.9): serves reads directly off remote storage. **Likely incompatible with
    the pure-Go modernc driver** (VFS is a loadable native extension) — verify
    before assuming; if it needs cgo/mattn, it's out unless s3lite adds a build
    tag / alternate driver.
- **Promotion:** a follower may attempt `AcquireLease` (e.g. when it observes the
  leader's lease expired). On success: restore-to-latest, then start replicating as
  the new writer. This is automatic failover.

### Fencing

Rely on litestream's `Lease.Generation` + the renew-fail-stops discipline above.
Surface `Generation` (via `IsLeader`/events or a getter) so a consumer can fence
external side effects if it needs to. Document the invariant: **a writer must cease
all writes the instant a renew fails.**

## Consumer shape (informational — gitmote)

Boots in `Auto`: becomes a read-only follower serving clones immediately; promotes
to writer when it acquires the lease (the old writer released on drain, or its
lease lapsed after a crash); rejects/redirects pushes while not leader. This
dissolves the deploy-overlap problem (single writer by construction), removes the
need for the stop-first drain hack, and adds crash failover. See gitmote
`docs/evolution/reader-writer-split.md`.

## Verify

Integration tests need a **conditional-write-capable** S3 (recent MinIO supports
`If-None-Match`/`If-Match` — verify the pinned MinIO; else drive the litestream
`s3.Leaser` against a fake `S3API`, as litestream's own `s3/leaser_test.go` does).

- **Mutual exclusion:** two s3lite instances, same replica, both `Auto` → exactly
  one becomes leader; the other is a read-only follower and never replicates.
- **Handoff:** leader `Close`/`ReleaseLease` → follower acquires and promotes;
  assert no interval where both replicate (check generations / lock owner).
- **Lease loss:** block/΅fail the leader's renew → it stops replicating within
  ~`TTL/3`; a waiting follower acquires after expiry.
- **Fencing / zombie writer:** pause a leader past its TTL so a follower promotes;
  when the paused leader resumes, its renew and any write attempt fail with
  `ErrLeaseNotHeld` — it cannot corrupt the replica.
- **Backward compat:** `Role` unset behaves exactly as today (always-writer, no
  lock object created).
- **Follower reads:** a follower serves committed data and converges to new writes
  within the chosen freshness interval.

## Spikes / open questions

1. Mirror litestream's own leaser wiring: read `cmd/litestream` `replicate`/"write
   mode" + config docs (PR #1073) for the canonical acquire/renew/gate pattern.
2. Follower freshness: VFS (modernc compatibility?) vs `restore -f` vs periodic
   `Restore`. Decide and document.
3. Does the pinned MinIO (integration tests) support conditional writes? If not,
   pin a newer one or use a fake `S3API`.
4. Lease TTL/renew tuning and clock-skew behaviour; what `AcquireLease` should do
   under sustained contention (backoff).
5. Endpoint/region for the leaser client — reuse `S3Config` (it already carries
   Endpoint/Region/keys and sets path-style for custom endpoints like Scaleway).

## References

- litestream **v0.5.13** (latest as of 2026-06-30; s3lite pins v0.5.11 — bump).
- `litestream.Leaser`, `litestream.Lease`, `ErrLeaseNotHeld`, `LeaseExistsError`
  (`leaser.go`); `s3.Leaser` / `s3.NewLeaser` (`s3/leaser.go`,
  `DefaultLeaseTTL=30s`, `DefaultLeasePath="lock.json"`).
- PR #1073 "add distributed leasing with If-Match conditional writes" (v0.5.8/0.5.12).
- VFS read replicas + `PRAGMA litestream_write_enabled` (#1009, v0.5.9);
  `restore -f` follow mode (#1102, v0.5.9).
- Scaleway Object Storage conditional-writes support (`If-Match`/`If-None-Match`).
