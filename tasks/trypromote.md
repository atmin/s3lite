# TryPromote — on-demand lease acquisition

## Goal

Add a public `TryPromote(ctx) (bool, error)` that lets a follower attempt to
become the writer **right now** instead of waiting for the next background lease
poll. Strictly additive and opt-in: an instance that never calls it behaves
bit-identically to today (the background `leaseLoop` is unchanged). A consumer
(gitmote's receive-pack gate) calls it on the write path so that, during a
graceful rolling deploy, the push that would otherwise get a `503` instead
blocks for the restore and then serves — no handoff gap.

## Why

Today a follower promotes only when `leaseLoop` polls (`TTL/3`, ~10s default).
After the previous writer releases its lease on graceful `Close`, there is a
window where the successor is still a read-only follower and refuses writes.
Exposing the existing promote path on demand collapses that window to "the time
to restore" for the request that triggers it. On a **hard kill** the old lease
has not expired, so acquisition legitimately fails and the caller still falls
back to `503` until TTL — unchanged and correct.

## Background — the primitive already exists

All the machinery is present in [lease.go](../lease.go); this task only exposes
and hardens it for concurrent, caller-driven invocation.

- `tryPromote(ctx)` (lease.go) already does acquire → on success `promote`. It is
  currently called from **exactly one** place, the follower branch of
  `leaseLoop` (lease.go), so it never races itself.
- `promote(ctx, lease)` (lease.go) checks `isLeader` under `db.mu`, restores the
  latest replica with connection creation gated (`connector.swapFiles`), then
  `becomeLeaderLocked` under `db.mu`. The restore runs **outside** `db.mu`.
- `IsLeader()` (s3lite.go) is a cheap pure getter guarded by `db.mu`. **Keep it
  that way** — do not fold acquisition into it (many callers hit it on read
  paths; a blocking/acquiring `IsLeader` would kick off restores on reads and
  break the contract for existing users). The acquiring behavior gets its own
  named verb.
- Single-writer safety is the `leaser.AcquireLease` `If-None-Match` CAS. This
  task makes promotion happen *sooner*, never *more often* — two instances still
  cannot both win. No new safety argument is needed.

## The one real problem: concurrent invocation

Once `TryPromote` is public, it can be called from N HTTP handlers at once **and**
concurrently with the background `leaseLoop`. Today's `promote` is not safe under
that: two callers can both pass the initial `!isLeader` check (the check and the
restore are not mutually excluded), both restore, and both reach
`becomeLeaderLocked`. A single-flight guard is the crux of this task.

## Change

Files: `lease.go` (logic), `s3lite.go` (struct field + public method + `Close`
interaction), `README.md` (doc the method), plus tests.

1. **Add a promote guard** to `DB` (s3lite.go) — a dedicated mutex, separate from
   `db.mu`, held across the whole acquire+restore so at most one promotion runs
   at a time:
   ```go
   promoteMu sync.Mutex // serialises promotion attempts (background loop + TryPromote)
   ```
   Do not reuse `db.mu`: the restore is slow and must not block `IsLeader` /
   `Generation` / `tryRenew` for its duration.

2. **Extract `tryPromoteOnce(ctx) (bool, error)`** in lease.go as the single
   guarded promotion path, and route both callers through it:
   ```go
   func (db *DB) tryPromoteOnce(ctx context.Context) (bool, error) {
       if db.IsLeader() {            // fast path, no S3 I/O, no lock contention
           return true, nil
       }
       db.promoteMu.Lock()
       defer db.promoteMu.Unlock()
       if db.IsLeader() {            // recheck: the loop (or another caller) may have promoted us
           return true, nil
       }
       lease, err := db.leaser.AcquireLease(ctx)
       if err != nil {
           var held *litestream.LeaseExistsError
           if errors.As(err, &held) {
               // Still held by a live writer elsewhere (or by us, if the loop just
               // promoted — covered by the recheck above). Normal no-op.
               return db.IsLeader(), nil
           }
           if ctx.Err() != nil {
               return false, ctx.Err()
           }
           return false, err
       }
       if err := db.promote(ctx, lease); err != nil {
           db.releaseQuietly(ctx, lease)
           return false, err
       }
       return true, nil
   }
   ```
   - Replace the body of the background `tryPromote(ctx)` so its follower-branch
     call becomes `if _, err := db.tryPromoteOnce(ctx); err != nil { db.logger.Warn(...) }`
     — preserving today's "log and move on" loop behavior exactly.
   - `promote` keeps its own top-of-function `isLeader` recheck under `db.mu` as
     defense in depth; with `promoteMu` it will not fire, but leave it.

3. **Public method** in s3lite.go:
   ```go
   // TryPromote attempts to acquire the writer lease immediately rather than
   // waiting for the next background poll. It returns true if this instance is
   // (or, after restoring the latest replica, has just become) the writer, and
   // false if the lease is still held by a live writer elsewhere. On a follower
   // it blocks for the restore while promoting; bound ctx to cap that wait.
   //
   // It never promotes two writers — acquisition is the same lease CAS the
   // background loop uses. Unleased sole writers (no s3:// BackupTo) are always
   // the writer, so this returns true without I/O. Safe to call concurrently.
   func (db *DB) TryPromote(ctx context.Context) (bool, error) {
       return db.tryPromoteOnce(ctx)
   }
   ```

4. **Close / shutdown interaction** (s3lite.go): a `TryPromote` racing `Close`
   must not resurrect a torn-down instance. Simplest correct approach — gate on
   the loop-cancel/closed state that `Close` already sets and have `tryPromoteOnce`
   return `(false, ErrClosed-or-similar)` if the instance is closing. Verify what
   `Close` sets today (`loopCancel`, store teardown) and pick the existing signal;
   add a small `closed` flag if none is usable. Document that callers stop calling
   `TryPromote` once they have begun `Close`.

## Verify

Tests in `lease_internal_test.go` (fake leaser is already injectable via
`newLeaserFunc`), all under `go test -race`:

- **Concurrent single-flight**: start a follower whose fake leaser reports the
  lease free; fire N goroutines calling `TryPromote` at once. Assert exactly one
  restore/promote happens (count promote calls or `OnPromote` firings == 1) and
  all N return `(true, nil)`.
- **Still-held → false**: fake leaser returns `LeaseExistsError`; `TryPromote`
  returns `(false, nil)` and the instance stays a read-only follower
  (`IsLeader()==false`, handle is `query_only`).
- **Race with the background loop**: enable the loop with a short TTL and hammer
  `TryPromote` concurrently; assert no double-restore, no two `becomeLeaderLocked`,
  `-race` clean, and `IsLeader()` ends true.
- **Already leader**: `TryPromote` on the writer returns `(true, nil)` and does no
  S3 I/O (fake leaser records zero `AcquireLease` calls).
- **Sole writer (no s3:// BackupTo)**: `TryPromote` returns `(true, nil)`
  immediately.
- **Promoted handle is fresh + writable**: after a `TryPromote` that promotes,
  a write succeeds and reads see the latest restored state (reuse the existing
  promote/restore test helpers).
- **ctx cancellation**: a cancelled ctx makes `TryPromote` return promptly with a
  non-nil error and leaves the instance a follower.

Then `gofmt`/`goimports`, `golangci-lint run`, `go test ./... -race`, and update
`README.md`'s leasing section to document `TryPromote` alongside `IsLeader` /
`OnPromote`. Cut a `v0.4.0` tag — additive API, no breaking change.

## Not in scope

Wiring gitmote's receive-pack gate to call `TryPromote` is a **separate change in
gitmote**, landed after this ships (gitmote cannot adopt until the method exists).
Do not touch gitmote here.
