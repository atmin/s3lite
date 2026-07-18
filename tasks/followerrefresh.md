# Continuous follower refresh — followers serve near-live reads

## Goal

Let a follower periodically refresh its local database from the shared replica so
it serves near-live reads instead of the frozen snapshot it restored at `Open`.
Opt-in via a new `Config.FollowerRefreshInterval`: when zero (the default) a
follower behaves bit-identically to today — restores once at `Open`, refreshes
only on promotion. When set, a background refresh pulls the leader's latest
committed state on that cadence and swaps it in underneath the stable `*sql.DB`,
read-only throughout. The single-writer invariant is untouched: this only makes
*followers* fresher, never a second writer.

## Why

Today followers serve the snapshot from their `Open` (or last promotion) and never
refresh — a documented limitation (README "Followers serve their Open-time
snapshot", [replica.go](../replica.go) restore path). That makes read scaling only
viable when stale-until-promotion data is acceptable. Continuous refresh turns
followers into genuine near-live read replicas (bounded staleness ≈ the refresh
interval + the leader's replication lag), which is the single highest-value read
feature and touches nothing about write coordination.

It is also the enabler for on-demand promotion: a follower that is already caught
up promotes in ~lease-acquire latency instead of paying a full cold restore — so
this composes directly with `TryPromote` (shipped).

## Background — the machinery already exists

The refresh is *exactly the restore-and-swap that `promote` already performs*,
minus taking the lease and minus becoming writer:

- `restoreDB(ctx, S3, BackupTo, LocalPath)` ([replica.go](../replica.go)) rebuilds
  the local file from the replica's latest state.
- `connector.swapFiles(readOnly, fn)` ([stableconn.go](../stableconn.go)) gates
  connection creation while `fn` rebuilds the file, then bumps the generation so
  pre-swap connections are dropped and re-dialed — no query observes a
  half-restored database. `promote` uses this today ([lease.go](../lease.go)
  `promote`); refresh calls it with `readOnly = true` so the handle stays
  `query_only`.
- POSIX unlink-while-open semantics make the in-place swap safe for connections
  that are merely *pooled* across it: `removeLocalDBFiles` unlinks the old path and
  `restoreDB` writes a fresh one, so a connection holding the old inode keeps
  reading old data until it is discarded on generation change. (See the one real
  risk below for in-flight queries.)
- `leaseLoop` ([lease.go](../lease.go)) already runs a background goroutine for the
  follower branch (`tryPromote`) — the natural home for the refresh tick.

## The one real problem: swapping the file under live readers

`swapFiles` guarantees no *new* connection sees a half-restored file and that
pre-swap connections are dropped afterward. What it does **not** guarantee is a
query that is *mid-execution on an existing connection* at the instant the file is
unlinked and rewritten. `promote` gets away with this because it is rare; a
refresh on a several-second cadence hits that boundary far more often, and a
mid-flight read spanning the swap can surface a transient SQLite I/O error.

This task accepts that boundary risk for the baseline (documented, rare, and
retryable — the connection is dropped and the next attempt dials the fresh file),
and keeps it small by:

1. **Skipping no-op refreshes.** Track the last-applied replica position
   (generation / max txid) and skip the whole restore+swap when the replica has
   not advanced. A quiet database does zero swaps, so idle followers never disturb
   readers. This is the most important mitigation — most ticks should be no-ops.
2. **Recommending a modest default interval** and documenting that reads may see a
   rare retryable error at a swap boundary.

The real fix — applying WAL frames into the open database without replacing the
file — is genuinely harder (litestream has no clean embedded live-tail API) and is
**out of scope**; see Not in scope.

## Change

Files: `s3lite.go` (config field, struct state, refresh method, loop wiring),
`lease.go` (refresh in the follower branch), `README.md` (document the field and
drop the limitation), plus tests.

1. **Config field** (s3lite.go):
   ```go
   // FollowerRefreshInterval, when > 0, makes a follower periodically restore the
   // leader's latest committed state from BackupTo and serve it read-only, giving
   // near-live reads with staleness bounded by roughly this interval plus the
   // leader's replication lag. When 0 (the default) a follower serves the snapshot
   // it restored at Open and refreshes only on promotion — bit-identical to prior
   // behaviour. Ignored without an s3:// BackupTo (nothing to follow) and while
   // this instance is the writer. Best-effort: a failed refresh is logged and the
   // follower keeps serving its current state.
   FollowerRefreshInterval time.Duration
   ```

2. **Single guarded refresh path** (`refreshFollowerOnce` in lease.go), mutually
   exclusive with promotion so a refresh cannot clobber a promotion (or vice
   versa) and never runs while leader:
   ```go
   func (db *DB) refreshFollowerOnce(ctx context.Context) error {
       if db.IsLeader() {
           return nil // writers never self-refresh; they own the latest state
       }
       db.promoteMu.Lock()         // shared with tryPromoteOnce (already in lease.go)
       defer db.promoteMu.Unlock()
       if db.IsLeader() {          // recheck: a concurrent promote may have won
           return nil
       }
       advanced, pos, err := db.replicaAdvanced(ctx, db.lastRefreshPos)
       if err != nil {
           return err
       }
       if !advanced {
           return nil // replica unchanged since last refresh — no swap
       }
       swapErr := db.connector.swapFiles(true, func() error { // stays read-only
           if err := removeLocalDBFiles(db.cfg.LocalPath); err != nil {
               return err
           }
           if err := restoreDB(ctx, db.cfg.S3, db.cfg.BackupTo, db.cfg.LocalPath); err != nil {
               return err
           }
           return precreateWAL(ctx, db.cfg.LocalPath)
       })
       if swapErr != nil {
           return swapErr
       }
       db.lastRefreshPos = pos
       return nil
   }
   ```
   - `replicaAdvanced` queries the replica client for its latest position
     (generation + max txid, via the same client `newReplicaClient` builds) and
     compares to `db.lastRefreshPos`. Seed `lastRefreshPos` from the `Open`-time
     restore so the first tick after `Open` is a no-op when nothing has changed.
   - Reuses the `promoteMu` guard already added for `TryPromote`
     ([lease.go](../lease.go) `tryPromoteOnce`).

3. **Loop wiring** (lease.go `leaseLoop` follower branch): drive the refresh on its
   own cadence, independent of the promote/renew tick. Simplest: track
   `nextRefresh` and, in the follower branch, call `refreshFollowerOnce` when the
   interval has elapsed and `FollowerRefreshInterval > 0`, logging and continuing
   on error (mirroring `tryPromote`'s log-and-move-on). Keep it in the single
   `leaseLoop` goroutine so refresh, promote, and renew never run concurrently
   within one instance; `promoteMu` still guards against `TryPromote` callers.

4. **Optional hook** (s3lite.go): `OnRefresh(func())` fired after a successful
   swap, symmetric with `OnPromote`/`OnDemote`, for consumers that want to bust
   caches. Nice-to-have — include only if cheap; not required for the feature.

5. **Close interaction:** none new — the refresh runs inside `leaseLoop`, which
   `CloseContext` already cancels and waits out (`loopCancel` + `wg.Wait`) before
   teardown.

## Verify

Tests in `lease_internal_test.go` / `integration_test.go` (fake leaser via
`newLeaserFunc`; real MinIO path in integration), all under `go test -race`:

- **Refreshes to newer state:** follower with `FollowerRefreshInterval` set; write
  on the leader and sync; assert the follower observes the new rows within ~one
  interval, its handle stays `query_only`, and the cached `*sql.DB` is unchanged.
- **No-op when unchanged:** with a quiet replica, assert zero swaps across several
  intervals (count `swapFiles`/restore invocations == 0) so idle followers don't
  churn connections.
- **Interval 0 is bit-identical:** default config performs no background restore;
  the follower serves its `Open` snapshot only (existing follower tests still
  pass unchanged).
- **Never two writers / no clobber:** enable refresh and a short lease TTL, free
  the lease so the follower promotes while refresh ticks; assert `-race` clean, no
  double restore, exactly one `becomeLeaderLocked`, and the winner ends writable
  (`promoteMu` mutual exclusion holds).
- **Refresh error is non-fatal:** make the replica client fail one refresh; assert
  the follower logs and keeps serving its previous state (`IsLeader()==false`,
  reads still work), then recovers on the next successful tick.
- **Reads survive a swap:** drive continuous reads across a refresh boundary and
  assert they either succeed or fail retryably (dropped connection), never return
  corrupt rows.

Then `gofmt`/`goimports`, `golangci-lint run`, `go test ./... -race`; update
`README.md` — document `FollowerRefreshInterval` in the leasing section and remove
the "continuous follower refresh is not yet implemented" limitation. Additive API,
cut a minor tag.

## Not in scope

- **Incremental / live WAL tailing.** Applying only the new WAL frames into the
  open database (instead of a full restore + file swap) would eliminate the swap
  boundary risk and the per-refresh restore cost on large DBs, but litestream has
  no clean embedded live-tail API and it is a substantially deeper change. The
  baseline (full restore + gated swap, skipped when the replica hasn't advanced)
  is correct and simple; incremental is a future optimisation, not this task.
- **Routing / read-your-writes.** This gives bounded-staleness reads, not
  linearizable reads across the writer and followers. Nothing routes for you
  (unchanged); a client that must read its own writes still targets the leader.
