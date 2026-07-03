# Make Close durable — flush replication before returning

Self-contained task. No prior context needed beyond this file and the s3lite
source (`s3lite.go`, `replica.go`).

## Problem

`DB.Close()` is documented as "flushes pending replication, stops litestream,
and closes the database" — but it does **not** actually guarantee a durable
flush. A process that opens s3lite, writes, and closes **before litestream's
periodic sync tick fires** replicates *nothing* to the S3 replica. The committed
data is silently lost on a clean, orderly shutdown.

This was hit in production by a consumer (gitmote): a short-lived one-shot command
(`Open` → `INSERT` admin/token/repo → `Close`) left the S3 replica **empty**, so
the long-running server that later restored from that replica saw no data and
crash-looped. Workaround was to call `Sync(ctx)` explicitly before `Close()`.
Every consumer having to remember `Sync()` before `Close()` is a footgun; a clean
`Close()` losing acknowledged writes is surprising and unsafe.

Note the scope: s3lite's whole point is durability for serverless containers,
which are **routinely** short-lived or SIGTERM'd on scale-down/redeploy. "Flush on
graceful shutdown" is core, not an edge case. (A *hard* kill losing a sub-second
window is acceptable and out of scope — this is about orderly `Close`.)

## Current code (`s3lite.go`)

```go
// Close flushes pending replication, stops litestream, and closes the database.
func (db *DB) Close() error {
	var firstErr error
	save := func(err error) { if err != nil && firstErr == nil { firstErr = err } }
	save(db.DB.Close())                              // (1) closes *sql.DB
	save(db.closeReplication(context.Background()))  // (2) store.Close — no explicit flush
	return firstErr
}

// Sync blocks until the replica is caught up with the current database state.
func (db *DB) Sync(ctx context.Context) error {
	if db.lsDB == nil { return nil }
	return db.lsDB.SyncAndWait(ctx)                  // this is what Close is missing
}

func (db *DB) closeReplication(ctx context.Context) error {
	if db.store != nil { return db.store.Close(ctx) }
	return nil
}
```

`Close` never calls `SyncAndWait`; `store.Close(context.Background())` does not
reliably perform a final durable sync of the current position for a process that
never hit a periodic tick. Also note `context.Background()` — unbounded, so any
flush that *is* added must be bounded or an unreachable replica hangs `Close`.

## Desired behaviour

A successful `Close()` on a replicated DB **guarantees the replica reflects all
committed writes** (or returns an error). No caller should need a manual `Sync()`
first.

## Approach — reuse litestream, don't reinvent

Two paths; prefer whichever the current litestream provides, to avoid duplicating
flush logic:

1. **Upgrade litestream and use its shutdown-sync (preferred if available).**
   litestream **v0.5.13** `Store` exposes `ShutdownSyncTimeout` and
   `ShutdownSyncInterval` fields (s3lite currently pins v0.5.11). **Verify** that
   setting these makes `Store.Close(ctx)` perform a bounded final sync (read
   `store.go` `Close` in v0.5.13). If so: bump the dep, set a sensible
   `ShutdownSyncTimeout` on the store in `Open`, and `Close` becomes durable with
   no bespoke sync code.

2. **Flush explicitly in `Close` (fallback / belt-and-suspenders).** Call
   `SyncAndWait` before tearing down, bounded by a timeout:
   ```go
   func (db *DB) Close() error {
       var firstErr error
       save := func(err error) { if err != nil && firstErr == nil { firstErr = err } }
       if db.lsDB != nil {
           ctx, cancel := context.WithTimeout(context.Background(), db.shutdownSyncTimeout)
           save(db.lsDB.SyncAndWait(ctx)) // flush WHILE the WAL/db is still open
           cancel()
       }
       save(db.DB.Close())
       save(db.closeReplication(context.Background()))
       return firstErr
   }
   ```
   Ordering matters: `SyncAndWait` before `db.DB.Close()` (litestream reads the
   WAL; flush before the sql handle goes away), then stop the store.

Whichever path: **bound the flush with a timeout** so an unreachable replica
degrades to "best-effort flush + error" instead of hanging shutdown.

## API surface

`Close()` takes no context today. Keep it backward compatible:
- Add `Config.ShutdownSyncTimeout time.Duration` (default e.g. 30s) used by the
  flush above; and/or
- Add `CloseContext(ctx context.Context) error` for callers that want to control
  the deadline (e.g. a server wiring its graceful-shutdown ctx), with `Close()`
  delegating to it with the default timeout.

Update the `Close` doc comment to state the durability guarantee and the timeout.

## Verify

- **Reproduce + fix (the core test):** open with a `file://` replica, `INSERT` a
  row, `Close()` **without** calling `Sync()`. Then open a *second* s3lite with
  `RestoreFrom` = that replica into a *fresh* `LocalPath`, and assert the row is
  present. Fails today; passes after the fix. (`file://` replicas exercise the
  same flush path and need no S3.)
- **Short-lived process:** same as above but with the write-then-close happening
  faster than any periodic sync interval — proves it's not tick-dependent.
- **Bounded on unreachable replica:** point at an unreachable `s3://` endpoint;
  `Close()` returns within ~`ShutdownSyncTimeout` with an error, does not hang.
- Existing tests still pass; `Close()` with no replica (`BackupTo` empty) is still
  a fast no-op.

## Downstream cleanup (informational)

Once `Close` is durable, the consumer workaround can be removed: gitmote calls
`md.Sync(ctx)` in its bootstrap command and in its server graceful-shutdown path
purely to compensate for this bug. They become redundant (harmless) after the fix.

## References

- litestream v0.5.13 `Store.ShutdownSyncTimeout` / `ShutdownSyncInterval` (new
  since the pinned v0.5.11).
- `litestream.DB.SyncAndWait(ctx)` (`db.go`) — already wrapped by s3lite `Sync`.
