# Restore over an existing local file leaves litestream's local LTX state stale

## Why

Diagnosing the crash-reacquire rewind (landed) mapped out litestream's local state:
a writer's replication position lives as L0 LTX files under the meta directory
`.<name>-litestream/ltx/` beside `LocalPath`, and litestream computes its resume
position — and the next TXID it will write — from those files at startup (`db.Pos()`;
`verify` reads the newest local L0's header against the live WAL).

s3lite's restore paths — `restoreLocalFromReplica` (the Open-direct fork guard) and
`rebuildLocalFromReplica` (promote) — replace the database via `removeLocalDBFiles`,
which clears `path`, `-wal`, `-shm`, and `-txid` but **not the meta directory**. A
node that was previously a leader on the same `LocalPath` therefore starts
replicating after a takeover-restore with L0 state from its *old, discarded* lineage:
a position that can sit ahead of, behind, or diverged from the replica lineage it
just restored. Suspected consequences range from a bogus full snapshot at a colliding
TXID to silently stalled uploads (litestream's replica sync only uploads local L0
files whose TXID exceeds the remote position). Nothing today decides which.

## What is true today (read before designing)

- `TestOpenDirectTakeoverRestores` and `TestPromoteTakeoverRestores` assert the
  restore decision and the restored content, but never **write after** the takeover,
  and their deferred `Close` errors are discarded — onward replication over a stale
  meta dir is unasserted.
- The handoff tests that do verify onward replication after a promote all use fresh
  local paths (no prior leader tenure), so no meta dir exists to go stale.
- litestream has `DB.ResetLocalState` for exactly this, but s3lite's restore paths
  run before any `litestream.DB` exists; clearing would be a plain file op (the meta
  path is `filepath.Join(dir, "."+file+"-litestream")` — see `litestream.NewDB`).
- The resume paths (self-succession, clean restart) must **keep** the meta dir — it
  *is* the position that makes the resumed tail ship. Only paths that discard the
  local lineage may clear it.

## Sketch (settle the shape at pickup)

- Repro first, in-process: leader ships a lineage and crashes (`simulateCrash`); a
  successor on another path takes over the expired lease, extends the replica, closes
  cleanly; the original returns via Open-direct (takeover → restore), **writes**,
  `Sync`s and `Close`s with errors checked; a fresh restore must contain the
  post-takeover writes. Decide from the failure (if any) whether the fix is clearing
  the meta dir on the restore paths or something litestream already handles.
- If it reproduces, mirror the check onto the promote-path restore
  (`rebuildLocalFromReplica`) — same stale-state shape, different entry.

## Verify

- The repro (or its green twin) lands in the invariant #9 test list; INVARIANTS.md
  notes the outcome either way.
- `go test ./...` (with `-race`) and the `integration`-tagged suite pass.
