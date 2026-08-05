# litestream `verify()` resumes past a single external WAL restart

Read against **litestream v0.5.15** (`db.go`), found 2026-07-22 while fixing the
crash-reacquire rewind ([../../INVARIANTS.md](../../INVARIANTS.md) #9).

## What upstream does

On a cold start litestream calls `db.verify()` to decide whether it can resume
shipping from its saved WAL position or must re-snapshot. When the WAL was fully
backfilled *and restarted by an external checkpointer* between litestream
positions, verify reaches the wrong conclusion three ways at once:

1. it sees the salt change, but `lastPageMatch` passes anyway — it compares
   against the **stale old-salt bytes** that still sit beyond the restart point;
2. `detectFullCheckpoint` returns false, because it only detects **unknown**
   salts, and a *single* restart leaves the WAL holding nothing but salts
   litestream already knows (two-plus restarts is what it catches);
3. so `snapshotting=false` and the resume starts from offset 32.

Every frame between litestream's saved offset and the old WAL's end is silently
dropped from the lineage. It surfaces as data loss only when a page allocation —
a leaf split plus its parent linkage — falls in the skipped span, which is why
it reads as sporadic corruption rather than as a missing tail.

The sound fix upstream: on a cold start, a salt change **without proof of having
synced to the old WAL's end** must snapshot rather than resume.

## What s3lite does about it

s3lite is no longer exposed. Replicated writer connections set
`wal_autocheckpoint(0)` ([../../stableconn.go](../../stableconn.go), `buildDSN`)
so litestream owns checkpointing and no external checkpointer exists to create
the restart. The restore paths also clear litestream's position state outright
rather than lean on verify noticing a discarded lineage — INVARIANTS.md #9 says
so in as many words: the masking is a heuristic we must not depend on.

The litestream **CLI** monitoring a third-party application that checkpoints
explicitly is still exposed. Deliberately not carried as a fork patch — the fork
stays minimal ([../litestream-fork.md](../litestream-fork.md)) — so this is an
upstream issue worth filing with the s3lite repro shape
(`TestCrashRestartResumedTenureSurvivesRestore`).
