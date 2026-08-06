# Restore progress callback — live percentage and stall detection

A `Config.OnRestoreProgress func(applied, total int64)` callback, fired while a
whole-database restore runs, so an embedding application can show a live
percentage/ETA and detect a *stalled* restore (bytes not advancing ⇒ a hung
range read). It completes what restore-observability started (landed, v0.11.0):
the log lines say "restore started / finished in Dms"; this says "42%, still
moving."

## Why

The idea was parked on "unproven need — no consumer has asked for a percentage
rather than a phase." That condition has now been met twice over, and the two
consumers want **different halves**, which is what decides the mechanism:

- The embedding filesystem daemon wants **stall detection**: its cold-start
  front door waits on a `restoring` phase that is liveness-only, so a wedged
  restore is indistinguishable from a slow one. The no-fork alternative — a
  hard deadline around the restore — reintroduces exactly the
  clock-a-slow-link-can-trip problem that liveness-without-a-ceiling was
  chosen to avoid; only progress data distinguishes *stuck* from *slow*.
- The CLI ([cli.md](cli.md), now on the frontier) wants the **bar**: a human
  staring at a cold multi-GB open is the progress bar's reason to exist, and
  no watchdog can substitute for it.

One hook serves both, and it is the same hook. The payoff scales with database
size, which is exactly the case the phase-only logging serves worst.

## What is true today (read before designing)

- **`restoreDB`** (replica.go, logger-aware since v0.11.0) drives
  `replica.Restore` with `NewRestoreOptions()` and gets back only an error.
  All three whole-DB restore paths funnel through it — Open's initial cold
  restore, the promote rebuild, the Open-direct fork guard — so one plumbing
  point covers every consumer-visible restore.
- **`RestoreOptions`** (litestream db.go:3323) has no progress surface:
  `OutputPath`, `TXID`, `Timestamp`, `Parallelism`, `Follow`,
  `FollowInterval`, `IntegrityCheck`.
- **`total` exists but stays inside litestream**: `Restore`'s main path
  computes the plan (`CalcRestorePlan`, litestream replica.go:698 → :1526)
  as `[]*ltx.FileInfo` whose `Size` sums to the bytes about to be applied.
- **`applied` flows through `internal.NewResumableReader`** (litestream
  replica.go:723), one reader per plan file — and `Parallelism` downloads
  several concurrently, so any counting must be atomic and the callback must
  tolerate concurrent or out-of-order increments.
- **The V3 branch is out of scope**: `Restore` only diverts to `RestoreV3`
  for v0.3.x-format backups behind a `ReplicaClientV3` check (litestream
  replica.go:686-696); s3lite replicas are never that format.
- **Structural constraint** (recorded by restore-observability, still true):
  the cold restore runs inside `Open` before the `*DB` handle exists, so the
  surface must be a pre-`Open` `Config` field — a pollable accessor cannot
  help the path that matters most.
- **The fork is a patch ledger** (docs/litestream-fork.md): one row per
  carried commit, each pinned by a committed test, currently three rows,
  tagged `v0.5.16-s3lite.1`. This patch becomes row 4 and follows the house
  rules — minimal, optional, inert when unset, upstreamable.

## Sketch (settle the shape at pickup)

Two rungs, fork first — each lands and tags in its own repo before the next
consumes it:

1. **Fork: an optional progress hook on `RestoreOptions`** — a nil-default
   `OnProgress func(applied, total int64)` (or equivalent) field. `total`
   fires once when the plan is computed; `applied` accumulates atomically
   through a counting reader wrapped around each `ResumableReader`. No
   behaviour change when nil — inert for every existing caller, same posture
   as ledger row 2. Ledger row + pin test + tag (`v0.5.16-s3lite.2`).
   Genuinely upstreamable; note that in the ledger's status column.
2. **s3lite: `Config.OnRestoreProgress`**, beside `Logger` — plumbed through
   `restoreDB` (which already carries the logger, so the seam exists) and
   fired for all three whole-DB paths. Decide at pickup how raw the firing
   is: pass the fork's concurrent increments straight through (document
   "concurrent, keep it cheap") or serialize/coalesce in s3lite so consumers
   get monotonic samples. Lean toward s3lite absorbing the awkwardness — two
   consumers should not both have to.

**Out of scope:** progress for the follower's incremental refresh
(`advanceFollowFile` applies small deltas; a phase would be noise), any stall
*policy* (that belongs to consumers), and any consumer surfacing.

## Verify

- **Fork pin test** (the ledger's contract): a restore over a multi-file plan
  fires `total` once and monotonically-summing `applied` reaching `total`;
  with the field nil, byte-identical behaviour to before — in the same shape
  as the existing rows' pins, against the `s3/` and `file/` clients.
- **s3lite unit, cold path**: a fresh `Open` against a non-empty replica
  fires the callback with samples ending at `(total, total)`; an empty
  replica ("nothing to restore") never fires it; a nil callback restores
  exactly as today. Reuse the `restoreDBFunc` injection scaffolding.
- **s3lite unit, rebuild path**: a takeover promote's restore fires the same
  callback — the plumbing point covers it by construction; assert it anyway.
- **Concurrency**: a restore with `Parallelism > 1` under `-race`, asserting
  the samples never regress past coalescing (whatever rung 2 decided).
- README (Config docs) and the `Logger` field comment gain the callback
  sentence; docs/litestream-fork.md gains row 4.
- Release: tag the s3lite version that carries this (v0.12.0) — it is the
  release the filesystem daemon's consumer task is gated on.
