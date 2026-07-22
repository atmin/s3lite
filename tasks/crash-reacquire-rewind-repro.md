# Crash-reacquire rewind — reproduce a consumer-observed restore that lost a successor's synced tenure

## Why

A consumer's end-to-end harness (a daemon embedding s3lite over MinIO,
pinned to **v0.5.0**, default 30 s TTL) reproducibly observed this sequence
lose data:

1. Writer runs on machine M, committing and replicating continuously.
2. Writer is `SIGKILL`ed. The lease lingers; the local file (main + WAL)
   survives on M.
3. The writer restarts on M with the **same `LocalPath`**, retrying `Open`
   until the TTL expires and the direct acquire succeeds (the Open-direct
   re-entry of INVARIANTS.md #9).
4. The successor serves for ~40 s: commits new state, replication running,
   then exits via a clean `Close` (whose error the harness discarded —
   see below).
5. A fresh node (no local file) restores from the replica — and gets the
   **pre-kill state only**. The successor's entire tenure is missing:
   tens of seconds of synced commits, not the documented sub-second window
   (README, INVARIANTS.md #4/#5). Two runs reproduced it identically, with
   the restored state referencing exactly the pre-kill tip both times.

v0.5.0 predates all of invariant #9's machinery (the `.leasegen` sidecar,
`local-ahead-promote`, `open-direct-fork-guard`, both landed in v0.6.0), so
the plausible mechanism is the un-guarded resume shipping a forked or
mis-positioned lineage whose restore later resolves to the pre-kill prefix.
But that is a suspicion, not a diagnosis — the point of this task is a
repro that decides. Either HEAD already closes it (then the repro lands as
the missing full-fidelity regression and ≤v0.5.x consumers get a "must
upgrade" note), or something is still live (restore's generation/lineage
selection, resume position over an un-checkpointed WAL, a lying final
flush) and gets fixed here.

## What is true today (read before designing)

- Invariant #9's tests cover this chain **in-process only**:
  `TestOpenDirectCrashSelfSuccessionResumesTail` uses `installFakeLeaser`
  (no real lock CAS), a `file://` replica, and `simulateCrash` — which
  closes the `*sql.DB` and therefore **checkpoints the WAL into the main
  file**, something a real `SIGKILL` never does. The resume decision and
  the onward replication are asserted, but never over a genuinely dirty
  WAL, a real lease object, or real S3.
- The re-exec crash harness (`crash_test.go`: `TestCrashChild`,
  `crashChildCmd`, ack protocol) already delivers a real `SIGKILL` with a
  live WAL — but each child today gets a fresh `LocalPath`; no scenario
  reuses the dead writer's file, and none exercises the lease at all
  (children run unleased `BackupTo` writers).
- `Open` as writer fails with `LeaseExistsError` while the lease is held;
  the reacquire-after-expiry loop is the caller's (step 3 above). LeaseTTL
  is configurable per instance (`Config.LeaseTTL`).
- Invariant #4 (`Close` flushes everything, bounded, or **returns an
  error**) is load-bearing for the repro: the observing harness discarded
  `Close`'s error, so "the flush failed loudly and nobody looked" and "the
  flush claimed success but the restore disagrees" are indistinguishable
  from its logs. The repro must assert `Close() == nil` to split them —
  if it turns out non-nil, the finding is documentation/consumer-guidance,
  not lineage corruption.
- Integration conventions: MinIO via testcontainers behind the
  `integration` build tag (TESTING.md); the observation was over real S3
  semantics, so a `file://`-only repro that passes is not conclusive.

## Sketch (settle the shape at pickup)

Extend the crash harness with a reacquire scenario rather than building new
scaffolding:

- Child A (`crash` mode, leased: `RoleWriter`, short `LeaseTTL`, S3/MinIO
  replica): inserts with acks; parent `SIGKILL`s it mid-stream after at
  least one synced prefix.
- Parent waits out the TTL, then runs child B in `clean` mode with the
  **same `LocalPath`** (env-carried, plus TTL/owner), inserting a disjoint,
  recognizable batch; B asserts its own `Sync` and `Close` return nil and
  prints the clean-done marker.
- Parent restores to a fresh path (no local file) and asserts: **every row
  of B's batch present** (clean close loses nothing — the observed failure
  is their total absence), plus a consistent prefix of A's acks with
  integrity intact (#5).
- Run the same scenario twice: `file://` in the default suite (fast,
  keeps CI coverage even if it cannot reproduce the original), and MinIO
  under the `integration` tag (the fidelity the observation had). If HEAD
  reproduces the rewind anywhere, bisect the suspects above before fixing;
  if it does not, attempt the same against v0.5.0 to confirm the
  observation is the fixed fork-guard gap, and record the outcome (and the
  upgrade guidance) in INVARIANTS.md #9's prose.

## Verify

- The new harness scenario green on `file://` and MinIO, wired into the
  invariant #9 test list.
- If a fix was needed: the failing case named in INVARIANTS.md with the
  test that now enforces it; if not: #9's prose notes the full-fidelity
  coverage and that the ≤v0.5.x rewind is closed by the v0.6.0 guards.
- `go test ./...` (with `-race`) and the `integration`-tagged suite pass.
