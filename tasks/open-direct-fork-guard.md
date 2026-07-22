# Open-direct fork guard — a returning writer must not resume a forked local file

## Why

`tasks/local-ahead-promote.md` (landed, INVARIANTS.md #9) guards the **`promote()`**
path so a leased writer that crashes and restarts *while its lease is still held*
resumes its local committed tail instead of restoring the replica over it. But that
guard only covers the path reached when the lease was **held at Open** (→ follower →
the loop's `promote()`).

There is a second way a returning leased writer becomes the writer: if its lease has
**already expired** by the time it reopens, `Open` acquires the lease **directly** and
calls `becomeLeaderLocked` — and, because `LocalPath` still exists, it does **no
restore** and resumes the local file unguarded. That is correct (and desirable) for the
common cases — a clean restart, or a crash-restart with no successor — but it is
**unsound** when a successor took over in the interim: another instance acquired the
lease, wrote, forked the replica lineage, then finished (released or let its lease
expire). The returning writer then resumes its **forked** local file and litestream
ships it on top of the successor's lineage — a lineage conflict / corruption, not mere
data loss.

This is a **pre-existing** hazard: today's `Open` resumes-local unconditionally when
`LocalPath` exists, and local-ahead-promote did not touch it. The invariant to
establish: **no path to writer may resume a local file that has forked from the replica
lineage.** `promote()` now honours this via the generation guard; the Open-direct path
does not yet.

## What is true today (read before designing)

- `Open` restores only when `LocalPath` is **missing** (s3lite.go, the `os.IsNotExist`
  block). A returning writer whose file exists is never restored at Open.
- For `RoleWriter`/`RoleAuto`, `Open` then calls `AcquireLease`. On success (lease
  free/expired) it goes straight to `becomeLeaderLocked` → `startReplicationLocked`
  (litestream resumes the local file) → `openWriterLocked`. No restore, no guard —
  this is the Open-direct path.
- `RoleAuto` with a **held** lease falls back to follower → the loop's `promote()`,
  which **is** guarded (INVARIANTS #9). `RoleFollower` never acquires at Open. So the
  gap is specifically `RoleWriter`/`RoleAuto` acquiring at Open with an existing
  `LocalPath`.
- The landed guard's tools are reusable: `<LocalPath>.leasegen`
  (`writeLeaseGen`/`readLeaseGen`), `promoteNeedsRestore`, `rebuildLocalFromReplica`.
- `restoreDB`/`restoreDBFunc` refuse an existing output path, so an Open-direct restore
  branch needs `removeLocalDBFiles(LocalPath)` first (as `rebuildLocalFromReplica`
  does). It also runs **before the connector exists** (the connector is created inside
  `startReplicationLocked`), so it is a plain pre-connector file op, not a `swapFiles`
  rebuild.

## The catch — the generation signal is ambiguous here

The `promote()` guard resumes in place iff `acquiredGen == persistedGen + 1`. That
holds on the promote path because it is reached only when the prior lock **survived**
(held, then expired), so the generation is monotone and reliable (INVARIANTS #8). The
Open-direct path lacks that guarantee:

- After a **clean** `Close`, `ReleaseLease` **deletes** the lock, so the next acquire
  resets generation to **1** (INVARIANTS #8). A cleanly-restarted writer then sees
  `acquiredGen (1) != persistedGen (G) + 1`, and the `promote()`-style guard would
  decide **restore** — a full re-download on every clean restart, where today it
  resumes for free. That is an unacceptable **performance regression** for the most
  common path.
- Worse, a clean release by *us* and a clean release by a *successor* both leave the
  lock deleted → generation reset to 1. Generation alone cannot tell "I cleanly closed
  and came back" (safe to resume) from "a successor cleanly handed off after forking"
  (must restore).

So the guard cannot be dropped in verbatim. The spike must find a signal that separates:

1. **clean self-restart** (lock gone, replica ≡ local) → **resume** (keep today's fast path),
2. **crash self-restart, no successor** (lock survived at our gen) → **resume** (recover tail),
3. **takeover-then-return** (a successor acquired since our tenure) → **restore** (discard fork).

Cases 1 and 2 must stay resume; only 3 restores. Mind the safety asymmetry: erring
toward **restore** is corruption-safe (it risks only the sub-second tail, like today's
`promote()` over-restore), while erring toward **resume** risks **corruption** (shipping
a fork). So an ambiguous case must default to restore — but not so eagerly that case 1
regresses into a full re-download.

## Candidate signals (for the spike to weigh)

- **Pre-acquire lock inspection.** Read the lock object *before* acquiring. If it still
  exists at exactly our tenure's generation (`persistedGen`, untouched) → no takeover →
  resume (case 2). If gone or at a higher generation → ambiguous. Cleanly catches case
  2 and case 3-via-higher-gen, but the clean-release cases (1 and a successor's clean
  handoff both leave the lock gone) stay indistinguishable by the lock alone.
- **LTX lineage / checksum check.** Compare the local file's lineage to the replica's:
  if the replica is a prefix of the local committed history (same `PostApplyChecksum`
  chain) → resume (cases 1, 2); if they diverge → restore (case 3). This is the
  airtight fork test and it *does* separate case 1 from case 3 (a successor's handoff
  forks the lineage; our own clean close does not). It was rejected for
  local-ahead-promote because `litestream.DB.Pos()` reads local L0 which lags the WAL —
  but here that lag errs toward **restore** (corruption-safe), and `promote()` already
  covers crash-while-held tail recovery, so a conservative Open-direct is acceptable.
  Cost: reading local + replica LTX positions/checksums.
- **Combine:** a cheap pre-acquire lock check for the "lock survived at our gen"
  fast-resume (case 2), plus recognising our own just-released lock for the clean
  restart (case 1), falling back to the lineage check only when still ambiguous.

The spike decides. "Conservative — resume only when provably safe, else restore" is
acceptable scope, **provided** the clean self-restart (case 1) still resumes with no
full-restore regression, since that is the dominant path.

## How to work this task

1. **Design spike first (Item 0).** Settle the signal that separates cases 1/2 (resume)
   from case 3 (restore) without regressing the clean restart, and where in `Open` the
   decision + (conditional) pre-connector restore go. Write it here and get sign-off
   before implementing.
2. Then the items, each with the full gate (`go vet ./...`,
   `go test -race -count=1 ./...`, `go test -tags=integration ./...` — see TESTING.md
   for the Docker socket).
3. Keep README/INVARIANTS.md in sync in the same item that changes behaviour;
   INVARIANTS #9 should grow to cover the Open-direct path (or gain a sibling).
4. Match `lease_internal_test.go` style; reuse `simulateCrash`, `fakeLock`
   (`steal`, `expire`), `installRestoreCounter`, and the `.leasegen` helpers from
   local-ahead-promote.

## Item 0 — [ ] Design spike: the resume-vs-restore signal at Open-direct

Deliverable: the chosen signal, why it separates clean-restart / crash-self-succession
(resume) from takeover (restore) without regressing the common clean restart, the
divergence cases it does and does not protect, and where the decision + restore sit in
`Open` — each mapped to a planned test.

## Item 1 — [ ] Guarded Open-direct acquire

When `Open` acquires the lease directly (`RoleWriter`/`RoleAuto`) **and** `LocalPath`
existed at Open, apply the Item 0 signal: resume in place (today's behaviour) when
provably safe; otherwise `removeLocalDBFiles(LocalPath)` + `restoreDB` into `LocalPath`
before `becomeLeaderLocked`. Capture `localExisted` before the restore-if-missing and
`precreateWAL` blocks (both can create the file). Fresh deploys (LocalPath missing) and
the follower→`promote()` path are unchanged.

## Item 2 — [ ] Tests

Reuse the seams; each test names the hazard it pins:

- *Clean self-restart resumes (no regression):* writer writes, clean `Close`, reopen
  `RoleAuto`/`RoleWriter` on the same LocalPath acquiring at Open — resumes in place, no
  restore (assert via `installRestoreCounter`).
- *Crash self-restart after expiry resumes the tail:* crash (no release), let the lease
  expire so the reopen acquires at Open (not via the loop) — the tail survives, no
  restore.
- *Takeover-then-return restores:* a successor acquired + advanced the replica then
  finished; the returning writer acquiring at Open restores rather than shipping its
  fork.
- *Ambiguous / unreadable signal → restore fallback,* logged.

## Item 3 — [ ] Docs + invariants

Extend INVARIANTS #9 (or add a sibling) to state the no-fork-resume guarantee for the
Open-direct path too; README durability note that the leased crash-restart guarantee
holds whether the lease was still held or already expired at reopen.

## Out of scope

- Multi-writer conflict resolution beyond the lease — divergence resolution stays "the
  replica lineage wins," made explicit.
- Incremental restore on the restore branch (bandwidth) — see the refresh task's
  follow-up note.

## Done when

A returning leased writer never resumes a local file that forked from the replica,
whether it re-enters as writer via `Open`-direct-acquire or via `promote()`; the clean
restart still resumes without a full re-download; the divergence rules are written down
and pinned by tests; the full gate passes. Then delete this file and update
tasks/README.md.
