# Invariants

The guarantees s3lite makes, and the tests that will fail if any of them breaks.
This is the page to read if you are deciding whether to trust the library with your
data. Each invariant names the test(s) that enforce it; unless noted, they run in the
default `go test ./...` suite (no Docker), and the whole suite runs under `-race`.

The one-line version: **at most one writer per replica at any time, that writer's
acked-and-synced data is never lost, and every cached `*sql.DB` handle keeps working
across role changes.**

---

## 1. Single writer per `s3://` replica

At most one instance ever holds the lease, so at most one instance replicates to a
given replica. The lease is a compare-and-swap on the object store's `lock.json`;
`RoleAuto` instances that lose the race open as read-only followers. After any
handoff settles, at most one live instance reports `IsLeader()`.

*Enforced by:* `TestAutoMutualExclusion`, `TestConcurrentAutoOpenSingleLeader`,
`TestWriterFailsWhenLeaseHeld`, and the chaos soak's invariant 1
(`TestChaosSingleWriterDurability`). Over real S3: `TestLeaseMutualExclusionAndHandoffS3`.

## 2. Fencing timing — a stalled writer steps down before its lease expires

A holder that cannot confirm a lease renewal stops replicating *before* the lease's
`ExpiresAt`, so a successor that acquires at expiry never overlaps it. The renew is
bounded by a deadline derived from the held lease (`ExpiresAt - LeaseTTL/6`); a renew
that hangs (an S3 black hole) forces a demotion rather than stalling past expiry. A
renew interrupted by `Close` is a shutdown, not a lost lease, and does not demote.

One inherent limit: expiry judgments compare `ExpiresAt` (stamped from the holder's
clock at acquire/renew time) against each instance's local clock, so severe clock
skew between instances erodes the fencing margin. The `LeaseTTL/6` deadline margin
absorbs modest skew; keep instance clocks NTP-disciplined and do not run TTLs short
enough that expected skew is a meaningful fraction of them.

*Enforced by:* `TestBlockingRenewDemotesBeforeExpiry`,
`TestShutdownDuringRenewDoesNotDemote`, `TestLeaseLossDemotesWriter`. Over real S3:
`TestLeaseStealFencesWriterS3` (a foreign lock replacement fails the holder's next
renew, which demotes and fences, and the successor continues the generation sequence).

## 3. Demotion fences the cached handle, including in-flight transactions

When an instance loses the lease, its `*sql.DB` stops accepting writes immediately —
not just on new connections. A transaction begun while leader cannot `Commit` after
demotion (it is rolled back), and a write on a checked-out `*sql.Conn` is rejected.
This prevents a demoted writer from committing locally on a lease it no longer holds
— a write that would never replicate and would vanish at the next restore. Reads are
deliberately *not* fenced: a stale reader sees a consistent old snapshot.

*Enforced by:* `TestCachedHandleFencedOnDemote`, `TestInFlightTxCannotCommitAfterDemote`,
`TestCheckedOutConnWriteFencedOnDemote`, `TestReaderTxAcrossRefreshSwapStillReads`
(the read non-fencing).

## 4. A clean `Close` is durable and bounded

`Close` flushes all committed writes to the replica before returning, so a fresh
instance restoring afterward sees everything — no separate `Sync` needed. The flush
is bounded by `ShutdownSyncTimeout`, so an unreachable replica makes `Close` return an
error rather than hang. `Close` is idempotent across sequential calls.

*Enforced by:* `TestCloseIsDurableWithoutExplicitSync`,
`TestCleanCloseAcrossProcessBoundary` (across a real process boundary),
`TestCloseBoundedOnUnreachableReplica`, `TestDoubleCloseIsIdempotent`. Over real S3:
`TestWriterSurvivesReplicaOutageS3` (writes keep succeeding locally through an S3
outage, `Sync` fails bounded instead of hanging, replication recovers when S3
returns, and the eventual `Close` loses nothing — including rows written while S3
was down).

## 5. A hard kill loses at most the unsynced tail; the restore is a consistent prefix

If a writer is `SIGKILL`ed (no clean shutdown), a fresh instance restored from the
replica passes `PRAGMA integrity_check` and contains a consistent *prefix* of the
committed writes — never a torn state, never holes. Only the sub-second window since
the last WAL sync can be lost.

*Enforced by:* `TestHardKillRestoresConsistentPrefix` (re-execs a child writer, kills
it, and asserts `count(*) == max(id)` with integrity intact), and the chaos soak's
end-state restore.

## 6. Follower staleness is bounded, and a failed refresh keeps current state

A follower serves the snapshot it restored at `Open`. With
`FollowerRefreshInterval` set it periodically brings itself up to the leader's latest
committed state, bounding staleness to roughly that interval plus replication lag. The
refresh is **incremental**: it applies only the LTX committed since the follower's
position to a private follow file — litestream's own `Restore(Follow)` resume, driven
by `advanceFollowFileFunc` — then atomically swaps a consistent copy of it into the
read path. It does not re-download the whole snapshot each tick. A refresh whose
advance *fails* leaves the follower serving its current state: the advance runs before
(and outside) the swap, and the publish copies into a temp before touching the live
files, so a failure never destroys the live database. Promotion and `Open` remain full
rebuilds by design. (The incremental path relies on a litestream fork; see
`LITESTREAM-FORK.md`.)

*Enforced by:* `TestFollowerRefreshSeesNewWrites`, `TestFollowerRefreshIsIncremental`,
`TestFollowerRefreshAdvanceFailureKeepsState`, `TestFollowerRefreshNoOpWhenUnchanged`,
`TestFollowerRefreshStaleTempFailureKeepsServing`,
`TestFollowerRefreshReestablishesWhenFollowFileUnusable`,
`TestFollowerRefreshEqualsFullRestore`, `TestFollowNeedsReestablish`,
`TestPromoteRestoreFailureLeavesServingFollower`.

## 7. The stable `*sql.DB` is never reassigned

The handle returned by `Open` is created once and never replaced, even across
promote/demote/refresh. Take it once (`database := db.DB`), hand it to repositories,
and keep using it: connections are transparently re-dialed against the current local
file in the current mode. Callers never need to re-fetch the handle.

A corollary: a rebuild (promote/refresh) can hold the connection gate for as long as
a full restore takes, but a query carrying its own deadline is never stuck behind it —
it fails at its deadline and the handle recovers once the swap releases.

*Enforced by:* `TestCachedHandleSurvivesPromotion`,
`TestCachedHandleConcurrentReadsAcrossPromotion`,
`TestFollowerRefreshConcurrentReadsSurviveSwap`, `TestFollowerRefreshReadsStayConsistent`,
the chaos soak's per-slot readers,
and — for the deadline corollary — `TestConnectHonoursContextDuringSwap` and
`TestQueryDeadlineNotStuckBehindSwap`.

## 8. `Generation` semantics — no more than documented

`Generation()` is unique among concurrent contenders and increases across takeovers
*only while the lock object survives* (expiry or a forced steal). A clean release
deletes the lock, so the next acquirer resets to 1. It is therefore **not** a durable
cross-handoff fencing token for external systems — after a clean handoff a consumer
sees `1 → 2 → 1`. Use it for distinguishing promotions within one instance's lifetime
and for diagnostics only.

*Enforced by:* `TestGenerationResetsOnCleanHandoff`.

## 9. A returning leased writer never silently rewinds — or forks — its committed tail

A leased writer that crashes and restarts on the same machine recovers what an unleased
writer would: it keeps the local file's committed tail rather than restoring the replica
over it, so writes acked after the last sync are not discarded by the instance's own
successor. It also never does the opposite harm — resuming a local file that a successor
has *forked* from, which would ship a divergent lineage over the replica (corruption, not
mere loss). The rule holds whichever way the writer re-enters:

- **Via the loop's `promote()`** — the lease was still *held* at reopen, so the instance
  came back a follower and promotes. The guard is the lease **generation**: only the
  holder writes, so a fork requires an acquire (which bumps the generation), and the
  local file resumes *in place* only when the just-acquired generation is exactly one
  past the generation the local tail was written under (recorded in `<LocalPath>.leasegen`
  while leader). Any gap (a successor acquired in between) or a missing/unreadable record
  restores. This leans on the generation only where #8 says it is reliable: promote is
  reached only when the prior lock *survived* — held, then expired — never after a clean
  release.

- **Via `Open`'s direct acquire** — the lease had *already expired* by reopen, so the
  instance re-acquires it straight away in `Open`, bypassing the loop. Here the generation
  is ambiguous, because a clean release resets it to 1 (#8), so a clean self-restart and a
  successor's clean handoff look identical by generation alone. Two signals prove a resume
  safe, both erring toward restore: **self-succession** (generation exactly one past the
  recorded one — the lock survived our tenure; recovers an unshipped tail, immune to the
  local file's L0-lags-WAL skew), and **clean restart** (a clean `Close` writes
  `<LocalPath>.cleanshutdown` with the replica position it synced to; if that marker is
  present and the replica has not advanced past it, no other writer wrote since, so the
  local file still equals the replica — resume for free, no re-download). A generation
  gap, an advanced replica, a missing/garbage marker, or an unreadable replica restores. A
  local file that was never a leased leader here (no recorded generation) is a fresh or
  externally-seeded start, not a returning writer, so it resumes in place unchanged —
  divergence for a brought-in file is out of scope (the lease is the multi-writer boundary).

So the sub-second loss window is at risk only on real machine loss or a true failover,
never on a plain process restart or clean restart; and a genuine takeover is always
restored, never resumed onto a fork.

The instance already computes this restore-vs-resume decision at every writer entry, and
`LastPromoteOutcome()` exposes it — `PromoteOutcome{Restored, Generation}`, valid after a
writer `Open` and each promotion — so a consumer holding state *derived* from the database
(caches, external blobs, queued deletions) can act on the same distinction the guards do.
Because both outcomes carry generation > 1, `Generation()` alone conflates a rewind-bearing
restore with a harmless resume; the accessor separates them, letting a consumer reconcile
derived state on a genuine takeover yet skip that pass on a plain restart. It reports both
entry paths (loop `promote` and `Open` direct acquire); a first-ever writer entry with no
prior local file reads as restored, erring the same conservative direction as the guards.
It is a read-only signal — it never alters a restore decision.

A resumed tail must also *ship*: the resume decision is worthless if replication then
skips what it resumed. The full-fidelity shape of that — a real `SIGKILL` leaving a
genuinely dirty WAL, a real lease, real S3, and a successor tenure that must survive a
fresh restore — runs across process boundaries in the crash harness's reacquire
scenario. Building it caught a rewind affecting every release through v0.6.0, leased
or not: SQLite's default per-connection autocheckpoint (1000 pages) let a returning
writer's first commits fully backfill and restart the large crash-recovered WAL before
litestream — whose protective read lock exists only once its lazy first sync has run —
had captured the dead tenure's tail. litestream then resumed from the restarted WAL,
and any page allocated in the skipped span (a leaf split and its parent linkage)
dropped out of the replicated lineage: a fresh restore could miss the crashed tenure's
acked tail and the successor's entire synced, cleanly-closed tenure, while the
successor's `Sync` and `Close` reported success. The fix: a replicated writer's
connections run with `wal_autocheckpoint(0)` (see `buildDSN`), so litestream owns
checkpointing outright — and it checkpoints only after capturing to the WAL end.
Consumers on v0.6.0 or earlier should upgrade.

The mirror of "a resumed tail must ship" is "a *restored* lineage must ship cleanly."
A restore discards this machine's local lineage for the replica's, so it clears not just
the SQLite files but litestream's local position — the L0 LTX files under
`.<name>-litestream/` that `db.Pos()` resumes from (`removeLitestreamMeta`, called only
by the two restore paths, never by a resume). Left behind, that position belongs to the
discarded lineage and can sit *ahead of* the restored replica (a crashed leader's monitor
captures WAL frames into local L0 before the separate replica upload ships them); the
next writer would then resume from it and ship the discarded tail back over the
successor's lineage — a fork, not mere loss. litestream happens to mask this today (its
`verify` re-snapshots when the freshly-restored WAL no longer matches the stale L0), but
that is a heuristic with a known resume unsoundness, so s3lite clears the position
outright rather than depend on it; recovery then routes through litestream's
well-tested "database behind replica" path (local position empty ⇒ refetched from the
replica), giving a clean, gap-free onward lineage. Resume paths (self-succession, clean
restart) must *keep* the meta directory — there it **is** the position that makes the
kept local tail ship.

*Enforced by:* `TestPromoteSelfSuccessionKeepsLocalTail`, `TestPromoteTakeoverRestores`,
`TestPromoteMissingGenerationRestores`, `TestPromoteNeedsRestoreDecision`,
`TestOpenDirectCleanRestartResumes`, `TestOpenDirectCrashSelfSuccessionResumesTail`,
`TestOpenDirectTakeoverRestores`, `TestOpenDirectAmbiguousSignalRestores`. The restore
paths' position-clearing (a stale L0 ahead of the restored replica must not survive to
ship the discarded lineage) is pinned by `TestOpenDirectTakeoverClearsStaleLitestreamState`
and `TestPromoteTakeoverClearsStaleLitestreamState`. Full
fidelity, across real process boundaries: `TestCrashRestartResumedTenureSurvivesRestore`
(SIGKILL with a dirty WAL, same-path restart, the resumed tail and the successor's
cleanly-closed tenure both survive a fresh restore) and, over a real lease and MinIO
under the `integration` tag, `TestCrashReacquireResumedTenureSurvivesRestoreS3` (also
asserts the reacquire resumes via self-succession). The connection pragma itself is
pinned by `TestBuildDSN`. The `LastPromoteOutcome()` accessor rides on the four
restore-vs-resume tests above (each asserts the reported outcome next to its restore
count) and is additionally pinned by `TestOpenFreshFirstWriterReportsRestored` (a
first-ever entry reads restored) and `TestFollowerReportsNoPromoteOutcome` (`ok == false`
before any promotion).

---

## The chaos soak

`TestChaosSingleWriterDurability` exercises invariants 1, 2, 5, and 7 together: four
`RoleAuto` instances over one lock and one replica, driven by a seeded stream of
writes, clean close+reopens, lock steals, and `TryPromote` storms. Throughout it
asserts at most one leader per settle and that no reader's view of durable rows
regresses; at the end it restores a fresh instance and checks that every
acked-and-synced row survived with the database intact. The seed is fixed and printed
on failure so any failure reproduces.
