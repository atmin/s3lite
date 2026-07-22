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

---

## The chaos soak

`TestChaosSingleWriterDurability` exercises invariants 1, 2, 5, and 7 together: four
`RoleAuto` instances over one lock and one replica, driven by a seeded stream of
writes, clean close+reopens, lock steals, and `TryPromote` storms. Throughout it
asserts at most one leader per settle and that no reader's view of durable rows
regresses; at the end it restores a fresh instance and checks that every
acked-and-synced row survived with the database intact. The seed is fixed and printed
on failure so any failure reproduces.
