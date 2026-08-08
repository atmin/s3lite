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
`docs/litestream-fork.md`.)

The same refresh is available **synchronously**: `(*DB).Refresh` publishes the replica's
latest state now and reports whether anything advanced, so a consumer can force freshness
immediately before a read rather than only on the interval (which it does not need set).
It is the interval tick's own body called from the caller's goroutine, so every property
above holds identically — and it never takes the pen: a follower stays a follower, and it
is a no-op returning `(false, nil)` on a writer, on an unchanged replica, and for a
promotion that wins the race for `promoteMu`.

Its cost is part of the contract, because it is built to be called per statement. The
"has the replica advanced?" probe reads **level 0 and the snapshot level only** — two
listings, on a client each instance builds once and reuses rather than one constructed
per call — and still reports exactly what a walk of all ten levels would. That holds
because litestream admits data at only three places: level-0 uploads, compaction of
level N-1 into level N, and a snapshot. Every compacted level's max therefore came from
level 0's max at compaction time, and level 0's own max never goes backwards (both
retention passes keep the newest file in a level), so level 0 dominates levels 1–8 at
all times. The snapshot level is the exception that must still be read: it is stamped
from the writer's *local* position, which can sit a sync ahead of the level-0 objects on
the replica. The full walk survives as the fallback for the one state that argument does
not cover — an empty level 0 with data above it, which litestream's own retention never
produces but an external bucket lifecycle policy could.

*Enforced by:* `TestFollowerRefreshSeesNewWrites`, `TestFollowerRefreshIsIncremental`,
`TestFollowerRefreshAdvanceFailureKeepsState`, `TestFollowerRefreshNoOpWhenUnchanged`,
`TestFollowerRefreshStaleTempFailureKeepsServing`,
`TestFollowerRefreshReestablishesWhenFollowFileUnusable`,
`TestFollowerRefreshEqualsFullRestore`, `TestFollowNeedsReestablish`,
`TestPromoteRestoreFailureLeavesServingFollower`. The synchronous entry point —
`TestRefreshPullsOnDemand` (advance, no-op, read-only, `OnRefresh` exactly on advance,
with no interval configured), `TestRefreshOnWriterIsNoOp`,
`TestRefreshRacesPromotionAndLoop` (concurrent `Refresh` × `TryPromote` × interval tick
under `-race`, through a promotion that genuinely wins), `TestRefreshAfterCloseIsClosed`.
The probe — `TestReplicaLatestTXIDSeesEveryLevel` (a transaction compacted out of level 0
is still seen, and a snapshot ahead of level 0 is too),
`TestReplicaLatestTXIDListingBudget` (the two-listing budget, and the walk only when
level 0 is empty), `TestProbeClientIsReusedAcrossProbes` and
`TestProbeClientInitFailureIsNotCached`.

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

- **Via the loop's `promote()`** — the instance came back a follower and promotes (its
  lease was still *held* at reopen, or it cleanly `YieldLease`d and re-promotes on demand).
  The primary guard is the lease **generation**: only the holder writes, so a fork requires
  an acquire (which bumps the generation), and the local file resumes *in place* on
  **self-succession** — the just-acquired generation is exactly one past the generation the
  local tail was written under (recorded in `<LocalPath>.leasegen` while leader). The
  generation check leans on #8 only where it is reliable — it *succeeds* only when the prior
  lock survived (held, then expired), never after a clean release, which resets it to 1. So
  promote also accepts the **clean-marker proof** the `Open` path uses (below): when the
  generation check fails but a `.cleanshutdown` marker is present and the replica has not
  advanced past it, no writer wrote since our clean close/yield, so the local file still
  equals the replica — resume in place instead of re-downloading what we just synced. This
  is what lets a cleanly *yielded* instance re-promote in place with no interim writer. A
  generation gap with no marker, an advanced replica, or an unreadable replica restores.

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
before any promotion). The promote-path clean-marker proof (the #9 extension that lets a
cleanly *yielded* instance re-promote in place) is pinned by `TestYieldedRepromotesInPlace`
and `TestYieldPeerWroteRepromoteRestores` (see #10).

## 10. `YieldLease` is a clean handoff, never a data hazard

`YieldLease` voluntarily relinquishes the lease while the instance stays alive as a
follower, so a live-but-idle holder can free the pen for a peer's next write instead of
pinning it until death, deploy, or `Close`. It is a *relinquishment*, not a loss, and it
is atomic:

- **Clean handoff, never a data hazard.** The lock object is deleted only after
  fence → final sync → replication stopped, so at the moment of release the replica tip
  equals the local tip: a successor acquiring immediately sees every acked write. Single
  writer is preserved — a yield only ever *removes* a holder, and the If-Match delete
  cannot remove a successor's lock. The yield fences the handle read-only up front (the
  same hard fence as demotion, #3) so an in-flight transaction cannot commit into the tail
  after the final sync has captured it — a racing write is either fenced or shipped, never
  acked-but-unshipped past the release.
- **A yield that cannot complete aborts atomically.** A final-sync failure or a breach of
  the lease-expiry deadline (`ExpiresAt − LeaseTTL/6`, as in #2) aborts: the instance
  attempts a renew and, on success, un-fences and stays the leader (the loop resumes
  renewing); on failure it takes the standard demote path. It is never left
  fenced-but-holding or released-but-unsynced.
- **Yield is not demote.** A successful yield — and a yield whose release finds the lock
  already gone (stolen after our fresh renew; the tail is already shipped, so nothing is at
  risk) — never fires `OnDemote` and never drops a tail. `OnDemote` means "lost the lease
  while relying on it"; a yield is the opposite, a deliberate hand-back. Only the
  abort-then-demote path (a genuine loss mid-yield) fires it. Demote's own semantics
  (push-free fencing, lock left in place) are untouched.
- **A restarted yielded instance resumes for free.** The yield writes the same
  `.cleanshutdown` marker a clean `Close` does (at the synced replica position), so a
  yielded instance that is later *restarted* resumes in place through the unchanged
  Open-direct clean-restart path (#9) rather than re-downloading the database.

`Config.OnDemandPromotion` makes passivity the steady state so a yield is not self-defeating:
a follower promotes only via explicit `TryPromote` (the consumer's write path), so a yielded
holder is not immediately re-grabbed by its own background loop. `Open`'s direct acquire is
deliberately unchanged (bootstrap — first-writer schema creation — and expired-crash
re-entry keep working, at the cost of one harmless acquire-then-idle-yield per boot when the
lock happens to be free). **Passivity never parks an unshipped tail:** an instance that may
hold one — a follower `Open` that finds `.leasegen` present with no clean marker (a crashed
ex-leader whose lock had not yet expired), or one that just `demote`d (its cancelled-context
stop may have dropped a tail) — enters *eager recovery* and keeps background-promoting until
the next writer entry settles the tail (self-succession ships it, or the restore proof
discards it). A cleanly yielded instance carries both the marker and `.leasegen`, so it is
correctly passive: the marker is written only after the tail is fully shipped.

*Enforced by:* `TestYieldReleasesOnlyAfterReplicaCaughtUp`,
`TestWriteRacingYieldEitherFencedOrShipped`, `TestYieldAbortsOnSyncFailureStaysLeader`,
`TestYieldBoundedByLeaseExpiry`, `TestYieldStolenMidYieldCompletesAsFollower`,
`TestYieldNotLeader`, `TestDoubleYield`, `TestYieldThenImmediateAcquireByPeer`,
`TestYieldedRepromotesInPlace`, `TestYieldPeerWroteRepromoteRestores`,
`TestOnDemandFollowerNeverBackgroundPromotes`, and the regression guard
`TestCrashedLeaderStaysEagerUnderOnDemand`. The chaos soak
(`TestChaosSingleWriterDurability`) additionally injects random yields between write bursts,
so its single-writer, no-reader-regression, and every-acked-row-survives assertions all hold
across yields. Over a real lease and MinIO under the `integration` tag:
`TestYieldHandoffS3` (yield over real conditional writes; a peer acquires immediately and
round-trips data both directions).

## 11. An encrypted replica's objects are ciphertext, and a wrong or absent key fails cleanly

With `Config.EncryptionKey` set, every LTX object s3lite writes under `BackupTo` leaves
the process already encrypted — on every path, because `newReplicaClient` is the only
constructor of a replica client, so replication, restore, the follower's incremental
advance, the latest-TXID probe and remote compaction all go through the same decorator.
Nothing on the replica is plaintext, and reading it without the right key yields a
**typed error, never data and never something that looks like a corrupt database**:
`ErrKeyMismatch` for a wrong key, `ErrReplicaEncrypted` for a missing one,
`ErrObjectNotEncrypted` for a plaintext object under `RequireEncrypted`.

Three properties carry the weight:

- **Sizes reported upward are plaintext sizes, by exact arithmetic.** litestream treats
  a listed size as load-bearing — its `ResumableReader` compares it against its offset
  to tell a premature EOF from a real one, and restore rejects an object smaller than an
  LTX header — so the decorator converts every listed size rather than estimating.
- **A read can start at an interior boundary.** Recovery from a dropped connection
  reopens at an arbitrary *plaintext* offset, which is why the format is framed AEAD and
  not a single seal, and why the resume path is tested through a real restore against a
  client that cuts the body mid-object.
- **Tampering is an error, not bytes.** Each frame authenticates over the object header,
  its own index, and a final-frame flag, and its key is derived from the object's
  identity (`level ‖ minTXID ‖ maxTXID`). So a flipped byte, a truncated or dropped
  trailing frame, a reordered or duplicated frame, and a body moved between two object
  names all fail. What a *streaming* AEAD promises is precisely this: a frame is
  released only after it authenticates, so a tampered object yields at most an authentic
  strict prefix and then an error — never modified bytes, never the whole object. That
  is sufficient here because restore builds a temp file it renames only on success, so a
  failed read leaves no partially-restored database.
- **The frame layout itself is pinned against committed golden vectors, not just
  round-tripped.** Every test above seals with the same code that opens, so a changed
  derivation, header layout, or nonce/AAD composition would still pass them all — a
  self-consistent format change orphans every existing replica with the suite green.
  `testdata/golden.json` fixes the salt and asserts exact derivation, header, and
  object-ciphertext bytes independent of round-tripping.

Encryption is opt-in and inert when unconfigured: with no key, no wrapper is installed
at all and the bytes on the wire are what they were before the feature existed. What it
does **not** hide is stated plainly in the README (object names, sizes, timestamps,
`lock.json`, local files); the only lock-file change is that an encrypted instance with
no explicit `Owner` publishes an opaque random id instead of a hostname. Key rotation is
deliberately out of scope — the per-object salt means in-place rotation would be a full
replica rewrite.

*Enforced by:* the format itself — `TestEncryptSizeArithmetic` (every plaintext length
across several frames, against really-sealed bytes),
`TestEncryptRoundTrip`, `TestEncryptRangedReads`, `TestEncryptTamperIsAlwaysAnError`,
`TestEncryptIdentityBinding`, `TestEncryptWrongKey`, `TestEncryptSaltIsPerObject`,
`TestEncryptCiphertextRevealsNothing`, `TestParseEncHeader`, and — against fixed-salt
golden vectors rather than a round-trip — `TestEncryptGoldenVectors`. The decorator against a
real backend — `TestEncryptedClientRoundTripFileBackend`,
`TestEncryptedClientSingleFrameObject`, `TestEncryptedClientKeyCache`,
`TestEncryptedClientRewrittenObjectSelfHeals`, `TestEncryptedClientMixedMode`,
`TestEncryptedClientDelegates`, and — the resume proof, driven through litestream's own
`ResumableReader` with a connection-dropping client —
`TestEncryptedRestoreResumesThroughDroppedConnections`. End to end —
`TestEncryptedReplicaRoundTrip`, `TestEncryptedReplicaSurvivesCompaction` (write,
restore, compact, restore again), `TestEncryptedReplicaRetentionExpiresSuperseded`,
`TestEncryptedReplicaKeyHandling` (wrong and absent keys both typed, neither leaving a
partial database), `TestEncryptedReplicaMixedWindow`, `TestEncryptedFollowerRefresh` (a
follower catches up encrypted, and one without the key fails cleanly),
`TestEncryptedInstanceOwnerIsOpaque`, `TestEncryptedConfigValidation`. The opt-in proof
is `TestUnencryptedReplicaInstallsNoWrapper` — plus the entire suite above running
unchanged. Encryption does not disturb the lifecycle:
`TestChaosSingleWriterDurabilityEncrypted` runs the whole chaos soak with a key set, so
invariants 1, 2, 5 and 7 hold through encrypted handoffs, promote restores and the final
restore. Over real object storage under the `integration` tag:
`TestEncryptedReplicaRoundTripS3` (the bucket holds only ciphertext; the
`litestream-timestamp` metadata the fork's second patch preserves is present and real;
wrong and absent keys are typed) and `TestEncryptedLeaseHandoffS3` (two encrypted
instances hand the writer role back and forth over one real lease, and `lock.json` stays
plaintext but carries no hostname).

(Encryption relies on the second patch in the litestream fork; see
`docs/litestream-fork.md`.)

---

## 12. A replica a build is too old to read says so, and is never called corruption

A replica written by a newer s3lite fails with `ErrReplicaFormatNewer` — "upgrade" —
rather than with whatever internal the LTX decoder happened to raise. The old binary is
the one that has to deliver this message, so it cannot be told about the format that
defeated it; the classification is made from evidence it already holds.

That evidence is what makes it a reading rather than a guess. The probe runs only on an
already-failed read, and only after the object's bytes are known to be **authentic** —
its frames authenticated under the configured key, or the replica is plaintext and there
was never anything to authenticate. Authentic bytes are exactly what some writer wrote,
because damage in transit or at rest fails the tag first (invariant 11). Bytes that are
authentic, announce themselves as LTX, and still do not parse can only come from a
format this build predates.

The classifier is bounded by what it declines to claim, which matters more than what it
claims: a body that is not LTX at all, and a truncated one, both stay unattributed and
leave the original error to speak. A newer format is not short, it is unfamiliar —
telling someone to upgrade over a damaged replica would send them away from the real
problem. Only the header and the first page are read, since a format a reader is behind
fails at the first structure it does not know.

*Enforced by:* `TestReplicaWrittenByANewerBuildIsNamed` (keyed and plaintext: a real
object of the replica's own, re-sealed with an undefined page-header flag bit set, is
named and leaves no partial database — the shape of the real incident, an ltx v0.5.2
writer's `PageHeaderFlagSize` meeting a v0.5.1 reader) and
`TestProbeFormatCauseDeclinesWhatItCannotAttribute` (the quiet cases: parses-here,
not-LTX, empty, truncated mid-header and mid-page).

---

## The chaos soak

`TestChaosSingleWriterDurability` exercises invariants 1, 2, 5, and 7 together: four
`RoleAuto` instances over one lock and one replica, driven by a seeded stream of
writes, clean close+reopens, lock steals, and `TryPromote` storms. Throughout it
asserts at most one leader per settle and that no reader's view of durable rows
regresses; at the end it restores a fresh instance and checks that every
acked-and-synced row survived with the database intact. The seed is fixed and printed
on failure so any failure reproduces.

`TestChaosSingleWriterDurabilityEncrypted` runs the identical scenario with a
`Config.EncryptionKey` set, so the same invariants are asserted while every read and
write on every path goes through the encrypting client (#11).
