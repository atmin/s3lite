# Yield lease — voluntary release-on-idle, promotion on demand

Promoted from [../ideas/cooperative-yield.md](../ideas/cooperative-yield.md) —
the **release-on-idle slice only**. The want-write marker, ticketing, and
min/max-hold fairness machinery stay parked: they are needed only when a peer
wants the pen while the holder is *actively* writing, and no consumer has that
workload. This task covers the workload a consumer does have: a bursty,
migratory writer — write where you are, read everywhere — where the holder
goes quiet between bursts and a peer's first write should acquire immediately
instead of waiting out the TTL or being refused indefinitely.

Both prerequisites the idea set for itself are met: incremental follower
refresh shipped (handoff is cheap), and a concrete consumer wants the token to
migrate between live instances.

## Why

Today the holder is sticky: it renews until it dies, deploys, or `Close`s.
`TryPromote` already collapses the takeover gap when the lock is *free* — but
nothing ever frees it while the holder lives. A live-but-idle holder pins the
pen, so a peer's write returns read-only forever (or, after a crash, waits the
full TTL). Release-on-idle inverts the steady state: the lock is held only
during active writing; between bursts it is free, and whoever writes next
acquires instantly through the promotion path that already exists.

The consumer owns idle detection (it knows its own activity); s3lite owns the
mechanism: a safe voluntary release that keeps the instance alive as a
follower, and a promotion mode that doesn't immediately re-grab what was just
released.

## What is true today (read before designing)

- Only two paths give up the lease, both wrong for this: `CloseContext` does
  the right release (final `SyncAndWait` → clean-shutdown marker →
  `ReleaseLease` last) but tears the whole instance down; `demote` keeps the
  instance alive as a follower but is fencing — it stops replication with a
  cancelled context (**no final push — drops the unshipped tail**), never
  deletes the lock object, and fires `OnDemote`.
- `followerTick` background-promotes greedily: any follower acquires a free
  lock within ~TTL/3. A bare yield is therefore **self-defeating** — the
  yielder's own loop (or an idle peer's) re-grabs the pen within a tick, and
  the system ping-pongs restores while writes elsewhere still see read-only.
- `RoleAuto` `Open` direct-acquires when the lock is free. This is load-bearing
  for bootstrap (first writer creates/migrates schema) and for crash recovery
  (an expired ex-leader re-enters via `openDirectNeedsRestore`, resuming and
  shipping its tail).
- `promoteNeedsRestore` accepts only the generation self-succession proof; the
  clean-marker + replica-unchanged proof lives only in `openDirectNeedsRestore`
  (the `Open`-direct path). A clean release resets the generation to 1
  (INVARIANTS.md #8), so a yielded instance re-promoting in-process would
  always fail the generation check and needlessly restore the state it just
  finished syncing up.
- Concurrency discipline: renew, follower-tick, and refresh are confined to
  the single `leaseLoop` goroutine; `TryPromote` runs on the caller guarded by
  `promoteMu`; `tryRenew` is deadline-bounded by `ExpiresAt − TTL/6` so a
  stalled holder demotes before a successor can acquire (INVARIANTS.md #2).
- `becomeLeaderLocked` removes the clean-shutdown marker at tenure start and
  records the lease generation in `<LocalPath>.leasegen`; a crash therefore
  leaves `.leasegen` present with **no** marker — locally distinguishable from
  a clean exit without any network probe.

## Sketch (settle exact shapes at pickup)

Three additive pieces; existing consumers see zero behavior change unless they
opt in.

**1. `YieldLease(ctx context.Context) error`** — leader-only voluntary
handoff that stays alive as a follower. Sequence:

1. Renew once up front (a fresh full-TTL window to complete the yield in).
2. Fence the connector read-only — the same hard fence demote uses, so
   in-flight transactions cannot commit past this point (rides invariant #3's
   machinery).
3. Final sync (`SyncAndWait`), bounded by `ExpiresAt − TTL/6` like `tryRenew`;
   assert local tip == replica tip after it (belt-and-braces — the fence makes
   one sync sufficient).
4. Stop replication with a **live** context (the final push is wanted here —
   the exact opposite of demote).
5. Write the clean-shutdown marker with the synced replica TXID (reusing
   `.cleanshutdown` means a yielded instance that is *restarted* also resumes
   for free through the existing `openDirectNeedsRestore` path, unchanged).
6. `ReleaseLease` (If-Match delete) — release last, after the sync, exactly
   like `Close`.
7. Flip to follower: `isLeader = false`, seed `lastRefreshPos` to the marker
   TXID (next refresh tick is a no-op unless a successor writes), enter
   passive mode (piece 2).

Failure atomicity: a sync failure or deadline breach **aborts** the yield —
attempt a renew; on success un-fence and stay leader (the loop resumes
renewing), on failure take the standard demote path. Never a half-state.
`ReleaseLease` returning `ErrLeaseNotHeld` (stolen mid-yield) completes as
follower — the tail is already shipped, so unlike a real demote there is
nothing at risk — and does **not** fire `OnDemote`; no yield path ever fires
`OnDemote` (it means "lost the lease while relying on it"; a yield is
relinquishment). Called while not leader: `ErrNotLeader` sentinel (double
yield included).

Serialization: yield must be strictly serialized with renew, follower-tick,
refresh, and `TryPromote`. Suggested mechanism: execute it on the `leaseLoop`
goroutine via a request/reply channel (a new `select` case, immediate — not
next-tick) while holding `promoteMu`; that makes the no-concurrent-transition
property true by construction rather than by lock choreography.

**2. `Config.OnDemandPromotion bool`** — when set, the follower branch of
`leaseLoop` skips background promotion entirely; a follower promotes only via
explicit `TryPromote` (the consumer's write path). `Open`'s direct acquire is
deliberately **unchanged** — bootstrap (schema creation/migration) and the
expired-crash re-entry keep working exactly as today, at the cost of one
harmless acquire-then-idle-yield cycle per boot when the lock happens to be
free.

The exception that makes passivity sound — **eager recovery**: an instance
that may hold an unshipped local tail must keep background-promoting until the
tail's fate is settled, or on-demand mode turns a crash into silent data loss
(the tail sits parked until a peer's takeover discards it). Eager mode is
entered when (a) a follower `Open` finds `.leasegen` present with no
clean-shutdown marker — a crashed ex-leader whose lock had not yet expired —
or (b) an in-process `demote` fires (its cancelled-context stop may have
dropped a tail). It is cleared on the next successful writer entry (the tail
ships via self-succession, or the restore proof discards it). A cleanly
yielded instance has marker + `.leasegen` both present ⇒ passive, correctly:
the marker is written only after the tail is fully shipped.

**3. Extend `promoteNeedsRestore` with the clean-marker proof** — hoist the
marker + replica-unchanged check shared with `openDirectNeedsRestore` so the
in-process promote path accepts it too: a yielded instance re-promoting with
no interim writer resumes in place (no restore, no re-download of state it
just pushed). One replica probe, only when the generation proof fails and a
marker exists; a probe failure errs to restore, the same conservative
direction as everything else in #9.

## Invariants (write these into INVARIANTS.md before the code — new #10, plus
a #9 extension)

The parked idea demanded a written fencing-safety argument up front; this is
its skeleton:

- **Yield is a clean handoff, never a data hazard.** The lock object is
  deleted only after fence → final sync → replication stopped, so at the
  moment of release `replica tip == local tip`: a successor acquiring
  immediately sees every acked write. Single-writer is preserved — yield only
  ever *removes* a holder, and the If-Match delete cannot remove a
  successor's lock.
- **A yield that cannot complete aborts atomically**: the instance is either
  still the leader with renewals running, or took the standard demote path —
  never fenced-but-holding, never released-but-unsynced.
- **Yield is not demote**: it never fires `OnDemote` and never drops a tail;
  demote's semantics (push-free fencing, lock left in place) are untouched.
- **Passivity never parks an unshipped tail**: `OnDemandPromotion` defers
  promotion only for instances whose local state is provably shipped (clean
  marker) or was never a leader's; eager recovery governs the rest.
- **#9 extension**: the promote-path resume decision now also accepts the
  clean-marker + replica-unchanged proof; the erring direction (restore when
  unproven) is unchanged.

## Verify

Unit, on the fake-leaser scaffolding (`newLeaserFunc`), each an adverse case:

- `TestYieldReleasesOnlyAfterReplicaCaughtUp` — instrument release; assert
  lag == 0 at the moment of the delete, under an injected slow sync.
- `TestWriteRacingYieldEitherFencedOrShipped` — a transaction in flight when
  yield starts either fails to commit (fenced) or its rows are in the replica
  before release; no acked-but-unshipped row exists post-yield.
- `TestYieldAbortsOnSyncFailureStaysLeader` — replica outage mid-yield:
  still leader, un-fenced, writes work, renewals continue.
- `TestYieldBoundedByLeaseExpiry` — a black-holed sync aborts before
  `ExpiresAt`; never fenced past expiry with the lock still ours.
- `TestYieldStolenMidYieldCompletesAsFollower` — release returns
  `ErrLeaseNotHeld`; instance ends follower, `OnDemote` NOT fired.
- `TestYieldNotLeader` / `TestDoubleYield` — sentinel, idempotence.
- `TestYieldThenImmediateAcquireByPeer` — a peer acquires instantly after
  yield (no TTL wait) and its restore contains every pre-yield row.
- `TestYieldedRepromotesInPlace` — yield, no interim writer, `TryPromote`:
  resumed (restore count 0, `LastPromoteOutcome.Restored == false`).
- `TestYieldPeerWroteRepromoteRestores` — interim writer advanced the
  replica: re-promote restores and sees the peer's rows.
- `TestOnDemandFollowerNeverBackgroundPromotes` — config on, lock free,
  ticks elapse: still follower; explicit `TryPromote` still promotes.
- `TestCrashedLeaderStaysEagerUnderOnDemand` — **the regression guard**:
  dirty tail (`.leasegen`, no marker), config on: background re-acquire
  still happens after expiry, self-succession resumes, tail ships.

Chaos: add random yields between write bursts to
`TestChaosSingleWriterDurability`'s op stream; the existing assertions (at
most one leader per settle, no reader regression, every acked-and-synced row
survives the end restore) must hold unchanged.

Integration (`-tags integration`, MinIO): `TestYieldHandoffS3` — yield over
real conditional writes, peer acquires immediately, round-trips data both
directions. For the steal variant remember MinIO ignores If-Match on an
absent key: steal by overwrite, not delete.

Docs: README leasing section (`YieldLease`, `OnDemandPromotion`, when to use
them), INVARIANTS.md #10 + #9 extension, and flip
[../ideas/cooperative-yield.md](../ideas/cooperative-yield.md) to point here
for the promoted slice.
