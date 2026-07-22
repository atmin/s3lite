# Local-ahead promote — promotion must never rewind state the replica has not seen

## Why

The unleased writer's crash-restart story is s3lite's best one: reopen with the
existing LocalPath, nothing restores, litestream resumes from the local file and
ships whatever the crash left unreplicated — no committed write is lost. The
*leased* writer's story is currently worse: it restarts while its own stale
lease is still held, so RoleAuto opens it as a follower; when the TTL expires,
the loop promotes — and `promote()` → `rebuildLocalFromReplica` unconditionally
restores the replica over the local file. Every transaction the crashed writer
committed after its last replica sync — writes it acked to its callers — is
discarded *by its own successor on the same machine*. The replication-window
loss we document for permanent machine loss is being inflicted on a machine
that survived.

The invariant this task establishes: **no promotion may silently discard local
committed state the replica does not contain.** Restore on promote only when
the replica is genuinely ahead; otherwise promote in place and let replication
ship the local tail up, exactly like the unleased restart.

## What is true today (read before designing)

- `promote()` (lease.go) rebuilds from the replica unconditionally — first
  promotion, clean handoff, expiry takeover, and self-succession after a crash
  all take the same `rebuildLocalFromReplica` path.
- The refresh path does *not* hit the self-succession case: `seedRefreshPos`
  seeds `lastRefreshPos` from the replica's latest TXID at Open, so a follower
  serving a local-ahead file never refresh-swaps unless the replica *advances*
  — which means another writer exists and the lineage has genuinely moved on
  (see the catch below).
- The "where is the replica" half exists: `replicaLatestTXIDFunc` (replica.go)
  probes the max TXID across all levels. The "where am I locally" half does
  not: s3lite's `restoreDB` is non-follow and writes no `<path>-txid` sidecar,
  and an ex-writer's local file has no sidecar at all.
- tasks/incremental-follower-refresh.md (in progress) adds position sidecars
  and LTX-reading plumbing on the follower side — reuse it, don't duplicate.

## The catch — divergence and equal positions

TXIDs order a single lineage; an expiry takeover can fork one. If our crashed
writer sat at TXID 12 with 11–12 unshipped, and a successor restored from the
replica at 10 and wrote its own 11–13, then the replica's 13 > our 12 and
restoring is *correct* — our 11–12 became window loss the moment the successor
took over, and shipping them now would corrupt the lineage. Equal positions are
the nasty edge: a replica at 12 written by a successor is a different 12 than
ours. So "skip the restore when local ≥ replica" is unsound and "local >
replica" still mistakes a forked history for a safe one. The spike must decide
what makes the comparison sound — candidates: compare positions only when the
lease history proves no takeover happened in between (lease generation
semantics, or the lock object's absence after a clean release); an
ltx-header/checksum lineage check; or conservatively skip the restore *only in
the provable self-succession case* and document everything else as window
loss. Provable-self-succession-only is acceptable scope — it is the case that
actually recovers data.

## How to work this task

1. **Design spike first (Item 0).** Settle (a) how to read an ex-writer file's
   local position (litestream's local LTX/metadata state, a position record
   s3lite maintains on the writer side, or a transient `litestream.DB` open),
   and (b) the soundness rule from the catch above. Write the result into this
   file under Item 0 and get sign-off before implementing.
2. Then the items, each with the full gate (`go vet ./...`,
   `go test -race -count=1 ./...`, `go test -tags=integration ./...` — see
   TESTING.md for the Docker socket).
3. Keep README and INVARIANTS.md in sync in the same item that changes
   behavior.
4. Sequencing: after tasks/incremental-follower-refresh.md — its sidecar and
   LTX machinery is this task's foundation.

## Item 0 — [x] Design spike: local position source + soundness rule

**Result (2026-07-22): the sound signal is the lease *generation*, not a position
comparison. No local-position read is needed — which also avoids a soundness trap in
the position approach.** Get sign-off on this before Item 1.

### The key realization

Only the lease-holder writes (invariant #1). Therefore a **fork can only happen via a
lease acquisition** by some other instance. Every acquire over an expired lock bumps
`Lease.Generation` by exactly one (`s3/leaser.go`: `generation = existing.Generation +
1`); a renew *preserves* the generation (same file, line ~147). So a whole write
tenure carries a single generation, and any intervening writer is visible as a
generation bump. **The generation is the lineage token.** We do not need to read or
compare TXIDs/checksums to detect a fork; the generation already encodes "did someone
else take the pen."

### Why NOT the position/checksum approach (the task's original framing)

`litestream.DB.Pos()` returns `ltx.Pos{TXID, PostApplyChecksum}` — a real lineage
fingerprint — but it is computed from the local **L0 LTX** in the meta dir
(`db.go` `Pos()` → `MaxLTX()`), which **lags the SQLite WAL**: a crashed writer can
have committed frames in the WAL that litestream had not yet captured into L0. Reading
`Pos()` would then report a TXID *behind* the replica and we would wrongly restore,
discarding exactly the tail we are trying to save. Capturing the WAL first means
starting replication before deciding — the thing we are trying to gate. The
ltx-checksum lineage check has the same lag problem and is strictly more code. The
generation signal sidesteps all of it: it never looks at positions.

### Mechanism (local position source: none; instead persist the generation)

The writer records the generation of the lease it holds to a small local sidecar,
`<LocalPath>.leasegen` (decimal, fsynced), written in `becomeLeaderLocked` *before*
accepting writes. Renews keep the same generation, so it is written once per tenure.
For an ex-writer this file survives the crash (Open-as-follower never restores while
`LocalPath` exists, so it is untouched). It is orthogonal to litestream's sidecars and
is not touched by `removeLocalDBFiles`; `becomeLeaderLocked` always overwrites it with
the current generation, so a stale value after a restore-promote is harmless.

### Soundness rule (the promote decision)

At promote, we have just acquired the lease at `acquiredGen` (passed into `promote()`
by `tryPromoteOnce`). Let `persistedGen` be the value in `<LocalPath>.leasegen`.

- **`acquiredGen == persistedGen + 1` → PROMOTE IN PLACE** (skip the restore). This is
  provable self-succession: the lock we took over was still at `persistedGen`, so *no
  other instance acquired* since our tail was written, so the replica is exactly the
  prefix we shipped and our local file is a strict, same-lineage extension. Promoting
  in place = start litestream on the existing local file and let it ship the unshipped
  tail — **bit-identical to the unleased crash-restart path** (`Open` → no restore →
  `startReplicationLocked`), which already recovers committed-but-unshipped (and even
  committed-but-uncaptured-in-L0) writes.
- **anything else → RESTORE** (today's behaviour): `acquiredGen >= persistedGen + 2`
  (a successor acquired — and possibly forked — in between; the acquire alone proves
  the risk regardless of whether it wrote), a reset to `1` after a successor's clean
  release, or a missing/unreadable/`0` sidecar (no proof). Loud log on the fallback.

**Why `+1` is airtight:** any successor acquire sets the lock to `persistedGen + 1`
(theirs), so our subsequent acquire yields `>= persistedGen + 2`; a clean release
deletes the lock so our acquire resets to `1` (`!= persistedGen + 1` for
`persistedGen >= 1`). The *only* way to observe exactly `persistedGen + 1` is an
untouched lock — no intervening acquire. This is the same generation-monotonicity the
fencing invariant (#1/#2) already relies on; the "equal-TXID fork" nasty edge is
covered for free, because the forking successor had to acquire (→ bump), so we never
mistake its equal-TXID replica for ours.

### Divergence cases → tests (Item 2)

| Case | Generation seen | Decision | Test |
|---|---|---|---|
| Self-succession, no successor ever | `persistedGen + 1` | promote in place, tail shipped | *self-succession preserves the tail* |
| Fresh writer crash before first ship | `persistedGen + 1` | promote in place (recovers data an empty-replica restore would erase) | (covered by the self-succession test with an empty replica) |
| Successor took over + advanced replica (fork) | `>= persistedGen + 2` | restore, fork discarded | *takeover divergence still restores* |
| Missing / unreadable `.leasegen` | n/a | restore (conservative), logged | *unreadable position → restore fallback* |

### Scope note / deviation from the task's assumptions

This does **not** reuse tasks/incremental-follower-refresh's `.follow` sidecar / LTX
machinery (the task anticipated it would): the generation signal is sound without
reading any position, so pulling in LTX-reading would be dead weight. Recorded here so
the reviewer knows it was a deliberate call, not an oversight.

## Item 1 — [ ] Guarded promote (lease-generation self-succession)

Two parts:

1. **Persist the generation.** In `becomeLeaderLocked`, after `startReplicationLocked`
   succeeds and before accepting writes, write `lease.Generation` to
   `<LocalPath>.leasegen` (fsynced). This runs on every become-leader (first promote,
   clean handoff, expiry takeover, self-succession) so the value always reflects the
   generation the local tail is being written under. Add a small `readLeaseGen` /
   `writeLeaseGen` pair (mirror the existing sidecar helpers).

2. **Gate the restore in `promote()`.** Compute `needRestore` from the Item 0 rule
   (`acquiredGen == persistedGen + 1` → in place; else restore). Keep the existing
   `swapFiles(false, …)` wrapper — it flips the handle writable and bumps the
   generation so followers re-dial — but make its `fn` conditional: run
   `rebuildLocalFromReplica` only when `needRestore`, otherwise a no-op that leaves the
   local bytes intact. `becomeLeaderLocked` then starts litestream on the local file
   and ships the tail, exactly like the unleased restart. A missing/unreadable
   `.leasegen` falls back to restore, loud in the log. The restore branch and its
   atomic-swap path are unchanged (INVARIANTS.md #6/#7).

## Item 2 — [ ] Tests

Reuse the `lease_internal_test.go` seams; each test opens with the hazard it
pins:

- *Self-succession preserves the tail:* writer commits, dies without a final
  sync, reopens RoleAuto on the same LocalPath, promotes after the TTL — the
  unshipped transactions are present and subsequently replicate.
- *Takeover divergence still restores:* a successor advanced the replica; the
  returning machine's fork is discarded — pinned as the documented resolution,
  not left accidental.
- *Unreadable position → restore fallback,* logged.

## Item 3 — [ ] Docs + invariants

README durability note (a leased writer's crash-restart now matches the
unleased one); INVARIANTS.md gains the no-silent-rewind invariant naming its
tests; Config docs only if the spike demanded a knob.

## Out of scope

- Incremental promote (bandwidth) — already noted as a follow-up in the
  refresh task; this task is about *what* promote may discard, not how much it
  downloads.
- Multi-writer conflict resolution beyond the lease — divergence resolution
  stays "the replica lineage wins," made explicit rather than accidental.

## Done when

A leased writer that crashes and reopens on the same machine loses nothing an
unleased writer wouldn't; the divergence rules are written down and pinned by
tests; the full gate passes. Then delete this file and update tasks/README.md.
