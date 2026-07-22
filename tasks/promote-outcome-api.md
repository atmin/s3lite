# Promote outcome — expose restored vs resumed-in-place to consumers

## Why

A consumer that layers "was my state possibly rewound?" logic on top of s3lite
can observe only `Generation()`, and the generation conflates the two writer-entry
outcomes that matter to it:

- **takeover restore** — the local file was replaced by the replica's state;
  anything the previous holder acked but had not synced is gone (the documented
  window loss). Generation > 1.
- **self-succession resume** — the local committed tail was kept in place and
  ships up; nothing was discarded. Also generation > 1 (exactly persisted+1).

The concrete shape: a consumer that keeps state derived from the database
(caches, externally stored blobs, queued deletions) must treat a restore as a
possible rewind — pause destructive maintenance (e.g. garbage collection) and
run a reconciliation pass before trusting the metadata again. The only signal
available today is "generation > 1 at promote". That rule is sound, but it
also fires on every same-machine crash-restart (self-succession), where s3lite
provably kept the tail and nothing needs reconciling: a needless freeze +
reconciliation cycle per plain restart. s3lite already computes the decision
(`promoteNeedsRestore`, `openDirectNeedsRestore`) and logs it — it just does
not expose it.

## What is true today (read before designing)

- `promote()` (lease.go) takes the decision from `promoteNeedsRestore` and runs
  `rebuildLocalFromReplica` only on restore; `Open`'s direct-acquire path takes
  it from `openDirectNeedsRestore` and runs `restoreLocalFromReplica`. Both log
  the outcome with its reason; neither records it anywhere a caller can read.
- `OnPromote(func())` carries no arguments; changing its signature breaks the
  public API. `Open` returns only `(*DB, error)`.
- Out of scope: `Open`'s restore-if-missing when no local file exists, and a
  follower's refresh swaps — neither is a writer-entry decision. Scope is the
  two paths above.

## Sketch (settle the shape at pickup)

Additive only. Candidates, roughly in order of preference:

- A method — `LastPromoteRestored() bool`, or a small accessor returning
  `PromoteOutcome{Restored bool, Generation int64}` — valid after
  Open-as-writer and after each promotion, stored under `mu` beside `isLeader`.
- A second callback (`OnPromoteOutcome(func(PromoteOutcome))`) alongside
  `OnPromote` — more surface; only worth it if a consumer needs the outcome
  synchronously with the transition.

Whatever the shape: it must cover BOTH entry paths (loop/`TryPromote` promote
and Open-direct acquire), and a first-ever writer entry (no prior local file —
restored or freshly created) should read as restored — erring the same
conservative direction as the guards themselves.

## Verify

- Unit, reusing the invariant-9 scaffolding (`simulateCrash`,
  `installRestoreCounter`): self-succession promote reports resumed; takeover
  promote reports restored; Open-direct clean-restart resume reports resumed;
  Open-direct takeover reports restored; fresh first Open reports restored.
- README (leasing section) + INVARIANTS.md #9 document the accessor.
