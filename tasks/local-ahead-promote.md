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

## Item 0 — [ ] Design spike: local position source + soundness rule

Deliverable: this section filled in with the chosen mechanism, the rule, and
the divergence cases it does and does not protect, each mapped to a planned
test.

## Item 1 — [ ] Guarded promote

Restore only when the replica is ahead by the Item 0 rule; otherwise promote
in place — flip the handle writable, start replication from the local file,
never touch its bytes. An unreadable or ambiguous local position falls back to
today's restore: conservative, and loud in the log. The existing atomic-swap
path stays untouched for the restore branch (INVARIANTS.md #6/#7).

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
