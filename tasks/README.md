# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- [promote-outcome-api.md](promote-outcome-api.md) — additive API exposing
  whether a writer entry (promote or Open-direct) restored the replica or
  resumed the local file in place; lets a consumer reconciling derived state
  after a possible rewind skip the needless pass on self-succession.

(Landed: **restore-stale-litestream-state** — the restore paths
(`restoreLocalFromReplica`, `rebuildLocalFromReplica`) now clear litestream's
`.<name>-litestream/` position state (`removeLitestreamMeta`) alongside the SQLite files,
so a returning ex-leader cannot resume replication from its discarded lineage's L0 — a
position that can sit ahead of the restored replica and ship the fork back over it. The
in-process repro showed litestream currently *masks* the hazard (its `verify` re-snapshots
when the restored WAL no longer matches the stale L0), but that heuristic is
known-unsound, so s3lite clears the position rather than depend on it; INVARIANTS.md #9.
**crash-reacquire-rewind-repro** — the consumer-observed rewind reproduced on
HEAD and was a live bug in every release through v0.6.0, leased or not: SQLite's
default per-connection autocheckpoint could restart a large crash-recovered WAL before
litestream's lazy first sync captured the dead tenure's tail, silently dropping any
page allocation in the skipped span from the lineage. Fixed by `wal_autocheckpoint(0)`
on replicated writer connections (litestream owns checkpointing); the full-fidelity
crash-harness scenario (real SIGKILL, dirty WAL, real lease, MinIO) now enforces
INVARIANTS.md #9 end-to-end. **open-direct-fork-guard** — close the sibling of local-ahead-promote: a
returning leased writer whose lease had *already expired* by reopen takes the
`Open`-direct-acquire path; it now resumes in place only on provable self-succession
(lease generation) or a clean restart (a `.cleanshutdown` marker whose replica has not
advanced), and otherwise restores rather than shipping a forked history; INVARIANTS.md
#9. **incremental-follower-refresh** — apply only new LTX per tick via litestream's
`Restore(Follow)` resume, on the `atmin/litestream@v0.5.15-s3lite.1` fork; see
`../LITESTREAM-FORK.md`. **local-ahead-promote** — guard promote against silently
rewinding a self-succeeding writer's committed tail; INVARIANTS.md #9. The
correctness-hardening task — fencing, durability, and adversity gaps.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
