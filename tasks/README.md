# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- [crash-reacquire-rewind-repro.md](crash-reacquire-rewind-repro.md) — a
  consumer on v0.5.0 observed a fresh restore missing a successor's entire
  synced tenure after SIGKILL → same-path reacquire → clean Close; build the
  full-fidelity repro (real kill, real S3, dirty WAL) and either land it as
  the missing #9 regression or fix what is still live.
- [promote-outcome-api.md](promote-outcome-api.md) — additive API exposing
  whether a writer entry (promote or Open-direct) restored the replica or
  resumed the local file in place; lets a consumer reconciling derived state
  after a possible rewind skip the needless pass on self-succession.

(Landed: **open-direct-fork-guard** — close the sibling of local-ahead-promote: a
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
