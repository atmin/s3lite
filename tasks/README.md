# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- **[open-direct-fork-guard.md](open-direct-fork-guard.md)** — close the sibling of
  the landed local-ahead-promote fix: a returning leased writer whose lease has
  *already expired* by reopen takes the `Open`-direct-acquire path and resumes its
  local file unguarded, so a successor that took over and finished in the interim
  could leave it resuming a forked history (corruption, not just loss). Its own spike
  because the generation signal is ambiguous after a clean release (INVARIANTS.md #8) —
  guarding it naively would regress clean restarts into full restores.

(Landed: **incremental-follower-refresh** — apply only new LTX per tick via
litestream's `Restore(Follow)` resume, on the `atmin/litestream@v0.5.15-s3lite.1`
fork; see `../LITESTREAM-FORK.md`. **local-ahead-promote** — guard promote against
silently rewinding a self-succeeding writer's committed tail; INVARIANTS.md #9. The
correctness-hardening task — fencing, durability, and adversity gaps.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
