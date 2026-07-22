# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

_No scheduled tasks right now._

**Discovered, not yet scheduled:** the `Open`-direct-acquire path has the same
divergence hazard the local-ahead-promote task fixed for `promote()` — a returning
leased writer whose lease has *already expired* by reopen resumes its local file
unguarded, so a successor that took over and finished in the interim could leave it
resuming a forked history. Not folded into local-ahead-promote because the generation
signal is ambiguous after a clean release (INVARIANTS.md #8), so guarding it naively
would regress clean restarts into full restores — it needs its own spike.

(Landed: **incremental-follower-refresh** — apply only new LTX per tick via
litestream's `Restore(Follow)` resume, on the `atmin/litestream@v0.5.15-s3lite.1`
fork; see `../LITESTREAM-FORK.md`. **local-ahead-promote** — guard promote against
silently rewinding a self-succeeding writer's committed tail; INVARIANTS.md #9. The
correctness-hardening task — fencing, durability, and adversity gaps.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
