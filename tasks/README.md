# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- **[incremental-follower-refresh.md](incremental-follower-refresh.md)** — make
  follower refresh apply only the LTX committed since its current position, instead
  of re-restoring the whole DB every tick. Unshelved 2026-07-22: the spike's plan
  (a self-contained bounded LTX-apply, Items 1–4) is ready to build against the
  still-pinned litestream v0.5.14 — or, newly on the table, a litestream fork
  carrying the small resume-validation fix that shrinks the port to a
  `Restore(Follow)` resume.
- **[local-ahead-promote.md](local-ahead-promote.md)** — promotion must never
  rewind local committed state the replica has not seen: a leased writer that
  crashes and reopens currently loses its unshipped tail to its own successor's
  restore. Builds after the refresh task (reuses its position machinery).

(The correctness-hardening task — fencing, durability, and adversity gaps — has landed.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
