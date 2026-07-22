# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- **[incremental-follower-refresh.md](incremental-follower-refresh.md)** — make
  follower refresh apply only the LTX committed since its current position, instead
  of re-restoring the whole DB every tick. Approach DECIDED (2026-07-22): took the
  litestream fork carrying the resume-fix (`atmin/litestream@v0.5.15-s3lite.1`, see
  `../LITESTREAM-FORK.md`), which unblocks Option A — drive litestream's own
  `Restore(Follow)` resume against a private follow-file and publish via copy-and-swap.
  Re-probe against v0.5.15 passed; the self-contained LTX-apply port is dropped.
  Items 1–4 are ready to build.
- **[local-ahead-promote.md](local-ahead-promote.md)** — promotion must never
  rewind local committed state the replica has not seen: a leased writer that
  crashes and reopens currently loses its unshipped tail to its own successor's
  restore. Builds after the refresh task (reuses its position machinery).

(The correctness-hardening task — fencing, durability, and adversity gaps — has landed.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
