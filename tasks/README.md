# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- **[local-ahead-promote.md](local-ahead-promote.md)** — promotion must never
  rewind local committed state the replica has not seen: a leased writer that
  crashes and reopens currently loses its unshipped tail to its own successor's
  restore. Builds on the follower-refresh position machinery (now landed).

(The incremental-follower-refresh task — apply only new LTX per tick via litestream's
`Restore(Follow)` resume, on the `atmin/litestream@v0.5.15-s3lite.1` fork; see
`../LITESTREAM-FORK.md` — has landed. The correctness-hardening task — fencing,
durability, and adversity gaps — has landed.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
