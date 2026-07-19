# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- **[incremental-follower-refresh.md](incremental-follower-refresh.md)** — *(SHELVED)*
  make follower refresh apply only the LTX committed since its current position, instead
  of re-restoring the whole DB every tick. The design spike ran and settled the
  approach: litestream's native incremental paths are both dead ends (per-tick
  `Restore(Follow)` resume is rejected once a snapshot sits behind the position;
  in-process fcntl locks block a consistent copy of a live-followed file), leaving only
  a self-contained LTX-apply reimplemented in s3lite (~150–200 lines, proven feasible).
  Shelved rather than built now given the port's size and drift risk; the file has the
  evidence and the ready-to-build plan.

(The correctness-hardening task — fencing, durability, and adversity gaps — has landed.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
