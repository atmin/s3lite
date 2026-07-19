# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- **[incremental-follower-refresh.md](incremental-follower-refresh.md)** — make
  follower refresh apply only the LTX committed since its current position, instead of
  re-restoring the whole DB every tick. Design-first: the integration approach is open
  (litestream's only incremental path is in-place, infinite follow), so it opens with a
  design spike and a pause for sign-off.

(The correctness-hardening task — fencing, durability, and adversity gaps — has landed.)

See [../ideas/](../ideas/) for design directions captured but not scheduled.
