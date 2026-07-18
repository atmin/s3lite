# Ideas

Design directions worth capturing but **not** committed to. Unlike
[../tasks/](../tasks/) — which holds intended-to-implement units of work that get
deleted once they land — an idea here may sit indefinitely, get refined, or be
dropped. **Nothing in this directory should be implemented without an explicit
decision to promote it into `tasks/` first.**

Each file states the idea, why it's interesting, and — importantly — the reasons
it isn't a task yet (the tradeoff, the constraint, or the missing prerequisite).

- [cooperative-yield.md](cooperative-yield.md) — bounded-fairness writer handoff: a
  waiter signals intent, the holder voluntarily yields the lease at a safe point,
  with min/max hold to prevent both thrashing and starvation. **Do not implement yet.**
- [forwarded-writes.md](forwarded-writes.md) — followers forward writes to the
  current leader (rqlite-style) for a multi-writer *feel* with one physical writer.
  **Do not implement yet** — platform-dependent and against the scale-to-zero grain.
