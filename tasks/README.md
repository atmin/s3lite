# Tasks — the frontier

Intended-to-implement units of work, each self-contained (pickable without prior
context). One file per unit, `Why → What is true today → Sketch → Verify`,
**deleted once it lands** — the commits and [../INVARIANTS.md](../INVARIANTS.md)
are the record, so this list never becomes a changelog.

- [restore-observability.md](restore-observability.md) — log the restore operation as a
  lifecycle event on the application logger; the initial cold restore on first `Open` is
  silent today. A live progress callback is noted but deferred (needs a litestream-fork hook).

Candidates worth capturing but not committed to live in [ideas/](ideas/) —
promote one up to this directory when it's ready to land.
