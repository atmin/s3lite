# Notes — the evidence base

Field notes from reading the source s3lite sits on — litestream and ltx — and
from the failures that reading explained. These are the *evidence* the decisions
in [../../INVARIANTS.md](../../INVARIANTS.md) and the workarounds in the code
cite: what upstream actually does, and why s3lite guards rather than trusts it.
Not decisions themselves, and not a to-do list.

Each note names the upstream version it was read against — upstream moves, and a
note that outlives its version is worse than none. The pinned versions are in
[../../go.mod](../../go.mod); the fork's own patches are the
[patch ledger](../litestream-fork.md), a separate thing: a ledger row is a fix we
*carry*, a note here is a hazard we *route around*.

- [litestream-verify-unsoundness.md](litestream-verify-unsoundness.md) —
  `db.verify()` resumes past a single external WAL restart, silently dropping the
  uncaptured frames. Why replicated writer connections set
  `wal_autocheckpoint(0)`; INVARIANTS.md #9.
- [ltx-compact-error-swallowing.md](ltx-compact-error-swallowing.md) —
  `ltx.Compactor.Compact` drops input-header errors, so *any* unreadable restore
  input surfaces as `decode header: EOF`. Why `replica.go` re-probes the restore
  plan to name an encryption cause; INVARIANTS.md #11.

Both are upstream-issue candidates, unfiled.
