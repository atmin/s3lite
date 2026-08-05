# Restore progress callback — live percentage and stall detection

## The idea

A `Config.OnRestoreProgress func(applied, total int64)` callback, fired while a
restore runs, so an embedding application can show a live percentage / ETA and
detect a *stalled* restore (bytes not advancing ⇒ a hung range read). It
complements the restore lifecycle logging
([../restore-observability.md](../restore-observability.md)): the log
lines say "restore started / finished in Dms"; this says "42%, still moving."

Mechanism:

- `total` comes from litestream's restore plan (`CalcRestorePlan` → `infos`,
  `Σ info.Size`), known once the plan is computed.
- `applied` comes from a counting reader wrapping each `ResumableReader` as bytes
  flow during apply.
- Both live *inside* litestream's `replica.Restore`, which today returns only an
  error — so this needs a progress hook added to the vendored fork (an optional
  `OnProgress` on `RestoreOptions`, or a channel), then s3lite plumbs it from
  `restoreDB` up to the `Config` callback.
- It must be a **pre-`Open`** `Config` field, not a pollable accessor: the
  initial cold restore runs inside `Open` before the `*DB` handle exists, so the
  cold path — the one that matters most — can only be observed over a channel
  configured before the call. (Same structural constraint the observability task
  records.)

## Why it's interesting

- The initial cold restore is the one `Open` step that can take minutes (a large
  database pulled from the replica on a fresh machine). A live bar/ETA is a real
  UX win over an opaque "restoring…" spinner for an application that blocks on
  `Open` at startup.
- **Stall detection is the more valuable half.** "Alive but making no progress"
  — a hung range read — is invisible to a process-liveness check; a callback
  whose `applied` stops advancing catches it.
- It reuses litestream's already-computed plan; the delta is a counting reader
  plus a callback surface.

## Why it's parked (not a task)

- **Unproven need.** The restore-observability logging (start/complete +
  duration) already gives an embedding app enough to show a "restoring…" state
  and to log the wait. No consumer has asked for a *percentage* rather than a
  *phase*.
- **It's a fork change.** It touches the `atmin/litestream` fork
  ([docs/litestream-fork.md](../../docs/litestream-fork.md)), not just s3lite — a heavier
  maintenance and upstream-rebase surface than a logger line, worth taking on
  only for a concrete requirement.
- **Payoff scales with database size**, which is consumer-specific: a
  metadata-sized DB usually restores fast enough that a phase beats a bar.

## If we ever pick this up

- **Prerequisite:** restore-observability landed first — the logging is the cheap
  80%; this is the expensive 20% layered on top.
- **Settle the fork surface first:** keep the litestream hook minimal and
  optional (an `OnProgress func(applied, total int64)` on `RestoreOptions`, nil
  by default) so rebasing onto upstream stays clean; document it in
  docs/litestream-fork.md.
- **Then s3lite:** a `Config.OnRestoreProgress` field, plumbed through
  `restoreDB` (which will already carry the logger from the observability task);
  fire it for both the initial-`Open` restore and the promote/open-direct
  rebuild.
- **If stall detection — not a UI bar — is the real goal, weigh the cheaper
  alternative first:** a restore watchdog/deadline inside s3lite (a timeout
  around `restoreDB`, or a no-progress kill switch) answers "detect a hung
  restore" with *no* fork change. Decide which problem is actually being solved
  before committing to the callback.
