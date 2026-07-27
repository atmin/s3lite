# Restore observability — log the restore operation as a lifecycle event

## Why

s3lite logs its writer-lifecycle transitions on the application logger —
`"promoted to writer"` (lease.go:723), `"yielded lease; now a follower"`
(lease.go:406), `"follower refreshed"` (lease.go:659), and the promote-time
restore *decision* (`"promote will restore … (takeover)"`, lease.go:929;
`"open will restore …"`, lease.go:1013). But it never logs the restore
*operation itself*: no start, no completion, no duration or size. And the
**initial cold restore** on first `Open` — the restore-if-missing branch
(s3lite.go:385-391 → `restoreDB`, replica.go:103) — emits **nothing at all**,
not even a decision line, because it is not a promote decision.

That is the one lifecycle step that can take seconds to minutes: a large
database pulled from the replica on a machine that has never seen it. It runs
*inside* `Open`, before the handle is returned, so an embedding application that
blocks on `Open` at startup has no signal the restore is even happening — only a
hang indistinguishable from a stall. Any consumer that wants to surface a
"restoring…" state to its own users has nothing to attach it to.

logging.go:13 already claims restore is a logged lifecycle event alongside
promote/demote. For the initial-Open path that claim is currently false; this
task makes it true.

## What is true today (read before designing)

- **Initial restore:** `Open` (s3lite.go:385-391) calls `restoreDB`
  (replica.go:103) when the local file is missing. `restoreDB` builds a
  throwaway `litestream.Replica` and calls `replica.Restore` — no logging on
  `db.logger`. litestream's own restore-plan log is Debug (litestream
  replica.go:693) and s3lite gates litestream to WARN+ (logging.go:34), so even
  that is dropped. The path is silent end to end.
- **Promote / open-direct restore:** `promoteNeedsRestore` /
  `openDirectNeedsRestore` log the *decision* (lease.go:929, 1013), then
  `rebuildLocalFromReplica` / `restoreLocalFromReplica` run the restore via
  `restoreDBFunc` — again with no operation-level log. `restoreDBFunc ==
  restoreDB` in production; Open's initial restore calls `restoreDB` directly
  (the injection comment at replica.go:96-101), so both call sites funnel through
  the same function.
- **`restoreDB` takes no logger** — its signature is `(ctx, s3Cfg, rawURL,
  destPath)` and it returns only an error. `db.logger` is set during
  construction (s3lite.go:340-346), before the restore call site, so it is
  available to pass in.
- **The plan exists but only inside litestream:** `CalcRestorePlan` → `infos`
  (litestream replica.go:688) carries the file count and per-file `.Size`.
  `replica.Restore` returns only an error, so byte/file totals are not handed
  back to s3lite today.
- **Structural constraint:** the initial `restoreDB` runs before the `*DB`
  handle exists, so anything a consumer observes must arrive over a channel
  configured *before* `Open` — `cfg.Logger`, or a `Config` callback — never a
  post-hoc accessor on the returned handle. This is why "log it" (a pre-`Open`
  channel) is the natural fit and a pollable getter is not.

## Sketch (settle the shape at pickup)

Additive only; `cfg.Logger` is already the lifecycle channel every consumer
filters.

- **Core: log the restore operation on `db.logger`.** An Info
  `"s3lite: restoring from replica"` (with the replica URL) before the restore,
  and `"s3lite: restore complete"` with elapsed duration after. Do it once,
  inside a logger-aware `restoreDB` (pass `db.logger` in), so it covers *both*
  the initial-Open path and the promote/open-direct rebuild in one place. Match
  the natural level of the existing promote/yield/refresh lines so an app already
  watching `db.logger` sees restore beside the transitions it sees now.
- **Size/count are a nice-to-have, not load-bearing.** Include them only if
  cheap — e.g. `os.Stat` the restored file for a byte count after completion —
  rather than duplicating litestream's plan calculation. Skip if it adds real
  coupling.

- **Out of scope — a live progress callback.** A `Config.OnRestoreProgress(applied,
  total int64)` fired during the restore would give a live %/ETA and stall
  detection (progress not advancing ⇒ a hung range read). It needs a progress
  hook *inside* the vendored litestream `Restore` — `total` from the plan,
  `applied` from a counting reader wrapping each `ResumableReader` — i.e. a fork
  change (see LITESTREAM-FORK.md). The need is unproven: a "restoring…" state
  driven by the start/complete lines above covers the common case. Captured as
  [../ideas/restore-progress-callback.md](../ideas/restore-progress-callback.md);
  promote it only if a consumer genuinely wants a progress bar rather than a phase.

## Verify

- Unit, reusing the restore scaffolding (`installRestoreCounter`, the
  `restoreDBFunc` injection point): a fresh `Open` against a non-empty replica
  emits a restore start+complete pair on a captured logger; a self-succession
  resume emits neither (nothing is restored); a takeover promote emits the pair.
- Explicitly assert the **initial-Open** restore path logs — the previously
  silent case is the point of the task.
- README (leasing/logging section) and the logging.go comment: state that the
  restore operation is logged, so the documented "restore is a lifecycle event"
  claim is true for every path.
