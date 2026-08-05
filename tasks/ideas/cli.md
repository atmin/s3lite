# A `sqlite3`-familiar CLI over s3lite

> **Status: idea, not a task.** The shape is settled (see the two decisions below);
> what is missing is an exported synchronous refresh and a decision about batch input.
> **Do not implement yet.**

## The idea

s3lite is library-only — there is no `cmd/`. Give it a shell:

```
s3lite --role=writer   s3://bucket/db.sqlite   # acquire the lease, stream every second
s3lite --role=follower s3://bucket/db.sqlite   # read-only, fresh pull per statement
```

Semantics are exactly today's, unchanged: reads always come from the local file, a
writer streams WAL to the replica every second, `RoleWriter` fails to open when the
lease is held. The CLI is packaging, not architecture — a REPL plus flag parsing over
`Open`, the returned `*sql.DB`, and `Close`.

Two decisions that shape it:

- **Familiar, not drop-in.** No promise of `sqlite3` compatibility. `sqlite3`'s
  `shell.c` is ~28k lines and being a true drop-in means owning all of it. Target the
  handful of dot-commands people actually type — `.tables`, `.schema`, `.mode`,
  `.dump`, `.import`, `.quit` — and say plainly in `--help` that this is a familiar
  shell, not a compatible one. Scripts that shell out to `sqlite3` keep using
  `sqlite3` against the local file.
- **Follower cadence: a fresh pull per statement.** Not a `FollowerRefreshInterval`
  timer. At an interactive prompt human think-time dominates any pull, so
  per-statement freshness is the least surprising contract available: what you read is
  what the replica had when you pressed Enter. No staleness knob to explain.

## Why it's interesting

- **It is the missing consumer for features that already shipped.** A REPL session is
  long-lived and mostly idle — precisely the release-on-idle shape of `YieldLease` +
  `Config.OnDemandPromotion` ([cooperative-yield.md](cooperative-yield.md), landed
  slice). Right now that path exists for hypothetical callers; a CLI makes it the
  default lifecycle. It is also the first caller that would genuinely want
  [restore-progress-callback.md](restore-progress-callback.md): a human staring at a
  cold multi-GB open is the progress bar's reason to exist, which is exactly what that
  idea is parked for lacking.
- **Ctrl-C is INVARIANTS.md #4 in user-facing form.** "A clean `Close` is durable and
  bounded" stops being an internal contract and becomes the thing that decides whether
  a person loses their last statement. The crash tests already cover it; the CLI is
  where it gets seen.
- **The refresh path is already shaped right for per-statement.** In
  `refreshFollowerOnce` ([lease.go:635](../../lease.go#L635)) the S3 fetch+apply runs
  *outside* the connector gate and only the local copy+rename runs under it, so a pull
  never blocks the read path for the duration of a network round trip. And an unchanged
  replica early-outs at `pos <= db.lastRefreshPos` with no advance and no swap.

## Why it's parked (not a task)

- **No exported synchronous refresh exists.** `refreshFollowerOnce` is unexported and
  documented "called only from `leaseLoop`"; `Sync` is a no-op for a follower
  ([s3lite.go:894](../../s3lite.go#L894)); `TryPromote` refreshes but also takes the
  pen. Per-statement freshness needs a new public entry point — roughly
  `(*DB).Refresh(ctx) (bool, error)` — safe to call from an arbitrary goroutine and
  serialised against promotion on `promoteMu`. That is a real API addition to a
  correctness-critical path, not CLI plumbing, so it wants its own task.
- **A per-statement tip probe costs ~10 LISTs.** `replicaLatestTXID`
  ([replica.go:208](../../replica.go#L208)) loops levels 0 through
  `litestream.SnapshotLevel` (= 9) calling `MaxLTXFileInfo` sequentially, building a
  throwaway client each call. So *every* statement pays ten sequential round trips
  before discovering nothing changed — order 200-300ms against real S3. Interactively
  that hides behind think-time, but it makes the naive implementation embarrassing on
  a bucket that is idle, and it is per-keystroke-of-Enter cost on a metered API. A
  cheap single-object tip probe is probably a prerequisite rather than a nicety.
- **Batch input breaks the cadence.** `s3lite db.sqlite < script.sql` with 10k
  statements means 10k tip probes for a file that is logically one read of one
  version. Per-statement is right for a TTY and wrong for a pipe, so the rule has to
  be "fresh per statement when interactive, once at start when not" — which means the
  contract we chose for its simplicity has an exception before any code is written.
- **Refresh must not fire inside an explicit transaction.** The publish bumps the
  connector generation and in-flight connections re-dial against the new state
  ([lease.go:661](../../lease.go#L661)). A human typing `BEGIN;` … `COMMIT;` at the
  prompt must not have the file swapped underneath mid-transaction, so the REPL has to
  track transaction depth and suppress pulls between `BEGIN` and `COMMIT`/`ROLLBACK`
  — which means parsing enough SQL to know, or asking the driver. Neither is free.

## If we ever pick this up

In order: (1) a cheap replica-tip probe that does not walk ten levels, (2) exported
`Refresh`, (3) the REPL, with the interactive/piped cadence split and transaction-depth
suppression designed up front rather than discovered. Steps 1 and 2 are useful to the
library on their own and are where the value is; step 3 is a weekend once they exist.

Explicitly out of scope: an S3-primary VFS with lazy page hydration. That is a
different product with a different latency profile (a cold index scan becomes N range
GETs), and the local-file-is-the-read-path promise above is the whole point of this one.
