# A `sqlite3`-familiar CLI over s3lite

s3lite is library-only — there is no `cmd/`. Give it a shell:

```
s3lite --role=writer   s3://bucket/db.sqlite   # acquire the lease, stream every second
s3lite --role=follower s3://bucket/db.sqlite   # read-only, fresh pull per statement
```

Semantics are exactly today's, unchanged: reads always come from the local
file, a writer streams WAL to the replica every second, `RoleWriter` fails to
open when the lease is held. The CLI is packaging, not architecture — a REPL
plus flag parsing over `Open`, the returned `*sql.DB`, and `Close`. It lands
last on this frontier because it consumes what sits above it:
[exported-refresh.md](exported-refresh.md) supplies the per-statement pull, and
`Config.OnRestoreProgress` (landed) the cold-open progress bar.

## Why

- **It is the missing consumer for features that already shipped.** A REPL
  session is long-lived and mostly idle — precisely the release-on-idle shape
  of `YieldLease` + `Config.OnDemandPromotion` (the landed slice of
  [ideas/cooperative-yield.md](ideas/cooperative-yield.md)). Today that path
  exists for hypothetical callers; the CLI makes it the default lifecycle.
  It is also the first caller that genuinely wants the restore progress bar —
  a human staring at a cold multi-GB open is its reason to exist.
- **Ctrl-C is INVARIANTS.md #4 in user-facing form.** "A clean `Close` is
  durable and bounded" stops being an internal contract and becomes the thing
  that decides whether a person loses their last statement. The crash tests
  already cover it; the CLI is where it gets seen.
- **The refresh path is already shaped right for per-statement.** In
  `refreshFollowerOnce` (lease.go:635) the network fetch+apply runs against a
  private follow file and only the atomic publish touches the connector, so a
  pull never blocks the read path for a round trip — and an unchanged replica
  early-outs with no advance and no swap. `Refresh` exports exactly this.

## What is true today (read before designing)

- **The two decisions from the idea stage stand**, and are the product:
  - **Familiar, not drop-in.** No promise of `sqlite3` compatibility —
    `shell.c` is ~28k lines and a true drop-in means owning all of it. Target
    the dot-commands people actually type — `.tables`, `.schema`, `.mode`,
    `.dump`, `.import`, `.quit`, `.help` — and say plainly in `--help` that
    this is a familiar shell, not a compatible one. Scripts that shell out to
    `sqlite3` keep using `sqlite3` against the local file.
  - **Follower cadence: a fresh pull per statement — when interactive.** At a
    prompt, human think-time dominates any pull, so per-statement freshness
    is the least surprising contract: what you read is what the replica had
    when you pressed Enter. Piped input (`s3lite … < script.sql`) is
    logically one read of one version: refresh once at start, never
    per-statement. TTY detection on stdin decides which contract applies.
- **The idea's three parking reasons are all discharged**: no exported
  refresh and a disqualifying probe cost → [exported-refresh.md](exported-refresh.md),
  scheduled above; batch cadence → decided, per the split above; transaction
  suppression → decided, below.
- **Refresh must not fire inside an explicit transaction.** The publish bumps
  the connector generation and in-flight connections re-dial against the new
  state — a human typing `BEGIN;` … `COMMIT;` must not have the file swapped
  underneath mid-transaction. The REPL tracks explicit transaction depth by
  statement-prefix inspection (`BEGIN` / `COMMIT` / `END` / `ROLLBACK`,
  case-insensitive, leading comments stripped) — the subset a REPL actually
  sees, not a SQL parser — and suppresses `Refresh` while depth > 0. `.help`
  states the rule so the behaviour is a contract, not a surprise.
- `module github.com/atmin/s3lite` — the binary is `cmd/s3lite`, CGO-free
  like everything else.

## Sketch (settle the shape at pickup)

- **`cmd/s3lite`**: flags (`--role`, S3 endpoint/region/credentials with the
  same env fallbacks the library documents, `--mode` defaults), one
  positional `s3://bucket/path.sqlite`, then the REPL loop: read a statement
  (semicolon-terminated, multi-line), maybe `Refresh`, execute, render per
  `.mode`.
- **Writer lifecycle**: open with `Config.OnDemandPromotion` and idle-yield
  via `YieldLease`, so a forgotten prompt releases the pen and the first
  statement after idleness re-promotes through the write path. The follower
  role never promotes — a write statement fails read-only, with a hint
  naming `--role=writer`.
- **Cold open**: `Config.OnRestoreProgress` drives a stderr progress bar;
  `Config.Logger` at Info to stderr so the v0.11.0 restore lines and
  promote/yield events narrate the session.
- **Ctrl-C**: first interrupt cancels the in-flight statement (context on
  the exec); at an idle prompt (or second interrupt) it exits through a clean
  `Close`, bounded by `ShutdownSyncTimeout`, and says what it flushed.
- **`.dump` / `.import`** operate through ordinary SQL over the open handle —
  no reaching around the connector into the file.
- **Out of scope**, explicitly: an S3-primary VFS with lazy page hydration
  (a different product with a different latency profile — the
  local-file-is-the-read-path promise is the whole point of this one), any
  `sqlite3` compatibility claim, and Windows/packaging concerns beyond
  `go install`.

## Verify

- **Cadence, piped**: a scripted stdin with N statements performs exactly one
  refresh (count via an injected probe/refresh seam) — the 10k-statement
  script pays one pull, not 10k.
- **Cadence, interactive**: with the TTY contract forced on, each statement
  is preceded by one `Refresh`, and a row committed by a concurrent writer is
  visible on the next Enter (integration: two processes, MinIO).
- **Transaction suppression**: `BEGIN` … statements … `COMMIT` with the
  replica advancing mid-transaction sees one consistent snapshot throughout,
  and the pull resumes on the first post-`COMMIT` statement.
- **Writer lifecycle**: an idle session yields the lease (a second CLI's
  `--role=writer` open succeeds after the idle window… then fails again once
  the first types a write that re-promotes); Ctrl-C mid-session loses no
  committed statement — reopen and count.
- **Cold open**: against a populated replica, the bar renders and ends at
  100%; against an empty one, no bar and a clean prompt.
- **Dot-commands**: golden-output tests for `.tables`/`.schema`/`.mode`
  rendering; `.help` names both the familiar-not-compatible stance and the
  transaction rule.
- The suite, `-race`, and `GOMAXPROCS=2`, as the house rules require.
