# Incremental follower refresh — apply only new LTX instead of re-restoring the full DB

**Status: ACTIVE — approach DECIDED (Option C fork + Option A managed follow).**
Unshelved 2026-07-22; shelved 2026-07-19. History: the original spike (Item 0) found
both litestream-native options dead and settled on a ~150–200-line self-contained
LTX-apply port (Option (iii)); it was shelved over the port's size/drift risk.
**That decision is now superseded.** Option C (a litestream fork carrying the small
resume-validation fix) was taken: the fix landed on fork `atmin/litestream`, s3lite
consumes it via `go.mod` `replace` at tag **`v0.5.15-s3lite.1`** (pin moved v0.5.14
→ v0.5.15), and a **re-probe against the consumed v0.5.15 code passed** (2026-07-22,
see Item 0). With the fix in place **Option A is unblocked and chosen**: drive
litestream's own `Restore(Follow)` resume against a private follow-file and publish
via the existing copy-and-swap. **This deletes the (iii) port entirely** — litestream's
`applyNewLTXFiles` already does the level-0 apply *and* the gap-fill (`fillFollowGap`)
we were going to reimplement. See `LITESTREAM-FORK.md` for the fork's sync/exit
workflow; file/track the upstream PR and drop the fork when it lands. Line references
are against the working tree at task creation — trust function/type names over line
numbers.

## Why

Today **every** follower refresh re-downloads and rebuilds the *entire* database from
the replica, even when the follower is only a few transactions behind. With
`FollowerRefreshInterval` set to seconds and a multi-GB DB, that is a full-snapshot
download every tick — wasteful bandwidth and latency for what is usually a tiny
delta. This is the README's documented *"periodically restoring the latest state
(not incremental WAL tailing yet)"* limitation. The goal: a refresh should fetch and
apply **only the LTX committed since the follower's current position**.

Scope this task to the **follower refresh** path — the hot, repeated one. Promotion
(a full restore once per handoff) and Open (once per cold start) are rare; leave them
full-restore for now and note incremental promote as a follow-up.

## What is true today (read before designing)

- Every restore goes through `restoreDB` (replica.go): a plain
  `litestream.Restore` with only `OutputPath` set — **non-follow, full rebuild**. It
  refuses an existing output path, computes a restore plan from the *latest snapshot +
  subsequent WAL* (`CalcRestorePlan`), compacts it into a temp file, and atomically
  renames it into place. It does **not** reuse the follower's existing bytes and does
  **not** write a `<path>-txid` sidecar.
- `refreshFollowerOnce` and `promote` (lease.go) call the shared
  `rebuildLocalFromReplica`, which restores into a fresh `<LocalPath>.restoring` temp
  via `restoreDBFunc` and then atomically swaps it in under the connector gate
  (`stableConnector.swapFiles`). Readers hold the old inode until the generation bump
  re-dials them onto the new file — so reads across a swap are always a consistent
  snapshot, never torn. This atomicity is invariant #6/#7 in `INVARIANTS.md` and must
  be preserved.
- `refreshFollowerOnce` already skips no-op ticks by probing the replica's latest
  TXID (`replicaLatestTXIDFunc`) and comparing to `lastRefreshPos` (loop-confined).
  So the follower already tracks "where the replica is" vs "where I am".
- `removeLocalDBFiles` already deletes `<path>-txid`, so the codebase anticipates the
  litestream TXID sidecar.
- The `restoreDBFunc` package var is the fault-injection seam used by the refresh
  tests; keep using it.

## The catch (why this is design-first, not mechanical)

litestream (through v0.5.14, the current release) has **only** one incremental
mechanism, `RestoreOptions.Follow`:

- With `Follow: true` on a fresh path it does the initial full restore, writes a
  `<path>-txid` sidecar (`WriteTXIDFile`), then enters `Replica.follow` — an
  **infinite** loop that every `FollowInterval` calls `applyNewLTXFiles`, which
  **patches new pages directly into the open `O_RDWR` database file** (`WriteAt`) and
  advances the `-txid` sidecar. It returns only when its ctx is cancelled.
- With `Follow: true` on an *existing* path that has a valid `-txid` sidecar, it
  **resumes** incrementally from that TXID (skipping the snapshot re-download),
  after validating retention has not pruned past it.
- `applyNewLTXFiles` is **unexported**; `WriteTXIDFile` / `ReadTXIDFile` are exported
  package functions. There is **no public "apply new LTX up to latest, then return"**
  (bounded catch-up) — only the infinite follow loop.

The conflict: `follow` mutates the DB file **in place, bypassing SQLite's locking**.
s3lite serves reads from that exact file through live SQLite connections. Applying
pages underneath live readers is unsafe (torn reads / lock-protocol violation). So we
cannot simply point `Follow` at the follower's read file.

## How to work this task

1. **Design spike first (Item 0).** Choose the integration approach, write it up in
   this file under Item 0, run the whole gate is *not* required yet — **stop and get
   sign-off on the design before implementing.** The options below are a starting
   point, not a decision.
2. Then implement in small, separately-verifiable items, each with the full gate
   (`go vet ./...`, `go test -race -count=1 ./...`, plus
   `go test -tags=integration ./...` — this path touches the s3 restore path, so run
   integration once Docker is available; see TESTING.md for the Colima socket).
3. Keep docs and `INVARIANTS.md` in sync **in the same item** that changes behavior.
4. Match the existing test style (`lease_internal_test.go`): each test opens with a
   comment naming the production hazard and the invariant it pins; reuse the seams
   (`restoreDBFunc`, `replicaLatestTXIDFunc`, `fakeLock`, `waitFor`).

Commit style (from `git log`): lowercase conventional prefix, imperative mood; one
commit per item; do not commit unless asked.

---

## Item 0 — [x] Design spike: choose the integration approach

**Original result (2026-07-19), against litestream v0.5.14:** Option (iii) — a
self-contained bounded LTX-apply inside s3lite — was the only workable approach;
Options A and B were dead ends (proven below). Decision then: shelve.

**SUPERSEDED by the v0.5.15 re-probe (2026-07-22).** The pin moved to v0.5.15 with
the fork resume-fix (`atmin/litestream@v0.5.15-s3lite.1`), so the spike was re-run
against the exact consumed code. Result: **Option A is now viable and is the chosen
approach; the (iii) port is dropped.** Evidence, all confirmed against the consumed
module:
- The fork's regression test `TestReplica_Restore_Follow_ResumeAheadOfSnapshot`
  reproduces Item 0's exact scenario (follow-file ahead of a snapshot marker) and now
  **passes** — resume is no longer rejected. The snapshot marker is written as garbage
  bytes yet resume succeeds, proving resume applies only the delta and never
  re-reads/re-downloads the snapshot.
- litestream's `applyNewLTXFiles` (replica.go) already implements the level-0 apply
  loop **and** the gap-fill-from-higher-levels case (`fillFollowGap`) — the ~150–200
  lines (iii) was going to port. Driving `Restore(Follow)` gets all of it, plus the
  page-1 header rewrite, truncate-to-commit, and `-txid` sidecar advance, for free.
- The goroutine + cancel-once-caught-up mechanism the s3lite integration needs is
  itself exercised by `TestReplica_Restore_Follow_ContextCancellation` (clean
  `Sync`+`Close`, returns nil on ctx cancel) and `TestReplica_Restore_Follow_StaleTXID`
  (the pruned/retention fallback error path). Full follow suite: green.

**The chosen design is written up in "Item 0.5 — the decided approach" below; Items
1–4 are re-planned around it.** The A/B/(iii) analysis that follows is kept for the
record — A's "DEAD" verdict is what the re-probe overturned.

### A — per-tick `Restore(Follow)` resume — DEAD (resume validation) → **OVERTURNED by the v0.5.15 re-probe; now the CHOSEN path (see Item 0.5)**

`Restore` refuses to resume a follow-file whose saved `-txid` is **ahead of the
latest snapshot** (`replica.go` ~line 591: `txid > latestSnapshot.MaxTXID` →
`"cannot resume follow mode: saved TXID … is ahead of latest snapshot … delete … to
re-restore"`). s3lite runs litestream's **default 24h `SnapshotInterval`**
(`startReplicationLocked` doesn't override it), so a following file is almost always
ahead of the last snapshot. Every resume would therefore error and fall back to a
full restore — **zero incremental benefit.** Probe confirmed: snapshot forced at
TXID 1, follow-file at TXID 11 → resume rejected with exactly that error. (The check
looks like a genuine litestream bug: a position ahead of the last snapshot is
perfectly resumable while the intervening level-0/1 LTX still exist. See Option C.)

### B — persistent `Follow` goroutine + consistent copy — DEAD (in-process locks) → **now MOOT: Item 0.5 quiesces (cancels) the follow before copying, so there is no concurrent mutation to snapshot; the resume-fix makes restarting cheap**

To copy the continuously-mutated private follow-file at a consistent boundary we'd
need to cooperate with litestream's apply lock. In v0.5.14 `applyLTXFile` **does**
lock (`internal.LockFileExclusive`) and rewrites the page-1 header to DELETE journal
mode + randomizes the schema cookie — but the lock is **POSIX `fcntl`**
(`internal/lock_unix.go`, `F_SETLKW` on the SQLite pending/shared byte ranges), which
by design does **not** conflict between threads of the *same* process. s3lite would
run the follow goroutine and the copy in one process, so we cannot get a consistent
snapshot this way. Restarting the goroutine to quiesce it re-hits the Option A resume
bug. (This also corrects "The catch" above: the in-place mutation is lock-guarded for
*cross-process* SQLite readers, but that guard is useless to us in-process.)

### (iii) — self-contained bounded apply — WORKS (proven) → **DROPPED in favor of Option A; kept for reference. Its whole body (the level-0 apply loop + `fillFollowGap`) is what litestream already implements internally, so Option A reuses it instead of porting it.**

Drive the apply ourselves from `saved+1`, so the resume validation never runs and
there is no concurrent writer to race the copy. Using only **public** primitives —
`ReplicaClient.LTXFiles(ctx, 0, saved+1, false)` + `OpenLTXFile` + `ltx.Decoder`
(`DecodeHeader`/`DecodePage`/`Header().Commit`) — read each new level-0 LTX, `WriteAt`
its pages into the private file, `Truncate` to `Commit*pageSize`, `Sync`, advance the
saved TXID, then copy-and-swap exactly as today. **Correctness argument for reads:**
unchanged from today — the apply is synchronous into a private file no SQLite reader
opens, and the read file is only ever replaced by the existing atomic temp+rename
under `swapFiles`, so reads are never torn (invariants #6/#7 preserved). **Probe
result:** snapshot at TXID 1, base file at TXID 11 (23 rows); after the leader
advanced to TXID 12, the manual delta-apply reached TXID 12 with 25 rows == the
leader — no snapshot re-download.

**Cost / why shelved:** (iii) must port ~150–200 lines of litestream's apply logic —
the level-0 apply loop, single-file page application, **and** the gap-fill-from-
higher-levels case (`fillFollowGap`) for when level-0 files were compacted away before
the follower caught up. `ltx.Decoder` insulates us from the page format, and the
"equals a full restore" test (Item 3) guards correctness, but it is duplicated logic
to re-verify on each litestream bump. For **small** DBs the current full restore is
already cheap, so the win is real only for large DBs / short refresh intervals.

**Failure/rollback & seams (for Items 1–2):** new follower-only seam
`advanceFollowFileFunc` (mirror `restoreDBFunc`) does the bounded apply into
`<LocalPath>.follow` (+ its own `-txid` sidecar via `WriteTXIDFile`); the writer never
calls it. Retention/pruned case (saved `-txid` behind the earliest retained snapshot)
is detected up front (read saved `-txid`; compare to the earliest `SnapshotLevel`
`MinTXID`) and falls back to discarding the follow-file and a full restore. Any apply
error leaves the live read file untouched (no swap) → keeps serving current state.
The `.follow` file is a private cache — delete it in `CloseContext` (best-effort,
after the loop stops) and cover it in `removeLocalDBFiles`.

**Option C (upstream) as the tidiest long-term:** fixing the resume "ahead of
snapshot" validation upstream — or exposing a public bounded apply — would let a much
smaller integration replace the port. File it upstream regardless; if it lands, prefer
it over (iii) when resuming.

---

### The candidate approaches as originally framed (kept for reference)

- **A — private follow-file + copy-and-swap.** Keep a persistent private replica copy
  that **no SQLite reader ever opens**, kept current by applying new LTX (with its own
  `-txid` sidecar). Each refresh tick: bring the private copy up to the latest TXID,
  then copy/clone it into a fresh temp and atomically swap it in as the read file
  exactly as today. Preserves the atomic-swap read model and all invariants; the S3
  savings (no snapshot re-download) are kept, at the cost of a local file copy per
  refresh (local copy ≪ S3 download; consider reflink/`clonefile` on supported FS).
  Needs a **bounded** incremental apply — see the sub-question below.
- **B — background `Follow` goroutine feeding swaps.** Run litestream `Follow` in a
  cancellable goroutine against the private follow-file; the refresh tick just
  snapshots that file (at a `-txid` boundary) and swaps it in. Uses litestream's
  public API as-is but needs care to copy the followed file at a consistent TXID
  boundary (the follow loop is mutating it).
- **C — upstream a bounded apply.** Contribute a public "apply new LTX and return"
  (essentially exposing `applyNewLTXFiles`) to litestream, then Option A becomes
  clean. Larger blast radius (external dependency/PR) but the tidiest long-term.

**Sub-question (ANSWERED by the spike):** litestream exposes only infinite `Follow`,
not a bounded catch-up. (i) driving `Follow` in a goroutine = Option B → **dead**
(in-process fcntl locks). (ii) upstreamed bounded apply = Option C → not available
today. (iii) reimplementing the apply loop over `ReplicaClient` + `ltx` inside
s3lite → **the answer** (proven). On gating: no size/opt-in threshold — the feature is
already opt-in via `FollowerRefreshInterval`; if (iii) is built, make it the default
refresh behavior with a full-restore fallback (decided during the spike).

**Original recommendation (Option A) was REJECTED by the 2026-07-19 spike, then
UN-REJECTED by the 2026-07-22 re-probe** once the fork resume-fix removed the
"ahead of snapshot" rejection. It is now the chosen approach — see Item 0.5.

---

## Item 0.5 — [x] The decided approach (Option A via managed `Restore(Follow)`)

**Chosen 2026-07-22.** Keep a private follow-file that **no SQLite reader ever
opens**, kept current by litestream's own `Restore(Follow)`, and publish to readers
via the existing copy-and-swap. s3lite owns only the private-file lifecycle and the
swap (both already invariant-covered); litestream owns the LTX apply (its tested
responsibility).

**State:** `<LocalPath>.follow` + its `<LocalPath>.follow-txid` sidecar (private
cache). The live read file and its swap machinery are unchanged.

**Per refresh tick (`refreshFollowerOnce`):**
1. Probe the replica's latest TXID (`replicaLatestTXIDFunc`). If the `.follow` sidecar
   (`litestream.ReadTXIDFile`) already equals it, skip — no-op tick (today's
   `lastRefreshPos` optimization, re-keyed onto the follow sidecar).
2. Advance `.follow` to that target by running, in a goroutine with a cancellable ctx,
   `litestream.Restore{OutputPath: <.follow>, Follow: true, FollowInterval: <short,
   e.g. 25–50ms>}`. Existing `.follow`+valid sidecar → **resumes incrementally**
   (the fix; no snapshot download). Missing `.follow` (first tick, or after a pruned
   fallback) → initial full restore into `.follow`+sidecar, then follow.
3. Poll `ReadTXIDFile(.follow)` until it reaches the step-1 target (or the goroutine
   returns an error); then **cancel the ctx and join** the goroutine. On ctx cancel
   the follow loop `Sync`s, `Close`s, and returns nil at a commit boundary
   (litestream `replica.go` `follow`; proven by
   `TestReplica_Restore_Follow_ContextCancellation`). The private file is now
   quiescent — no concurrent writer to race (this is why Option B's fcntl problem is
   moot).
4. Copy the quiescent `.follow` into `<LocalPath>.restoring` and swap it in under
   `swapFiles` exactly as today (atomic temp+rename; invariants #6/#7 preserved).
   Advance `lastRefreshPos`.

**Retention/pruned fallback:** if step 2 returns litestream's "saved TXID … behind the
earliest snapshot … history has been pruned" error (`replica.go` resume validation;
covered by `TestReplica_Restore_Follow_StaleTXID`), discard `.follow`+sidecar and do a
full restore into `.follow` to re-establish the base, then continue.

**Failure isolation:** any error in steps 2–3 leaves the live read file untouched (no
swap) → keep serving current state (invariant #6). Retry next tick.

**Seams / ownership:** replace the (iii) `advanceFollowFileFunc` (a hand-ported apply)
with a thinner `advanceFollowFileFunc` package var wrapping steps 2–3 (drive
`Restore(Follow)` + catch up + cancel/join). Tests inject failures / count work through
it; `restoreDBFunc` still drives the full-restore fallback and initial establish. This
is **follower-only** — the writer owns its state and must never run it. `.follow` +
sidecar are private caches: delete them in `CloseContext` (best-effort, after the loop
stops) and cover them in `removeLocalDBFiles` (which already handles `<path>-txid`).

**Known costs / things to verify while building (were not in the port):**
- *Per-tick goroutine lifecycle* — start / cancel / bounded-join with no leak (assert
  in Item 3).
- *"Caught up" is polled to a target captured at tick start* — if the leader commits
  more mid-catch-up we publish up to target and get the rest next tick (bounded; fine).
- *First-apply latency* — the follow ticker waits one `FollowInterval` before its first
  apply, so use a short interval for catch-up. Per-tick cost; acceptable.
- *Header rewrite* — litestream's apply rewrites page-1 to DELETE journal mode and
  randomizes the schema cookie. Should match today's restore output (readers re-dial
  the swapped inode and re-read schema anyway); the Item 3 "equals a full restore" test
  is the guard — verify journal mode / integrity there.
- *Copy cost* — still a local file copy per tick (as the port planned); consider
  reflink/`clonefile` on supported FS.

---

## Item 1 — [ ] Managed follow-file advance (`advanceFollowFileFunc`)

Implement the seam from Item 0.5: advance `<LocalPath>.follow` to the replica's latest
by driving `litestream.Restore{OutputPath: <.follow>, Follow: true, FollowInterval:
<short>}` in a goroutine with a cancellable ctx, polling `litestream.ReadTXIDFile` to
the target captured by the caller, then cancelling + joining the goroutine at a commit
boundary. Do **not** reimplement the apply — litestream's `applyNewLTXFiles` already
does level-0 apply + `fillFollowGap`. Handle the two non-steady cases: (a) missing
`.follow` → the same `Restore(Follow)` call does the initial full restore + sidecar
before following; (b) pruned/retention → detect litestream's "behind the earliest
snapshot" resume error, discard `.follow`+sidecar, re-establish via a full restore.
Route through a package-var seam `advanceFollowFileFunc` (mirror `restoreDBFunc`) so
tests can inject failures / count work. **Follower-only** — the writer owns its state
and must never call it. Guarantee a bounded join with no goroutine leak on cancel,
error, or ctx timeout.

## Item 2 — [ ] Wire it into `refreshFollowerOnce`, preserving atomic swaps

Replace the full re-restore in the refresh path with: advance the private follow-file
by the delta (Item 1), then copy a quiescent snapshot of it into `<LocalPath>.restoring`
and swap under `swapFiles` exactly as today. A **failed** advance must leave the live
read file untouched (keep serving current state — invariant #6). Pruned-past-follower
(retention) → the Item 1 fallback re-establishes, then the tick proceeds. Keep
`lastRefreshPos` / the no-op-tick skip working, re-keyed onto the `.follow` sidecar.

## Item 3 — [ ] Tests

Reuse `lease_internal_test.go` seams:
- *Refresh applies only the delta:* after an initial restore, a leader commits a small
  batch and syncs; the follower's next refresh must fetch/apply only the new LTX, not
  re-download the snapshot. Assert via a seam that counts LTX fetches (or bytes, or the
  `.follow` sidecar advancing) — not a full-snapshot fetch. (litestream's own
  `TestReplica_Restore_Follow_ResumeAheadOfSnapshot` already proves the primitive is
  incremental; this test pins the *s3lite integration*.)
- *Reads stay consistent across incremental refresh:* reuse the monotonic-count and
  concurrent-reads-survive-swap patterns; no torn reads, converges to the new state.
- *Failed advance keeps serving current state:* inject a failure through
  `advanceFollowFileFunc` and assert the follower keeps answering with its pre-failure
  data (invariant #6), then recovers when the injection clears.
- *Retention pruned → full-restore fallback:* simulate the replica pruning past the
  follower's `.follow` sidecar; the refresh must fall back to a full restore and
  converge.
- *No goroutine leak:* the per-tick follow goroutine always joins on cancel/error/
  timeout (e.g. `goleak`, or a counted seam).
- *Correctness vs full restore:* the incremental final state equals a fresh full
  restore's state (`integrity_check` ok, same rows) — this is also the guard for the
  page-1 header/journal-mode rewrite noted in Item 0.5.

## Item 4 — [ ] Docs + invariants

- README "How it works" diagram: the follower **refresh** edge should say it applies
  only new LTX (incremental), while promote/Open remain full rebuilds — keep the
  distinction honest.
- README Limitations: update the *"not incremental WAL tailing yet"* line.
- `Config.FollowerRefreshInterval` doc comment: note refresh is now incremental.
- `INVARIANTS.md` #6: keep the staleness/keeps-current-state wording accurate under
  the new mechanism, and name the new test(s).
- Note the litestream fork dependency: refresh now relies on the resume-fix carried in
  `atmin/litestream@v0.5.15-s3lite.1` (see `LITESTREAM-FORK.md`). When the upstream PR
  lands, follow that doc's exit steps and confirm this path still passes on stock
  litestream.

---

## Out of scope (follow-ups, note but do not build)

- Incremental **promote** (restore-before-becoming-writer). Rare; revisit after
  refresh lands.
- Incremental **Open** cold-start restore. There is nothing local to build on at cold
  start, so this is inherently a full restore.
- True live WAL tailing with sub-tick latency (continuous apply rather than
  interval-driven).

## Done when

The design is signed off, follower refresh applies only the delta with all reads
consistent and all invariants preserved, `go vet` + `go test -race ./...` +
`go test -tags=integration ./...` pass, and every changed doc/invariant names a test
that fails if it breaks. Then delete this file and update `tasks/README.md`.
