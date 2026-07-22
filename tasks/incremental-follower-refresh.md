# Incremental follower refresh — apply only new LTX instead of re-restoring the full DB

**Status: ACTIVE (unshelved 2026-07-22; shelved 2026-07-19).** The design spike
ran (see Item 0 below) and settled the approach: the two litestream-native options
are both dead ends, and the only workable path is a self-contained LTX-apply
reimplemented inside s3lite (Option (iii), proven feasible). It was shelved over
the port's size and drift risk; unshelved now that a consumer needs a chatty,
short-interval follower. The pinned litestream is still v0.5.14 — the version the
spike verified — so Item 0's findings stand without a re-probe; re-run the probe
only if the pin moves before Items 1–4 land. Everything needed is in this file —
Item 0 records the evidence; Items 1–4 are the concrete (iii) plan. No prior
conversation context is assumed. Line references are against the working tree at
task creation — trust function/type names over line numbers.

**Option C's calculus has changed:** waiting on upstream is no longer the only
form it can take — carrying a litestream *fork* with the small resume-validation
fix (Item 0, Option A's blocker) is on the table, which would shrink this task to
a per-tick `Restore(Follow)` resume and delete most of the (iii) port. Weigh
fork-and-fix against the port at pickup; file the upstream issue either way and
drop the fork when upstream lands it.

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

**Result (2026-07-19): Option (iii) — a self-contained bounded LTX-apply inside
s3lite — is the only workable approach. Options A and B are dead ends, proven below.
Decision: shelve (see status header); build Items 1–4 as the (iii) plan when picked
up.** The findings were verified against **litestream v0.5.14** (the pinned version)
with a throwaway probe test (`Restore(Follow)` behavior + a manual delta-apply);
re-run an equivalent probe before trusting these across a version bump.

### A — per-tick `Restore(Follow)` resume — DEAD (resume validation)

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

### B — persistent `Follow` goroutine + consistent copy — DEAD (in-process locks)

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

### (iii) — self-contained bounded apply — WORKS (proven)

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

**Original recommendation (Option A) was REJECTED by the spike** — see the Item 0
result above. It cannot work because per-tick `Restore(Follow)` resume is rejected once
a snapshot sits behind the follow-file's position (the normal steady state).

---

## Item 1 — [ ] (Option (iii)) Incremental apply on a private follow-file

Implement the self-contained bounded apply (Item 0 result): advance
`<LocalPath>.follow` from its `-txid` to the replica's latest by reading level-0 LTX
after `saved` via `ReplicaClient.LTXFiles`/`OpenLTXFile` + `ltx.Decoder`, `WriteAt`-ing
pages and `Truncate`-ing to `Commit*pageSize`, then advancing the sidecar with
`WriteTXIDFile`. **Port the gap-fill-from-higher-levels case too** (`fillFollowGap`):
when level-0 files after `saved` were already compacted away, bridge from levels 1..8.
Route it through a package-var seam `advanceFollowFileFunc` (mirror `restoreDBFunc`) so
tests can inject failures and count work. The writer must never run this (it owns the
state); it is follower-only. Fresh follow-file (first tick / after a pruned fallback):
a full restore into `.follow` establishes the base + sidecar.

## Item 2 — [ ] Wire it into `refreshFollowerOnce`, preserving atomic swaps

Replace the full re-restore in the refresh path with: advance the private follow-file
by the delta, then swap a consistent copy into place under `swapFiles` exactly as
today. A **failed** incremental apply must leave the live read file untouched (keep
serving current state — invariant #6). If the replica has pruned past the follower's
position (retention), fall back to a full restore. Keep `lastRefreshPos` / the
no-op-tick skip working.

## Item 3 — [ ] Tests

Reuse `lease_internal_test.go` seams:
- *Refresh applies only the delta:* after an initial restore, a leader commits a small
  batch and syncs; the follower's next refresh must fetch/apply only the new LTX, not
  re-download the snapshot. Assert via a seam that counts LTX fetches (or bytes, or the
  `-txid` advancing) — not a full-snapshot fetch.
- *Reads stay consistent across incremental refresh:* reuse the monotonic-count and
  concurrent-reads-survive-swap patterns; no torn reads, converges to the new state.
- *Failed incremental apply keeps serving current state:* inject an apply failure and
  assert the follower keeps answering with its pre-failure data (invariant #6), then
  recovers when the injection clears.
- *Retention pruned → full-restore fallback:* simulate the replica pruning past the
  follower's `-txid`; the refresh must fall back to a full restore and converge.
- *Correctness vs full restore:* the incremental final state equals a fresh full
  restore's state (`integrity_check` ok, same rows).

## Item 4 — [ ] Docs + invariants

- README "How it works" diagram: the follower **refresh** edge should say it applies
  only new LTX (incremental), while promote/Open remain full rebuilds — keep the
  distinction honest.
- README Limitations: update the *"not incremental WAL tailing yet"* line.
- `Config.FollowerRefreshInterval` doc comment: note refresh is now incremental.
- `INVARIANTS.md` #6: keep the staleness/keeps-current-state wording accurate under
  the new mechanism, and name the new test(s).

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
