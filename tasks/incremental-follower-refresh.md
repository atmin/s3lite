# Incremental follower refresh — apply only new LTX instead of re-restoring the full DB

**Status: design-first.** The integration approach is genuinely open (see "The
catch"), so this task starts with a design spike and a pause for sign-off before any
implementation. Everything needed to start is in this file; no prior conversation
context is assumed. Line references are against the working tree at task creation —
trust function/type names over line numbers.

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

## Item 0 — [ ] Design spike: choose the integration approach

Produce a short written decision (append it here) covering the approach, the
correctness argument for consistent reads, the failure/rollback story, and the
new/changed seams. Candidate approaches:

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

**Sub-question the spike must answer:** litestream exposes only infinite `Follow`, not
a bounded catch-up. Decide between (i) driving `Follow` in a goroutine (Option B),
(ii) depending on an upstreamed bounded apply (Option C), or (iii) reimplementing the
LTX-apply loop over litestream's `ReplicaClient` + `ltx` primitives inside s3lite
(more code, no upstream dependency). Weigh each against the payoff — for **small**
DBs the current full restore is already cheap, so be honest about whether the
complexity earns its keep, and consider gating incremental behind a size/opt-in
threshold if not.

**Recommendation to evaluate first:** Option A with a private follow-file, because it
keeps the atomic-swap read guarantees intact and confines all the litestream
in-place-mutation to a file no reader touches. Confirm or reject it in the spike.

---

## Item 1 — [ ] (design-dependent) Incremental apply on a private follow-file

Implement the chosen mechanism to advance a private follow-file from its current
`-txid` to the replica's latest, downloading/applying only the delta. Route it
through a package-var seam (mirror `restoreDBFunc`) so tests can inject failures and
count work. The writer must never run this (it owns the state); it is follower-only.

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
