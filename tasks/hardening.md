# Correctness hardening — close the fencing, durability, and adversity gaps

**Priority: highest.** This library's pitch is "correct under concurrency, with no
coordinator" — the items below are the places where a July 2026 coverage
assessment found that promise either unverified or (items 1–4) actually violated.
Everything needed to implement is in this file; no prior conversation context is
assumed. Line references are against the working tree at task creation — trust
function names over line numbers, since earlier items shift later lines.

## How to work this task (read first)

Work the items **strictly in order** — they are sorted by severity, and later
tests build on earlier fixes (e.g. the chaos test assumes the renew deadline
exists).

For **each** item:

1. **Prove the claim before fixing.** Where an item asserts a bug, first write
   the test that should expose it and watch it fail (or, for races, fail under
   `-race`). If it does *not* fail as predicted, stop — do not force a fix.
   Record what you found and raise it at the pause instead.
2. Implement the fix. Keep doc comments and README claims in sync with behavior
   **in the same item** — several items exist precisely because docs and code
   drifted apart.
3. Run the full gate: `go vet ./...` and `go test -race -count=1 ./...`
   (plus `go test -tags=integration ./...` when the item touches the s3 path and
   Docker is available).
4. Mark the item's checkbox `[x]` in this file.
5. **Stop.** Output: a short summary of what changed and why, the names of the
   new/changed tests and what each proves, and a proposed commit message. Then
   wait for the user to verify and commit before touching the next item.

Commit message style (from `git log`): lowercase conventional prefix, imperative
mood — `fix:`, `test:`, `ci:`, `docs:`; `!` for breaking changes. One commit per
item. Do not commit yourself unless the user asks you to.

Test style: match `lease_internal_test.go` — every test opens with a short
comment saying what production hazard it models and what invariant it pins.
Use the existing seams (`newLeaserFunc`, `replicaLatestTXIDFunc`, `fakeLock`,
`waitFor`) and extend them rather than inventing parallel machinery.

---

## Item 1 — [x] Fencing hole: a hung renew lets two writers overlap

**Claim.** The documented invariant (Config.LeaseTTL doc, README) is: *a holder
that cannot renew stops replicating before the TTL lets anyone else acquire.*
The code cannot guarantee that. `leaseLoop` (lease.go) calls `tryRenew` with the
loop's **unbounded** background context, and `tryRenew` passes it straight into
`db.leaser.RenewLease`. A renew that hangs (network black hole — S3 requests
have no overall response timeout) stalls the single loop goroutine **past the
lease's ExpiresAt** while litestream keeps replicating from its own goroutines.
A successor acquires at expiry and both writers push LTX files to the same
replica prefix.

**Fix.** In `tryRenew`, bound the renew call by a deadline derived from the held
lease: `renewCtx` with deadline `lease.ExpiresAt.Add(-margin)` where
`margin = leaseTTL / 6` (proportional, so short-TTL tests still work; demote
itself is local and fast — it closes the store with an already-cancelled
context). On any renew error, distinguish the two cancellation sources:

```go
renewCtx, cancel := context.WithDeadline(ctx, lease.ExpiresAt.Add(-db.leaseTTL/6))
defer cancel()
newLease, err := db.leaser.RenewLease(renewCtx, lease)
if err != nil {
    if ctx.Err() != nil {
        return nil // parent cancelled: shutting down; let Close handle teardown
    }
    return err // includes renewCtx deadline exceeded → caller demotes
}
```

The existing `ctx.Err() != nil → return nil` check must test the **parent** ctx,
not `renewCtx` — a deadline expiry must demote, a shutdown must not. This is the
trap; get it right and comment it.

Also fix the lying comment on `tryRenew` ("returns an error only when the lease
is genuinely lost") — after this change it errors when the lease is lost **or
cannot be confirmed before expiry**, which is exactly what fencing requires.
Update the `Config.LeaseTTL` doc if wording needs it.

Optional, same spirit, small: bound the follower's `AcquireLease` in
`tryPromoteOnce` at `leaseTTL` so a hung acquire cannot wedge the loop (it blocks
promotion and refresh, though not fencing). Skip if it complicates anything.

**Tests** (`lease_internal_test.go`):
- *Blocking renew demotes before expiry:* extend `fakeLeaser` with an injectable
  `renewBlocks` mode where `RenewLease` blocks until its ctx is done, then
  returns `ctx.Err()`. Open a writer with a short TTL, capture the lease's
  `ExpiresAt`, let the renew tick hit the blocking renew, and assert `OnDemote`
  fires **before** `ExpiresAt` (record `time.Now()` in the callback). Also
  assert the handle is fenced (writes rejected) after demote. Without the fix
  this test hangs/demotes far too late — verify it fails first.
- *Shutdown during renew still returns nil:* existing behavior — a renew
  interrupted by Close must not demote. Cover it explicitly if no current test
  does (check `tryRenew`'s ctx-cancelled branch coverage).

---

## Item 2 — [x] Data race: `Sync` reads `db.lsDB` without the mutex

**Claim.** `DB.Sync` (s3lite.go) reads `db.lsDB` with no lock. `demote` (via
`stopReplicationLocked`) writes `db.lsDB = nil` under `db.mu`, and promotion
(via `becomeLeaderLocked` → `startReplicationLocked`) sets it under `db.mu`.
A consumer calling `Sync` concurrently with any role transition is a data race.
The `-race` suite passes today only because no test calls `Sync` during a
transition.

**Fix.** Snapshot under the mutex:

```go
func (db *DB) Sync(ctx context.Context) error {
    db.mu.Lock()
    lsDB := db.lsDB
    db.mu.Unlock()
    if lsDB == nil {
        return nil
    }
    return lsDB.SyncAndWait(ctx)
}
```

Note the benign TOCTOU: a demote can close the store between snapshot and
`SyncAndWait`; that surfaces as an error from litestream, which is correct
(the sync did not happen). Add one line to `Sync`'s doc comment: calling it
concurrently with a demotion may return an error.

While here, audit the other lifecycle fields for unsynchronized access from
public methods. (Assessment result: `connector`, `leaser`, `logger`, `cfg` are
set once in `Open` before any goroutine starts — fine; `lastRefreshPos` is
loop-confined and documented — fine. `lsDB` via `Sync` was the only hole.
Re-verify rather than trusting this.)

**Tests:**
- *Sync races demotion:* writer with short TTL; one goroutine hammers
  `db.Sync(ctx)` in a loop while the test steals the lock and the writer
  demotes; then again across a follower promotion. Assert only that nothing
  panics and the suite stays `-race` clean — the race detector is the oracle.
  Confirm it fails under `-race` before the fix.
- *Sync is a no-op without a replica:* trivial — a follower / unreplicated DB
  returns nil (covers the currently-untested nil branch).

---

## Item 3 — [ ] A failed refresh/promote restore destroys the follower's local state

**Claim.** `Config.FollowerRefreshInterval` docs promise a failed refresh leaves
the follower "serving its current state". But the swap callbacks in
`refreshFollowerOnce` and `promote` (lease.go) run `removeLocalDBFiles` **first**
and then `restoreDB`. A network drop between the delete and a completed restore
leaves no local database at all: the next connection creates an empty SQLite
file and every query fails with "no such table" until some later refresh
succeeds. The promote failure path is worse — it "remains a read-only follower"
of a deleted file. `TestFollowerRefreshErrorIsNonFatal` only fails the *probe*
(`replicaLatestTXIDFunc`), never the restore, so the destructive window is
untested.

**Fix.** Make the rebuild atomic: restore into a temp path in the same
directory, and only after the restore succeeds tear down the old files and
rename the temp into place. Concretely, inside the swap callback:

1. `tmp := cfg.LocalPath + ".restoring"`; remove any stale `tmp` first.
2. `restoreDB(ctx, cfg.S3, restoreURL, tmp)`.
3. **Empty-replica case:** `restoreDB` returns nil without creating a file when
   the replica is empty (`isEmptyReplica`). If `tmp` does not exist after a nil
   return, fall back to today's behavior: remove local files, `precreateWAL`.
   (This is the fresh-bucket promote path — don't break it.)
4. On restore error: return the error **without having touched the live files**
   — the follower keeps serving its pre-swap state. This is what makes the doc
   claim true.
5. On success: `removeLocalDBFiles(cfg.LocalPath)`, `os.Rename(tmp, cfg.LocalPath)`,
   `precreateWAL`. A crash between remove and rename leaves no local file, which
   the next `Open` handles by restoring — already safe.

Both `refreshFollowerOnce` and `promote` use this; extract a shared helper
(e.g. `rebuildLocalFromReplica(ctx) error`) so they cannot drift.

**Seam:** introduce `var restoreDBFunc = restoreDB` (package var, mirroring
`newLeaserFunc` / `replicaLatestTXIDFunc`) and route the helper through it so
tests can inject restore failures.

Fix the `promote` failure path comment ("remain a read-only follower") — after
this change it is finally accurate.

**Tests:**
- *Refresh restore failure keeps serving current state:* leader writes+syncs a
  row; follower with refresh enabled sees it; inject `restoreDBFunc` failure and
  advance the replica (write+sync on leader, or fake the probe to report a
  higher TXID); across several failing ticks the follower must keep answering
  queries with its pre-failure data (not "no such table", not empty). Then clear
  the injection and assert it converges to the new state. **Write this test
  first against the current code and watch it fail** — it should observe the
  destroyed state.
- *Promote restore failure leaves a serving follower:* free the lease, inject
  restore failure, let promotion fail; assert the instance is still a follower,
  still serves its old snapshot read-only, and the lease was released
  (`fakeLock.lease == nil`) so another instance could take over. (This also
  finally exercises `releaseQuietly` — 0% coverage today.)
- *Fresh-bucket promote still works:* follower promotes against an empty
  replica (no rows ever synced) and ends up a writable empty DB — pins the
  empty-replica fallback in step 3.

---

## Item 4 — [ ] `Generation` is documented as monotonic but resets on clean handoff

**Claim.** The `Generation()` doc calls it "a monotonic fencing token bumped on
each takeover". But a clean `Close` **deletes** the lock object, so the next
acquirer starts again at generation 1 — `TestHandoffOnClosePromotesFollower`'s
own comment admits this ("the successor's generation resets to 1; this matches
s3.Leaser"). A consumer fencing external side effects sees 1 → 2 → **1**, which
defeats fencing exactly when it matters.

**Fix (docs, not epoch).** A true epoch would require persisting state in
`lock.json`, which litestream's `s3.Leaser` owns — out of scope here. Instead
make the docs honest and pin the behavior:

- Rewrite the `Generation()` doc comment: the token is unique among *concurrent
  contenders* and increases across takeovers **only while the lock object
  survives** (expiry/steal); a clean release deletes the lock and resets the
  sequence. State plainly: **do not use this as a cross-handoff fencing token
  for external systems.** Say what it *is* good for: telling apart two
  promotions within one instance's lifetime, logging/diagnostics.
- Update any README mention to match.
- If a real fencing token is wanted later, note the direction in one line
  (epoch persisted next to the replica data, outside lock.json) — but do not
  build it now.

**Test:** *Generation sequence across steal and clean handoff:* writer at gen 1;
steal → new holder at gen 2; old holder demotes; clean release; fresh acquire →
assert gen is 1 again. The test exists to make the reset **loud and
intentional** — its comment should say the doc guarantees exactly this and no
more.

---

## Item 5 — [ ] Crash-kill harness: back the README's hard-kill claim

**Claim.** README: "A hard kill can lose only the sub-second window since the
last WAL sync; a clean Close loses nothing." Zero tests kill a process. The
restore-consistency half of the claim (a hard-killed writer's replica restores
to a *consistent prefix* of acked writes, never a torn state) is the single most
important untested invariant in the library.

**Fix.** Add a crash harness test (new file `crash_test.go`, default suite —
file:// replica only, no Docker):

- **Child mode:** the test binary re-execs itself
  (`exec.Command(os.Args[0], "-test.run=TestCrashChild")` with a marker env var
  carrying the temp paths; `TestCrashChild` skips unless the env var is set).
  The child opens s3lite with `BackupTo: file://<replica>`, then loops:
  `INSERT INTO items(id) VALUES (i)` for i = 1, 2, 3, …, printing each acked `i`
  to stdout (flush per line) — never calling Close.
- **Parent:** reads the child's stdout until ~50 rows are acked, then
  `cmd.Process.Kill()` (SIGKILL — no cleanup runs). Waits it out. Then opens a
  **fresh** instance with `RestoreFrom: file://<replica>` at a new LocalPath and
  asserts:
  1. `PRAGMA integrity_check` returns `ok`;
  2. rows are exactly `{1..k}` for some `k` — `count(*) == max(id)`, no holes
     (prefix consistency: litestream replays WAL frames in order);
  3. `k <= lastAcked` (the tail window may be lost — that is the documented
     deal; losing *acked-and-synced* data is not, but this test doesn't sync
     per-row, so only prefix+integrity are asserted).
- Keep runtime bounded (a few seconds). If the replica has synced nothing yet at
  kill time, `k == 0` with an intact empty restore is a legal outcome — assert
  consistency, not progress. Make the row target and timings generous enough to
  be reliable on a loaded CI runner.

**Also:** a second, cheap child variant that **does** call `Close` and asserts
the restored DB contains *everything* acked ("a clean Close loses nothing") —
this duplicates `TestCloseIsDurableWithoutExplicitSync` but through a real
process boundary, which is the point.

---

## Item 6 — [ ] Buggy-consumer fencing: in-flight transactions survive demote; double Close

**Claim (a).** The demote fence (`connector.setMode(true)`) only invalidates
connections at **pool reuse** (`ResetSession`/`IsValid`). A transaction begun
before demotion holds its connection checked out, so its statements and
`Commit()` never pass a staleness check: the demoted writer **commits locally
after `IsLeader()` is false**. The write never replicates (replication is
stopped) and silently vanishes at the next promote-restore — meanwhile the
follower serves phantom rows. Same hole for a checked-out `*sql.Conn`.

**Fix (a).** Fence at the driver level in stableconn.go:

- Wrap the transaction: `genConn.BeginTx` returns a `genTx{driver.Tx, conn}`
  whose `Commit()` checks `conn.stale()` first and, if stale, rolls back and
  returns `driver.ErrBadConn` (the connection is discarded; the caller gets a
  retryable error and the WAL changes are rolled back — nothing persists, so
  the fence holds).
- Check `stale()` at the top of `genConn.ExecContext` (return
  `driver.ErrBadConn`) to fence autocommit writes on checked-out conns.
  **Deliberately do not** fence `QueryContext`/`PrepareContext`: a reader on a
  pre-swap connection holds the old inode and sees a consistent (stale)
  snapshot — killing reads would churn follower refresh for no correctness
  gain. Comment this asymmetry; it is the design.
- Residual known hole (document in the `DB` doc comment, don't fix): a prepared
  statement executed on an already-checked-out stale conn bypasses the wrapper.

Note for the swap paths: generation bumps happen on demote (fence — desired),
promote, and refresh swaps (connections there are read-only; a read tx whose
`Commit` fails is the documented "rare, retryable error"). Re-run the whole
suite: this change must not break `TestCachedHandleSurvivesPromotion` or the
refresh tests.

**Claim (b).** Double `Close` is unspecified: the second call runs
`SyncAndWait` on an already-closed litestream DB (CloseContext never nils
`lsDB`) and returns whatever that errors with.

**Fix (b).** Make `CloseContext` idempotent: under `mu`, if `db.closed` is
already set, return nil immediately. Document: safe to call Close more than
once (sequentially); concurrent double-Close is not strengthened.

**Claim (c) — docs only.** Two consumer foot-guns need explicit doc warnings
(on `OnDemote`/`OnPromote` and in the README leasing section): calling `Close`
from inside a callback deadlocks (`CloseContext` waits on the loop that is
running the callback — already documented as "must not call Close", say *why*);
and a follower can escape `query_only` via `PRAGMA query_only=0`, writing
locally — such writes never replicate and are destroyed by the next
restore/refresh.

**Tests:**
- *In-flight tx cannot commit after demote:* leader begins a tx, inserts;
  steal the lock; wait for `OnDemote`; `tx.Commit()` must fail; assert the row
  is absent locally (fresh query) and — after promoting another instance —
  absent from the replica's restored state. **Write it first; today Commit
  succeeds.**
- *Checked-out conn write fenced:* `conn := db.Conn(ctx)` on the leader; demote;
  `conn.ExecContext(INSERT)` must fail.
- *Reader tx across refresh swap still reads:* follower with refresh; hold a
  read tx across a swap; queries inside it keep succeeding against the old
  snapshot (pins the deliberate non-fencing of reads).
- *Double Close:* open → Close → Close returns nil; also Close on a follower,
  then Close again. No panic, no error.

---

## Item 7 — [ ] Remaining lease-lifecycle edges

Four smaller gaps, one item, one commit — all in `lease_internal_test.go`
unless noted.

**(a) Failed-promotion thrash.** Migrations run on every promotion
(`openWriterLocked`), so a follower with a broken migration acquires the lease,
does a **full restore**, fails, releases, and retries every TTL/3 forever.
Test: follower whose `Migrations` contain invalid SQL; free the lease; across
several loop ticks assert it never becomes leader, keeps serving reads, and the
lease ends **released** after each attempt (successors are not blocked).
Then add a cheap guard: exponential backoff on consecutive promotion failures
(e.g. skip the next N ticks, doubling up to ~TTL, reset on success) — keep it
tiny, inside the loop's follower branch. If it turns out ugly, ship the test
plus a `// TODO backoff` and say so at the pause.

**(b) Concurrent Open race.** `TestAutoMutualExclusion` opens sequentially.
Add: two `Open`s with `RoleAuto` launched simultaneously (same `fakeLock`,
separate LocalPaths); exactly one ends leader, the other a serving follower;
`-race` clean.

**(c) Migrations at promote-time.** No promoting follower in the suite carries
`Migrations`. Test: follower opened with a migration the leader never ran
(e.g. `CREATE TABLE IF NOT EXISTS extra …`); promote it; assert the migration
applied and pre-existing replicated data survived.

**(d) Open-failure releases the lease.** `becomeLeaderLocked` failing during
`Open` must release the just-acquired lease (`Open`'s error path). Test:
`RoleWriter` with a bad migration; `Open` fails **and** `fakeLock.lease == nil`.

---

## Item 8 — [ ] CI

There is no `.github/`. Add `.github/workflows/ci.yml`:

- **test** job (ubuntu-latest): `actions/setup-go` with
  `go-version-file: go.mod`; `gofmt -l .` must be empty; `go vet ./...`;
  `go test -race -count=1 ./...`. Set a sensible timeout (crash harness and
  lease tests are timing-based; give the job ~10 min).
- **integration** job (ubuntu-latest, Docker is preinstalled):
  `go test -tags=integration -race -count=1 ./...`. Allow ~15 min; run on push
  to master and PRs, same as test.

Nothing fancier — no lint config, no matrix — unless the user asks. Update
TESTING.md with one line pointing at CI.

---

## Item 9 — [ ] Chaos test + INVARIANTS.md

**Chaos/soak test** (`chaos_test.go`, default suite, seeded and bounded): the
Jepsen-shaped closer. K=4 instances (`RoleAuto`, some with
`FollowerRefreshInterval`, short TTL) over one `fakeLock` and one file://
replica. A seeded `rand.New(rand.NewSource(…))` drives ~seconds of random ops:

- leader writes a batch and `Sync`s (record acked+synced rows in a model set);
- leader `Close`s cleanly (later reopened as a fresh instance);
- lock stolen (simulated expiry);
- `TryPromote` storms on random followers.

Invariants asserted throughout and at the end:
1. after each settle, **at most one** live instance reports `IsLeader()`;
2. follower read counts never regress (reuse the monotonic-count pattern from
   `TestFollowerRefreshConcurrentReadsSurviveSwap`);
3. at the end, a fresh instance restored from the replica passes
   `integrity_check` and contains **every row in the model set** (acked+synced
   writes on a cleanly-closed leader are never lost; rows dropped by
   steal-demotion are recorded as *allowed-lost*, not in the model set);
4. `-race` clean.

Keep default runtime ≤ ~10s; gate a longer variant behind `!testing.Short()`.
Print the seed on failure so runs are reproducible.

**INVARIANTS.md** (repo root): the invariants currently live scattered across
README, Config docs, and test comments — and have already drifted once (item 4).
One page, one section per invariant, each naming its enforcing test(s):

1. single writer per s3:// replica (lease CAS; mutual-exclusion + chaos tests);
2. fencing timing — a non-renewing holder stops replicating before ExpiresAt
   (item 1's test);
3. demote fences the cached handle, including in-flight transactions (item 6);
4. clean Close is durable; bounded by ShutdownSyncTimeout (existing Close
   tests, crash harness clean-close variant);
5. hard kill loses at most the unsynced tail; restore is a consistent prefix
   (item 5);
6. follower staleness bounds; refresh failure keeps current state (item 3);
7. the stable `*sql.DB` is never reassigned (cached-handle tests);
8. Generation semantics — exactly what item 4's doc says, no more.

Link it from the README. Wording should be readable by a consumer deciding
whether to trust the library, not just by maintainers.

---

## Done when

All nine boxes are checked, `go vet` + `go test -race ./...` +
`go test -tags=integration ./...` pass, CI is green on master, and every
invariant in INVARIANTS.md names at least one test that fails if it breaks.
Then delete this file (repo convention: tasks are removed once landed) and
update tasks/README.md.
