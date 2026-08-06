# Exported refresh — a public synchronous pull, and a probe that can afford it

The CLI's follower cadence ([cli.md](cli.md)) is a fresh pull per statement,
which needs two things the library does not export: a synchronous
"refresh now" entry point, and a replica-tip probe cheap enough to call that
often. Split out of the CLI because both are library work valuable on their
own — any embedding application gets "force freshness before this read" — and
because the CLI is a weekend once they exist.

## Why

- **No exported synchronous refresh exists.** `refreshFollowerOnce` is
  unexported and documented "Called only from leaseLoop" (lease.go:627);
  `Sync` is a no-op for a follower (s3lite.go:890-896); `TryPromote`
  refreshes but also takes the pen. Per-statement freshness needs a public
  entry point that refreshes *without* promoting.
- **The naive probe cost is disqualifying.** `replicaLatestTXID`
  (replica.go:208-228) builds a throwaway client each call — by design, and
  its comment says so — then lists levels 0 through `litestream.SnapshotLevel`
  sequentially. So every "did anything change?" pays ~10 sequential listings
  plus client construction: order 200–300 ms against real S3 to usually learn
  *nothing changed*. Once a second from the lease loop that is invisible;
  per-Enter from a REPL it is both embarrassing and metered-API cost.

## What is true today (read before designing)

- **`refreshFollowerOnce`** (lease.go:635-672) already has the right
  synchronous shape: holds `promoteMu` (serialized against promotion by
  construction — lease.go:626), rechecks leadership inside the lock, probes
  via `replicaLatestTXIDFunc`, early-outs on `pos <= db.lastRefreshPos` with
  no network apply and no swap, advances the *private follow file* off the
  gate, then atomically publishes via `db.connector.swapFiles` — a failure at
  any point leaves the live read files serving their current state.
- **What "called only from leaseLoop" actually protects is not obvious** —
  the body already takes `promoteMu` and rechecks. Find the real remaining
  assumption (context lifetime, eager-mode interplay, logging cadence) and
  either discharge it or document it on the new entry point; do not export
  blind.
- **The probe lists every level for a reason**: a transaction compacted
  upward out of level 0 must still be seen (the comment at replica.go:202-207
  says exactly this). Any cheaper probe must preserve that property.
- **New transactions enter at level 0 only**; higher levels receive data via
  compaction of what level 0 already had. If that invariant holds (verify it
  against litestream's compaction and retention code before leaning on it),
  a non-empty level 0's `MaxTXID` is the global max, and the full walk is
  only needed when level 0 is empty (idle database after retention).
- **`OnRefresh`** (s3lite.go:885+) already notifies after a successful
  refresh — the new entry point should fire it identically, whichever path
  invoked the refresh.

## Sketch (settle the shape at pickup)

**The probe first**, because its cost decides whether the export is usable:

- Candidates, cheapest-to-verify first: **(a)** level-0-first early-exit —
  one listing in the common case, full walk only when level 0 is empty;
  requires the level-0 invariant above, verified not assumed. **(b)**
  parallel level listings over one shared client — no invariant needed,
  still N requests but ~1 round trip of wall clock. **(c)** a reusable
  client held by the DB rather than built per call — composes with either.
- The bar, regardless of mechanism: an unchanged-replica probe costs O(1)
  listings in the common case and constructs no per-call client. Measure
  before and after against MinIO; the number goes in the commit message.

**Then the export**: `(*DB).Refresh(ctx) (advanced bool, err error)` —
safe from any goroutine, serialized with promotion on `promoteMu`, a no-op
`(false, nil)` on a writer, `true` only when the published state actually
advanced. Body is `refreshFollowerOnce`'s, shared not duplicated; the
lease-loop caller keeps its cadence and logging unchanged.

**Out of scope:** any CLI code, any change to follower cadence defaults, and
promotion semantics — `Refresh` never takes the pen.

## Verify

- **Probe correctness**: a transaction that has been compacted out of level 0
  is still seen by the cheap probe — the exact case the full walk existed
  for, now a pinned regression test with an injected client.
- **Probe cost**: an unchanged-replica probe performs the budgeted number of
  listings (count calls via the injected client), and no client is
  constructed per call.
- **`Refresh` semantics**: follower behind the replica → `(true, nil)` and a
  subsequent read sees the new row; unchanged replica → `(false, nil)` with
  no apply and no swap; writer → `(false, nil)` untouched; `OnRefresh` fires
  on advance exactly as the loop path does.
- **Races**: concurrent `Refresh` × `TryPromote` × lease-loop tick under
  `-race` — `promoteMu` serialization means no torn rebuild, and a promote
  winning mid-`Refresh` degrades to the no-op arm.
- **In-flight reads**: a `Refresh` that swaps under an open read keeps the
  reader on its snapshot (the connector-generation semantics the follower
  tests already pin — extend, don't re-prove).
- The suite at `GOMAXPROCS=2` as well (the Close-ordering lesson), and the
  README's follower section gains the `Refresh` sentence.
