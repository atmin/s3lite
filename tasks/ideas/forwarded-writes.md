# Forwarded writes — one physical writer, everyone forwards

> **Status: parked. Do not implement yet.** Platform-dependent and against the
> scale-to-zero grain. Captured so the reasoning isn't lost.

## The idea

Give every instance a *writable-looking* handle without a second physical writer.
The distributed analogue of a single-threaded event loop: one thread (the leader)
does all the writing; everyone else feeds work into it. Followers forward their
`INSERT`/`UPDATE`/transaction to the current leader over RPC; the leader applies
them in its natural serial order and replicates as usual. This is rqlite's model
(Raft + forward-to-leader), minus Raft — s3lite's lease already elects the leader.

- The lease object publishes the **leader's address** so followers know where to
  forward.
- Correctness stays trivial: the SQLite file still has exactly one writer. No OCC,
  no rebase, no conflict handling.
- Pairs with continuous follower refresh for reads, so an instance can read locally
  and write remotely — a full read/write handle everywhere.

## Why it's interesting

- It's the *cheapest* way to make s3lite feel multi-writer: no handoff cost (the
  token never moves), no state-transfer tax, no new consistency model.
- It removes the "nothing routes for you" caveat from the README — the library
  could route writes itself.

## Why it's parked (not a task)

- **Container networking often forbids it.** Forwarding needs an instance to dial a
  *specific other instance*:
  - Orchestrators with service discovery (k8s headless Services / StatefulSets,
    Nomad, plain Docker networks) — feasible.
  - Fly.io — feasible via per-Machine `.internal` addresses.
  - **Scale-to-zero FaaS (Cloud Run, Lambda, Knative, most "just deploy it"
    platforms) — generally not possible.** Instances sit behind a load balancer
    with no stable per-instance address and no mesh. These are exactly s3lite's
    core target.
- **Against the grain.** Forwarding requires the leader to be continuously
  reachable at a known address — which cuts against the writer scaling to zero, the
  property that defines s3lite. You can't forward to a leader that isn't listening.
- **New surface.** An RPC transport, auth between instances, leader-address
  publication/staleness handling, and retry-on-leadership-change all become
  correctness-critical — a lot of machinery for a capability only some platforms
  can host.

## If we ever pick this up

Scope it to platforms with instance addressability (document k8s/Fly, exclude pure
FaaS), publish the leader address via the lease with a freshness/fencing check, and
define behaviour when the leader moves mid-forward (retry against the new address,
bounded). Treat it as an optional transport, never the default.
