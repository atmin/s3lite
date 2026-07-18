# Cooperative yield — bounded-fairness writer handoff

> **Status: parked. Do not implement yet.** Captured from a brainstorm. This is a
> real design, but it is not scheduled and should not be built until we decide the
> use case justifies it. It presupposes a *migratory writer* (the lease moving
> between live instances on demand), which s3lite does not do today.

## The idea

Today the lease holder writes until it dies, deploys, or `Close`s — it never yields
to a peer that wants to write. That is correct and optimal for single-writer apps,
but it means a long-lived holder *starves* any other instance that would like a
turn. Cooperative yield adds **bounded fairness** without a coordinator, using only
more object-store operations:

1. **Signal.** An instance that wants to write sets a `want-write` marker — a small
   S3 object (or a field alongside `lock.json`) carrying a ticket / requester id.
2. **Observe.** The holder's renew loop ([../lease.go](../lease.go) `tryRenew`,
   which already ticks at `TTL/3`) checks the marker each renewal.
3. **Yield.** If a waiter is present **and** the holder is past a `minHold` floor
   **and** it is at a transaction boundary, the holder finishes its current txn,
   flushes, and releases the lease — handing off cleanly (this is `Close`'s release
   path minus the shutdown).
4. **Hand off fairly.** The waiter that set the marker gets first refusal (FIFO-ish
   via the ticket), so peers don't thundering-herd the acquire CAS.

Two bounds keep it sane:

- **`minHold` (anti-thrash floor):** never yield faster than a handoff costs
  (restore + lease ops). Without it, two eager writers ping-pong the token and the
  system does nothing but restore. The floor makes each holder do a worthwhile
  batch of writes per turn.
- **`maxHold` (anti-starvation ceiling):** guarantee eventual yield even under
  constant local write load, so a busy holder cannot pin the token forever.

Net effect: a distributed, coordinator-free, bounded-fairness `RWMutex` for the
writer role.

## Why it's interesting

- It's the missing fairness primitive if the writer ever becomes *migratory* — it
  turns "one instance hogs the lease" into "instances take fair turns."
- It reuses primitives that already exist: the lease CAS, the renew loop, and
  `Close`'s clean-release path. The only new state is one marker object and a
  min/max hold policy.
- It composes cleanly with on-demand promotion (`TryPromote`) and continuous
  follower refresh (`FollowerRefreshInterval`), both shipped: a waiter that is
  already refreshed promotes fast, and yield gives it the token.

## Why it's parked (not a task)

- **No consumer needs it yet.** It only matters once the writer role is meant to
  *migrate between live instances* on demand. s3lite's model today is a sticky
  writer that hands off on death/deploy — for which trypromote already collapses
  the gap. Fairness among competing live writers is a use case we don't have.
- **Handoff cost is the permanent tax.** Every yield pays a restore on the new
  holder (until continuous follower refresh makes that cheap). Bursty-per-node
  write patterns win; anything approaching per-write alternation loses to the
  sticky model. Whether real workloads fall on the right side is unproven.
- **More moving parts on the correctness-critical path.** A `want-write` marker,
  ticketing, and the min/max hold state machine all interact with the fencing
  guarantees (`demote`, lease generation). It deserves its own careful safety
  argument before any code — the current single-writer invariant is the crown
  jewel and must not regress.

## If we ever pick this up

Prerequisites, in order: (1) continuous follower refresh (so handoff is cheap),
(2) a concrete workload that wants the token to migrate between live instances.
Only then design the marker format, ticketing, and the min/max hold policy — with a
written fencing-safety argument up front.
