# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- [trypromote.md](trypromote.md) — on-demand `TryPromote(ctx)` so a follower can acquire the lease immediately instead of waiting for the poll (closes the graceful-deploy handoff gap for consumers that opt in)
