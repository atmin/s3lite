# Tasks — the frontier

Intended-to-implement units of work, each self-contained (pickable without prior
context). One file per unit, `Why → What is true today → Sketch → Verify`,
**deleted once it lands** — the commits and [../INVARIANTS.md](../INVARIANTS.md)
are the record, so this list never becomes a changelog.

The list is ordered — tasks land sequentially, top first.

- [restore-progress-callback.md](restore-progress-callback.md) — `Config.OnRestoreProgress` for live %/stall detection; carries the litestream-fork `RestoreOptions` hook as ledger row 4, and its release unblocks the embedding daemon's consumer task
- [exported-refresh.md](exported-refresh.md) — a public synchronous `(*DB).Refresh` plus a replica-tip probe cheap enough to call per statement; library value on its own, prerequisite for the CLI
- [cli.md](cli.md) — the `sqlite3`-familiar shell: fresh pull per interactive statement, once per pipe, idle-yield writer lifecycle, restore progress bar on cold open

Candidates worth capturing but not committed to live in [ideas/](ideas/) —
promote one up to this directory when it's ready to land.
