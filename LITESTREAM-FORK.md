# litestream fork

s3lite depends on a **fork** of litestream, not the upstream release, to carry one
small fix. This doc explains why, how it's wired, how to keep it current, and how to
drop it. If you just want to build s3lite, you don't need to do anything — the fork is
pinned in `go.mod`/`go.sum` and resolves automatically.

## Why the fork exists

litestream's follow-mode crash-recovery **rejected resuming** whenever the saved
`-txid` was ahead of the *latest snapshot* — which is the normal steady state, since
snapshots default to every 24h while deltas are continuous. That rejection blocks the
incremental follower-refresh work (see `tasks/incremental-follower-refresh.md`): every
resume would fall back to a full snapshot re-download.

The fix bounds the check by the newest TXID across *all* levels instead of just the
latest snapshot. It's ~23 lines in `replica.go` plus a regression test
(`TestReplica_Restore_Follow_ResumeAheadOfSnapshot`).

An upstream PR to `benbjohnson/litestream` is open for this fix. **Upstream PR:**
[benbjohnson/litestream#1385](https://github.com/benbjohnson/litestream/pull/1385).
Until it merges and ships in a release, we carry the fork.

## How it's wired

- Fork repo: `github.com/atmin/litestream`.
- The fork's `go.mod` intentionally **keeps `module github.com/benbjohnson/litestream`**
  (unchanged). That is what lets a `replace` by module path work without touching any
  import in s3lite.
- s3lite `go.mod`:

  ```
  require github.com/benbjohnson/litestream v0.5.15          // the base we track
  replace github.com/benbjohnson/litestream => github.com/atmin/litestream v0.5.15-s3lite.1
  ```

- The consumed ref is an **immutable tag** `v0.5.15-s3lite.1` = upstream `v0.5.15` +
  the single fix commit. A tag (not a branch) decouples s3lite from any force-push to
  the fork's PR branch and keeps `go.sum` reproducible; no submodule, no vendoring, no
  special CI steps.

### Branches on the fork

- `fix/follow-resume-ahead-of-snapshot` — the PR branch, based on upstream `main`.
  **Leave it based on `main`** for the upstream PR; don't rebase it onto a release tag.
- The `v0.5.15-s3lite.N` tags are what s3lite consumes.

## Sync workflow (pull in new upstream releases)

When upstream cuts a new release and you want it under the fix (the fix is still
unmerged), rebase the one fix commit onto the new version and cut the next tag:

```bash
cd ~/dev/litestream
git fetch upstream --tags
# Replay the single fix commit onto the new upstream version (e.g. v0.5.16):
git rebase --onto v0.5.16 <old-base> fix/follow-resume-ahead-of-snapshot
git push --force-with-lease origin fix/follow-resume-ahead-of-snapshot   # PR branch
git tag -a v0.5.16-s3lite.1 -m "v0.5.16 + follow-resume fix (for s3lite)"
git push origin v0.5.16-s3lite.1
```

Then bump s3lite:

```bash
cd ~/dev/s3lite
go get github.com/benbjohnson/litestream@v0.5.16           # move the base require
go mod edit -replace=github.com/benbjohnson/litestream=github.com/atmin/litestream@v0.5.16-s3lite.1
go mod tidy
go build ./... && go vet ./... && go test -count=1 ./...
```

Note: `git pull --rebase` on the fix branch rebases onto the fork's own copy of the
branch, **not** upstream — always `git fetch upstream` + rebase onto the upstream tag.

**Moving the base is a pin change:** re-verify anything that was validated against the
old version. For the follower-refresh feature specifically, re-run the follow-mode
probe (`go test -run TestReplica_Restore_Follow ./` in the fork) after the bump.

## Exit workflow (when the upstream PR merges)

Once the fix ships in an upstream release, drop the fork entirely:

```bash
cd ~/dev/s3lite
go mod edit -dropreplace=github.com/benbjohnson/litestream
go get github.com/benbjohnson/litestream@<version-with-the-fix>
go mod tidy
go build ./... && go vet ./... && go test -count=1 ./...
```

The `require` line was always present, so dropping the `replace` just falls back to
upstream. Then remove the `replace`-pointer comment in `go.mod`, delete this file, and
drop the fork-dependency note from `tasks/incremental-follower-refresh.md` /
`INVARIANTS.md`. The fork repo/tags can be deleted at your leisure.
