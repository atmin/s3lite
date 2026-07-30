# litestream fork — the patch ledger

s3lite depends on a **fork** of litestream, not the upstream release, to carry a small
set of patches. If you just want to build s3lite you need do nothing: the fork is
pinned in `go.mod`/`go.sum` and resolves automatically.

The fork used to be temporary — one commit, one open upstream PR, "drop it when the PR
merges". One of the patches it now carries has no upstream release to wait for, so the
fork is **permanent infrastructure**. This doc is therefore a ledger of what is carried
and why, plus the automation that makes carrying it cost nothing. The exit workflow is
"drop a patch", not "drop the fork".

## The ledger

One row per carried commit, in apply order. The fork lives while any row is open.

| # | patch | why s3lite needs it | status |
|---|---|---|---|
| 1 | follow-mode resume ahead of snapshot | litestream refused to resume follow mode whenever the saved `-txid` was ahead of the *latest snapshot* — which is the normal steady state (snapshots default to 24h, deltas are continuous). Without it every incremental follower refresh degrades to a full snapshot re-download. Pins: `TestReplica_Restore_Follow_ResumeAheadOfSnapshot`. | **upstream PR [#1385](https://github.com/benbjohnson/litestream/pull/1385) open** — drop this row when it ships in a release |
| 2 | caller-supplied LTX timestamp on `WriteLTXFile` | Every backend records the LTX header's timestamp as object metadata (timestamp-based restore and retention read it back) and obtains it by teeing the upload through `ltx.PeekHeader`. That makes "the body is a readable LTX stream" a hard requirement, so a caller that *transforms* the bytes on the way out — s3lite's client-side encryption — cannot use `WriteLTXFile` at all. The patch adds an optional `litestream.LTXTimestamper` interface on the body: implement it and the backend takes the timestamp from the caller and uploads verbatim; otherwise it peeks exactly as before. Pins: `TestReplicaClient_WriteLTXFile_LTXTimestamp` in both `s3/` and `file/`. | **carried** — propose upstream as the generic body-transform seam (it is equally the hook compression would need) |

Patch 2 is inert for every existing consumer: today's callers pass an `*os.File`
(`replica.go`), an `io.PipeReader` (`compactor.go`) and the snapshot reader (`db.go`),
none of which implement the interface, so they all keep the peek path.

The branch also carries **fork infrastructure** commits, which are not ledger rows
because they change no library behaviour and never need to go upstream: the sync
workflow and `patches/` themselves, plus a one-line
`if: github.repository == 'benbjohnson/litestream'` guard on upstream's tag- and
schedule-triggered workflows. That guard exists because a `v*` tag push runs upstream's
`release.yml`, which fails trying to publish a GitHub release to `benbjohnson/litestream`
— noise on every tag the sync cuts, and plainly wrong if it ever succeeded. The fork
publishes nothing but its own tags.

## How it's wired

- Fork repo: `github.com/atmin/litestream`, branch **`s3lite`** = newest upstream
  release tag + the ledger's commits in order.
- The fork's `go.mod` intentionally **keeps `module github.com/benbjohnson/litestream`**
  (unchanged). That is what lets a `replace` by module path work without touching a
  single import in s3lite.
- s3lite `go.mod`:

  ```
  require github.com/benbjohnson/litestream v0.5.15          // the base we track
  replace github.com/benbjohnson/litestream => github.com/atmin/litestream v0.5.15-s3lite.3
  ```

- The consumed ref is an **immutable tag** — `v0.5.15-s3lite.3` is upstream `v0.5.15`
  plus the ledger. A tag (not a branch) keeps `go.sum` reproducible and decouples
  s3lite from any force-push to the fork; no submodule, no vendoring, no special CI.
- The tag name encodes its own base (`v0.5.15-s3lite.3` sits on `v0.5.15`), which is
  why the sync automation below needs no state file to know what to rebase from.
- `patches/` in the fork holds `git format-patch` output for the series, so a lost
  branch is a `git am --3way` away.

### Other branches on the fork

- `fix/follow-resume-ahead-of-snapshot` — the upstream PR branch for row 1, based on
  upstream `main`. **Leave it based on `main`**; don't rebase it onto a release tag.
- `main` — a plain mirror of upstream.

## Automated sync

`.github/workflows/sync-upstream.yml` in the fork runs weekly (and on
`workflow_dispatch`) and does the whole rebase-and-tag dance:

1. Fetch upstream tags. `NEW` = newest non-prerelease upstream release; `OLD` = the
   base encoded in the newest `*-s3lite.*` tag. Exit if they match.
2. `git rebase --onto $NEW $OLD s3lite`.
3. **Conflict → open an issue with the conflict state and stop.** A human resolves;
   that is the only manual case, and it is exactly the case that deserves eyes.
4. Clean → `go build ./... && go vet ./...`, then assert each ledger row's test still
   *exists* (a `-run` filter that matches nothing exits 0, so a silently dropped patch
   must fail the sync) and run them: `-run 'TestReplica_Restore_Follow|LTXTimestamp'`.
   Each patch has to prove it still does its job on the new base.
5. Refresh `patches/`, push the branch, tag `$NEW-s3lite.1`, push the tag.
6. `repository_dispatch` to s3lite.

On the s3lite side, `.github/workflows/litestream-pin.yml` receives that dispatch (or
runs manually), moves the `require` and the `replace` to the new base and tag, runs
`go mod tidy`, and opens a PR. **The bot PR's CI is the verification** — `ci.yml`
already runs the full test + integration suites on every PR — and a human merges when
green. That also preserves the old doc's "moving the base is a pin change, re-verify"
rule without anyone having to remember it.

Setup and operational notes:

- GitHub fires `schedule` only from a repository's **default branch**. The fork's
  default branch is therefore `s3lite`, not `main` (`main` stays as the plain upstream
  mirror). Done — noted because renaming or re-pointing it silently stops the weekly
  run, and `workflow_dispatch` would be the only thing left working.
- **A quiet repo disables its own schedule.** GitHub disables scheduled workflows after
  60 days without repository activity. The sync only commits when upstream cuts a
  release, so a two-month-quiet upstream can get the weekly run switched off; GitHub
  emails the repo admin, and re-enabling is a button in the fork's Actions tab.
  `workflow_dispatch` is unaffected. If the pin ever looks stale, check this first.
- Cross-repo `repository_dispatch` and PR creation need a PAT (`GITHUB_TOKEN` is
  repo-scoped): `S3LITE_DISPATCH_TOKEN` on the fork, and the pin workflow's PR
  creation uses the repo's own `GITHUB_TOKEN` with `pull-requests: write`. Tag pushes
  need `contents: write`. Without the PAT the sync still cuts the tag and just tells
  you to bump the pin by hand.

## Doing it by hand (the escape hatch)

```bash
cd ~/dev/litestream
git fetch upstream --tags
git rebase --onto v0.5.16 v0.5.15 s3lite       # replay the ledger onto the new base
go build ./... && go vet ./...
go test -count=1 -run 'TestReplica_Restore_Follow|LTXTimestamp' . ./s3 ./file
rm -f patches/*.patch
git format-patch v0.5.16..s3lite -o patches --no-signature -N --zero-commit \
  -- . ':(exclude)patches'
git commit -am 'chore(patches): refresh for base v0.5.16' || true
git push --force-with-lease origin s3lite
git tag -a v0.5.16-s3lite.1 -m 'litestream v0.5.16 + the s3lite patch ledger'
git push origin v0.5.16-s3lite.1
```

Then bump s3lite:

```bash
cd ~/dev/s3lite
go get github.com/benbjohnson/litestream@v0.5.16                     # move the base require
go mod edit -replace=github.com/benbjohnson/litestream=github.com/atmin/litestream@v0.5.16-s3lite.1
go mod tidy
gofmt -l . && go vet ./... && go test -race -count=1 ./... \
  && go test -tags=integration -race -count=1 ./...
```

Note: `git pull --rebase` on a fork branch rebases onto the *fork's* copy, not
upstream — always `git fetch upstream` and rebase onto the upstream tag.

## Dropping a patch

When a row's fix ships in an upstream release, remove that commit from the `s3lite`
branch, delete its ledger row, refresh `patches/`, and cut the next tag. Anything that
depended on the patch has to lose its dependency note too:

- row 1 → the follower-refresh notes in `README.md` and `INVARIANTS.md` #6;
- row 2 → the encryption notes in `README.md` and `INVARIANTS.md` #11, and the
  `LTXTimestamper` comment in `encryptclient.go`.

When the **last** row closes, drop the fork entirely:

```bash
cd ~/dev/s3lite
go mod edit -dropreplace=github.com/benbjohnson/litestream
go get github.com/benbjohnson/litestream@<version-with-everything>
go mod tidy
gofmt -l . && go vet ./... && go test -race -count=1 ./...
```

The `require` line was always present, so dropping the `replace` just falls back to
upstream. Then remove the `replace`-pointer comment in `go.mod`, delete this file, and
drop the fork references from `README.md` and `INVARIANTS.md`. The fork repo and tags
can go at your leisure.
