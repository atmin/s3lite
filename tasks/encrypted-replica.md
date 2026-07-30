# Encrypted replica — the bucket never sees plaintext

Opt-in client-side encryption of everything the replica client ships under
`BackupTo`, under a caller-supplied key. No key configured, nothing changes.

## Why

A consumer whose threat model includes the bucket operator — or anyone who
obtains read credentials — has no option today:

- **litestream dropped encryption in the LTX refactor.** The pinned fork
  rejects the config outright: *"not currently supported … revert back to
  Litestream v0.3.x"* (fork `cmd/litestream/main.go:1341`). What remains is
  SSE-C/SSE-KMS — server-side, so the provider decrypts on request.
- **s3lite has no encryption code at all.** Nothing between the local SQLite
  file and the object store.

So a product that wants to say "we cannot read your data" cannot be built on
s3lite, however it configures the bucket. This adds the one piece that makes the
claim true for the replica stream, and keeps it a `Config` field: with
`EncryptionKey` empty the object layout, the client types and the bytes on the
wire are what they are today.

What this does **not** hide, and the README must say so: object names encode
level and TXID range, object sizes are visible (so database size and write
volume are), object timestamps are in the metadata litestream already writes,
and `lock.json` stays plaintext (see below). Local state — `LocalPath`, the
`.<name>-litestream/` position dir, staging temp files — is out of scope and
rests on the host's disk encryption.

## What is true today (read before designing)

1. **There is exactly one seam, and it is narrow.** `newReplicaClient`
   (replica.go:18) is the only constructor of a replica client, feeding
   replication (s3lite.go:513), restore (replica.go:103) and the latest-TXID
   probe (replica.go:74). Wrap there and nothing escapes — including remote
   compaction, which uses the store's client. One catch: `wireReplica`
   (replica.go:57) type-asserts `*file.ReplicaClient` to set the back-reference,
   so wrap *after* wiring, or expose `Unwrap()`.

2. **Every backend peeks the LTX header on write — this is the blocker.**
   `WriteLTXFile` tees the upload stream through `ltx.PeekHeader` to extract
   `hdr.Timestamp` (fork `s3/replica_client.go:693`, identically
   `file/replica_client.go:168`), stores it as object metadata
   (`MetadataKeyTimestamp = "litestream-timestamp"`, `s3/replica_client.go:55`)
   and reads it back on the metadata path (`s3/replica_client.go:1463`) that
   timestamp-based restore and retention use. Hand it ciphertext and the write
   fails with *"extract timestamp from LTX header"*. **A pure decorator is
   therefore impossible** — the fork section below is the resolution.

3. **`info.Size` is load-bearing, and it comes from the LIST.**
   `info.Size = aws.ToInt64(obj.Size)` (`s3/replica_client.go:1571`);
   `WriteLTXFile` returns bytes written (`s3/replica_client.go:743`). Two
   consumers break if that becomes a ciphertext size: `ResumableReader`
   compares its offset against it to tell a premature EOF from a real one
   (`internal/resumable_reader.go`), and restore rejects
   `info.Size < ltx.HeaderSize` (`replica.go:706`). Sizes reported upward must
   be plaintext sizes.

4. **Ranged reads happen, so whole-object sealing is out.** Restore
   (`replica.go:713`) and remote compaction (`compactor.go:152`) wrap streams in
   `ResumableReader`, which reopens at an arbitrary byte offset via
   `OpenLTXFile(offset, 0)` (`internal/resumable_reader.go:72`) when a provider
   drops an idle connection — the failure mode that reader exists for. (The
   VFS's finer-grained `FetchPageIndex`/`FetchPage` ranged reads are not on
   s3lite's path today; the same framing serves them if they ever are.)

5. **Object identity is not a safe nonce.** Snapshot keys are
   `(SnapshotLevel, 1, pos.TXID)` (`db.go:2892`) and compaction outputs
   `(dstLevel, minTXID, maxTXID)` (`compactor.go:169`). A retried upload or a
   re-run compaction can rewrite the same key with *different* bytes — the LTX
   header timestamp alone differs — which under identity-derived nonces is
   keystream reuse. A per-object random salt is not optional.

6. **Retention is on by default and does not care.** `NewStore` defaults to
   `RetentionEnabled` with 24 h snapshot retention (`store.go:61,148`) and
   deletes by `CreatedAt` from the listing. So once encrypted writes take over
   an existing replica, superseded plaintext ages out on its own — modulo
   bucket versioning and soft-delete, which retain it regardless.

7. **`lock.json` is not on this path.** lease.go:56 builds an `s3.Leaser` with
   its own AWS client (lease.go:86). The body is
   `{generation, expires_at, owner}` (fork `leaser.go:31`) and `Owner` defaults
   to `os.Hostname()` (fork `s3/leaser.go:53`).

8. **`golang.org/x/crypto v0.52.0` is already in go.mod** (indirect), carrying
   `chacha20poly1305` and `hkdf` — a promotion to direct, no new module.

9. **The default suite is `file://` only** (TESTING.md), so the file backend has
   to work encrypted too, not just s3.

## Sketch (settle the shape at pickup)

### API — additive, two fields

```go
// EncryptionKey, when non-empty, encrypts every LTX object written to the
// replica with the caller's 32-byte key. Reads require the same key; a wrong
// key is a clean error, never data. Empty (the default) writes plaintext,
// byte-identical to a build without this feature.
EncryptionKey []byte

// RequireEncrypted refuses to read a plaintext object from an encrypted
// replica. Leave false while a previously-plaintext replica still holds
// pre-key objects; set it once every live object is encrypted, which closes
// the downgrade path where anyone with bucket write substitutes a crafted
// plaintext LTX file.
RequireEncrypted bool
```

Typed errors (`ErrReplicaEncrypted`, `ErrKeyMismatch`) so a consumer can tell
"you forgot the key" from "this bucket is not yours" and neither surfaces as a
corrupt database.

Deliberately **not** in scope: key rotation and envelope/rewrap. s3lite stays a
single-key primitive; a consumer that wants cheap rotation wraps its own key
above s3lite and keeps the master here. Say so in the doc comment, because the
per-object salt means an in-place rotation would be a full rewrite.

### Object format

```
header : magic "S3LE" | version | frame-size code | salt(32)
body   : frame(0) … frame(n-1)     each = 64 KiB plaintext + 16-byte tag,
                                   final frame short; no trailing empty frame
                                   except for an empty object
```

- `key = HKDF-SHA256(EncryptionKey, salt, info = "s3lite-ltx-v1" ‖ level ‖
  minTXID ‖ maxTXID)` — the identity binding is what stops an adversary with
  bucket write from moving a body between object names (nothing else
  authenticates the key).
- ChaCha20-Poly1305, nonce = frame counter, AAD = frame index ‖ final flag (the
  STREAM construction), so dropping trailing frames is an error rather than a
  short read.
- **Plaintext size is exact arithmetic**, which is what keeps §3 honest:
  `body = ct − hdr; n = ceil(body / (F+16)); pt = body − 16n`. Overhead 0.024 %.

### The decorator, method by method

- `WriteLTXFile` — peek the *plaintext* header for the timestamp ourselves,
  hand the inner client a stream that carries it (the fork hook, below),
  stream-encrypt in constant memory, return `FileInfo` with the plaintext size.
- `LTXFiles` — pass through, convert every `Size`.
- `OpenLTXFile(offset, size)` — map the plaintext range to a frame-aligned
  ciphertext range, decrypt, trim the edges. An `offset > 0` open needs the
  salt, so it costs a second GET for the header; cache derived keys by
  `(level, minTXID, maxTXID)` in a small LRU so the resume path stays one GET.
- `Init` / `Type` / `SetLogger` / `DeleteLTXFiles` / `DeleteAll` — delegate.
- **Mixed mode** — detect per object (our magic vs `LTX1`). Plaintext passes
  through while `RequireEncrypted` is false, so a replica that predates the key
  keeps restoring while retention ages it out; with the flag set, plaintext is
  refused.

### `lock.json` stays plaintext — decided, and documented

It is not on the ReplicaClient path (§7), its body is coordination state with no
application data, and it is the one object an operator inspects by hand. CAS
would work fine on ciphertext (`If-Match` is over the ETag), so the only gain
would be hiding the owner string — and that is better fixed directly: **when
`EncryptionKey` is set and `Owner` is empty, default `Owner` to an opaque random
per-instance id instead of the hostname.** Unencrypted consumers keep the
diagnostic hostname they have today; encrypted ones stop publishing machine
names. The README states what the lock leaks either way: generation, expiry,
owner.

## The fork — we are keeping it, so make it cheap

The timestamp hook (§2) has no upstream release to wait for, so
LITESTREAM-FORK.md's "drop the fork when the PR merges" plan no longer holds.
Restate the fork as **permanent infrastructure with a patch ledger**, and
automate the sync so carrying it costs nothing.

### Ledger, not a story

LITESTREAM-FORK.md becomes one row per carried commit, in apply order:

| # | patch | status |
|---|---|---|
| 1 | follow-mode resume ahead of snapshot | upstream PR #1385 open — drop when it ships |
| 2 | caller-supplied LTX timestamp on `WriteLTXFile` | carried; propose upstream as a generic body-transform seam |

The fork lives while any row is open; the exit workflow becomes "drop a patch",
not "drop the fork". Propose patch 2 upstream anyway — it is the natural seam
for *any* body transform (encryption, compression), so the row can eventually
close.

### Patch 2 is ten lines and inert for everyone else

In each backend's `WriteLTXFile`, ahead of the peek:

```go
if t, ok := r.(interface{ LTXTimestamp() time.Time }); ok {
    timestamp = t.LTXTimestamp()
} else {
    // …existing PeekHeader path, unchanged…
}
```

Today's callers pass `*os.File` (`replica.go:240`), a pipe reader
(`compactor.go:169`) and the snapshot reader (`db.go:2892`) — none implement it,
so every existing consumer keeps the peek. Applies to `s3/` and `file/` (§9),
and ships with a regression test in each.

### Automated sync

The tag name already encodes the base — `v0.5.15-s3lite.2` sits on `v0.5.15` —
so no state file is needed to know what to rebase from.

Fork branch `s3lite` = newest upstream release tag + the ledger's commits in
order. `.github/workflows/sync-upstream.yml` in the fork, weekly +
`workflow_dispatch`:

1. `git fetch upstream --tags`; `NEW` = newest non-prerelease upstream release.
2. `OLD` = version prefix of the newest `*-s3lite.*` tag. Exit if `NEW == OLD`.
3. `git rebase --onto $NEW $OLD s3lite`.
4. Conflict → open an issue with the conflict body and stop. A human resolves;
   that is the only manual case and it is exactly the case that deserves eyes.
5. Clean → `go build ./... && go vet ./...` plus the ledger's own tests
   (`-run 'TestReplica_Restore_Follow|LTXTimestamp'`) — each patch must prove it
   still does its job on the new base.
6. Push the branch, `git tag -a $NEW-s3lite.1`, push the tag, then
   `repository_dispatch` to s3lite.

s3lite side, `.github/workflows/litestream-pin.yml`, on that dispatch (+
manual): `go get` the new base, `go mod edit -replace` the new tag, `go mod
tidy`, open a PR. The existing ci.yml already runs test + integration on PRs, so
**the bot PR's CI is the verification** and a human merges when green — which
also preserves the "moving the base is a pin change, re-verify" rule the current
doc states, without anyone having to remember it.

Notes for whoever wires it: cross-repo dispatch and PR creation need a PAT
(`GITHUB_TOKEN` is repo-scoped); tag pushes need `contents: write`. Keep
`git format-patch` output under `patches/` in the fork so a lost branch is a
`git am --3way` away, and keep the hand-run commands in LITESTREAM-FORK.md as
the escape hatch.

## Verify

- **Round-trip both backends** — `file://` in the default suite, `s3://` under
  `-tags=integration` (MinIO). Write, restore, compact, restore again.
- **Frame boundaries** — reads at 0, `F-1`, `F`, `F+1`, and across the final
  frame; a plaintext length that is an exact multiple of `F`; an empty object.
- **The resume path** — `OpenLTXFile` with `offset > 0` matches the plaintext
  slice, plus a fault-injecting client that drops the connection mid-restore so
  `ResumableReader` genuinely reopens through the decorator.
- **Size arithmetic as a property test** — for every plaintext length in
  `0 … 3F+7`, the derived plaintext size equals the real one.
- **Tamper is always an error, never bytes** — flipped byte in a frame,
  truncated final frame, dropped trailing frames, and a body swapped between two
  object names (the identity binding).
- **Key handling** — wrong key and absent key both fail with the typed error and
  no partially-restored database; with `RequireEncrypted`, a plaintext object is
  refused; without it, a plaintext snapshot plus encrypted LTX restores
  correctly (the mixed window).
- **Retention still expires superseded files** with encryption on (integration).
- **A follower opens with the key** and fails cleanly without it.
- **Encryption does not disturb the lifecycle** — run one chaos/crash scenario
  with a key set; lease, fencing and durability invariants hold unchanged.
- **The opt-in proof** — with no key configured, no wrapper is installed and
  objects are plain LTX; the entire existing suite passes untouched, `-race`.
- INVARIANTS.md gains a numbered invariant: *an encrypted replica's objects are
  ciphertext, and a wrong or absent key fails cleanly rather than returning
  data* — naming the tests above.
- Docs in the same change: README (a client-side-encryption section stating
  plainly what is and is not hidden, including `lock.json`), TESTING.md if the
  matrix grows, LITESTREAM-FORK.md rewritten as the ledger + the automation.
- Full chain: `gofmt` → `go vet ./...` → `go test -race -count=1 ./...` →
  `go test -tags=integration -race -count=1 ./...`.
