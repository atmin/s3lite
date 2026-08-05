# Encryption golden vectors — pin the one format that silently orphans a replica

## Why

The bucket format is s3lite's only compatibility surface, and the encryption
frame layout is the part of it with no reader that would complain. A wrong
`Config.EncryptionKey` fails loudly ([../INVARIANTS.md](../INVARIANTS.md) #11); a
*changed derivation* fails silently, because the writer and the reader change
together. Every existing encrypted replica becomes permanently unreadable and
nothing in CI notices.

That is not a hypothetical. Two mutations, each a plausible line of a future
refactor, were run against the full default suite:

| Mutation | Effect on every existing replica | `go test -race ./...` |
| --- | --- | --- |
| `encKeyInfoPrefix` → any other string | unreadable forever | **passes** |
| frame nonce `s.nonce[4:]` → `s.nonce[0:]` | unreadable forever | **passes** |

The reason is that all eleven encryption tests are *symmetric*: `RoundTrip`,
`RangedReads`, `Tamper`, `WrongKey`, `IdentityBinding`, `SaltIsPerObject` all seal
with the same code that opens. `TestEncryptSizeArithmetic` is the same shape one
level up — it round-trips `encCiphertextSize` against `encPlaintextSize` and
bounds overhead at "under 0.03%", so a header that grew to 1 KiB would still pass.
Self-consistency is exactly what a format change preserves.

dipperfs pins its crypto derivations against a committed `internal/enc/testdata/golden.json`
and states in its CONTRIBUTING that regenerating a vector to make a test pass is
never the fix. s3lite has the same surface, the same stakes, and no such pin.

## What is true today (read before designing)

The format, from `encrypt.go`'s package comment — the vectors must pin each line
of this, not restate it:

```
header : magic "S3LE"(4) | version(1) | frame-size code(1) | reserved(2) | salt(32)   = 40 bytes
body   : frame(0) … frame(n-1), each encFrameSize plaintext bytes + a 16-byte tag,
         final frame short (empty only for an empty object)
key    : HKDF-SHA256(EncryptionKey, salt, "s3lite-ltx-v1" ‖ level(4) ‖ minTXID(8) ‖ maxTXID(8))
nonce  : frame index, big-endian, at nonce[4:12]
AAD    : the whole 40-byte header ‖ idx(8) ‖ final(1)
AEAD   : ChaCha20-Poly1305
```

What already helps:

- **`encObjectKey(master, salt, level, minTXID, maxTXID)` takes the salt
  explicitly**, so derivation vectors need no production change at all.
- **`newRawSealingReader` and `sealForTest` already take a `frameCode`** — a
  parameter that exists, per its own comment, "only so tests can exercise every
  plaintext length across several frames cheaply". `testFrameCode = 10` (1 KiB)
  keeps vectors small. The precedent for a test-shaped seam is set.
- **`encVersion = 1` and `parseEncHeader` refuse anything else**, so a
  *deliberate* format change already has a clean mechanism. The gap is only the
  undeliberate one.

What blocks a whole-object vector:

- **`newEncHeader(frameCode)` draws the salt from `crypto/rand` with no injection
  seam.** Fixing the salt is the one production change this task needs.
- **`encObjectKey` returns a `cipher.AEAD`, not the subkey**, so the derived bytes
  cannot be asserted directly today.

## Sketch (settle the shape at pickup)

1. **Two seams, both narrow.** Split `encDeriveSubkey(master, salt, level,
   minTXID, maxTXID) ([]byte, error)` out of `encObjectKey`, which then calls it —
   the derived bytes become assertable. Add `newEncHeaderWithSalt(frameCode uint8,
   salt []byte)`, with `newEncHeader` reduced to the `rand.Read` caller that
   delegates to it. Nothing else changes.

2. **`testdata/golden.json`, committed, three groups.** Hex throughout, with a
   `format_version` field mirroring `encVersion`:
   - `derivation` — `{key, salt, level, min_txid, max_txid} → subkey` (32 bytes).
     Pins the info-string prefix, the field order, the widths, the endianness, the
     hash, and the output length. This is the group that catches mutation 1.
   - `header` — `{frame_code, salt} → header` (40 bytes). Pins offsets, the
     version byte, and the reserved bytes staying zero.
   - `object` — `{key, level, min_txid, max_txid, salt, frame_code, plaintext} →
     ciphertext`, at lengths `0, 1, frameSize-1, frameSize, frameSize+1,
     2*frameSize+7` under `frame_code: 10`. Pins framing, the nonce layout, the
     AAD composition and the final flag together. This is the group that catches
     mutation 2. Under a 1 KiB frame the whole group is a few KB of hex.

3. **Say what a failing vector means, where someone will read it.** A line in
   `encrypt.go`'s package comment and one in
   [../CONTRIBUTING.md](../CONTRIBUTING.md)'s testing section: a changed vector is
   an on-disk format change that orphans every existing replica — bump
   `encVersion` deliberately, never regenerate the vector to make the test pass.
   Without this the golden file is a speed bump, not a gate.

Deliberately out of scope: `lock.json` (plaintext by design, README), the local
files (host disk encryption), and key rotation (not supported, on purpose).

## Verify

- `go test -race ./...` and `go test -tags=integration -race ./...` pass with no
  changes to any existing test — the vectors are purely additive.
- **The acceptance check is that the two mutations above now fail.** Re-run both:
  `encKeyInfoPrefix` → any other string must fail `derivation`; `s.nonce[4:]` →
  `s.nonce[0:]` must fail `object`. A vector set that does not fail these is not
  done.
- Four more that must fail: swapping `minTXID`/`maxTXID` in the info string;
  writing a non-zero reserved byte; changing `encHeaderSize`; bumping
  `encVersion` without regenerating (it must fail *loudly*, as the reminder that
  the bump is the deliberate path).
