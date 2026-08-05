# `ltx.Compactor.Compact` swallows input-header errors

Read against **superfly/ltx v0.5.1** (`compactor.go:82-87`), found 2026-07-30
while building the encrypted replica
([../../INVARIANTS.md](../../INVARIANTS.md) #11).

## What upstream does

`Compact` reads each input's header under a **named return** `retErr` and, on
failure, does a bare `return` — which returns `nil`. litestream's restore then
does `pw.CloseWithError(c.Compact(ctx))`, so the output pipe closes *cleanly* and
the real cause is replaced downstream by:

```
decode database: decode header: EOF
```

The one-line upstream fix is `retErr = err` before the bare `return`.

## Why it matters

*Any* failure to read a restore input is reported as what looks like a corrupt
database: a wrong encryption key, a tampered or truncated object, a backend
error. It affects plain litestream too, not only s3lite — the class of bug where
the error message points at the victim instead of the cause.

For an encrypted replica it is acute. "Your key is wrong" and "your database is
destroyed" are the same string, and only one of them is recoverable.

## What s3lite does about it

s3lite works around it rather than depending on it. After a failed restore,
`annotateEncryptionError` / `probeEncryptionCause`
([../../replica.go](../../replica.go)) re-open the restore-plan objects and name
the cause as a typed error — `ErrKeyMismatch`, `ErrReplicaEncrypted`,
`ErrObjectNotEncrypted` — which is what INVARIANTS.md #11's "fails cleanly"
means in practice.

If the upstream fix ever lands, that probe can be replaced by plain error
propagation and this note becomes a ledger row's epitaph rather than a standing
hazard.
