# Tasks

Intended-to-implement units of work, each self-contained (pickable without prior
context). Delete a file once it lands.

- [single-writer-lease.md](single-writer-lease.md) — enforce single-writer by a
  lease (reusing litestream's `s3.Leaser`) + read-only followers, so N instances
  run safely as one writer + many readers. Enables safe rolling deploys, failover,
  and read scaling.
