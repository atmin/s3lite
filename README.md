# s3lite

Embedded SQLite with S3-backed durability for serverless containers.

s3lite wraps [litestream](https://litestream.io) and a CGO-free SQLite driver so your serverless container can use SQLite as if it were a managed database: restore on startup from S3, replicate continuously to S3, and expose a standard `*sql.DB`.

## Use case

A small Go service deployed as a serverless container with ephemeral storage. Data fits comfortably in SQLite. You want managed-database durability without operating a database.

## Usage

```go
db, err := s3lite.Open(ctx, s3lite.Config{
    LocalPath:   "/tmp/db.sqlite3",
    RestoreFrom: "s3://my-bucket/db",
    BackupTo:    "s3://my-bucket/db",
    S3: s3lite.S3Config{
        Region:          os.Getenv("AWS_REGION"),
        Endpoint:        os.Getenv("AWS_ENDPOINT_URL"), // for MinIO/Scaleway/etc.
        AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
    },
    Migrations: []string{
        `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)`,
        `CREATE INDEX IF NOT EXISTS users_email ON users(email)`,
    },
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// db embeds *sql.DB — use it directly
rows, err := db.QueryContext(ctx, "SELECT id, email FROM users")
```

Point `RestoreFrom` and `BackupTo` at the same URL — restore what you've been backing up. On first deploy the replica is empty; `Open` handles that as a no-op and starts with a fresh DB.

## Single writer + read followers (leasing)

litestream requires exactly one writer per replica. By default (`RoleOff`) s3lite
does not enforce that — every instance with `BackupTo` set replicates as a
writer, so you must guarantee a single instance yourself. Set `Config.Role` to
have s3lite enforce single-writer by a **lease** (litestream's `s3.Leaser`, stored
at `<BackupTo path>/lock.json`), so N instances run safely as one writer + many
read-only followers:

```go
db, err := s3lite.Open(ctx, s3lite.Config{
    LocalPath: "/tmp/db.sqlite3",
    BackupTo:  "s3://my-bucket/db", // leasing requires an s3:// replica
    S3:        s3cfg,
    Role:      s3lite.RoleAuto, // acquire the lease if free, else follow
    Migrations: []string{ /* ... */ },
})
...
if db.IsLeader() {
    // safe to write
}
db.OnPromote(func()      { /* started accepting writes */ })
db.OnDemote(func(err error) { /* stop accepting writes now */ })
```

Roles:
- **`RoleWriter`** — acquire the lease or fail `Open` with `*litestream.LeaseExistsError`.
- **`RoleFollower`** — open read-only, never replicate; promote to writer if the
  lease becomes free.
- **`RoleAuto`** — acquire if free (writer) else follow. The mode a serverless
  consumer wants: safe rolling deploys (handoff by lease), writer failover, and
  read scaling, all by construction.

The holder renews at `LeaseTTL/3` (default TTL 30s); a holder that cannot renew
**stops replicating immediately** (before the TTL could let anyone else acquire),
so two writers never overlap. `Close` releases the lease so a successor takes over
at once instead of waiting out the TTL.

Followers serve the snapshot they restored at `Open` and refresh on **promotion**
(a follower reopens after restoring the latest state before it starts writing);
continuous follower refresh is not yet implemented. Consequently a follower
replaces its embedded `*sql.DB` when it promotes — `RoleAuto`/`RoleFollower`
consumers should gate access on `IsLeader`/`OnPromote` rather than caching the
handle across a role change.

## Configuration

s3lite itself reads no environment variables. Pass S3 settings via `S3Config`.
Empty fields fall through to the AWS SDK's default credential chain (env vars,
`~/.aws/config`, IAM roles), so on EC2/ECS/Lambda you can leave credentials
blank and rely on the instance role.

## Limitations

- Single writer per replica. Enforce it yourself (one instance) or let s3lite
  enforce it with a lease — see [Single writer + read followers](#single-writer--read-followers-leasing).
- Restore happens on Open — cold starts pay this cost (typically sub-second for small DBs).
- Followers serve their Open-time snapshot and only refresh on promotion;
  continuous follower refresh is not yet implemented.
- A clean `Close` is durable: it flushes all committed writes to the replica
  before returning (bounded by `Config.ShutdownSyncTimeout`, default 30s). Only a
  *hard* crash/kill can lose the sub-second window since litestream's last sync.
