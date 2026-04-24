# s3lite

Embedded SQLite with S3-backed durability for serverless containers.

s3lite wraps [litestream](https://litestream.io) and a CGO-free SQLite driver so your serverless container can use SQLite as if it were a managed database: restore on startup from S3, replicate continuously to S3, and expose a standard `*sql.DB`.

## Use case

A small Go service deployed as a serverless container with ephemeral storage. Data fits comfortably in SQLite. You want managed-database durability without operating a database.

## Usage

```go
db, err := s3lite.Open(ctx, s3lite.Config{
    LocalPath:   "/tmp/db.sqlite3",
    RestoreFrom: "s3://my-bucket/db", // omit on first deploy
    BackupTo:    "s3://my-bucket/db",
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

## Configuration

S3 credentials are read from the environment:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ENDPOINT_URL` (for non-AWS S3, e.g. Scaleway or MinIO)

## Limitations

- Single writer. Run exactly one container instance.
- Restore happens on Open — cold starts pay this cost (typically sub-second for small DBs).
- Sub-second write-loss window if the container crashes before litestream syncs.
