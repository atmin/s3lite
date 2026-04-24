# Manual S3 smoke test

The automated suite uses `file://` replicas and requires no external services.
Use the recipe below to validate against real S3-compatible storage.

## MinIO (local)

```bash
docker run --rm -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address :9001
```

Create a bucket named `test` via the console at http://localhost:9001, then:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000
```

## Scaleway Object Storage

```bash
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_REGION=fr-par
export AWS_ENDPOINT_URL=https://s3.fr-par.scw.cloud
```

## Smoke test program

Create `cmd/smoke/main.go` in a scratch directory:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/atmin/s3lite"
)

func main() {
	ctx := context.Background()
	bucket := os.Args[1] // e.g. s3://test/smokedb

	s3cfg := s3lite.S3Config{
		Region:          os.Getenv("AWS_REGION"),
		Endpoint:        os.Getenv("AWS_ENDPOINT_URL"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}

	// Write pass
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: "/tmp/smoke.sqlite3",
		BackupTo:  bucket,
		S3:        s3cfg,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, val TEXT)`,
		},
	})
	if err != nil {
		log.Fatal("open:", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO kv VALUES ('hello','world')`); err != nil {
		log.Fatal("insert:", err)
	}
	if err := db.Sync(ctx); err != nil {
		log.Fatal("sync:", err)
	}
	if err := db.Close(); err != nil {
		log.Fatal("close:", err)
	}
	os.Remove("/tmp/smoke.sqlite3")

	// Restore pass
	db2, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   "/tmp/smoke-restored.sqlite3",
		RestoreFrom: bucket,
		S3:          s3cfg,
	})
	if err != nil {
		log.Fatal("restore open:", err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRowContext(ctx, `SELECT val FROM kv WHERE key='hello'`).Scan(&val); err != nil {
		log.Fatal("query:", err)
	}
	fmt.Println(val) // should print: world
}
```

Run with:

```bash
go run ./cmd/smoke s3://test/smokedb
```
