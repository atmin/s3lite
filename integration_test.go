//go:build integration

package s3lite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/atmin/s3lite"
)

func TestRestoreRoundTripS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	endpoint, s3cfg := startMinIO(ctx, t, "test")
	_ = endpoint // endpoint is baked into s3cfg.Endpoint

	root := t.TempDir()
	bucketURL := "s3://test/smokedb"

	db1Path := filepath.Join(root, "db1.sqlite3")
	db1, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: db1Path,
		BackupTo:  bucketURL,
		S3:        s3cfg,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`,
		},
	})
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('hello')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2Path := filepath.Join(root, "db2.sqlite3")
	db2, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   db2Path,
		RestoreFrom: bucketURL,
		S3:          s3cfg,
	})
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("query: %v", err)
	}
	if name != "hello" {
		t.Fatalf("expected hello, got %s", name)
	}
}

func TestFirstDeployEmptyBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	_, s3cfg := startMinIO(ctx, t, "fresh")
	bucketURL := "s3://fresh/firstdb"

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: filepath.Join(t.TempDir(), "first.sqlite3"),
		BackupTo:  bucketURL,
		S3:        s3cfg,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)`,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `INSERT INTO kv VALUES ('x', '1')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync after first deploy: %v", err)
	}
}

func TestLeaseMutualExclusionAndHandoffS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	_, s3cfg := startMinIO(ctx, t, "leased")
	bucketURL := "s3://leased/db"
	root := t.TempDir()
	ttl := 3 * time.Second

	openAuto := func(owner string) *s3lite.DB {
		t.Helper()
		db, err := s3lite.Open(ctx, s3lite.Config{
			LocalPath:  filepath.Join(root, owner+".sqlite3"),
			BackupTo:   bucketURL,
			S3:         s3cfg,
			Role:       s3lite.RoleAuto,
			Owner:      owner,
			LeaseTTL:   ttl,
			Migrations: []string{`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`},
		})
		if err != nil {
			t.Fatalf("open %s: %v", owner, err)
		}
		return db
	}

	// Two Auto instances against one replica: exactly one wins the lease.
	db1 := openAuto("a")
	db2 := openAuto("b")
	defer db2.Close()

	if !db1.IsLeader() || db2.IsLeader() {
		t.Fatalf("expected exactly one leader (a); a=%v b=%v", db1.IsLeader(), db2.IsLeader())
	}

	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('leased-row')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Durable Close flushes the row and releases the lease for the successor.
	if err := db1.Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for !db2.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("follower did not promote after leader released the lease")
		}
		time.Sleep(200 * time.Millisecond)
	}

	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("promoted follower cannot read replicated row: %v", err)
	}
	if name != "leased-row" {
		t.Fatalf("expected leased-row, got %s", name)
	}
}

func startMinIO(ctx context.Context, t *testing.T, bucket string) (endpoint string, cfg s3lite.S3Config) {
	t.Helper()

	container, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate minio: %v", err)
		}
	})

	endpoint, err = container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	endpoint = "http://" + endpoint

	cfg = s3lite.S3Config{
		Region:          "us-east-1",
		Endpoint:        endpoint,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	}

	s3Client := s3sdk.New(s3sdk.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(container.Username, container.Password, ""),
		UsePathStyle: true,
	})
	if _, err := s3Client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	return endpoint, cfg
}
