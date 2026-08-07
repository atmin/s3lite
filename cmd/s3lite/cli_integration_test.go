//go:build integration

package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atmin/s3lite"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benbjohnson/litestream"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// TestFollowerShellSeesConcurrentWriterS3 is the interactive freshness contract
// over a real object store: a row a peer commits between two prompts is there on
// the next Enter, with no interval configured and no restart.
func TestFollowerShellSeesConcurrentWriterS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := startMinIOForCLI(ctx, t, "shell")
	const bucketURL = "s3://shell/db"
	root := t.TempDir()

	writer, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:  filepath.Join(root, "writer.sqlite3"),
		BackupTo:   bucketURL,
		S3:         cfg,
		Role:       s3lite.RoleWriter,
		Owner:      "writer",
		Migrations: []string{`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`},
	})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()

	commit := func(id int) {
		t.Helper()
		if _, err := writer.ExecContext(ctx, `INSERT INTO t VALUES (?)`, id); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
		if err := writer.Sync(ctx); err != nil {
			t.Fatalf("sync %d: %v", id, err)
		}
	}
	commit(1)

	follower, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:         filepath.Join(root, "follower.sqlite3"),
		BackupTo:          bucketURL,
		S3:                cfg,
		Role:              s3lite.RoleFollower,
		Owner:             "follower",
		OnDemandPromotion: true,
	})
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer follower.Close()
	follower.SetMaxOpenConns(1)

	// Each runShell is one press of Enter at the prompt.
	if got := shellCount(t, follower); got != "1" {
		t.Fatalf("first prompt read %q rows, want 1", got)
	}
	commit(2)
	if got := shellCount(t, follower); got != "2" {
		t.Fatalf("the row a peer committed between prompts was not visible on the next Enter: read %q, want 2", got)
	}
}

func shellCount(t *testing.T, db handle) string {
	t.Helper()
	var out, errOut bytes.Buffer
	r := newREPL(db, strings.NewReader("SELECT count(*) FROM t;\n"), &out, &errOut)
	r.interactive = true
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("shell: %v\nstderr:\n%s", err, errOut.String())
	}
	return strings.TrimSpace(strings.ReplaceAll(out.String(), "s3lite> ", ""))
}

// TestIdleShellYieldsLeaseToPeerS3 is the shell's writer lifecycle end to end: a
// peer is refused while the session holds the pen, acquires once the prompt has
// been idle, and is refused again after the session's next statement takes it back.
func TestIdleShellYieldsLeaseToPeerS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := startMinIOForCLI(ctx, t, "idle")
	const bucketURL = "s3://idle/db"
	root := t.TempDir()

	// openPeer is the second CLI: --role=writer, which either takes the lease or
	// fails with *litestream.LeaseExistsError.
	openPeer := func() (*s3lite.DB, error) {
		return s3lite.Open(ctx, s3lite.Config{
			LocalPath:         filepath.Join(root, "peer.sqlite3"),
			BackupTo:          bucketURL,
			S3:                cfg,
			Role:              s3lite.RoleWriter,
			Owner:             "peer",
			LeaseTTL:          3 * time.Second,
			OnDemandPromotion: true,
		})
	}
	refused := func(what string) {
		t.Helper()
		db, err := openPeer()
		if err == nil {
			db.Close()
			t.Fatalf("the peer took the lease %s", what)
		}
		var held *litestream.LeaseExistsError
		if !errors.As(err, &held) {
			t.Fatalf("the peer failed %s with %v, want *litestream.LeaseExistsError", what, err)
		}
	}

	shell, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:         filepath.Join(root, "shell.sqlite3"),
		BackupTo:          bucketURL,
		S3:                cfg,
		Role:              s3lite.RoleAuto,
		Owner:             "shell",
		LeaseTTL:          3 * time.Second,
		OnDemandPromotion: true,
		Migrations:        []string{`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`},
	})
	if err != nil {
		t.Fatalf("open shell: %v", err)
	}
	defer shell.Close()
	shell.SetMaxOpenConns(1)
	if !shell.IsLeader() {
		t.Fatal("the shell did not take the lease at open")
	}
	refused("while the shell held it")

	// The session goes quiet; a peer keeps trying until the idle hand-back lets it
	// in, and the shell's next statement is typed only once that has happened.
	var handedOff atomic.Bool
	go func() {
		for ctx.Err() == nil {
			if peer, err := openPeer(); err == nil {
				peer.Close() // a clean release, so the shell can take it back
				handedOff.Store(true)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	var out, errOut bytes.Buffer
	r := newREPL(shell, blockUntil(handedOff.Load, "INSERT INTO t VALUES (1);\n"), &out, &errOut)
	r.interactive = true
	r.promote = true
	r.idleYield = 200 * time.Millisecond
	if err := r.run(ctx); err != nil {
		t.Fatalf("shell: %v\nstderr:\n%s", err, errOut.String())
	}
	if !handedOff.Load() {
		t.Fatal("the idle prompt never handed the lease over")
	}
	if !strings.Contains(errOut.String(), "released the write lease") {
		t.Fatalf("the hand-back went unreported:\n%s", errOut.String())
	}
	if !shell.IsLeader() {
		t.Fatal("the statement after the idle window did not take the pen back")
	}
	refused("after the shell wrote again")

	var n int
	if err := shell.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the re-promoted session holds %d rows (err %v), want 1", n, err)
	}
}

// startMinIOForCLI is the root package's MinIO harness, which is unexported there.
// The shell is a separate package, and duplicating forty lines beats exporting a
// test fixture from the library.
func startMinIOForCLI(ctx context.Context, t *testing.T, bucket string) s3lite.S3Config {
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
	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	endpoint = "http://" + endpoint

	client := s3sdk.New(s3sdk.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(container.Username, container.Password, ""),
		UsePathStyle: true,
	})
	if _, err := client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return s3lite.S3Config{
		Region:          "us-east-1",
		Endpoint:        endpoint,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	}
}
