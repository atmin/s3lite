//go:build integration

package s3lite_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/benbjohnson/litestream"
	mobyclient "github.com/moby/moby/client"
	tc "github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/atmin/s3lite"
)

func TestRestoreRoundTripS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	env := startMinIO(ctx, t, "test")

	root := t.TempDir()
	bucketURL := "s3://test/smokedb"

	db1Path := filepath.Join(root, "db1.sqlite3")
	db1, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: db1Path,
		BackupTo:  bucketURL,
		S3:        env.cfg,
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
		S3:          env.cfg,
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

	env := startMinIO(ctx, t, "fresh")
	bucketURL := "s3://fresh/firstdb"

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: filepath.Join(t.TempDir(), "first.sqlite3"),
		BackupTo:  bucketURL,
		S3:        env.cfg,
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

	env := startMinIO(ctx, t, "leased")
	bucketURL := "s3://leased/db"
	root := t.TempDir()
	ttl := 3 * time.Second

	openAuto := func(owner string) *s3lite.DB {
		t.Helper()
		db, err := s3lite.Open(ctx, s3lite.Config{
			LocalPath:  filepath.Join(root, owner+".sqlite3"),
			BackupTo:   bucketURL,
			S3:         env.cfg,
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

	waitForCond(t, 30*time.Second, db2.IsLeader, "follower to promote after leader released the lease")

	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("promoted follower cannot read replicated row: %v", err)
	}
	if name != "leased-row" {
		t.Fatalf("expected leased-row, got %s", name)
	}
}

func TestFollowerIncrementalRefreshS3(t *testing.T) {
	// Over real object storage (MinIO), a follower with FollowerRefreshInterval catches
	// up to the leader's writes through the incremental follow path — proving
	// litestream's Restore(Follow) resume works against the s3 ReplicaClient, not just
	// the file:// client the unit tests use. The private .follow file and its advancing
	// TXID sidecar confirm the incremental path ran (a full re-restore would write only
	// LocalPath, never a .follow sidecar).
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	env := startMinIO(ctx, t, "followrefresh")
	bucketURL := "s3://followrefresh/db"
	root := t.TempDir()

	leader, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:  filepath.Join(root, "leader.sqlite3"),
		BackupTo:   bucketURL,
		S3:         env.cfg,
		Role:       s3lite.RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`},
	})
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leader.Close()

	insert := func(name string) {
		t.Helper()
		if _, err := leader.ExecContext(ctx, `INSERT INTO items (name) VALUES (?)`, name); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
		if err := leader.Sync(ctx); err != nil {
			t.Fatalf("sync %s: %v", name, err)
		}
	}
	insert("v1")

	followerPath := filepath.Join(root, "follower.sqlite3")
	follower, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:               followerPath,
		BackupTo:                bucketURL,
		S3:                      env.cfg,
		Role:                    s3lite.RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer follower.Close()
	cached := follower.DB

	insert("v2")
	waitForCond(t, 30*time.Second, func() bool {
		var n int
		return cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n) == nil && n == 2
	}, "follower to catch up to v2 over s3")

	before, err := litestream.ReadTXIDFile(followerPath + ".follow")
	if err != nil || before == 0 {
		t.Fatalf("incremental follow file not established over s3: txid=%s err=%v", before, err)
	}

	insert("v3")
	waitForCond(t, 30*time.Second, func() bool {
		var n int
		return cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n) == nil && n == 3
	}, "follower to resume and catch up to v3 over s3")

	after, err := litestream.ReadTXIDFile(followerPath + ".follow")
	if err != nil {
		t.Fatalf("read follow sidecar: %v", err)
	}
	if after <= before {
		t.Fatalf("follow sidecar must advance in place across incremental ticks: before=%s after=%s", before, after)
	}
}

func TestLeaseStealFencesWriterS3(t *testing.T) {
	// The real-S3 replay of the fake suite's steal, validating fakeLock's fidelity to
	// s3.Leaser over MinIO: planting a foreign, already-expired lock.json rotates the
	// object's etag, so the writer's next renew fails its If-Match precondition (412 →
	// ErrLeaseNotHeld) and the writer must demote and fence. The planted lease is
	// expired, so the next acquirer takes over at generation+1 — either instance may
	// win that race; the invariant is exactly one leader, serving the replicated data.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	env := startMinIO(ctx, t, "steal")
	bucketURL := "s3://steal/db"
	root := t.TempDir()
	ttl := 2 * time.Second

	db1, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:  filepath.Join(root, "w1.sqlite3"),
		BackupTo:   bucketURL,
		S3:         env.cfg,
		Role:       s3lite.RoleWriter,
		Owner:      "w1",
		LeaseTTL:   ttl,
		Migrations: []string{`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`},
	})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer db1.Close()
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('first')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}

	db2, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: filepath.Join(root, "w2.sqlite3"),
		BackupTo:  bucketURL,
		S3:        env.cfg,
		Role:      s3lite.RoleAuto,
		Owner:     "w2",
		LeaseTTL:  ttl,
	})
	if err != nil {
		t.Fatalf("open contender: %v", err)
	}
	defer db2.Close()
	if db2.IsLeader() {
		t.Fatal("contender must open as a follower while w1 holds the lease")
	}

	demoted := make(chan error, 1)
	db1.OnDemote(func(err error) { demoted <- err })

	// Plant a foreign, expired lease over the live lock object (unconditional PUT —
	// the state change an expiry-takeover leaves behind). The generation seeds the
	// successor's fencing token: acquire over an expired lease bumps it by one.
	const plantedGeneration = 7
	body, err := json.Marshal(litestream.Lease{
		Generation: plantedGeneration,
		ExpiresAt:  time.Now().Add(-time.Minute),
		Owner:      "thief",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket:      aws.String("steal"),
		Key:         aws.String("db/lock.json"), // "<BackupTo path>/lock.json"
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}); err != nil {
		t.Fatalf("plant foreign lease: %v", err)
	}

	// The writer's next renew must fail the etag precondition and demote.
	select {
	case <-demoted:
	case <-time.After(30 * time.Second):
		t.Fatal("writer did not demote after its lock was replaced over real S3")
	}
	if db1.IsLeader() {
		t.Fatal("demoted writer must not report itself as leader")
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('fenced')`); err == nil {
		t.Fatal("demoted writer's handle must reject writes")
	}

	// The planted lease is expired, so the system converges back to exactly one
	// leader — the re-polling demoted instance or the contender, whichever acquires.
	waitForCond(t, 30*time.Second, func() bool {
		return db1.IsLeader() != db2.IsLeader() && (db1.IsLeader() || db2.IsLeader())
	}, "exactly one leader after the steal settles")

	winner, loser := db1, db2
	if db2.IsLeader() {
		winner, loser = db2, db1
	}
	// Acquiring over the expired planted lease continues its generation sequence.
	if got := winner.Generation(); got != plantedGeneration+1 {
		t.Fatalf("winner generation: got %d, want %d (planted %d + 1)", got, plantedGeneration+1, plantedGeneration)
	}
	// The winner restored the pre-steal state and owns writes; the loser is fenced.
	var name string
	if err := winner.QueryRowContext(ctx, `SELECT name FROM items WHERE name='first'`).Scan(&name); err != nil {
		t.Fatalf("winner cannot read the pre-steal row: %v", err)
	}
	if _, err := winner.ExecContext(ctx, `INSERT INTO items (name) VALUES ('second')`); err != nil {
		t.Fatalf("winner cannot write: %v", err)
	}
	if _, err := loser.ExecContext(ctx, `INSERT INTO items (name) VALUES ('nope')`); err == nil {
		t.Fatal("loser must reject writes")
	}
}

func TestWriterSurvivesReplicaOutageS3(t *testing.T) {
	// Replication-path network fault: S3 disappears mid-run and comes back. The
	// container is paused, not stopped — a paused MinIO black-holes requests (the
	// nastier failure mode: nothing fails fast, everything hangs) and keeps its port
	// mapping, which a stop/start cycle does not guarantee. The local-first promise
	// under an outage is: writes keep succeeding locally, Sync fails bounded (no
	// hang), replication resumes on recovery, and a clean Close afterwards is fully
	// durable. The lease TTL is long so no renew falls inside the outage — this
	// isolates the replication path from lease fencing, which has its own test above.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	env := startMinIO(ctx, t, "outage")
	bucketURL := "s3://outage/db"

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: filepath.Join(t.TempDir(), "writer.sqlite3"),
		BackupTo:  bucketURL,
		S3:        env.cfg,
		Role:      s3lite.RoleWriter,
		Owner:     "writer",
		LeaseTTL:  5 * time.Minute,
		// litestream retries loudly (and correctly) all through the outage; those
		// expected WARNs would drown the test log.
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Migrations: []string{`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('before')`); err != nil {
		t.Fatalf("insert before outage: %v", err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync before outage: %v", err)
	}

	// Take S3 away: pause freezes MinIO so every request black-holes, while the
	// endpoint and the container's data stay exactly as they are.
	docker, err := tc.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatalf("docker client: %v", err)
	}
	defer docker.Close()
	containerID := env.container.GetContainerID()
	if _, err := docker.ContainerPause(ctx, containerID, mobyclient.ContainerPauseOptions{}); err != nil {
		t.Fatalf("pause minio: %v", err)
	}
	unpaused := false
	defer func() { // never leave it paused if an assertion below bails out
		if !unpaused {
			_, _ = docker.ContainerUnpause(context.Background(), containerID, mobyclient.ContainerUnpauseOptions{})
		}
	}()

	// Local-first: the application keeps writing as if nothing happened.
	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('during')`); err != nil {
		t.Fatalf("a write during the outage must succeed locally: %v", err)
	}

	// A bounded Sync fails at its deadline instead of hanging into the black hole.
	syncCtx, syncCancel := context.WithTimeout(ctx, 3*time.Second)
	start := time.Now()
	err = db.Sync(syncCtx)
	syncCancel()
	if err == nil {
		t.Fatal("Sync during the outage must return an error")
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("Sync did not honour its deadline during the outage; took %v", elapsed)
	}

	// S3 comes back; replication must recover without reopening anything.
	if _, err := docker.ContainerUnpause(ctx, containerID, mobyclient.ContainerUnpauseOptions{}); err != nil {
		t.Fatalf("unpause minio: %v", err)
	}
	unpaused = true
	waitForCond(t, 60*time.Second, func() bool {
		syncCtx, syncCancel := context.WithTimeout(ctx, 3*time.Second)
		defer syncCancel()
		return db.Sync(syncCtx) == nil
	}, "replication to recover after the outage")

	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('after')`); err != nil {
		t.Fatalf("insert after recovery: %v", err)
	}
	if err := db.Close(); err != nil { // durable flush must now succeed
		t.Fatalf("close after recovery: %v", err)
	}

	// Everything — including the row written while S3 was down — survived to the replica.
	restored, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(t.TempDir(), "restored.sqlite3"),
		RestoreFrom: bucketURL,
		S3:          env.cfg,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	defer restored.Close()
	var n int
	if err := restored.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatalf("read restored: %v", err)
	}
	if n != 3 {
		t.Fatalf("restored %d rows, want 3 (before, during, after)", n)
	}
}

// minioEnv is a running MinIO container plus everything a test needs to talk to it:
// the s3lite config pointing at it, and a raw SDK client for out-of-band object
// manipulation (planting lock files, creating buckets).
type minioEnv struct {
	container *tcminio.MinioContainer
	endpoint  string
	cfg       s3lite.S3Config
	client    *s3sdk.Client
}

func startMinIO(ctx context.Context, t *testing.T, bucket string) minioEnv {
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

	cfg := s3lite.S3Config{
		Region:          "us-east-1",
		Endpoint:        endpoint,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	}

	client := s3sdk.New(s3sdk.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(container.Username, container.Password, ""),
		UsePathStyle: true,
	})
	if _, err := client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	return minioEnv{container: container, endpoint: endpoint, cfg: cfg, client: client}
}

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}
