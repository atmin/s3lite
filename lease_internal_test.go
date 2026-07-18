package s3lite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

// fakeLock is an in-memory model of a single lease lock with the same CAS
// semantics as s3.Leaser (generation fencing, etag preconditions), so the
// leader-lifecycle orchestration can be tested without a conditional-write S3.
type fakeLock struct {
	mu    sync.Mutex
	lease *litestream.Lease
	etagN int
}

func (fl *fakeLock) newLeaseLocked(gen int64, owner string, ttl time.Duration) *litestream.Lease {
	fl.etagN++
	l := &litestream.Lease{
		Generation: gen,
		ExpiresAt:  time.Now().Add(ttl),
		Owner:      owner,
		ETag:       fmt.Sprintf("etag-%d", fl.etagN),
	}
	fl.lease = l
	return cloneLease(l)
}

func (fl *fakeLock) acquire(owner string, ttl time.Duration) (*litestream.Lease, error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.lease != nil && !fl.lease.IsExpired() {
		return nil, &litestream.LeaseExistsError{Owner: fl.lease.Owner, ExpiresAt: fl.lease.ExpiresAt}
	}
	gen := int64(1)
	if fl.lease != nil {
		gen = fl.lease.Generation + 1
	}
	return fl.newLeaseLocked(gen, owner, ttl), nil
}

func (fl *fakeLock) renew(prev *litestream.Lease, owner string, ttl time.Duration) (*litestream.Lease, error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.lease == nil || fl.lease.ETag != prev.ETag {
		return nil, litestream.ErrLeaseNotHeld
	}
	return fl.newLeaseLocked(fl.lease.Generation, owner, ttl), nil
}

func (fl *fakeLock) release(prev *litestream.Lease) error {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.lease == nil {
		return nil
	}
	if fl.lease.ETag != prev.ETag {
		return litestream.ErrLeaseNotHeld
	}
	fl.lease = nil
	return nil
}

// steal forcibly takes the lock for a new owner, simulating another instance
// acquiring it after expiry (bumps generation + etag, invalidating the holder).
func (fl *fakeLock) steal(owner string, ttl time.Duration) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	gen := int64(1)
	if fl.lease != nil {
		gen = fl.lease.Generation + 1
	}
	fl.newLeaseLocked(gen, owner, ttl)
}

func cloneLease(l *litestream.Lease) *litestream.Lease {
	c := *l
	return &c
}

type fakeLeaser struct {
	lock  *fakeLock
	owner string
	ttl   time.Duration
}

func (f *fakeLeaser) Type() string { return "fake" }
func (f *fakeLeaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
	return f.lock.acquire(f.owner, f.ttl)
}
func (f *fakeLeaser) RenewLease(ctx context.Context, l *litestream.Lease) (*litestream.Lease, error) {
	return f.lock.renew(l, f.owner, f.ttl)
}
func (f *fakeLeaser) ReleaseLease(ctx context.Context, l *litestream.Lease) error {
	return f.lock.release(l)
}

// installFakeLeaser points newLeaserFunc at fakes bound to a shared lock for the
// duration of the test. Each Open gets a leaser carrying its own Config.Owner.
func installFakeLeaser(t *testing.T, lock *fakeLock) {
	t.Helper()
	prev := newLeaserFunc
	newLeaserFunc = func(_ context.Context, _ S3Config, _ string, ttl time.Duration, owner string, _ *slog.Logger) (litestream.Leaser, error) {
		if owner == "" {
			owner = "anon"
		}
		return &fakeLeaser{lock: lock, owner: owner, ttl: ttl}, nil
	}
	t.Cleanup(func() { newLeaserFunc = prev })
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

const itemsSchema = `CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`

func TestUnleasedFileBackupIsSoleWriter(t *testing.T) {
	// A file:// replica has no conditional-write lease, so the real leaser rejects
	// it and RoleAuto (the default) degrades to the sole writer — no leaser built.
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:   "file://" + t.TempDir(),
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if !db.IsLeader() {
		t.Fatal("an unleased sole writer should always be leader")
	}
	if db.Generation() != 0 {
		t.Fatalf("an unleased sole writer should have no lease generation, got %d", db.Generation())
	}
	if db.leaser != nil {
		t.Fatal("an unleased instance must not build a leaser")
	}
}

func TestLeasedRoleRequiresS3Backup(t *testing.T) {
	// RoleWriter and RoleFollower demand a lease, so a non-s3 BackupTo (which the
	// real leaser rejects) is a config error rather than a silent uncoordinated writer.
	ctx := context.Background()

	for _, role := range []Role{RoleWriter, RoleFollower} {
		_, err := Open(ctx, Config{
			LocalPath: filepath.Join(t.TempDir(), "db.sqlite3"),
			BackupTo:  "file://" + t.TempDir(),
			Role:      role,
		})
		if err == nil {
			t.Errorf("%s with a file:// BackupTo: Open succeeded, want an error", role)
		}
	}
}

func TestCloseBoundedOnUnreachableReplica(t *testing.T) {
	// A leased writer whose s3 replica is unreachable must still Close within
	// ShutdownSyncTimeout rather than hang on the final flush. A fake lease lets
	// Open reach the writer state without real S3, and a precreated DB makes Open
	// skip restore-from-replica — leaving only the replication endpoint dead.
	installFakeLeaser(t, &fakeLock{})
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	if err := precreateWAL(ctx, path); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, Config{
		LocalPath:           path,
		BackupTo:            "s3://s3lite-unreachable-bucket/prefix",
		Role:                RoleWriter,
		ShutdownSyncTimeout: 2 * time.Second,
		S3: S3Config{
			Region:          "us-east-1",
			Endpoint:        "http://127.0.0.1:1", // nothing listening
			AccessKeyID:     "x",
			SecretAccessKey: "y",
		},
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('x')`); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err = db.Close()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected Close to error on unreachable replica")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("Close did not honour ShutdownSyncTimeout; took %v", elapsed)
	}
}

func TestWriterAcquiresAndReleasesLease(t *testing.T) {
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:   "file://" + t.TempDir(),
		Role:       RoleWriter,
		Owner:      "writer-1",
		LeaseTTL:   time.Second,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !db.IsLeader() || db.Generation() != 1 {
		t.Fatalf("expected leader at generation 1, got leader=%v gen=%d", db.IsLeader(), db.Generation())
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Close must release the lease so a successor can take over immediately.
	if lock.lease != nil {
		t.Fatalf("Close should have released the lease, still held by %s", lock.lease.Owner)
	}
}

func TestWriterFailsWhenLeaseHeld(t *testing.T) {
	lock := &fakeLock{}
	lock.steal("other-instance", time.Minute)
	installFakeLeaser(t, lock)

	_, err := Open(context.Background(), Config{
		LocalPath: filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleWriter,
		Owner:     "writer-2",
	})
	var held *litestream.LeaseExistsError
	if !errors.As(err, &held) {
		t.Fatalf("expected *litestream.LeaseExistsError, got %v", err)
	}
}

func TestAutoMutualExclusion(t *testing.T) {
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	open := func(owner string) *DB {
		db, err := Open(ctx, Config{
			LocalPath:  filepath.Join(t.TempDir(), owner+".sqlite3"),
			BackupTo:   replicaDir,
			Role:       RoleAuto,
			Owner:      owner,
			LeaseTTL:   time.Second,
			Migrations: []string{itemsSchema},
		})
		if err != nil {
			t.Fatalf("open %s: %v", owner, err)
		}
		return db
	}

	db1 := open("a")
	defer db1.Close()
	db2 := open("b")
	defer db2.Close()

	leaders := 0
	for _, d := range []*DB{db1, db2} {
		if d.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaders)
	}
	if !db1.IsLeader() || db2.IsLeader() {
		t.Fatalf("db1 acquired first so should lead; db1=%v db2=%v", db1.IsLeader(), db2.IsLeader())
	}
}

func TestHandoffOnClosePromotesFollower(t *testing.T) {
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()
	ttl := 300 * time.Millisecond

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleAuto,
		Owner:      "leader",
		LeaseTTL:   ttl,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !db1.IsLeader() {
		t.Fatal("db1 should be leader")
	}

	// Follower starts before the row exists, so only promotion's restore can
	// surface it — this exercises restore-to-latest on promote.
	db2, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  replicaDir,
		Role:      RoleAuto,
		Owner:     "follower",
		LeaseTTL:  ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.IsLeader() {
		t.Fatal("db2 should be a follower while db1 holds the lease")
	}

	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('handoff')`); err != nil {
		t.Fatal(err)
	}
	// Durable Close flushes the row and releases the lease.
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, db2.IsLeader, "follower to promote")

	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("promoted follower cannot read replicated row: %v", err)
	}
	if name != "handoff" {
		t.Fatalf("expected handoff, got %s", name)
	}
	// After a clean release the lock is deleted, so the successor's generation
	// resets to 1 (this matches s3.Leaser). It must still hold a real lease.
	if got := db2.Generation(); got < 1 {
		t.Fatalf("promoted follower should hold a lease, got generation %d", got)
	}
}

func TestLeaseLossDemotesWriter(t *testing.T) {
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	ttl := 300 * time.Millisecond

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:   "file://" + t.TempDir(),
		Role:       RoleWriter,
		Owner:      "writer",
		LeaseTTL:   ttl,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	demoted := make(chan error, 1)
	db.OnDemote(func(err error) { demoted <- err })

	// Another instance steals the lock; the writer's next renew must fail and it
	// must stop leading (fencing) before the TTL could let two writers overlap.
	lock.steal("thief", time.Minute)

	select {
	case <-demoted:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not demote after losing the lease")
	}
	if db.IsLeader() {
		t.Fatal("demoted writer must not report itself as leader")
	}
}
