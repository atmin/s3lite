package s3lite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
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
	lock        *fakeLock
	owner       string
	ttl         time.Duration
	acquireN    atomic.Int64 // number of AcquireLease attempts that reached the lock
	renewN      atomic.Int64 // number of RenewLease attempts that reached the leaser
	renewBlocks atomic.Bool  // model a renew into a network black hole (see RenewLease)
}

func (f *fakeLeaser) Type() string { return "fake" }
func (f *fakeLeaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
	if err := ctx.Err(); err != nil { // mirror the real leaser: acquisition does I/O
		return nil, err
	}
	f.acquireN.Add(1)
	return f.lock.acquire(f.owner, f.ttl)
}
func (f *fakeLeaser) RenewLease(ctx context.Context, l *litestream.Lease) (*litestream.Lease, error) {
	f.renewN.Add(1)
	if f.renewBlocks.Load() {
		// A renew into a network black hole: S3 requests have no overall response
		// timeout, so the call returns nothing until the caller's ctx is done. This
		// is the hazard item 1 fences against — the renew must not outlive its lease.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

func TestCachedHandleSurvivesPromotion(t *testing.T) {
	// Reproduces the production hazard: a caller grabs the *sql.DB once (the
	// obvious `database := s3db.DB`, then hands it to repositories) and reuses
	// that exact handle. It must keep working across a promote even though promote
	// rebuilds the local file underneath — the handle identity never changes and
	// the pool transparently re-dials the restored, now-writable database.
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

	// Capture the concrete handle exactly as an application would, before any role
	// change. Everything below uses this pointer, never db2.DB.
	cached := db2.DB
	if err := cached.PingContext(ctx); err != nil {
		t.Fatalf("cached follower handle unusable: %v", err)
	}
	// A follower handle is read-only.
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('nope')`); err == nil {
		t.Fatal("expected write on a read-only follower to fail")
	}

	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('handoff')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil { // durable flush + lease release
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, db2.IsLeader, "follower to promote")

	// The SAME cached handle must now see the replicated row and accept writes.
	var name string
	if err := cached.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("cached handle cannot read replicated row after promote: %v", err)
	}
	if name != "handoff" {
		t.Fatalf("expected handoff, got %q", name)
	}
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('after-promote')`); err != nil {
		t.Fatalf("cached handle cannot write after promote: %v", err)
	}
	if cached != db2.DB {
		t.Fatal("handle identity changed across promote; callers that cached it would break")
	}
}

func TestCachedHandleConcurrentReadsAcrossPromotion(t *testing.T) {
	// Hammer the cached handle with concurrent reads while a promote rebuilds the
	// file underneath. Reads may fail transiently during the swap (downtime is
	// acceptable), but the connector must never race, never wedge the handle, and
	// must recover to steady success once promotion completes.
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
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('seed')`); err != nil {
		t.Fatal(err)
	}

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
	cached := db2.DB

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var n int
				_ = cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n)
			}
		}()
	}

	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, db2.IsLeader, "follower to promote under load")
	close(stop)
	wg.Wait()

	// After the storm settles, the same handle must read and write cleanly.
	var n int
	if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatalf("cached handle broken after concurrent promote: %v", err)
	}
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('post')`); err != nil {
		t.Fatalf("cached handle cannot write after concurrent promote: %v", err)
	}
}

func TestCachedHandleFencedOnDemote(t *testing.T) {
	// A writer that loses its lease must stop accepting writes on the exact handle
	// the caller cached, without that handle being closed out from under them.
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

	cached := db.DB
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('while-leader')`); err != nil {
		t.Fatalf("leader handle should accept writes: %v", err)
	}

	demoted := make(chan error, 1)
	db.OnDemote(func(err error) { demoted <- err })
	lock.steal("thief", time.Minute)

	select {
	case <-demoted:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not demote after losing the lease")
	}

	// Same handle, now fenced: writes must be rejected, reads must still work.
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('after-demote')`); err == nil {
		t.Fatal("demoted handle must reject writes")
	}
	if err := cached.PingContext(ctx); err != nil {
		t.Fatalf("demoted handle should still serve reads: %v", err)
	}
}

func TestTryPromoteConcurrentSingleFlight(t *testing.T) {
	// N concurrent TryPromote calls on a follower whose lease is free must produce
	// exactly one restore/promote (single-flight under promoteMu) and all return
	// (true, nil). A long TTL keeps the background loop from ticking, so every
	// promotion here comes from TryPromote.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.IsLeader() {
		t.Fatal("db should start as a follower")
	}

	var promotes atomic.Int64
	db.OnPromote(func() { promotes.Add(1) })

	const n = 16
	results := make([]bool, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = db.TryPromote(ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: unexpected error %v", i, errs[i])
		}
		if !results[i] {
			t.Errorf("goroutine %d: TryPromote returned false, want true", i)
		}
	}
	if got := promotes.Load(); got != 1 {
		t.Fatalf("expected exactly one promotion under concurrency, got %d", got)
	}
	if !db.IsLeader() {
		t.Fatal("db should be leader after TryPromote")
	}
}

func TestTryPromoteStillHeldReturnsFalse(t *testing.T) {
	// A lease held by a live writer elsewhere makes TryPromote a no-op: (false, nil),
	// and the instance stays a read-only follower.
	lock := &fakeLock{}
	lock.steal("other-instance", time.Minute)
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ok, err := db.TryPromote(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("TryPromote should return false while the lease is held elsewhere")
	}
	if db.IsLeader() {
		t.Fatal("instance must stay a follower")
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE x (i INTEGER)`); err == nil {
		t.Fatal("follower handle must reject writes (query_only)")
	}
}

func TestTryPromoteAlreadyLeaderDoesNoIO(t *testing.T) {
	// On the writer, TryPromote is a pure fast-path getter: no AcquireLease call.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:   "file://" + t.TempDir(),
		Role:       RoleWriter,
		Owner:      "writer",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.IsLeader() {
		t.Fatal("writer should be leader")
	}

	fl := db.leaser.(*fakeLeaser)
	before := fl.acquireN.Load()
	ok, err := db.TryPromote(ctx)
	if err != nil || !ok {
		t.Fatalf("TryPromote on leader: ok=%v err=%v, want true/nil", ok, err)
	}
	if got := fl.acquireN.Load() - before; got != 0 {
		t.Fatalf("TryPromote on the leader must do no AcquireLease, got %d", got)
	}
}

func TestTryPromoteSoleWriter(t *testing.T) {
	// With no s3:// BackupTo there is no leaser; the sole writer is always the
	// writer, so TryPromote returns (true, nil) immediately without touching one.
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
	if db.leaser != nil {
		t.Fatal("a non-s3 BackupTo must not build a leaser")
	}

	ok, err := db.TryPromote(ctx)
	if err != nil || !ok {
		t.Fatalf("sole writer TryPromote: ok=%v err=%v, want true/nil", ok, err)
	}
}

func TestTryPromotePromotedHandleIsFreshAndWritable(t *testing.T) {
	// After a TryPromote that actually promotes, the handle sees the previous
	// writer's latest state (restored) and accepts writes.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('before')`); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  replicaDir,
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Release the leader so the lease is free, then promote on demand.
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	ok, err := db2.TryPromote(ctx)
	if err != nil || !ok {
		t.Fatalf("TryPromote after release: ok=%v err=%v, want true/nil", ok, err)
	}

	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("promoted handle cannot read restored row: %v", err)
	}
	if name != "before" {
		t.Fatalf("expected before, got %q", name)
	}
	if _, err := db2.ExecContext(ctx, `INSERT INTO items (name) VALUES ('after')`); err != nil {
		t.Fatalf("promoted handle cannot write: %v", err)
	}
}

func TestTryPromoteRacesBackgroundLoop(t *testing.T) {
	// A short TTL runs the background loop's own promotion attempts concurrently
	// with a storm of TryPromote calls. There must be no double restore (exactly
	// one promotion), the run must be -race clean, and the instance ends leader.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  60 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var promotes atomic.Int64
	db.OnPromote(func() { promotes.Add(1) })

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = db.TryPromote(ctx)
			}
		}()
	}
	wg.Wait()

	waitFor(t, 2*time.Second, db.IsLeader, "instance to promote")
	if got := promotes.Load(); got != 1 {
		t.Fatalf("expected exactly one promotion across loop + TryPromote, got %d", got)
	}
}

func TestTryPromoteContextCancelled(t *testing.T) {
	// A cancelled ctx makes TryPromote return promptly with a non-nil error and
	// leaves the instance a follower.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)

	db, err := Open(context.Background(), Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := db.TryPromote(ctx)
	if err == nil {
		t.Fatal("cancelled ctx should return an error")
	}
	if ok {
		t.Fatal("cancelled TryPromote must not report leadership")
	}
	if db.IsLeader() {
		t.Fatal("instance must remain a follower")
	}
}

func TestTryPromoteAfterCloseReturnsErrClosed(t *testing.T) {
	// A TryPromote that begins after Close must not resurrect the instance.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ok, err := db.TryPromote(ctx)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after Close, got ok=%v err=%v", ok, err)
	}
	if db.IsLeader() {
		t.Fatal("a closed instance must not become leader")
	}
}

func TestFollowerRefreshSeesNewWrites(t *testing.T) {
	// A follower with FollowerRefreshInterval set picks up writes the leader makes
	// after the follower opened, on the stable handle, without becoming writable.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if err := db1.Sync(ctx); err != nil { // publish the schema to the replica
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaDir,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.IsLeader() {
		t.Fatal("db2 should be a follower")
	}
	cached := db2.DB

	// A new row on the leader, published to the replica after the follower opened.
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('fresh')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool {
		var n int
		if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
			return false
		}
		return n == 1
	}, "follower to refresh to the new row")

	if cached != db2.DB {
		t.Fatal("refresh must not replace the stable handle")
	}
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('nope')`); err == nil {
		t.Fatal("a refreshed follower handle must stay read-only")
	}
}

func TestFollowerRefreshNoOpWhenUnchanged(t *testing.T) {
	// An idle replica must produce zero swaps: OnRefresh never fires while the
	// leader is quiet, so followers do not churn connections on every tick.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('seed')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaDir,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	var refreshes atomic.Int64
	db2.OnRefresh(func() { refreshes.Add(1) })

	time.Sleep(400 * time.Millisecond) // several refresh intervals, leader quiet

	if got := refreshes.Load(); got != 0 {
		t.Fatalf("idle follower must not swap; OnRefresh fired %d times", got)
	}
}

func TestFollowerRefreshDisabledByDefault(t *testing.T) {
	// With FollowerRefreshInterval unset (0), a follower serves its Open-time
	// snapshot and never picks up later writes — bit-identical to prior behaviour.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('seed')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  replicaDir,
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
		// FollowerRefreshInterval intentionally unset (0).
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	cached := db2.DB

	// The follower restored the seed row at Open.
	var n int
	if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("follower should serve the Open snapshot (1 row), got %d", n)
	}

	// A later write must NOT reach the follower — refresh is off.
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('second')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)

	if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("refresh is disabled; follower must still see only 1 row, got %d", n)
	}
}

func TestFollowerRefreshDoesNotClobberPromotion(t *testing.T) {
	// Refresh ticks and (external) TryPromote calls both serialise on promoteMu, so
	// a promotion racing refresh yields exactly one becomeLeaderLocked and a
	// writable, up-to-date handle. Run under -race.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()
	ttl := 80 * time.Millisecond

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   ttl,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('x')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaDir,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                ttl,
		FollowerRefreshInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	var promotes atomic.Int64
	db2.OnPromote(func() { promotes.Add(1) })

	// Free the lease; db2's loop promotes while refresh ticks and external
	// TryPromote calls contend for promoteMu.
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = db2.TryPromote(ctx)
			}
		}()
	}
	wg.Wait()

	waitFor(t, 3*time.Second, db2.IsLeader, "follower to promote")
	if got := promotes.Load(); got != 1 {
		t.Fatalf("expected exactly one promotion across refresh + promote, got %d", got)
	}
	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("promoted follower cannot read: %v", err)
	}
	if _, err := db2.ExecContext(ctx, `INSERT INTO items (name) VALUES ('y')`); err != nil {
		t.Fatalf("promoted follower cannot write: %v", err)
	}
}

func TestFollowerRefreshErrorIsNonFatal(t *testing.T) {
	// A refresh whose replica probe fails must be logged and shrugged off: the
	// follower keeps serving its current state and does not promote. The probe is
	// injected to fail so the test stays fast and deterministic (no real backend).
	lock := &fakeLock{}
	lock.steal("other-instance", time.Minute) // held elsewhere: no promotion
	installFakeLeaser(t, lock)

	prev := replicaLatestTXIDFunc
	replicaLatestTXIDFunc = func(context.Context, S3Config, string) (ltx.TXID, error) {
		return 0, errors.New("probe boom")
	}
	t.Cleanup(func() { replicaLatestTXIDFunc = prev })

	ctx := context.Background()
	db, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                "file://" + t.TempDir(),
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	time.Sleep(200 * time.Millisecond) // let several refresh ticks fail

	if db.IsLeader() {
		t.Fatal("a follower must not promote when refresh fails")
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("follower must keep serving despite refresh failures: %v", err)
	}
}

func TestFollowerRefreshConcurrentReadsSurviveSwap(t *testing.T) {
	// Reads on the cached handle across refresh swaps may fail transiently but must
	// never go backwards (torn read) and must converge to the full state. Run under
	// -race.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaDir,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	cached := db2.DB

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int
			for {
				select {
				case <-stop:
					return
				default:
				}
				var n int
				if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
					continue // a transient error at a swap boundary is acceptable
				}
				if n < last {
					t.Errorf("row count went backwards across a swap: %d < %d", n, last)
					return
				}
				last = n
			}
		}()
	}

	const batches = 10
	for i := 0; i < batches; i++ {
		if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('r')`); err != nil {
			t.Error(err)
			break
		}
		if err := db1.Sync(ctx); err != nil {
			t.Error(err)
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	waitFor(t, 5*time.Second, func() bool {
		var n int
		if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
			return false
		}
		return n == batches
	}, "follower to converge to all rows")

	close(stop)
	wg.Wait()
}

func TestBlockingRenewDemotesBeforeExpiry(t *testing.T) {
	// Fencing hazard: a renew that hangs in a network black hole must not stall the
	// lease loop past the held lease's ExpiresAt. If it did, litestream would keep
	// pushing LTX from its own goroutines while a successor acquires at expiry, so
	// two writers overlap on one replica. Invariant pinned: a holder that cannot
	// confirm its renewal demotes (stops replicating, fences the handle) strictly
	// before ExpiresAt.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	ttl := 600 * time.Millisecond

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

	// Block every renew from the first tick on (the first tick is TTL/3 ≈ 200ms
	// away, so setting this now beats it): the held lease never advances, so the
	// ExpiresAt we capture is exactly the one the blocking renew must beat.
	db.leaser.(*fakeLeaser).renewBlocks.Store(true)
	db.mu.Lock()
	expiresAt := db.lease.ExpiresAt
	db.mu.Unlock()

	cached := db.DB
	demotedAt := make(chan time.Time, 1)
	db.OnDemote(func(error) { demotedAt <- time.Now() })

	select {
	case at := <-demotedAt:
		if !at.Before(expiresAt) {
			t.Fatalf("demoted at %v, not before lease expiry %v: the fence fired too late, so two writers could overlap", at, expiresAt)
		}
	case <-time.After(ttl + time.Second):
		t.Fatal("writer never demoted while its renew hung: the loop stalled past expiry")
	}

	if db.IsLeader() {
		t.Fatal("demoted writer must not report itself as leader")
	}
	// Same handle, now fenced: writes must be rejected after demote.
	if _, err := cached.ExecContext(ctx, `INSERT INTO items (name) VALUES ('after-demote')`); err == nil {
		t.Fatal("demoted handle must reject writes")
	}
}

func TestShutdownDuringRenewDoesNotDemote(t *testing.T) {
	// The flip side of fencing: a renew interrupted by Close (the PARENT ctx is
	// cancelled) is a shutdown, not a lost lease. It must not demote or fire
	// OnDemote — Close alone owns teardown. This pins tryRenew's parent-ctx branch,
	// the trap in item 1: distinguishing a shutdown from a deadline expiry.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	ttl := 600 * time.Millisecond

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

	var demoted atomic.Bool
	db.OnDemote(func(error) { demoted.Store(true) })

	fl := db.leaser.(*fakeLeaser)
	fl.renewBlocks.Store(true)

	// Wait until the loop is parked inside the blocking renew, then Close. The
	// parent-ctx cancellation frees the renew well before its deadline (ExpiresAt −
	// TTL/6) could demote, so Close — not the deadline — ends the renew.
	waitFor(t, 3*time.Second, func() bool { return fl.renewN.Load() >= 1 }, "loop to enter renew")

	if err := db.Close(); err != nil {
		t.Fatalf("Close during a hung renew should be clean, got %v", err)
	}
	if demoted.Load() {
		t.Fatal("a renew interrupted by Close must not demote")
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

func TestSyncRacesRoleTransition(t *testing.T) {
	// DB.Sync reads db.lsDB, which demote (→ nil) and promote (→ a new store) both
	// write under db.mu. A consumer calling Sync during a role transition is a data
	// race on lsDB. The -race detector is the oracle: we assert only that nothing
	// panics and the run stays clean while Sync is hammered across a demote and the
	// re-promote that follows.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()
	ttl := 200 * time.Millisecond

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleAuto,
		Owner:      "writer",
		LeaseTTL:   ttl,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('seed')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil { // publish so the re-promote has state to restore
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = db.Sync(ctx) // may error mid-transition; only the race detector matters
			}
		}()
	}

	demoted := make(chan error, 1)
	db.OnDemote(func(err error) { demoted <- err })

	// Steal with a short TTL: the writer loses the lease on its next renew (lsDB →
	// nil under mu, racing the Sync reads), and the stolen lease then expires so the
	// demoted instance re-promotes (nil → new store under mu, racing again).
	lock.steal("thief", 100*time.Millisecond)

	select {
	case <-demoted:
	case <-time.After(3 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("writer did not demote after the steal")
	}
	waitFor(t, 3*time.Second, db.IsLeader, "demoted instance to re-promote")

	close(stop)
	wg.Wait()
}

func TestSyncNoReplicaIsNoOp(t *testing.T) {
	// Sync on an instance with no replication store — a follower never starts one —
	// is a nil no-op. Covers the currently-untested nil-lsDB branch.
	lock := &fakeLock{}
	lock.steal("other-instance", time.Minute) // held elsewhere: stay a follower
	installFakeLeaser(t, lock)
	ctx := context.Background()

	db, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  "file://" + t.TempDir(),
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.IsLeader() {
		t.Fatal("instance should be a follower with no replication store")
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("Sync without a replication store must be a nil no-op, got %v", err)
	}
}

// installRestoreFailSwitch routes restoreDBFunc through a switch: while the
// returned flag is false the real restore runs, once set it fails. Assigned once
// before any refresh/promote goroutine could read it, so flipping the atomic flag
// is the only concurrent mutation.
func installRestoreFailSwitch(t *testing.T) *atomic.Bool {
	t.Helper()
	var fail atomic.Bool
	prev := restoreDBFunc
	restoreDBFunc = func(ctx context.Context, s3 S3Config, url, dest string) error {
		if fail.Load() {
			return errors.New("restore boom")
		}
		return prev(ctx, s3, url, dest)
	}
	t.Cleanup(func() { restoreDBFunc = prev })
	return &fail
}

func TestFollowerRefreshRestoreFailureKeepsState(t *testing.T) {
	// A refresh whose restore fails must leave the follower serving its current
	// state, never an empty "no such table" database. The old rebuild deleted the
	// local files before restoring, so a mid-restore failure destroyed the follower;
	// the atomic rebuild leaves the live files untouched on failure. Once the failure
	// clears, the follower converges to the new state.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	failRestore := installRestoreFailSwitch(t)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	// Leader publishes one row; it holds the lease (long TTL) so the follower never
	// promotes and only refresh drives its restores.
	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('v1')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaDir,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	cached := db2.DB

	// The follower restored v1 at Open (Open's restore does not go through the
	// injected seam), so it serves one row now.
	waitFor(t, 3*time.Second, func() bool {
		var n int
		return cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n) == nil && n == 1
	}, "follower to serve its Open snapshot")

	// From here every refresh restore fails; advance the replica so the probe reports
	// something new and the follower keeps trying (and failing) to restore it.
	failRestore.Store(true)
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('v2')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Across many failing refresh ticks the follower must keep answering with its
	// pre-failure state — the destructive bug shows up here as an error ("no such
	// table") or an empty result.
	deadline := time.Now().Add(400 * time.Millisecond)
	goodReads := 0
	for time.Now().Before(deadline) {
		var n int
		if err := cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
			t.Fatalf("follower stopped serving during a failed refresh (state destroyed): %v", err)
		}
		if n != 1 {
			t.Fatalf("follower lost its state during a failed refresh: got %d rows, want 1", n)
		}
		goodReads++
		time.Sleep(10 * time.Millisecond)
	}
	if goodReads == 0 {
		t.Fatal("no reads happened during the failing window")
	}

	// Clear the failure; the follower must now converge to the advanced state.
	failRestore.Store(false)
	waitFor(t, 5*time.Second, func() bool {
		var n int
		return cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n) == nil && n == 2
	}, "follower to converge once restore recovers")
}

func TestPromoteRestoreFailureLeavesServingFollower(t *testing.T) {
	// A promote whose restore fails must leave a serving read-only follower (of its
	// intact snapshot, not a deleted file) and must release the lease it acquired so
	// another instance can take over. Exercises releaseQuietly on the failure path.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	failRestore := installRestoreFailSwitch(t)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	// A leader seeds one row, then cleanly closes so the lease is free.
	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('v1')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil { // durable flush + release
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  replicaDir,
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute, // long: no background promote ticks during the test
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// The follower restored v1 at Open.
	var n int
	if err := db2.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("follower should serve v1 at Open: n=%d err=%v", n, err)
	}

	// Promote now fails on the restore step.
	failRestore.Store(true)
	ok, err := db2.TryPromote(ctx)
	if ok || err == nil {
		t.Fatalf("promote should fail when restore fails: ok=%v err=%v", ok, err)
	}

	// Still a follower, still serving v1 read-only.
	if db2.IsLeader() {
		t.Fatal("a failed promote must leave the instance a follower")
	}
	if err := db2.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("follower must still serve v1 after a failed promote: n=%d err=%v", n, err)
	}
	if _, err := db2.ExecContext(ctx, `INSERT INTO items (name) VALUES ('nope')`); err == nil {
		t.Fatal("a failed promote must leave the handle read-only")
	}
	// The acquired lease was released, so a successor could take over.
	lock.mu.Lock()
	held := lock.lease
	lock.mu.Unlock()
	if held != nil {
		t.Fatalf("failed promote must release the lease, still held by %s", held.Owner)
	}
}

func TestPromoteFreshBucketEmptyReplica(t *testing.T) {
	// The empty-replica fallback in the atomic rebuild: a follower promoting against
	// a replica that never received data (a fresh bucket) must end up a writable,
	// empty DB with its migrations applied — not stuck on a missing temp file.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir() // empty: nothing ever synced here

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleFollower,
		Owner:      "follower",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.IsLeader() {
		t.Fatal("should open as a follower")
	}

	ok, err := db.TryPromote(ctx)
	if err != nil || !ok {
		t.Fatalf("fresh-bucket promote should succeed: ok=%v err=%v", ok, err)
	}
	if !db.IsLeader() {
		t.Fatal("should be leader after promoting against an empty replica")
	}
	// Writable, and migrations ran during promote so the schema exists.
	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('first')`); err != nil {
		t.Fatalf("promoted fresh DB must accept writes: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("promoted fresh DB should hold the one row just written: n=%d err=%v", n, err)
	}
}

func TestGenerationResetsOnCleanHandoff(t *testing.T) {
	// Generation is NOT a durable cross-handoff fencing token, and this test makes
	// that loud and intentional. It increases across takeovers only while the lock
	// object survives (expiry/steal); a clean release deletes the lock, so the next
	// acquirer resets to 1. The sequence a consumer sees is therefore 1 → 2 → 1 — the
	// Generation() doc guarantees exactly this and no more.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	// A writer acquires the empty lock → generation 1.
	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "w1.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "w1",
		LeaseTTL:   200 * time.Millisecond,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := db1.Generation(); got != 1 {
		t.Fatalf("fresh writer should be generation 1, got %d", got)
	}

	// A takeover that keeps the lock object alive (an expiry-steal) bumps the
	// sequence: the new holder is generation 2.
	lock.steal("w2", time.Minute)
	lock.mu.Lock()
	gen2 := cloneLease(lock.lease)
	lock.mu.Unlock()
	if gen2.Generation != 2 {
		t.Fatalf("takeover while the lock survives should be generation 2, got %d", gen2.Generation)
	}

	// The old holder loses the lease and demotes: it now reports no lease (0).
	waitFor(t, 3*time.Second, func() bool { return !db1.IsLeader() }, "old holder to demote")
	if got := db1.Generation(); got != 0 {
		t.Fatalf("demoted holder should report no lease (0), got %d", got)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// The generation-2 holder releases cleanly, which DELETES the lock object.
	if err := lock.release(gen2); err != nil {
		t.Fatalf("clean release of the gen-2 lease: %v", err)
	}
	lock.mu.Lock()
	stillHeld := lock.lease
	lock.mu.Unlock()
	if stillHeld != nil {
		t.Fatal("a clean release must delete the lock object")
	}

	// The next acquirer starts over at generation 1 — the reset that is exactly why
	// this must not be used as an external fencing token across handoffs.
	db3, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "w3.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "w3",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if got := db3.Generation(); got != 1 {
		t.Fatalf("after a clean handoff the fresh writer resets to generation 1, got %d", got)
	}
}

func TestInFlightTxCannotCommitAfterDemote(t *testing.T) {
	// A transaction begun while leader and still open at demotion must not commit:
	// the demoted writer no longer holds the lease, so a local commit would never
	// replicate and would vanish at the next promote-restore while a follower serves
	// phantom rows. The driver-level fence rolls the tx back and fails Commit.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()
	ttl := 300 * time.Millisecond

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   ttl,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if err := db1.Sync(ctx); err != nil { // publish the schema so a successor can restore it
		t.Fatal(err)
	}

	demoted := make(chan error, 1)
	db1.OnDemote(func(err error) { demoted <- err })

	// Begin a tx and insert while still the leader (connection dialed read-write).
	tx, err := db1.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO items (name) VALUES ('phantom')`); err != nil {
		t.Fatal(err)
	}

	// Lose the lease; wait for the demote fence to fire.
	lock.steal("thief", time.Minute)
	select {
	case <-demoted:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not demote after losing the lease")
	}

	// The commit must fail — the fence.
	if err := tx.Commit(); err == nil {
		t.Fatal("committing a tx after demote must fail (fencing)")
	}

	// The row must be absent locally: the fenced commit rolled it back.
	var n int
	if err := db1.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE name='phantom'`).Scan(&n); err != nil {
		t.Fatalf("read after fenced commit: %v", err)
	}
	if n != 0 {
		t.Fatalf("phantom row must not persist locally after a fenced commit, got %d", n)
	}

	// And absent from the replica's restored state — it never replicated.
	db2, err := Open(ctx, Config{
		LocalPath:   filepath.Join(t.TempDir(), "successor.sqlite3"),
		RestoreFrom: replicaDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if err := db2.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE name='phantom'`).Scan(&n); err != nil {
		t.Fatalf("read restored successor: %v", err)
	}
	if n != 0 {
		t.Fatalf("phantom row must not appear in the replica, got %d", n)
	}
}

func TestCheckedOutConnWriteFencedOnDemote(t *testing.T) {
	// A *sql.Conn checked out before demotion is pinned to its read-write connection
	// (no pool re-dial). The driver-level fence must reject writes on it after demote,
	// or the demoted writer would persist locally on a lease it no longer holds.
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

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `INSERT INTO items (name) VALUES ('ok')`); err != nil {
		t.Fatalf("checked-out conn should write while leader: %v", err)
	}

	demoted := make(chan error, 1)
	db.OnDemote(func(err error) { demoted <- err })
	lock.steal("thief", time.Minute)
	select {
	case <-demoted:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not demote")
	}

	// The same pinned connection must now reject writes.
	if _, err := conn.ExecContext(ctx, `INSERT INTO items (name) VALUES ('fenced')`); err == nil {
		t.Fatal("checked-out conn must reject writes after demote")
	}
}

func TestReaderTxAcrossRefreshSwapStillReads(t *testing.T) {
	// Reads are deliberately not fenced: a read transaction held across a follower
	// refresh swap keeps serving its consistent (old) snapshot rather than erroring.
	// This pins the design choice that refresh does not kill in-flight readers.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('a')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath:               filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:                replicaDir,
		Role:                    RoleFollower,
		Owner:                   "follower",
		LeaseTTL:                time.Minute,
		FollowerRefreshInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	cached := db2.DB
	waitFor(t, 3*time.Second, func() bool {
		var n int
		return cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n) == nil && n == 1
	}, "follower to serve its Open snapshot")

	// Open a read tx and pin its snapshot with a first read (1 row).
	tx, err := cached.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() // Rollback always succeeds; a Commit here may fail (stale) — the documented cost
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("initial read in tx: n=%d err=%v, want 1", n, err)
	}

	// Advance the replica so the follower performs a refresh swap under the open tx.
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('b')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		var m int
		return cached.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&m) == nil && m == 2
	}, "a fresh read to observe the swapped-in 2 rows")

	// The open tx must still read its consistent snapshot — 1 row, no error, no tear.
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatalf("read tx across the swap must keep serving: %v", err)
	}
	if n != 1 {
		t.Fatalf("read tx must see its pinned snapshot (1 row), got %d", n)
	}
}

func TestDoubleCloseIsIdempotent(t *testing.T) {
	// Close must be safe to call more than once sequentially: the second call is a
	// nil no-op, not a SyncAndWait on an already-closed litestream DB.
	t.Run("leader", func(t *testing.T) {
		lock := &fakeLock{}
		installFakeLeaser(t, lock)
		ctx := context.Background()
		db, err := Open(ctx, Config{
			LocalPath:  filepath.Join(t.TempDir(), "w.sqlite3"),
			BackupTo:   "file://" + t.TempDir(),
			Role:       RoleWriter,
			Owner:      "w",
			LeaseTTL:   time.Minute,
			Migrations: []string{itemsSchema},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("first close: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("second close must be a nil no-op, got %v", err)
		}
	})

	t.Run("follower", func(t *testing.T) {
		lock := &fakeLock{}
		lock.steal("other-instance", time.Minute) // held elsewhere: stays a follower
		installFakeLeaser(t, lock)
		ctx := context.Background()
		db, err := Open(ctx, Config{
			LocalPath: filepath.Join(t.TempDir(), "f.sqlite3"),
			BackupTo:  "file://" + t.TempDir(),
			Role:      RoleFollower,
			Owner:     "f",
			LeaseTTL:  time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if db.IsLeader() {
			t.Fatal("should be a follower")
		}
		if err := db.Close(); err != nil {
			t.Fatalf("first close: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("second close must be a nil no-op, got %v", err)
		}
	})
}

func TestBrokenMigrationPromotionDoesNotThrash(t *testing.T) {
	// A follower whose migration is invalid acquires the free lease, restores, fails
	// to migrate, and releases — potentially every tick, each doing a full restore.
	// It must never become leader, must keep serving reads, must leave the lease
	// released so a successor is not blocked, and the background loop must back off
	// rather than acquire+restore on every single tick.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	ttl := 150 * time.Millisecond // loop ticks at ttl/3 = 50ms

	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:   "file://" + t.TempDir(),
		Role:       RoleFollower,
		Owner:      "broken",
		LeaseTTL:   ttl,
		Migrations: []string{`THIS IS NOT VALID SQL`},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.IsLeader() {
		t.Fatal("should open as a follower")
	}
	fl := db.leaser.(*fakeLeaser)

	// Across ~36 ticks: never leader, and the read-only handle keeps serving.
	deadline := time.Now().Add(1800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if db.IsLeader() {
			t.Fatal("a follower with a broken migration must never become leader")
		}
		var one int
		if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
			t.Fatalf("follower must keep serving reads while promotion fails: %v", err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	attempts := fl.acquireN.Load()

	// The lease must end released (each failed promote releases it): a successor is
	// never blocked by the thrashing follower.
	waitFor(t, 2*time.Second, func() bool {
		lock.mu.Lock()
		defer lock.mu.Unlock()
		return lock.lease == nil
	}, "lease to be released after a failed promotion")

	// Backoff must throttle attempts well below one per tick (~36 ticks above);
	// without it the follower acquires + restores on every tick.
	if attempts == 0 {
		t.Fatal("expected the follower to attempt promotion at least once")
	}
	if attempts > 18 {
		t.Fatalf("promotion not backed off: %d acquire attempts over ~36 ticks", attempts)
	}
}

func TestConcurrentAutoOpenSingleLeader(t *testing.T) {
	// Two RoleAuto instances opened simultaneously against one lock must resolve to
	// exactly one leader and one serving follower, with no race on the lease CAS.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	type result struct {
		db  *DB
		err error
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db, err := Open(ctx, Config{
				LocalPath:  filepath.Join(t.TempDir(), fmt.Sprintf("db%d.sqlite3", i)),
				BackupTo:   replicaDir,
				Role:       RoleAuto,
				Owner:      fmt.Sprintf("inst-%d", i),
				LeaseTTL:   time.Minute,
				Migrations: []string{itemsSchema},
			})
			results[i] = result{db, err}
		}(i)
	}
	wg.Wait()

	leaders := 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("concurrent open %d failed: %v", i, r.err)
		}
		defer r.db.Close()
		if r.db.IsLeader() {
			leaders++
		} else if err := r.db.PingContext(ctx); err != nil { // the follower must still serve
			t.Fatalf("follower %d not serving: %v", i, err)
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader from concurrent Opens, got %d", leaders)
	}
}

func TestPromoteAppliesFollowerMigrations(t *testing.T) {
	// A follower can carry migrations the leader never ran; on promotion they must
	// apply, and the pre-existing replicated data must survive the promote-restore.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaDir := "file://" + t.TempDir()

	db1, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "leader.sqlite3"),
		BackupTo:   replicaDir,
		Role:       RoleWriter,
		Owner:      "leader",
		LeaseTTL:   time.Minute,
		Migrations: []string{itemsSchema}, // base schema only
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('kept')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil { // durable flush + release
		t.Fatal(err)
	}

	db2, err := Open(ctx, Config{
		LocalPath: filepath.Join(t.TempDir(), "follower.sqlite3"),
		BackupTo:  replicaDir,
		Role:      RoleFollower,
		Owner:     "follower",
		LeaseTTL:  time.Minute,
		Migrations: []string{
			itemsSchema,
			`CREATE TABLE IF NOT EXISTS extra (k TEXT)`, // never ran on the leader
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	ok, err := db2.TryPromote(ctx)
	if err != nil || !ok {
		t.Fatalf("promote: ok=%v err=%v", ok, err)
	}
	// The follower's own migration applied at promote-time.
	if _, err := db2.ExecContext(ctx, `INSERT INTO extra (k) VALUES ('v')`); err != nil {
		t.Fatalf("follower migration not applied at promote: %v", err)
	}
	// Pre-existing replicated data survived the promote-restore.
	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("replicated row lost across promote: %v", err)
	}
	if name != "kept" {
		t.Fatalf("expected kept, got %q", name)
	}
}

func TestOpenFailureReleasesLease(t *testing.T) {
	// If becomeLeaderLocked fails during Open (here, a broken migration), Open must
	// release the just-acquired lease so it does not block a successor for a full TTL.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()

	_, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		BackupTo:   "file://" + t.TempDir(),
		Role:       RoleWriter,
		Owner:      "writer",
		LeaseTTL:   time.Minute,
		Migrations: []string{`THIS IS NOT VALID SQL`},
	})
	if err == nil {
		t.Fatal("Open with a broken migration must fail")
	}
	lock.mu.Lock()
	held := lock.lease
	lock.mu.Unlock()
	if held != nil {
		t.Fatalf("Open failure must release the acquired lease, still held by %s", held.Owner)
	}
}
