// Package s3lite provides S3-backed embedded SQLite for serverless containers.
// It wraps litestream for continuous replication and a CGO-free SQLite driver.
package s3lite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	_ "modernc.org/sqlite"
)

// Config holds the options for Open.
type Config struct {
	// LocalPath is the on-disk path for the SQLite file. Required.
	LocalPath string

	// RestoreFrom is a replica URL (file:// or s3://) to restore from on startup
	// when LocalPath does not yet exist. Ignored if LocalPath already exists.
	// Safe to set on first deploy — an empty replica is not an error.
	RestoreFrom string

	// BackupTo is a replica URL (file:// or s3://) to replicate to continuously.
	// Omit to run without replication.
	BackupTo string

	// Migrations are SQL strings executed in order on every Open. Each must be
	// idempotent (e.g. CREATE TABLE IF NOT EXISTS) — there is no version table.
	Migrations []string

	// S3 configures s3:// replicas. Applies to both RestoreFrom and BackupTo.
	// Empty fields fall back to the AWS SDK's default credential and region chain
	// (env vars, ~/.aws/config, IAM roles). Set Endpoint for MinIO or other
	// S3-compatible providers — path-style addressing is enabled automatically.
	S3 S3Config

	// Logger receives s3lite's own lifecycle events (promote/demote/restore) at
	// their natural levels. litestream's internal logger is derived from this but
	// gated to WARN+, so its per-interval "replica sync" INFO chatter is dropped
	// while real replication problems still surface. When nil, a WARN+ stderr
	// logger is used. Set to slog.Default() to mirror the host application.
	Logger *slog.Logger

	// ShutdownSyncTimeout bounds the final durable flush performed by Close on a
	// replicated DB. Close blocks up to this long waiting for the replica to
	// catch up before it gives up (returning an error) rather than hanging on an
	// unreachable replica. When zero, DefaultShutdownSyncTimeout (30s) is used.
	// Ignored when BackupTo is empty. CloseContext ignores this in favour of the
	// caller-supplied context deadline.
	ShutdownSyncTimeout time.Duration

	// Role selects how an instance coordinates single-writer access. Coordination
	// needs the object store's atomic conditional write, so it applies only to an
	// s3:// BackupTo — where the lease lives at "<BackupTo path>/lock.json" and is
	// mandatory: an s3-replicated DB is always leased, never an uncoordinated
	// writer. Without an s3:// BackupTo (empty, or a file:// replica) there is no
	// shared WAL to coordinate on, so the instance is simply the sole writer and
	// Role is moot.
	//
	//   RoleAuto     — try to acquire; become writer on success, follower on
	//                  contention. The default, and the mode a serverless consumer
	//                  wants. With no s3:// BackupTo it degrades to the sole writer.
	//   RoleWriter   — acquire the lease or fail Open with *litestream.LeaseExistsError.
	//   RoleFollower — never replicate; open read-only and serve the restored
	//                  snapshot; promote to writer if the lease becomes free.
	//
	// RoleWriter and RoleFollower demand a lease, so Open fails if BackupTo is not
	// s3://. litestream requires exactly one writer per replica; the lease enforces
	// that by protocol so N instances run safely as one writer + many followers.
	Role Role

	// LeaseTTL is the lease duration for leased instances. The holder renews at
	// TTL/3; a holder that cannot renew stops replicating before the TTL lets anyone
	// else acquire, so two writers never overlap. When zero, DefaultLeaseTTL (30s)
	// is used. Ignored without an s3:// BackupTo.
	LeaseTTL time.Duration

	// Owner is the lease owner id recorded in the lock file for diagnostics. When
	// empty, litestream generates one (hostname:pid). Ignored without an s3:// BackupTo.
	Owner string
}

// Role selects how an instance coordinates single-writer access to an s3://
// replica. It has no effect without one — an unreplicated or file:// DB is always
// the sole writer, since there is no shared WAL to coordinate on.
type Role int

const (
	// RoleAuto acquires the lease if free (writer) or falls back to follower. The
	// default; with no s3:// BackupTo it is simply the sole writer.
	RoleAuto Role = iota
	// RoleWriter requires acquiring the lease; Open fails if it is already held.
	RoleWriter
	// RoleFollower never replicates; it serves the restored snapshot read-only and
	// promotes itself to writer if the lease becomes available.
	RoleFollower
)

func (r Role) String() string {
	switch r {
	case RoleAuto:
		return "RoleAuto"
	case RoleWriter:
		return "RoleWriter"
	case RoleFollower:
		return "RoleFollower"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

// DefaultShutdownSyncTimeout is the flush deadline Close uses when
// Config.ShutdownSyncTimeout is zero.
const DefaultShutdownSyncTimeout = 30 * time.Second

// DefaultLeaseTTL is the lease duration used when Config.LeaseTTL is zero.
// It matches litestream's s3.DefaultLeaseTTL.
const DefaultLeaseTTL = 30 * time.Second

// ErrClosed is returned by TryPromote when the instance is closing, so a
// promotion racing Close cannot resurrect a torn-down instance.
var ErrClosed = errors.New("s3lite: instance is closed")

// S3Config holds S3 connection settings. Callers are responsible for sourcing
// these values (e.g. from environment variables).
type S3Config struct {
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

// DB wraps *sql.DB with optional litestream replication.
// All *sql.DB methods are available directly via embedding.
//
// The embedded *sql.DB is stable for the whole life of the instance: it is
// created once and never reassigned, even across lease promote/demote. Callers
// may take it once (database := db.DB), hand it to repositories, and keep using
// it — connections are transparently re-dialed against the current local file in
// the current mode (writable as leader, read-only as follower). Use IsLeader or
// OnPromote/OnDemote only to make decisions about role; you never need them to
// keep a handle valid. A follower's handle rejects writes (query_only), so gate
// write paths on IsLeader if you serve traffic before promotion.
type DB struct {
	*sql.DB
	connector           *stableConnector
	lsDB                *litestream.DB
	store               *litestream.Store
	shutdownSyncTimeout time.Duration

	// Fields below support leased instances and are unused without an s3:// BackupTo.
	cfg      Config
	logger   *slog.Logger
	role     Role
	leaseTTL time.Duration
	leaser   litestream.Leaser

	// mu guards the lifecycle fields below and serialises role transitions. The
	// embedded *sql.DB itself is stable once Open returns — it is created exactly
	// once (via stableConnector) and never reassigned — so callers may cache and
	// use it across promote/demote without locking.
	mu       sync.Mutex
	lease    *litestream.Lease
	isLeader bool
	closed   bool // set by Close under mu; blocks a late promotion from resurrecting a torn-down instance

	// promoteMu serialises promotion attempts (the background leaseLoop and every
	// TryPromote caller) across the whole acquire+restore, so at most one runs at a
	// time. It is deliberately separate from mu: the restore is slow and must not
	// block IsLeader / Generation / tryRenew for its duration.
	promoteMu sync.Mutex

	onPromote func()
	onDemote  func(error)

	loopCancel context.CancelFunc
	wg         sync.WaitGroup
}

// Open opens or creates a SQLite database at cfg.LocalPath.
//
// Without an s3:// BackupTo the instance is the sole writer:
//  1. If RestoreFrom is set and LocalPath does not exist, restore from replica.
//  2. Start litestream replication if BackupTo is set (a file:// replica).
//  3. Open the SQLite connection and apply WAL pragmas.
//  4. Run Migrations in order.
//
// With an s3:// BackupTo the instance first coordinates via the lease (see
// Config.Role): a writer replicates and runs Migrations as above; a follower opens
// read-only, skips replication and Migrations, and serves the restored snapshot.
// Open returns *litestream.LeaseExistsError for RoleWriter when the lease is held.
//
// Call Close when done to flush replication, release the lease, and free
// resources.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.LocalPath == "" {
		return nil, errors.New("s3lite: LocalPath is required")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.LocalPath), 0o755); err != nil {
		return nil, err
	}

	shutdownSyncTimeout := cfg.ShutdownSyncTimeout
	if shutdownSyncTimeout <= 0 {
		shutdownSyncTimeout = DefaultShutdownSyncTimeout
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	db := &DB{
		cfg:                 cfg,
		logger:              logger,
		role:                cfg.Role,
		leaseTTL:            leaseTTL,
		shutdownSyncTimeout: shutdownSyncTimeout,
	}

	// Coordination (the lease) needs the object store's atomic conditional write,
	// which only an s3:// BackupTo provides. Build the leaser when a replica is
	// configured; the leaser builder rejects a non-s3 replica, in which case
	// RoleAuto falls back to the sole writer while RoleWriter/RoleFollower — which
	// demand a lease — surface the error. An s3:// replica therefore always leases:
	// there is no uncoordinated-writer-to-a-shared-WAL mode.
	if cfg.BackupTo != "" {
		leaser, err := newLeaserFunc(ctx, cfg.S3, cfg.BackupTo, leaseTTL, cfg.Owner, logger)
		switch {
		case err == nil:
			db.leaser = leaser
		case errors.Is(err, errNonS3Backup) && db.role == RoleAuto:
			// A non-s3 replica cannot be leased; RoleAuto is simply its sole writer.
		default:
			return nil, err
		}
	}
	leased := db.leaser != nil

	// Leased instances restore from the shared replica (BackupTo) when no explicit
	// RestoreFrom is given — that replica is the source of truth for the group.
	restoreFrom := cfg.RestoreFrom
	if leased && restoreFrom == "" {
		restoreFrom = cfg.BackupTo
	}
	if restoreFrom != "" {
		if _, err := os.Stat(cfg.LocalPath); os.IsNotExist(err) {
			if err := restoreDB(ctx, cfg.S3, restoreFrom, cfg.LocalPath); err != nil {
				return nil, err
			}
		}
	}

	if err := precreateWAL(ctx, cfg.LocalPath); err != nil {
		return nil, err
	}

	// No lease to coordinate on: sole writer, replicating to a file:// BackupTo if set.
	if !leased {
		if cfg.BackupTo != "" {
			if err := db.startReplicationLocked(ctx); err != nil {
				return nil, err
			}
		}
		if err := db.openWriterLocked(ctx); err != nil {
			db.closeReplication(ctx)
			return nil, err
		}
		db.isLeader = true
		return db, nil
	}

	// s3:// BackupTo: coordinate single-writer access via the lease.
	switch db.role {
	case RoleWriter, RoleAuto:
		lease, err := db.leaser.AcquireLease(ctx)
		if err != nil {
			var held *litestream.LeaseExistsError
			if db.role == RoleAuto && errors.As(err, &held) {
				if oerr := db.openFollowerLocked(ctx); oerr != nil {
					return nil, oerr
				}
				db.startLeaseLoop()
				return db, nil
			}
			return nil, err // RoleWriter: surface LeaseExistsError; both: surface others
		}
		if err := db.becomeLeaderLocked(ctx, lease); err != nil {
			_ = db.leaser.ReleaseLease(ctx, lease)
			return nil, err
		}
		db.startLeaseLoop()
		return db, nil

	case RoleFollower:
		if err := db.openFollowerLocked(ctx); err != nil {
			return nil, err
		}
		db.startLeaseLoop()
		return db, nil

	default:
		return nil, fmt.Errorf("s3lite: unknown %s", db.role)
	}
}

// precreateWAL creates the database in WAL mode when it does not yet exist, so
// litestream starts on an existing WAL-mode file. Without this, litestream would
// start on a non-WAL database and the app's PRAGMA journal_mode=WAL would switch
// the journal mode underneath it, causing locking protocol errors.
func precreateWAL(ctx context.Context, path string) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return err // nil if it exists, or a real stat error
	}
	tmpDB, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	_, err = tmpDB.ExecContext(ctx, "PRAGMA journal_mode=WAL")
	tmpDB.Close()
	return err
}

// startReplicationLocked builds and opens the litestream store for the current
// LocalPath. The caller must hold db.mu (or be in single-threaded Open).
func (db *DB) startReplicationLocked(ctx context.Context) error {
	client, err := newReplicaClient(db.cfg.S3, db.cfg.BackupTo)
	if err != nil {
		return err
	}

	lsDB := litestream.NewDB(db.cfg.LocalPath)
	replica := litestream.NewReplicaWithClient(lsDB, client)
	lsDB.Replica = replica
	wireReplica(client, replica)

	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: 10 * time.Second},
	}
	store := litestream.NewStore([]*litestream.DB{lsDB}, levels)
	// NewStore resets db.Logger inside its constructor, so override after. Gate
	// litestream to WARN+ so its per-interval "replica sync" INFO doesn't flood the
	// application log; s3lite logs its own lifecycle events via db.logger directly.
	lsLogger := litestreamLogger(db.logger)
	store.Logger = lsLogger.With(litestream.LogKeySystem, litestream.LogSystemStore)
	lsDB.SetLogger(store.Logger.With(litestream.LogKeyDB, filepath.Base(db.cfg.LocalPath)))
	// Make store.Close perform a bounded, durable final sync so a clean Close
	// flushes all committed writes. SetShutdownSyncTimeout propagates to lsDB.
	store.SetShutdownSyncTimeout(db.shutdownSyncTimeout)
	if err := store.Open(ctx); err != nil {
		return fmt.Errorf("s3lite: litestream open: %w", err)
	}

	db.lsDB = lsDB
	db.store = store
	return nil
}

// ensureHandleLocked creates the single stable *sql.DB the first time it is
// needed. sql.OpenDB is lazy, so this is safe to call before the local file
// exists. The caller must hold db.mu (or be in single-threaded Open).
func (db *DB) ensureHandleLocked() error {
	if db.connector != nil {
		return nil
	}
	drv, err := sharedDriver()
	if err != nil {
		return err
	}
	db.connector = newStableConnector(drv, db.cfg.LocalPath, false)
	db.DB = sql.OpenDB(db.connector)
	return nil
}

// openWriterLocked puts the stable handle into read-write mode, runs migrations,
// and verifies connectivity. On promote the local file has already been swapped
// into place; here we only flip the mode and migrate. The caller must hold db.mu
// (or be in single-threaded Open).
func (db *DB) openWriterLocked(ctx context.Context) error {
	if err := db.ensureHandleLocked(); err != nil {
		return err
	}
	db.connector.setMode(false)
	for i, m := range db.cfg.Migrations {
		if _, err := db.DB.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("s3lite: migration %d: %w", i, err)
		}
	}
	return db.DB.PingContext(ctx)
}

// openFollowerLocked puts the stable handle into read-only mode without
// replication or migrations. The caller must hold db.mu (or be in single-threaded
// Open).
func (db *DB) openFollowerLocked(ctx context.Context) error {
	if err := db.ensureHandleLocked(); err != nil {
		return err
	}
	db.connector.setMode(true)
	db.isLeader = false
	return db.DB.PingContext(ctx)
}

// Close durably flushes pending replication, stops litestream, and closes the
// database. On a replicated DB (BackupTo set) it guarantees the replica reflects
// all committed writes before returning — callers do not need a separate Sync.
// The flush is bounded by Config.ShutdownSyncTimeout (default 30s): if the
// replica cannot be reached in time, Close returns an error rather than hanging.
// With no replica configured it is a fast no-op beyond closing the DB.
// Returns the first non-nil error encountered.
func (db *DB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), db.shutdownSyncTimeout)
	defer cancel()
	return db.CloseContext(ctx)
}

// CloseContext is like Close but bounds the final durable flush by ctx instead
// of Config.ShutdownSyncTimeout. Use it to wire Close into an existing
// graceful-shutdown deadline. The database handle is always closed even if the
// flush errors or ctx expires. For a leader it also releases the lease so a
// successor can take over immediately rather than waiting out the TTL.
func (db *DB) CloseContext(ctx context.Context) error {
	// Stop the renew/promotion loop first and wait for it to exit, so it cannot
	// race the teardown below (e.g. renew a lease we are about to release, or
	// swap the sql handle mid-close).
	if db.loopCancel != nil {
		db.loopCancel()
	}
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	// Mark closed under mu before teardown: a TryPromote that is mid-restore will
	// see this when it reaches its own mu section (promote) and abort rather than
	// bring replication back up on a torn-down instance.
	db.closed = true

	var firstErr error
	save := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Flush while the WAL is still intact: closing the sql handle can checkpoint
	// and truncate the WAL, and litestream captures writes from WAL frames. The
	// explicit SyncAndWait guarantees the committed state reaches the replica
	// before teardown, bounded by ctx so an unreachable replica cannot hang Close.
	if db.lsDB != nil {
		save(db.lsDB.SyncAndWait(ctx))
	}
	if db.DB != nil {
		save(db.DB.Close())
	}
	save(db.closeReplication(ctx))
	// Release the lease last — after the final sync — so a successor only takes
	// over once our writes are durable. Best effort: a lost lease is already gone.
	if db.isLeader && db.lease != nil && db.leaser != nil {
		if err := db.leaser.ReleaseLease(ctx, db.lease); err != nil && !errors.Is(err, litestream.ErrLeaseNotHeld) {
			save(err)
		}
		db.lease = nil
	}
	db.isLeader = false
	return firstErr
}

// IsLeader reports whether this instance currently holds write access: it is the
// lease holder, or the sole writer when unleased (no s3:// BackupTo). Followers
// return false until they promote. Use it to gate whether the process should
// accept writes.
func (db *DB) IsLeader() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.isLeader
}

// TryPromote attempts to acquire the writer lease immediately rather than waiting
// for the next background poll. It returns true if this instance is (or, after
// restoring the latest replica, has just become) the writer, and false if the
// lease is still held by a live writer elsewhere. On a follower it blocks for the
// restore while promoting; bound ctx to cap that wait.
//
// It never promotes two writers — acquisition is the same lease CAS the background
// loop uses, so this makes promotion happen sooner, never more often. An unleased
// sole writer (no s3:// BackupTo) is always the writer, so this returns true
// without any I/O. Safe to call concurrently. Once Close has begun it returns
// ErrClosed; callers should stop calling TryPromote when they start shutting down.
func (db *DB) TryPromote(ctx context.Context) (bool, error) {
	return db.tryPromoteOnce(ctx)
}

// Generation returns the lease generation — a monotonic fencing token bumped on
// each takeover — or 0 when not holding a lease (including an unleased sole
// writer). A consumer can use it to fence external side effects against a stale writer.
func (db *DB) Generation() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.lease == nil {
		return 0
	}
	return db.lease.Generation
}

// OnPromote registers a callback fired after this instance becomes the writer
// (lease acquired and replication started). It must not call Close and should
// return quickly. Set it before Open returns control to other goroutines; it is
// not safe to change concurrently with a role transition.
func (db *DB) OnPromote(fn func()) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.onPromote = fn
}

// OnDemote registers a callback fired after this instance loses the lease and
// stops replicating (the err explains why). The consumer must stop accepting
// writes on demotion. It must not call Close and should return quickly.
func (db *DB) OnDemote(fn func(error)) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.onDemote = fn
}

// Sync blocks until the replica is caught up with the current database state.
// It is a no-op when BackupTo is not configured.
func (db *DB) Sync(ctx context.Context) error {
	if db.lsDB == nil {
		return nil
	}
	return db.lsDB.SyncAndWait(ctx)
}

func (db *DB) closeReplication(ctx context.Context) error {
	if db.store != nil {
		return db.store.Close(ctx)
	}
	return nil
}
