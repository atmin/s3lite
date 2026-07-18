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

	// Role selects single-writer coordination. The default (RoleOff) preserves the
	// original behaviour: whenever BackupTo is set the instance replicates as a
	// writer, unconditionally. Any other role requires an s3:// BackupTo (the
	// lease lives at "<BackupTo path>/lock.json" and needs conditional writes):
	//
	//   RoleWriter   — acquire the lease or fail Open with *litestream.LeaseExistsError.
	//   RoleFollower — never replicate; open read-only and serve the restored
	//                  snapshot; promote to writer if the lease becomes free.
	//   RoleAuto     — try to acquire; become writer on success, follower on
	//                  contention. The mode a serverless consumer wants.
	//
	// litestream requires exactly one writer per replica; the lease enforces that
	// by protocol so N instances run safely as one writer + many followers.
	Role Role

	// LeaseTTL is the lease duration for leased roles. The holder renews at TTL/3;
	// a holder that cannot renew stops replicating before the TTL lets anyone else
	// acquire, so two writers never overlap. When zero, DefaultLeaseTTL (30s) is
	// used. Ignored when Role is RoleOff.
	LeaseTTL time.Duration

	// Owner is the lease owner id recorded in the lock file for diagnostics. When
	// empty, litestream generates one (hostname:pid). Ignored when Role is RoleOff.
	Owner string
}

// Role selects how an instance coordinates single-writer access to a replica.
type Role int

const (
	// RoleOff disables leasing: BackupTo means "always replicate as writer".
	// This is the default and matches s3lite's original behaviour.
	RoleOff Role = iota
	// RoleWriter requires acquiring the lease; Open fails if it is already held.
	RoleWriter
	// RoleFollower never replicates; it serves the restored snapshot read-only and
	// promotes itself to writer if the lease becomes available.
	RoleFollower
	// RoleAuto acquires the lease if free (writer) or falls back to follower.
	RoleAuto
)

func (r Role) leased() bool { return r != RoleOff }

// DefaultShutdownSyncTimeout is the flush deadline Close uses when
// Config.ShutdownSyncTimeout is zero.
const DefaultShutdownSyncTimeout = 30 * time.Second

// DefaultLeaseTTL is the lease duration used when Config.LeaseTTL is zero.
// It matches litestream's s3.DefaultLeaseTTL.
const DefaultLeaseTTL = 30 * time.Second

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
// Concurrency note for leased roles (Role != RoleOff): the embedded *sql.DB is
// stable for the whole lifetime of a writer, but a follower replaces it when it
// promotes (it must reopen after restoring the latest state). Consumers running
// RoleFollower/RoleAuto should therefore gate database access on IsLeader (or an
// OnPromote/OnDemote callback) rather than caching the embedded handle across a
// role change.
type DB struct {
	*sql.DB
	lsDB                *litestream.DB
	store               *litestream.Store
	shutdownSyncTimeout time.Duration

	// Fields below support leased roles and are unused when Role is RoleOff.
	cfg      Config
	logger   *slog.Logger
	role     Role
	leaseTTL time.Duration
	leaser   litestream.Leaser

	mu       sync.Mutex // guards DB, lsDB, store, lease, isLeader
	lease    *litestream.Lease
	isLeader bool

	onPromote func()
	onDemote  func(error)

	loopCancel context.CancelFunc
	wg         sync.WaitGroup
}

// Open opens or creates a SQLite database at cfg.LocalPath.
//
// Lifecycle (RoleOff, the default):
//  1. If RestoreFrom is set and LocalPath does not exist, restore from replica.
//  2. Start litestream replication if BackupTo is set.
//  3. Open the SQLite connection and apply WAL pragmas.
//  4. Run Migrations in order.
//
// For leased roles (Role != RoleOff) the instance first coordinates via the
// lease (see Config.Role): a writer replicates and runs Migrations as above; a
// follower opens read-only, skips replication and Migrations, and serves the
// restored snapshot. Open returns *litestream.LeaseExistsError for RoleWriter
// when the lease is already held.
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

	if db.role.leased() {
		leaser, err := newLeaserFunc(ctx, cfg.S3, cfg.BackupTo, leaseTTL, cfg.Owner, logger)
		if err != nil {
			return nil, err
		}
		db.leaser = leaser
	}

	// Leased instances restore from the shared replica (BackupTo) when no explicit
	// RestoreFrom is given — that replica is the source of truth for the group.
	restoreFrom := cfg.RestoreFrom
	if db.role.leased() && restoreFrom == "" {
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

	// Decide the initial role and wire up replication / read-only accordingly.
	switch db.role {
	case RoleOff:
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
		return nil, fmt.Errorf("s3lite: unknown Role %d", int(db.role))
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

// openWriterLocked opens a writable sql handle, applies WAL pragmas, and runs
// migrations. The caller must hold db.mu (or be in single-threaded Open).
func (db *DB) openWriterLocked(ctx context.Context) error {
	sqlDB, err := openSQL(ctx, db.cfg.LocalPath, false)
	if err != nil {
		return err
	}
	for i, m := range db.cfg.Migrations {
		if _, err := sqlDB.ExecContext(ctx, m); err != nil {
			sqlDB.Close()
			return fmt.Errorf("s3lite: migration %d: %w", i, err)
		}
	}
	db.DB = sqlDB
	return nil
}

// openFollowerLocked opens a read-only sql handle without replication or
// migrations. The caller must hold db.mu (or be in single-threaded Open).
func (db *DB) openFollowerLocked(ctx context.Context) error {
	sqlDB, err := openSQL(ctx, db.cfg.LocalPath, true)
	if err != nil {
		return err
	}
	db.DB = sqlDB
	db.isLeader = false
	return nil
}

// openSQL opens the SQLite file and applies connection pragmas. Read-only mode
// pins query_only via the DSN so it applies to every pooled connection and skips
// the WAL journal-mode write (a follower must not mutate the file).
func openSQL(ctx context.Context, path string, readOnly bool) (*sql.DB, error) {
	dsn := path
	pragmas := []string{"PRAGMA busy_timeout=5000", "PRAGMA foreign_keys=ON"}
	if readOnly {
		dsn = path + "?_pragma=query_only(1)"
	} else {
		pragmas = append([]string{"PRAGMA journal_mode=WAL"}, pragmas...)
	}

	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	for _, pragma := range pragmas {
		if _, err := sqlDB.ExecContext(ctx, pragma); err != nil {
			sqlDB.Close()
			return nil, err
		}
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, err
	}
	return sqlDB, nil
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
// lease holder (leased roles) or the sole writer (RoleOff). Followers return
// false until they promote. Use it to gate whether the process should accept
// writes.
func (db *DB) IsLeader() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.isLeader
}

// Generation returns the lease generation — a monotonic fencing token bumped on
// each takeover — or 0 when not holding a lease (or RoleOff). A consumer can use
// it to fence external side effects against a stale writer.
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
