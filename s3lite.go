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
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
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

	// Synchronous sets PRAGMA synchronous on every writer connection: "OFF",
	// "NORMAL", "FULL", or "EXTRA" (case-insensitive). Empty means "NORMAL",
	// the default documented in the README's durability note: WAL keeps the
	// local file crash-consistent either way, and the un-fsynced WAL tail
	// NORMAL can lose on a hard crash is already within the replication
	// window. Set "FULL" when the application's own contract makes a commit
	// mean fsynced-to-local-disk (e.g. a server whose reply promises stable
	// storage) — that closes the local-crash window; the replication window
	// to the bucket remains. Followers are unaffected: their connections are
	// query_only and never write.
	Synchronous string

	// TxLock sets how transactions opened through database/sql (Begin,
	// BeginTx) start on writer connections: "deferred" (SQLite's default),
	// "immediate", or "exclusive" (case-insensitive) — the driver's _txlock
	// DSN parameter. A writer whose every transaction mutates wants
	// "immediate": the write lock is taken at BEGIN, so contention surfaces
	// as a busy_timeout wait up front instead of SQLITE_BUSY at the first
	// write inside the transaction. Followers are unaffected.
	TxLock string

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

	// FollowerRefreshInterval, when > 0, makes a follower periodically catch up to the
	// leader's latest committed state from BackupTo and serve it read-only, giving
	// near-live reads with staleness bounded by roughly this interval plus the
	// leader's replication lag. The catch-up is incremental — it fetches and applies
	// only the LTX committed since the follower's position, not a full snapshot each
	// tick — so it stays cheap even for a large database on a short interval. When 0
	// (the default) a follower serves the snapshot it restored at Open and refreshes
	// only on promotion — bit-identical to prior behaviour. Ignored without an s3://
	// BackupTo (nothing to follow) and while this instance is the writer. Best-effort:
	// a failed refresh is logged and the follower keeps serving its current state. A
	// refresh that lands new data swaps the local file underneath the stable handle, so
	// an in-flight read spanning the swap may see a rare, retryable error; ticks that
	// find nothing new do no swap.
	FollowerRefreshInterval time.Duration
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
//
// Fencing caveats for a misbehaving consumer:
//   - A follower can escape read-only mode with PRAGMA query_only=0 and write to its
//     local file. Such writes never replicate and are destroyed by the next
//     restore/refresh — do not do this.
//   - Demote fences the cached handle, including in-flight transactions and writes on
//     a checked-out *sql.Conn. One residual hole is not closed: a *sql.Stmt prepared
//     and then executed on a connection that was already checked out before the demote
//     bypasses the driver-level write fence. Prepared-statement writes should still be
//     gated on IsLeader.
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

	// promoteOutcome records how this instance most recently entered the writer role
	// (restored vs resumed in place); see LastPromoteOutcome. promoteOutcomeValid gates
	// it — false until the first writer entry, so a follower that has never promoted
	// reports "no outcome" rather than a misleading zero value.
	promoteOutcome      PromoteOutcome
	promoteOutcomeValid bool

	// promoteMu serialises promotion attempts (the background leaseLoop and every
	// TryPromote caller) across the whole acquire+restore, so at most one runs at a
	// time. It is deliberately separate from mu: the restore is slow and must not
	// block IsLeader / Generation / tryRenew for its duration.
	promoteMu sync.Mutex

	onPromote func()
	onDemote  func(error)
	onRefresh func()

	// lastRefreshPos is the replica TXID the follower last restored to. It gates
	// no-op follower refreshes (skip the swap when the replica has not advanced).
	// Accessed only from the leaseLoop goroutine (seeded at Open before the loop
	// starts), so it needs no lock.
	lastRefreshPos ltx.TXID

	// promoteBackoff / promoteSkip throttle the background loop's promotion attempts
	// after consecutive failures (e.g. a follower with a broken migration), so it does
	// not acquire+restore on every tick. Loop-confined: only leaseLoop touches them.
	promoteBackoff int
	promoteSkip    int

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
	// Normalize the connection pragmas up front so a typo fails Open loudly
	// instead of silently producing a broken DSN on the first connection.
	var err error
	if cfg.Synchronous, err = normalizeChoice("Synchronous", cfg.Synchronous,
		strings.ToUpper, "OFF", "NORMAL", "FULL", "EXTRA"); err != nil {
		return nil, err
	}
	if cfg.TxLock, err = normalizeChoice("TxLock", cfg.TxLock,
		strings.ToLower, "deferred", "immediate", "exclusive"); err != nil {
		return nil, err
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

	// Whether the local file already existed at Open, captured before the
	// restore-if-missing and precreateWAL blocks below (both can create it). The
	// Open-direct fork guard (openDirectNeedsRestore) only applies when a leased writer
	// resumes a pre-existing local file; a fresh restore/create is the replica lineage by
	// construction and needs no guard.
	_, statErr := os.Stat(cfg.LocalPath)
	localExisted := statErr == nil

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
		// A sole writer can never be taken over, so a resumed local file was not rewound;
		// only a first-ever entry with no prior local file reads as restored (the same
		// conservative default as the leased paths). Generation 0 — there is no lease.
		db.promoteOutcome = PromoteOutcome{Restored: !localExisted, Generation: 0}
		db.promoteOutcomeValid = true
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
				db.seedRefreshPos(ctx)
				db.startLeaseLoop()
				return db, nil
			}
			return nil, err // RoleWriter: surface LeaseExistsError; both: surface others
		}
		// Open-direct fork guard: acquiring here (the prior lease had already expired)
		// would otherwise resume the existing local file unconditionally. If a successor
		// could have taken over and forked the replica lineage since our tenure, restore
		// over the local file before becoming leader; a clean/self-succession restart
		// resumes in place. See openDirectNeedsRestore and INVARIANTS.md #9.
		//
		// A first-ever writer entry (no prior local file — restored from the shared
		// replica above, or freshly created) reads as restored, erring the same
		// conservative direction as the guard: the local bytes are the replica lineage,
		// not a resumed tail. See LastPromoteOutcome.
		restored := !localExisted
		if localExisted && db.openDirectNeedsRestore(ctx, lease) {
			if err := db.restoreLocalFromReplica(ctx); err != nil {
				_ = db.leaser.ReleaseLease(ctx, lease)
				return nil, err
			}
			restored = true
		}
		if err := db.becomeLeaderLocked(ctx, lease, restored); err != nil {
			_ = db.leaser.ReleaseLease(ctx, lease)
			return nil, err
		}
		db.startLeaseLoop()
		return db, nil

	case RoleFollower:
		if err := db.openFollowerLocked(ctx); err != nil {
			return nil, err
		}
		db.seedRefreshPos(ctx)
		db.startLeaseLoop()
		return db, nil

	default:
		return nil, fmt.Errorf("s3lite: unknown %s", db.role)
	}
}

// normalizeChoice validates a case-insensitive Config choice against its valid
// set, returning the canonical form (empty stays empty — the field's default).
func normalizeChoice(field, value string, canon func(string) string, valid ...string) (string, error) {
	if value == "" {
		return "", nil
	}
	v := canon(value)
	for _, ok := range valid {
		if v == ok {
			return v, nil
		}
	}
	return "", fmt.Errorf("s3lite: invalid %s %q (valid: %s)", field, value, strings.Join(valid, ", "))
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
	db.connector = newStableConnector(drv, db.cfg.LocalPath, false, db.cfg.Synchronous, db.cfg.TxLock, db.cfg.BackupTo != "")
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
//
// It is safe to call Close/CloseContext more than once sequentially: after the
// first call every later call is a nil no-op. (Concurrent calls from multiple
// goroutines are not made safe — serialise your shutdown.)
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

	// Idempotent: a second (sequential) Close must not run SyncAndWait on an
	// already-closed litestream DB. The first call closed everything below.
	if db.closed {
		return nil
	}
	// Mark closed under mu before teardown: a TryPromote that is mid-restore will
	// see this when it reaches its own mu section (promote) and abort rather than
	// bring replication back up on a torn-down instance.
	db.closed = true
	wasLeader := db.isLeader // for the clean-shutdown marker, before teardown clears it

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
	// Record a clean-shutdown marker so a later restart that re-acquires the lease
	// directly at Open can prove the local file still matches the replica and resume in
	// place instead of re-downloading it (see openDirectNeedsRestore). Only when we were
	// the leader of an s3-leased replica and every durable flush above succeeded — then
	// the replica reflects all our committed writes. Best-effort: a failed marker forces
	// a conservative restore on the next open, never a wrong resume, so it must not fail
	// Close.
	if db.leaser != nil && wasLeader && firstErr == nil {
		if txid, err := replicaLatestTXIDFunc(ctx, db.cfg.S3, db.cfg.BackupTo); err != nil {
			db.logger.Warn("s3lite: recording clean-shutdown marker failed; next restart will restore", "error", err)
		} else if err := writeCleanShutdown(db.cleanShutdownPath(), txid); err != nil {
			db.logger.Warn("s3lite: writing clean-shutdown marker failed; next restart will restore", "error", err)
		}
	}
	// Drop the private follower cache (only followers created it; a no-op otherwise).
	// Best-effort: a leftover .follow just gets re-established or resumed next run, so a
	// removal error must not fail Close.
	if err := removeLocalDBFiles(db.followPath()); err != nil {
		db.logger.Warn("s3lite: removing follow cache failed", "error", err)
	}
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

// Generation returns the lease generation, or 0 when not holding a lease (including
// an unleased sole writer). It is unique among concurrent contenders and increases
// across takeovers only while the lock object survives: an expiry or a forced steal
// bumps it (1 → 2 → 3 …), but a clean release (Close) deletes the lock, so the next
// acquirer starts again at 1.
//
// Because of that reset, do not use Generation as a cross-handoff fencing token for
// external systems: after a clean handoff a consumer sees the sequence go 1 → 2 → 1,
// which is exactly when a stale-writer fence would need it to keep increasing. What
// it is good for: telling apart two promotions within one instance's lifetime, and
// logging/diagnostics.
//
// A durable fencing token would require persisting an epoch next to the replica data
// (outside lock.json, which litestream's s3.Leaser owns); that is intentionally not
// built here.
func (db *DB) Generation() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.lease == nil {
		return 0
	}
	return db.lease.Generation
}

// PromoteOutcome reports how this instance entered the writer role, letting a consumer
// that keeps state derived from the database (caches, externally stored blobs, queued
// deletions) tell a rewind-bearing takeover from a harmless resume-in-place. Both
// outcomes carry the same Generation > 1 signal, so Generation alone cannot separate
// them — that is exactly the gap this type closes.
type PromoteOutcome struct {
	// Restored is true when writer entry replaced the local database with the replica's
	// committed state: a takeover (a successor acquired the lease and could have forked
	// the lineage since this instance's tenure — anything the previous holder acked but
	// had not synced is gone), or a first-ever writer entry with no prior local file.
	// Treat it as a possible rewind: pause destructive maintenance (e.g. garbage
	// collection) and reconcile derived state before trusting the metadata again.
	//
	// It is false only for a provable resume-in-place — self-succession (a crash and
	// restart on this machine with no takeover between) or a clean restart — where the
	// local committed tail was kept intact and nothing was discarded, so no
	// reconciliation is needed.
	Restored bool
	// Generation is the lease generation at that entry, or 0 for an unleased sole writer.
	Generation int64
}

// LastPromoteOutcome reports how this instance most recently became the writer (see
// PromoteOutcome). ok is true once this instance has entered the writer role — after a
// writer Open or a later promotion — and false on a follower that has never promoted.
// The value is stable until the next writer entry; a demotion leaves it reporting the
// last one. Read it inside OnPromote, or after Open/TryPromote reports this instance is
// the writer, to decide whether derived state may have been rewound.
func (db *DB) LastPromoteOutcome() (PromoteOutcome, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.promoteOutcome, db.promoteOutcomeValid
}

// OnPromote registers a callback fired after this instance becomes the writer
// (lease acquired and replication started). It must not call Close — the callback
// runs on the lease-loop goroutine and Close blocks waiting for that goroutine to
// exit, so calling Close from here deadlocks — and it should return quickly. Set it
// before Open returns control to other goroutines; it is not safe to change
// concurrently with a role transition.
func (db *DB) OnPromote(fn func()) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.onPromote = fn
}

// OnDemote registers a callback fired after this instance loses the lease and
// stops replicating (the err explains why). The consumer must stop accepting
// writes on demotion. It must not call Close — the callback runs on the lease-loop
// goroutine and Close blocks waiting for that goroutine to exit, so calling Close
// from here deadlocks — and it should return quickly.
func (db *DB) OnDemote(fn func(error)) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.onDemote = fn
}

// OnRefresh registers a callback fired after a follower refresh swaps in newer
// state from the replica (see Config.FollowerRefreshInterval), e.g. to bust caches
// built on the previous snapshot. It never fires for a writer or when a tick finds
// nothing new. It must not call Close — the callback runs on the lease-loop
// goroutine and Close blocks waiting for that goroutine to exit, so calling Close
// from here deadlocks — and it should return quickly. Set it before Open returns
// control to other goroutines.
func (db *DB) OnRefresh(fn func()) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.onRefresh = fn
}

// Sync blocks until the replica is caught up with the current database state.
// It is a no-op when there is no replication store (BackupTo unset, or a follower
// that does not replicate). Calling it concurrently with a demotion may return an
// error: a role transition can close the store between the snapshot below and
// SyncAndWait, which correctly reports that the sync did not happen.
func (db *DB) Sync(ctx context.Context) error {
	// Snapshot lsDB under mu: demote (→ nil) and promote (→ a new store) both write
	// it under mu, so reading it unlocked is a data race with any role transition.
	db.mu.Lock()
	lsDB := db.lsDB
	db.mu.Unlock()
	if lsDB == nil {
		return nil
	}
	return lsDB.SyncAndWait(ctx)
}

// ReplicationStatus is a point-in-time snapshot of the metadata replica's
// health. It is read from the replication engine's own cached view without any
// network I/O, so it stays responsive precisely when the bucket is unreachable
// — which is exactly when a caller asks. LastSyncAt and LastError update on
// every sync attempt; LocalTXID and RemoteTXID are the last-known local and
// replica tips, and RemoteTXID is reset to zero after a failed sync, so a
// stalled replica reads as a large lag rather than a misleading ~0.
type ReplicationStatus struct {
	// Replicating is true only for a writer that runs a replication store
	// (BackupTo set). An unreplicated sole writer and every follower report
	// false with the remaining fields zero — neither drives replication.
	Replicating bool
	// LastSyncAt is when the replica last advanced (any successful upload),
	// zero if it never has. Its staleness is the primary health signal.
	LastSyncAt time.Time
	// LocalTXID is the local committed tip litestream replicates from;
	// RemoteTXID is the last position confirmed on the replica. RemoteTXID
	// below LocalTXID is the unreplicated lag.
	LocalTXID  uint64
	RemoteTXID uint64
	// InSync is true when the replica has caught up to the local tip.
	InSync bool
	// LastError is the most recent sync/checkpoint error, empty when the last
	// attempt succeeded. It is the last op's error, not a sticky latch — a
	// later success clears it, so pair it with LastSyncAt staleness to judge a
	// sustained failure.
	LastError string
}

// ReplicationStatus reports the metadata replica's health without any network
// I/O (see ReplicationStatus). Safe to call concurrently and at any role: a
// follower or an unreplicated sole writer reports Replicating=false with the
// remaining fields zero.
func (db *DB) ReplicationStatus() ReplicationStatus {
	// Snapshot lsDB under mu like Sync does: promote (→ a new store) and demote
	// (→ nil) both write it under mu, so an unlocked read races a role change.
	db.mu.Lock()
	lsDB := db.lsDB
	db.mu.Unlock()
	if lsDB == nil {
		return ReplicationStatus{}
	}
	st := ReplicationStatus{
		Replicating: true,
		LastSyncAt:  lsDB.LastSuccessfulSyncAt(),
		LastError:   lsDB.SyncDiagnostic().Error,
	}
	if pos, err := lsDB.Pos(); err == nil {
		st.LocalTXID = uint64(pos.TXID)
	}
	if lsDB.Replica != nil {
		st.RemoteTXID = uint64(lsDB.Replica.Pos().TXID)
	}
	st.InSync = st.LocalTXID > 0 && st.LocalTXID == st.RemoteTXID
	return st
}

func (db *DB) closeReplication(ctx context.Context) error {
	if db.store != nil {
		return db.store.Close(ctx)
	}
	return nil
}
