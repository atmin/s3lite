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

	// Logger receives litestream's log records. When nil, INFO is suppressed
	// and WARN+ is written to stderr. Set to slog.Default() to mirror the
	// host application's logging.
	Logger *slog.Logger
}

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
type DB struct {
	*sql.DB
	lsDB  *litestream.DB
	store *litestream.Store
}

// Open opens or creates a SQLite database at cfg.LocalPath.
//
// Lifecycle:
//  1. If RestoreFrom is set and LocalPath does not exist, restore from replica.
//  2. Start litestream replication if BackupTo is set.
//  3. Open the SQLite connection and apply WAL pragmas.
//  4. Run Migrations in order.
//
// Call Close when done to flush replication and release resources.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.LocalPath == "" {
		return nil, errors.New("s3lite: LocalPath is required")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.LocalPath), 0o755); err != nil {
		return nil, err
	}

	db := &DB{}

	if cfg.RestoreFrom != "" {
		if _, err := os.Stat(cfg.LocalPath); os.IsNotExist(err) {
			if err := restoreDB(ctx, cfg.S3, cfg.RestoreFrom, cfg.LocalPath); err != nil {
				return nil, err
			}
		}
	}

	// Pre-create the database in WAL mode so litestream starts on an existing WAL-mode file.
	// Without this, litestream starts on a non-WAL database, then the app's PRAGMA journal_mode=WAL
	// switches the journal mode underneath it, causing locking protocol errors.
	if _, err := os.Stat(cfg.LocalPath); os.IsNotExist(err) {
		tmpDB, err := sql.Open("sqlite", cfg.LocalPath)
		if err != nil {
			return nil, err
		}
		_, err = tmpDB.ExecContext(ctx, "PRAGMA journal_mode=WAL")
		tmpDB.Close()
		if err != nil {
			return nil, err
		}
	}

	if cfg.BackupTo != "" {
		client, err := newReplicaClient(cfg.S3, cfg.BackupTo)
		if err != nil {
			return nil, err
		}

		lsDB := litestream.NewDB(cfg.LocalPath)
		replica := litestream.NewReplicaWithClient(lsDB, client)
		lsDB.Replica = replica
		wireReplica(client, replica)

		levels := litestream.CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: 10 * time.Second},
		}
		logger := cfg.Logger
		if logger == nil {
			logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		}
		store := litestream.NewStore([]*litestream.DB{lsDB}, levels)
		// NewStore resets db.Logger inside its constructor, so override after.
		store.Logger = logger.With(litestream.LogKeySystem, litestream.LogSystemStore)
		lsDB.SetLogger(store.Logger.With(litestream.LogKeyDB, filepath.Base(cfg.LocalPath)))
		if err := store.Open(ctx); err != nil {
			return nil, fmt.Errorf("s3lite: litestream open: %w", err)
		}

		db.lsDB = lsDB
		db.store = store
	}

	sqlDB, err := sql.Open("sqlite", cfg.LocalPath)
	if err != nil {
		db.closeReplication(ctx)
		return nil, err
	}

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := sqlDB.ExecContext(ctx, pragma); err != nil {
			sqlDB.Close()
			db.closeReplication(ctx)
			return nil, err
		}
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		db.closeReplication(ctx)
		return nil, err
	}

	for i, m := range cfg.Migrations {
		if _, err := sqlDB.ExecContext(ctx, m); err != nil {
			sqlDB.Close()
			db.closeReplication(ctx)
			return nil, fmt.Errorf("s3lite: migration %d: %w", i, err)
		}
	}

	db.DB = sqlDB
	return db, nil
}

// Close flushes pending replication, stops litestream, and closes the database.
// Returns the first non-nil error encountered.
func (db *DB) Close() error {
	var firstErr error
	save := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	save(db.DB.Close())
	save(db.closeReplication(context.Background()))
	return firstErr
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
