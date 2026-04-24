package s3lite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/benbjohnson/litestream"
	_ "github.com/ncruces/go-sqlite3/driver"
)

type Config struct {
	LocalPath   string
	RestoreFrom string
	BackupTo    string
	// Migrations are SQL strings executed in order on every Open. Each must be
	// idempotent (e.g. CREATE TABLE IF NOT EXISTS) — there is no version table.
	Migrations []string
}

type DB struct {
	*sql.DB
	lsDB  *litestream.DB
	store *litestream.Store
}

func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.LocalPath == "" {
		return nil, errors.New("s3lite: LocalPath is required")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.LocalPath), 0o755); err != nil {
		return nil, err
	}

	db := &DB{}

	if cfg.BackupTo != "" {
		client, err := newReplicaClient(cfg.BackupTo)
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
		store := litestream.NewStore([]*litestream.DB{lsDB}, levels)
		if err := store.Open(ctx); err != nil {
			return nil, fmt.Errorf("s3lite: litestream open: %w", err)
		}

		db.lsDB = lsDB
		db.store = store
	}

	sqlDB, err := sql.Open("sqlite3", cfg.LocalPath)
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
