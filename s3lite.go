package s3lite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
}

func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.LocalPath == "" {
		return nil, errors.New("s3lite: LocalPath is required")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.LocalPath), 0o755); err != nil {
		return nil, err
	}

	sqlDB, err := sql.Open("sqlite3", cfg.LocalPath)
	if err != nil {
		return nil, err
	}

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := sqlDB.ExecContext(ctx, pragma); err != nil {
			sqlDB.Close()
			return nil, err
		}
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, err
	}

	for i, m := range cfg.Migrations {
		if _, err := sqlDB.ExecContext(ctx, m); err != nil {
			sqlDB.Close()
			return nil, fmt.Errorf("s3lite: migration %d: %w", i, err)
		}
	}

	return &DB{sqlDB}, nil
}
