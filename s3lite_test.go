package s3lite_test

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/atmin/s3lite"
	_ "github.com/ncruces/go-sqlite3/driver"
)

func TestSQLiteDriverSmoke(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow("SELECT 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
}

func TestOpenCreatesFile(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	db, err := s3lite.Open(ctx, s3lite.Config{LocalPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
}

func TestOpenWALMode(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	db, err := s3lite.Open(ctx, s3lite.Config{LocalPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("expected wal, got %s", mode)
	}
}

func TestOpenReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	db, err := s3lite.Open(ctx, s3lite.Config{LocalPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := s3lite.Open(ctx, s3lite.Config{LocalPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db2.Close()
}

func TestOpenEmptyPathError(t *testing.T) {
	_, err := s3lite.Open(context.Background(), s3lite.Config{})
	if err == nil {
		t.Fatal("expected error for empty LocalPath")
	}
}

func TestMigrationsCreateSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: path,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('hello')`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "hello" {
		t.Fatalf("expected hello, got %s", name)
	}
}

func TestMigrationsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`,
	}

	db, err := s3lite.Open(ctx, s3lite.Config{LocalPath: path, Migrations: migrations})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := s3lite.Open(ctx, s3lite.Config{LocalPath: path, Migrations: migrations})
	if err != nil {
		t.Fatal(err)
	}
	db2.Close()
}

func TestBadMigrationReturnsError(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	_, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:  path,
		Migrations: []string{`THIS IS NOT SQL`},
	})
	if err == nil {
		t.Fatal("expected error for bad migration")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("file should still exist after failed migration, got: %v", statErr)
	}
}

func TestMigrationsRunInOrder(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	// If migrations ran out of order, the INSERT would fail because the table
	// wouldn't exist yet.
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: path,
		Migrations: []string{
			`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
			`INSERT INTO items (name) VALUES ('seed')`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var name string
	if err := db.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "seed" {
		t.Fatalf("expected seed, got %s", name)
	}
}

func TestMigrationErrorWrapsIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite3")

	_, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: path,
		Migrations: []string{
			`CREATE TABLE items (id INTEGER PRIMARY KEY)`,
			`THIS IS NOT SQL`,
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "migration 1") {
		t.Errorf("error should identify failing migration by index, got: %v", err)
	}
}

func TestBackupToFileReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite3")
	replicaDir := t.TempDir()

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: dbPath,
		BackupTo:  "file://" + replicaDir,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('hello')`); err != nil {
		t.Fatal(err)
	}

	// Poll for LTX files (litestream writes asynchronously).
	deadline := time.Now().Add(5 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(replicaDir, "ltx", "0", "*.ltx"))
		if len(matches) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for litestream to write LTX files")
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomLoggerReceivesLitestreamLogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	buf := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := filepath.Join(t.TempDir(), "test.sqlite3")
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: dbPath,
		BackupTo:  "file://" + t.TempDir(),
		Logger:    logger,
		Migrations: []string{
			`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES ('hello')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "replica sync") {
		t.Fatalf("expected litestream logs in custom logger output; got:\n%s", out)
	}
}

// lockedBuffer is bytes.Buffer with a mutex — litestream writes from a
// background goroutine so the test's reader and the writer race otherwise.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestBackupBadSchemeError(t *testing.T) {
	ctx := context.Background()
	_, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: filepath.Join(t.TempDir(), "test.sqlite3"),
		BackupTo:  "ftp://some/path",
	})
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
}

func TestRestoreRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	root := t.TempDir()
	replicaDir := filepath.Join(root, "replica")
	replicaURL := "file://" + replicaDir

	db1Path := filepath.Join(root, "db1.sqlite3")
	db1, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath: db1Path,
		BackupTo:  replicaURL,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('hello')`); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restore into DB2 and verify row is present.
	db2Path := filepath.Join(root, "db2.sqlite3")
	db2, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   db2Path,
		RestoreFrom: replicaURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	var name string
	if err := db2.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "hello" {
		t.Fatalf("expected hello, got %s", name)
	}
}

func TestRestoreFromEmptyReplicaSucceeds(t *testing.T) {
	ctx := context.Background()
	emptyDir := t.TempDir()

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(t.TempDir(), "db.sqlite3"),
		RestoreFrom: "file://" + emptyDir,
	})
	if err != nil {
		t.Fatalf("expected success for empty replica, got: %v", err)
	}
	db.Close()
}

func TestRestoreSkippedWhenLocalExists(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dbPath := filepath.Join(root, "db.sqlite3")

	// Pre-create the file with marker content.
	if err := os.WriteFile(dbPath, []byte("marker"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Open with RestoreFrom pointing at an empty dir — restore should be skipped.
	// The open will fail (not a valid SQLite file) but the file must be untouched.
	s3lite.Open(ctx, s3lite.Config{ //nolint:errcheck
		LocalPath:   dbPath,
		RestoreFrom: "file://" + t.TempDir(),
	})

	content, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "marker" {
		t.Fatal("restore clobbered existing local file")
	}
}
