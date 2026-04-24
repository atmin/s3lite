package s3lite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

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
