package s3lite_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/atmin/s3lite"
)

func Example_basic() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "s3lite-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "app.sqlite3")
	replicaURL := "file://" + filepath.Join(dir, "replica")

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   dbPath,
		RestoreFrom: replicaURL,
		BackupTo:    replicaURL,
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)`,
			`CREATE INDEX IF NOT EXISTS users_email ON users(email)`,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `INSERT INTO users (email) VALUES (?)`, "alice@example.com"); err != nil {
		log.Fatal(err)
	}

	var email string
	if err := db.QueryRowContext(ctx, `SELECT email FROM users LIMIT 1`).Scan(&email); err != nil {
		log.Fatal(err)
	}
	fmt.Println(email)

	// Output: alice@example.com
}
