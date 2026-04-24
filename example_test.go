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

// Example_s3 shows the S3 replica URL form. The caller sources S3 settings
// (from env, config file, secret manager, etc.) and passes them explicitly.
// Empty S3Config fields fall back to the AWS SDK's default credential chain.
func Example_s3() {
	ctx := context.Background()
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   "/tmp/app.sqlite3",
		RestoreFrom: "s3://my-bucket/db",
		BackupTo:    "s3://my-bucket/db",
		S3: s3lite.S3Config{
			Region:          os.Getenv("AWS_REGION"),
			Endpoint:        os.Getenv("AWS_ENDPOINT_URL"),
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		Migrations: []string{
			`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)`,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_ = db // use db as a standard *sql.DB
}
