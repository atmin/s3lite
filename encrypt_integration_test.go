//go:build integration

package s3lite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/atmin/s3lite"
)

// Client-side encryption over real object storage. The default suite covers the
// format and the file:// backend; these tests prove the s3 backend behaves the same
// — including the parts that only exist against a real store: object metadata for
// timestamp-based retention, and a lease handoff between two encrypted instances.

func encIntegrationKey(b byte) []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = b ^ byte(i*7)
	}
	return key
}

// listLTXObjects returns every LTX object body under a bucket prefix, keyed by object
// key, so a test can assert on the bytes the bucket actually holds.
func listLTXObjects(ctx context.Context, t *testing.T, client *s3sdk.Client, bucket, prefix string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	var token *string
	for {
		page, err := client.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if filepath.Ext(key) != ".ltx" {
				continue
			}
			body, err := client.GetObject(ctx, &s3sdk.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			b, err := io.ReadAll(body.Body)
			_ = body.Body.Close()
			if err != nil {
				t.Fatalf("read %s: %v", key, err)
			}
			out[key] = b
		}
		if !aws.ToBool(page.IsTruncated) {
			return out
		}
		token = page.NextContinuationToken
	}
}

// TestEncryptedReplicaRoundTripS3 is the s3-backend counterpart of the default
// suite's file:// round trip: write under a key, prove the bucket holds only
// ciphertext, restore into a fresh instance, and confirm a wrong key fails cleanly.
func TestEncryptedReplicaRoundTripS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	startedAt := time.Now()
	env := startMinIO(ctx, t, "encrypted")
	bucketURL := "s3://encrypted/db"
	key := encIntegrationKey(0x31)
	root := t.TempDir()

	const secret = "zz-unmistakable-plaintext-marker-zz"

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:     filepath.Join(root, "writer.sqlite3"),
		BackupTo:      bucketURL,
		S3:            env.cfg,
		Role:          s3lite.RoleWriter,
		LeaseTTL:      time.Minute,
		EncryptionKey: key,
		Migrations:    []string{`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`},
	})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	// Enough incompressible rows that a snapshot spans several 64 KiB frames on the
	// wire. LTX compresses pages, so patterned filler would collapse and quietly stop
	// crossing frame boundaries — the assertion below guards that.
	rnd := rand.New(rand.NewPCG(1, 2))
	filler := make([]byte, 96)
	for i := 0; i < 3000; i++ {
		for j := range filler {
			filler[j] = byte(rnd.Uint32())
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO items (name) VALUES (?)`,
			fmt.Sprintf("%s-%06d-%x", secret, i, filler)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	// Every object in the bucket is ciphertext, and none leaks a row value.
	objects := listLTXObjects(ctx, t, env.client, "encrypted", "db/")
	if len(objects) == 0 {
		t.Fatal("nothing was replicated to the bucket")
	}
	var totalBytes int
	for name, body := range objects {
		totalBytes += len(body)
		if len(body) < 4 || string(body[:4]) != "S3LE" {
			t.Fatalf("%s: object does not carry the s3lite encryption magic", name)
		}
		if bytes.Contains(body, []byte(secret)) {
			t.Fatalf("%s: object body contains plaintext row data", name)
		}
		if bytes.Contains(body, []byte("LTX1")) {
			t.Fatalf("%s: object body contains an LTX header", name)
		}
	}
	if totalBytes < 3*64*1024 {
		t.Fatalf("bucket holds only %d bytes; the payload should span several frames", totalBytes)
	}

	// The metadata timestamp the fork's second patch preserves must be present and
	// real. It is what timestamp-based restore and retention read back — retention
	// deletes by it — and it is the one thing a pure decorator could not have supplied,
	// because ciphertext cannot be peeked for an LTX header. A regressed hook would
	// leave it absent or at the zero time, so assert it lands in this run's window.
	// (Retention's own behaviour under encryption is pinned in the default suite by
	// TestEncryptedReplicaRetentionExpiresSuperseded.)
	for name := range objects {
		head, err := env.client.HeadObject(ctx, &s3sdk.HeadObjectInput{
			Bucket: aws.String("encrypted"),
			Key:    aws.String(name),
		})
		if err != nil {
			t.Fatalf("head %s: %v", name, err)
		}
		raw, ok := head.Metadata["litestream-timestamp"]
		if !ok || raw == "" {
			t.Fatalf("%s: missing litestream-timestamp metadata (the caller-supplied timestamp hook regressed)", name)
		}
		ts, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			t.Fatalf("%s: unparseable litestream-timestamp %q: %v", name, raw, err)
		}
		if ts.Before(startedAt.Add(-time.Hour)) || ts.After(time.Now().Add(time.Hour)) {
			t.Fatalf("%s: litestream-timestamp %v is not a real write time", name, ts)
		}
	}

	// A fresh instance with the same key restores everything.
	restored, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:     filepath.Join(root, "restored.sqlite3"),
		RestoreFrom:   bucketURL,
		S3:            env.cfg,
		EncryptionKey: key,
	})
	if err != nil {
		t.Fatalf("restore with the key: %v", err)
	}
	defer restored.Close()

	var n int
	if err := restored.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3000 {
		t.Fatalf("restored %d rows, want 3000", n)
	}
	var check string
	if err := restored.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&check); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if check != "ok" {
		t.Fatalf("integrity_check = %q", check)
	}

	// The wrong key fails with the typed error and leaves nothing behind.
	wrongPath := filepath.Join(root, "wrong.sqlite3")
	_, err = s3lite.Open(ctx, s3lite.Config{
		LocalPath:     wrongPath,
		RestoreFrom:   bucketURL,
		S3:            env.cfg,
		EncryptionKey: encIntegrationKey(0x32),
	})
	if !errors.Is(err, s3lite.ErrKeyMismatch) {
		t.Fatalf("wrong key over s3: got %v, want ErrKeyMismatch", err)
	}

	// So does no key at all.
	_, err = s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(root, "nokey.sqlite3"),
		RestoreFrom: bucketURL,
		S3:          env.cfg,
	})
	if !errors.Is(err, s3lite.ErrReplicaEncrypted) {
		t.Fatalf("no key over s3: got %v, want ErrReplicaEncrypted", err)
	}
}

// TestEncryptedLeaseHandoffS3 exercises the lifecycle with a key set: two encrypted
// instances over one real lease hand the writer role back and forth, each seeing the
// other's committed rows. It also pins the lock-file leak surface — an encrypted
// instance publishes an opaque owner, not its hostname — and that lock.json itself
// stays plaintext by design.
func TestEncryptedLeaseHandoffS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	env := startMinIO(ctx, t, "enchandoff")
	bucketURL := "s3://enchandoff/db"
	key := encIntegrationKey(0x34)
	root := t.TempDir()

	first, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:         filepath.Join(root, "first.sqlite3"),
		BackupTo:          bucketURL,
		S3:                env.cfg,
		Role:              s3lite.RoleAuto,
		LeaseTTL:          10 * time.Second,
		OnDemandPromotion: true,
		EncryptionKey:     key,
		Migrations:        []string{`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)`},
	})
	if err != nil {
		t.Fatalf("open first: %v", err)
	}
	defer first.Close()
	if !first.IsLeader() {
		t.Fatal("first instance should hold the lease")
	}
	if _, err := first.ExecContext(ctx, `INSERT INTO items (name) VALUES ('from-first')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// The lock object stays plaintext (it is litestream's leaser, not the replica
	// client) but must not carry a machine name once a key is configured.
	lock, err := env.client.GetObject(ctx, &s3sdk.GetObjectInput{
		Bucket: aws.String("enchandoff"),
		Key:    aws.String("db/lock.json"),
	})
	if err != nil {
		t.Fatalf("get lock.json: %v", err)
	}
	lockBody, err := io.ReadAll(lock.Body)
	_ = lock.Body.Close()
	if err != nil {
		t.Fatalf("read lock.json: %v", err)
	}
	if !bytes.Contains(lockBody, []byte("generation")) {
		t.Fatalf("lock.json should stay plaintext coordination state, got %q", lockBody)
	}
	if hostname, herr := os.Hostname(); herr == nil && hostname != "" && bytes.Contains(lockBody, []byte(hostname)) {
		t.Fatalf("lock.json leaks the hostname %q: %s", hostname, lockBody)
	}

	// Hand the lease over and let the peer take it.
	if err := first.YieldLease(ctx); err != nil {
		t.Fatalf("yield: %v", err)
	}

	second, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:         filepath.Join(root, "second.sqlite3"),
		BackupTo:          bucketURL,
		S3:                env.cfg,
		Role:              s3lite.RoleAuto,
		LeaseTTL:          10 * time.Second,
		OnDemandPromotion: true,
		EncryptionKey:     key,
	})
	if err != nil {
		t.Fatalf("open second: %v", err)
	}
	defer second.Close()

	if !second.IsLeader() {
		ok, perr := second.TryPromote(ctx)
		if perr != nil || !ok {
			t.Fatalf("second instance could not promote after the yield: ok=%v err=%v", ok, perr)
		}
	}

	// It must have restored the first instance's encrypted write...
	var name string
	if err := second.QueryRowContext(ctx, `SELECT name FROM items`).Scan(&name); err != nil {
		t.Fatalf("second query: %v", err)
	}
	if name != "from-first" {
		t.Fatalf("second instance sees %q, want from-first", name)
	}

	// ...and its own write must come back to the first instance on re-promotion.
	if _, err := second.ExecContext(ctx, `INSERT INTO items (name) VALUES ('from-second')`); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if err := second.YieldLease(ctx); err != nil {
		t.Fatalf("second yield: %v", err)
	}
	if ok, perr := first.TryPromote(ctx); perr != nil || !ok {
		t.Fatalf("first instance could not re-promote: ok=%v err=%v", ok, perr)
	}
	var n int
	if err := first.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatalf("first count: %v", err)
	}
	if n != 2 {
		t.Fatalf("after the round trip the first instance sees %d rows, want 2", n)
	}

	// Nothing along the way wrote a plaintext LTX object.
	for objName, body := range listLTXObjects(ctx, t, env.client, "enchandoff", "db/") {
		if len(body) < 4 || string(body[:4]) != "S3LE" {
			t.Fatalf("%s: a lifecycle path wrote a non-encrypted object", objName)
		}
	}
}
