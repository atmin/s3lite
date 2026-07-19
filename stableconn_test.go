package s3lite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// These tests pin the connector's promise that "a query with a deadline is not
// stuck behind a long-running swap" (rlock): a promote/refresh restore can hold the
// swap gate for however long a full S3 restore takes, and a caller-bounded Connect
// must honour its context instead of queueing behind it.

func TestConnectHonoursContextDuringSwap(t *testing.T) {
	// A swap holds the gate for writing while it rebuilds the local file. A Connect
	// racing it with a deadline must return ctx.Err() promptly — and the gate must
	// not be poisoned afterwards: the abandoned lock acquisition is released in the
	// background, so later swaps and connects proceed normally.
	drv, err := sharedDriver()
	if err != nil {
		t.Fatal(err)
	}
	c := newStableConnector(drv, filepath.Join(t.TempDir(), "db.sqlite3"), false)

	swapEntered := make(chan struct{})
	swapRelease := make(chan struct{})
	swapDone := make(chan error, 1)
	go func() {
		swapDone <- c.swapFiles(false, func() error { // fn runs with the gate held
			close(swapEntered)
			<-swapRelease
			return nil
		})
	}()
	<-swapEntered

	// The gate is held: a bounded Connect must give up at its deadline, not wait for
	// the swap to finish.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := c.Connect(ctx)
	elapsed := time.Since(start)
	if err == nil {
		conn.Close()
		t.Fatal("Connect during a held swap must fail with the caller's ctx error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Connect did not honour its deadline; blocked %v behind the swap", elapsed)
	}

	// Release the swap; the connector must be fully usable again.
	close(swapRelease)
	if err := <-swapDone; err != nil {
		t.Fatalf("swap: %v", err)
	}

	// The abandoned Connect's background lock acquisition must not leak a read
	// lock: a follow-up swap (write lock) and a fresh Connect both succeed.
	if err := c.swapFiles(false, nil); err != nil {
		t.Fatalf("swap after an abandoned Connect: %v", err)
	}
	conn, err = c.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect after the swap released: %v", err)
	}
	conn.Close()
}

func TestQueryDeadlineNotStuckBehindSwap(t *testing.T) {
	// The same guarantee through database/sql: a QueryRowContext with a deadline,
	// issued while a swap holds the gate, errors at the deadline instead of queueing.
	// MaxIdleConns(0) forces the query to dial a fresh connection, so it must pass
	// through the gated Connect rather than reuse a pooled pre-swap connection.
	ctx := context.Background()
	db, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "db.sqlite3"),
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(0)

	swapEntered := make(chan struct{})
	swapRelease := make(chan struct{})
	swapDone := make(chan error, 1)
	go func() {
		swapDone <- db.connector.swapFiles(false, func() error {
			close(swapEntered)
			<-swapRelease
			return nil
		})
	}()
	<-swapEntered

	qctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	var n int
	err = db.QueryRowContext(qctx, `SELECT count(*) FROM items`).Scan(&n)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a bounded query during a held swap must fail, not wait it out")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("query did not honour its deadline; blocked %v behind the swap", elapsed)
	}

	close(swapRelease)
	if err := <-swapDone; err != nil {
		t.Fatalf("swap: %v", err)
	}
	// Steady state restored: the same handle serves once the swap releases.
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		t.Fatalf("query after the swap released: %v", err)
	}
}
