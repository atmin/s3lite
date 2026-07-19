package s3lite_test

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/atmin/s3lite"
)

// This file backs the README's hard-kill claim: "A hard kill can lose only the
// sub-second window since the last WAL sync; a clean Close loses nothing." It drives
// a real writer in a child process, SIGKILLs it (no cleanup, no Close), and asserts
// the replica restores to a consistent prefix of the acked writes — never a torn
// state. A second variant Closes cleanly across the same process boundary and
// asserts nothing is lost.
//
// The child is TestCrashChild, re-exec'd from the parent via `os.Args[0]
// -test.run=^TestCrashChild$` with the paths and mode passed through env vars. It
// skips as a no-op during a normal test run.

const (
	crashEnvMarker  = "S3LITE_CRASH_CHILD"
	crashEnvMode    = "S3LITE_CRASH_MODE"
	crashEnvLocal   = "S3LITE_CRASH_LOCAL"
	crashEnvReplica = "S3LITE_CRASH_REPLICA"
	crashEnvRows    = "S3LITE_CRASH_ROWS"

	ackPrefix   = "ACK "
	cleanDone   = "CLEAN-DONE"
	childSchema = `CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)`
)

// TestCrashChild is the writer half of the crash harness. It runs only when
// re-exec'd with the marker env var set; otherwise it is skipped so it stays inert
// in a normal `go test` run. In "crash" mode it inserts rows forever (the parent
// SIGKILLs it); in "clean" mode it inserts a fixed count, Closes durably, and exits.
func TestCrashChild(t *testing.T) {
	if os.Getenv(crashEnvMarker) == "" {
		t.Skip("not the crash child (set via re-exec only)")
	}

	ctx := context.Background()
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:  os.Getenv(crashEnvLocal),
		BackupTo:   os.Getenv(crashEnvReplica),
		Migrations: []string{childSchema},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(2)
	}

	insert := func(i int) {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (id) VALUES (?)`, i); err != nil {
			fmt.Fprintln(os.Stderr, "child insert:", err)
			os.Exit(2)
		}
		fmt.Printf("%s%d\n", ackPrefix, i) // os.Stdout is unbuffered; the parent reads these live
	}

	if os.Getenv(crashEnvMode) == "clean" {
		rows, _ := strconv.Atoi(os.Getenv(crashEnvRows))
		for i := 1; i <= rows; i++ {
			insert(i)
		}
		if err := db.Close(); err != nil { // durable flush — this is the point of the variant
			fmt.Fprintln(os.Stderr, "child close:", err)
			os.Exit(2)
		}
		fmt.Println(cleanDone)
		os.Exit(0)
	}

	// crash mode: insert forever, never Close. The small pace keeps the ack log
	// bounded and gives litestream's ~1s monitor time to ship a prefix before the
	// parent kills us.
	for i := 1; ; i++ {
		insert(i)
		time.Sleep(2 * time.Millisecond)
	}
}

func parseAck(line string) (int, bool) {
	if !strings.HasPrefix(line, ackPrefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[len(ackPrefix):]))
	if err != nil {
		return 0, false
	}
	return n, true
}

func crashChildCmd(t *testing.T, mode, local, replica string, extraEnv ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(),
		crashEnvMarker+"=1",
		crashEnvMode+"="+mode,
		crashEnvLocal+"="+local,
		crashEnvReplica+"="+replica,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	return cmd
}

func TestHardKillRestoresConsistentPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness re-execs the test binary; skipped under -short")
	}

	root := t.TempDir()
	replicaURL := "file://" + filepath.Join(root, "replica")
	cmd := crashChildCmd(t, "crash", filepath.Join(root, "child.sqlite3"), replicaURL)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr // surface a child open/insert failure
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Read acks until we have a healthy count AND litestream has had time to sync at
	// least one prefix (monitor interval ~1s). lastAcked is finalised by draining
	// after the kill, so it always reflects the true last acked row.
	scanner := bufio.NewScanner(stdout)
	lastAcked := 0
	reachedTarget := false
	start := time.Now()
	for scanner.Scan() {
		if n, ok := parseAck(scanner.Text()); ok {
			lastAcked = n
		}
		if lastAcked >= 50 && time.Since(start) >= 2500*time.Millisecond {
			reachedTarget = true
			break
		}
	}
	if !reachedTarget {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("crash child exited before the row target (lastAcked=%d); see stderr above", lastAcked)
	}

	// Randomize when the kill lands relative to the child's insert/checkpoint/sync
	// cycle: the scanner loop above always breaks just after an ack, so without this
	// jitter the SIGKILL would sample only inter-insert gaps and never, say, a WAL
	// checkpoint or a mid-sync moment. The delay is logged so a failure reproduces.
	killDelay := time.Duration(rand.Intn(300)) * time.Millisecond
	t.Logf("hard kill: delaying SIGKILL by %v to vary where it lands", killDelay)
	time.Sleep(killDelay)

	// Hard kill: SIGKILL runs no cleanup — no Close, no final sync.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	// Drain the rest of the child's buffered acks so lastAcked is the true maximum.
	for scanner.Scan() {
		if n, ok := parseAck(scanner.Text()); ok {
			lastAcked = n
		}
	}
	_ = cmd.Wait() // reaps the killed process ("signal: killed" is expected)

	if lastAcked == 0 {
		t.Fatal("child never acked a row")
	}

	// A fresh instance restores from the replica the killed writer left behind.
	ctx := context.Background()
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(root, "restored.sqlite3"),
		RestoreFrom: replicaURL,
	})
	if err != nil {
		t.Fatalf("restore from a hard-killed replica: %v", err)
	}
	defer db.Close()

	// Integrity: the restored file is a valid SQLite database, never torn.
	var ig string
	if err := db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&ig); err != nil {
		t.Fatalf("integrity_check query: %v", err)
	}
	if ig != "ok" {
		t.Fatalf("restored database failed integrity_check: %q", ig)
	}

	// If nothing had synced by kill time the items table may not exist yet — that is
	// a legal empty restore (k == 0). Consistency holds vacuously.
	var tbl string
	err = db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name='items'`).Scan(&tbl)
	if errors.Is(err, sql.ErrNoRows) {
		t.Logf("hard kill: replica had no synced state yet (k=0); empty restore is consistent")
		return
	}
	if err != nil {
		t.Fatalf("look up items table: %v", err)
	}

	var count int
	var maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT count(*), max(id) FROM items`).Scan(&count, &maxID); err != nil {
		t.Fatalf("read restored rows: %v", err)
	}
	k := 0
	if maxID.Valid {
		k = int(maxID.Int64)
	}
	// Prefix consistency: the surviving rows are exactly {1..k} with no holes.
	// count == max(id) forces contiguity, since ids start at 1 and are unique.
	if count != k {
		t.Fatalf("torn restore: count(*)=%d but max(id)=%d — the prefix has holes", count, k)
	}
	// The tail window may be lost, but nothing beyond what the writer acked can appear.
	if k > lastAcked {
		t.Fatalf("restored a row (%d) the writer never acked (last acked %d)", k, lastAcked)
	}
	t.Logf("hard kill: writer acked %d rows, replica restored a consistent prefix of %d", lastAcked, k)
}

func TestCleanCloseAcrossProcessBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness re-execs the test binary; skipped under -short")
	}

	root := t.TempDir()
	replicaURL := "file://" + filepath.Join(root, "replica")
	const rows = 30

	cmd := crashChildCmd(t, "clean", filepath.Join(root, "child.sqlite3"), replicaURL,
		crashEnvRows+"="+strconv.Itoa(rows))
	out, err := cmd.CombinedOutput() // the clean child exits on its own
	if err != nil {
		t.Fatalf("clean child failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), cleanDone) {
		t.Fatalf("clean child did not report a durable Close:\n%s", out)
	}

	// After a clean Close through a real process boundary, the replica must hold
	// every acked row — a clean Close loses nothing.
	ctx := context.Background()
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(root, "restored.sqlite3"),
		RestoreFrom: replicaURL,
	})
	if err != nil {
		t.Fatalf("restore after clean close: %v", err)
	}
	defer db.Close()

	var count int
	var maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT count(*), max(id) FROM items`).Scan(&count, &maxID); err != nil {
		t.Fatalf("read restored rows: %v", err)
	}
	if count != rows || !maxID.Valid || int(maxID.Int64) != rows {
		t.Fatalf("clean Close must lose nothing: got count=%d max=%v, want %d rows 1..%d", count, maxID, rows, rows)
	}
}
