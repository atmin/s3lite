package s3lite_test

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"

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

	// Reacquire-scenario plumbing (see runCrashReacquireScenario): the clean child's
	// id offset, an explicit pre-Close Sync assertion, and the leased-writer settings
	// (role, TTL, owner, S3 endpoint) a MinIO child needs to coordinate for real.
	crashEnvBase       = "S3LITE_CRASH_BASE"
	crashEnvSync       = "S3LITE_CRASH_SYNC"
	crashEnvVerbose    = "S3LITE_CRASH_VERBOSE"
	crashEnvRole       = "S3LITE_CRASH_ROLE"
	crashEnvTTL        = "S3LITE_CRASH_TTL"
	crashEnvOwner      = "S3LITE_CRASH_OWNER"
	crashEnvS3Endpoint = "S3LITE_CRASH_S3_ENDPOINT"
	crashEnvS3Region   = "S3LITE_CRASH_S3_REGION"
	crashEnvS3Key      = "S3LITE_CRASH_S3_KEY"
	crashEnvS3Secret   = "S3LITE_CRASH_S3_SECRET"

	ackPrefix   = "ACK "
	cleanDone   = "CLEAN-DONE"
	childSchema = `CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)`

	// The successor's batch in the reacquire scenario: disjoint from the crash
	// child's 1..n ids so the restore can tell the two tenures apart.
	reacquireRows = 30
	reacquireBase = 1_000_000
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
	cfg := s3lite.Config{
		LocalPath:  os.Getenv(crashEnvLocal),
		BackupTo:   os.Getenv(crashEnvReplica),
		Migrations: []string{childSchema},
	}
	if os.Getenv(crashEnvVerbose) == "1" {
		// Surface s3lite lifecycle INFO (and litestream WARNs) on stderr for the
		// parent to inspect on failure.
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if os.Getenv(crashEnvRole) == "writer" {
		// Leased writer (the reacquire scenario over s3). INFO logging surfaces the
		// resume/restore decision on stderr so the parent can assert on it.
		cfg.Role = s3lite.RoleWriter
		cfg.Owner = os.Getenv(crashEnvOwner)
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		ttl, err := time.ParseDuration(os.Getenv(crashEnvTTL))
		if err != nil {
			fmt.Fprintln(os.Stderr, "child lease ttl:", err)
			os.Exit(2)
		}
		cfg.LeaseTTL = ttl
		cfg.S3 = s3lite.S3Config{
			Endpoint:        os.Getenv(crashEnvS3Endpoint),
			Region:          os.Getenv(crashEnvS3Region),
			AccessKeyID:     os.Getenv(crashEnvS3Key),
			SecretAccessKey: os.Getenv(crashEnvS3Secret),
		}
	}

	// The consumer's reacquire loop: a restart while the dead predecessor's lease
	// lingers sees LeaseExistsError until the TTL expires, then acquires directly at
	// Open — the Open-direct re-entry of INVARIANTS.md #9. Unleased children succeed
	// on the first pass.
	var db *s3lite.DB
	for {
		var err error
		if db, err = s3lite.Open(ctx, cfg); err == nil {
			break
		}
		var held *litestream.LeaseExistsError
		if errors.As(err, &held) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
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
		base, _ := strconv.Atoi(os.Getenv(crashEnvBase)) // 0 unless the scenario sets an offset
		for i := base + 1; i <= base+rows; i++ {
			insert(i)
		}
		if os.Getenv(crashEnvSync) == "1" {
			// The reacquire scenario asserts the batch is provably synced before Close,
			// so a later restore that misses it is a lineage bug, not a lost flush.
			if err := db.Sync(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "child sync:", err)
				os.Exit(2)
			}
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

// runCrashChildUntilKilled starts a crash-mode child, reads acks until a healthy
// count AND litestream has had time to sync at least one prefix (monitor interval
// ~1s), then SIGKILLs it after a randomized delay — no Close, no final sync. The
// jitter varies where the kill lands relative to the child's
// insert/checkpoint/sync cycle (the read loop always breaks just after an ack, so
// without it the kill would sample only inter-insert gaps, never a WAL checkpoint
// or a mid-sync moment); it is logged so a failure reproduces. The returned
// lastAcked is finalised by draining the pipe after the kill, so it always
// reflects the true last acked row.
func runCrashChildUntilKilled(t *testing.T, cmd *exec.Cmd) (lastAcked int) {
	t.Helper()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr // surface a child open/insert failure
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(stdout)
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

	killDelay := time.Duration(rand.Intn(300)) * time.Millisecond
	t.Logf("hard kill: delaying SIGKILL by %v to vary where it lands", killDelay)
	time.Sleep(killDelay)

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
	return lastAcked
}

func TestHardKillRestoresConsistentPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness re-execs the test binary; skipped under -short")
	}

	root := t.TempDir()
	replicaURL := "file://" + filepath.Join(root, "replica")
	lastAcked := runCrashChildUntilKilled(t,
		crashChildCmd(t, "crash", filepath.Join(root, "child.sqlite3"), replicaURL))

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

// runCrashReacquireScenario drives the consumer-observed crash-reacquire sequence
// (tasks/crash-reacquire-rewind-repro.md) end-to-end across real process boundaries:
//
//  1. Child A (crash mode) inserts and replicates continuously; once a synced
//     prefix exists the parent SIGKILLs it, leaving a genuinely dirty WAL, live
//     litestream state — and, when leased, a lingering lease.
//  2. Child B (clean mode) reopens the SAME LocalPath — the writer restarting on
//     the same machine, retrying Open until the dead writer's lease expires when
//     leased — inserts a disjoint batch, and asserts its own Sync and Close return
//     nil (invariant #4: the flush either succeeded or failed loudly).
//  3. A fresh instance (no local file) restores from the replica and must see B's
//     entire tenure (the observed failure was its total absence) AND all of A's
//     acked rows (the resumed local tail shipped onward, #9), contiguous and with
//     integrity intact (#5).
//
// childEnv carries the lease settings for the leased (s3) variant; nil runs the
// unleased sole-writer shape. Returns child B's combined output so callers can
// assert on the resume/restore decision it logged.
func runCrashReacquireScenario(t *testing.T, localPath, replicaURL string, s3cfg s3lite.S3Config, childEnv []string) string {
	t.Helper()

	lastAcked := runCrashChildUntilKilled(t,
		crashChildCmd(t, "crash", localPath, replicaURL, childEnv...))

	cleanEnv := append(append([]string{}, childEnv...),
		crashEnvRows+"="+strconv.Itoa(reacquireRows),
		crashEnvBase+"="+strconv.Itoa(reacquireBase),
		crashEnvSync+"=1",
	)
	out, err := crashChildCmd(t, "clean", localPath, replicaURL, cleanEnv...).CombinedOutput()
	if err != nil {
		t.Fatalf("successor child failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), cleanDone) {
		t.Fatalf("successor child did not report a durable Close:\n%s", out)
	}

	// The consumer's observing step: a fresh node with no local file restores from
	// the replica.
	ctx := context.Background()
	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(filepath.Dir(localPath), "restored.sqlite3"),
		RestoreFrom: replicaURL,
		S3:          s3cfg,
	})
	if err != nil {
		t.Fatalf("restore after reacquired tenure: %v", err)
	}
	defer db.Close()

	var ig string
	if err := db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&ig); err != nil {
		t.Fatalf("integrity_check query: %v", err)
	}
	if ig != "ok" {
		t.Fatalf("restored database failed integrity_check: %q", ig)
	}

	// B's tenure, in full: it was synced and cleanly closed, so a single missing row
	// is a bug — and its total absence is exactly the observed rewind.
	var bCount int
	var bMin, bMax sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*), min(id), max(id) FROM items WHERE id > ?`, reacquireBase).Scan(&bCount, &bMin, &bMax); err != nil {
		t.Fatalf("read successor tenure: %v", err)
	}
	if bCount == 0 {
		t.Fatalf("rewind reproduced: the successor's synced, cleanly-closed tenure is entirely missing (restore has only the pre-kill state, A max %d)", lastAcked)
	}
	if bCount != reacquireRows || bMin.Int64 != reacquireBase+1 || bMax.Int64 != reacquireBase+reacquireRows {
		t.Fatalf("successor tenure torn: got %d rows in [%d,%d], want %d rows %d..%d",
			bCount, bMin.Int64, bMax.Int64, reacquireRows, reacquireBase+1, reacquireBase+reacquireRows)
	}

	// A's tenure: the successor resumed A's local file, so its clean Close must have
	// shipped A's whole committed tail — every acked row, not just the pre-kill
	// synced prefix (#9) — contiguous (#5), plus at most the one insert that could
	// have committed after the last ack the parent read.
	var aCount int
	var aMax sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*), max(id) FROM items WHERE id <= ?`, reacquireBase).Scan(&aCount, &aMax); err != nil {
		t.Fatalf("read crashed tenure: %v", err)
	}
	k := int(aMax.Int64)
	if aCount != k {
		t.Fatalf("torn restore: crashed tenure has %d rows but max id %d — the prefix has holes", aCount, k)
	}
	if k < lastAcked {
		t.Fatalf("rewind reproduced: resumed tenure lost acked writes — restored tail ends at %d, child A acked %d", k, lastAcked)
	}
	if k > lastAcked+1 {
		t.Fatalf("restored %d rows the crashed child never acked (last acked %d)", k-lastAcked, lastAcked)
	}
	t.Logf("reacquire: A acked %d rows (restored %d), B's %d-row tenure fully present", lastAcked, k, reacquireRows)
	return string(out)
}

func TestCrashRestartResumedTenureSurvivesRestore(t *testing.T) {
	// The unleased (file:// replica) shape of the reacquire scenario: no lease, so
	// the same-LocalPath restart resumes the dirty local file unconditionally. Keeps
	// the resume-over-a-genuinely-dirty-WAL mechanics covered in the default suite;
	// the leased, real-S3 shape the consumer observed runs under the integration tag
	// (TestCrashReacquireResumedTenureSurvivesRestoreS3).
	if testing.Short() {
		t.Skip("crash harness re-execs the test binary; skipped under -short")
	}

	root := t.TempDir()
	runCrashReacquireScenario(t,
		filepath.Join(root, "node.sqlite3"), "file://"+filepath.Join(root, "replica"),
		s3lite.S3Config{}, nil)
}
