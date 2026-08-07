package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/atmin/s3lite"
	_ "modernc.org/sqlite"
)

// fakeDB is the shell's handle over a plain local SQLite file, counting the calls
// the cadence and lifecycle contracts are made of. It is the "injected refresh
// seam": the REPL's rules are about how often it pulls and promotes, which is
// exactly what a bucket-backed handle cannot be asked cheaply.
type fakeDB struct {
	*sql.DB

	mu         sync.Mutex
	refreshes  int
	promotes   int
	yields     int
	leader     bool
	promotable bool
	// onRefresh runs inside Refresh, so a test can advance the "replica" exactly
	// where a real pull would publish new state.
	onRefresh func()
}

func (f *fakeDB) Refresh(context.Context) (bool, error) {
	f.mu.Lock()
	f.refreshes++
	fn := f.onRefresh
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
	return true, nil
}

func (f *fakeDB) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader
}

func (f *fakeDB) TryPromote(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promotes++
	if f.promotable {
		f.leader = true
	}
	return f.leader, nil
}

func (f *fakeDB) YieldLease(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.yields++
	if !f.leader {
		return s3lite.ErrNotLeader
	}
	f.leader = false
	return nil
}

func (f *fakeDB) counts() (refreshes, promotes, yields int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshes, f.promotes, f.yields
}

// newFake opens a plain SQLite file (dsn suffix and all) as a shell handle.
func newFake(t *testing.T, path, dsnSuffix string) *fakeDB {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", path+dsnSuffix)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	sqlDB.SetMaxOpenConns(1) // the shell pins one connection; see run()
	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("ping sqlite: %v", err)
	}
	return &fakeDB{DB: sqlDB, leader: true, promotable: true}
}

// seeded returns a fake over a fresh file holding one table with one row.
func seeded(t *testing.T) *fakeDB {
	t.Helper()
	f := newFake(t, filepath.Join(t.TempDir(), "db.sqlite3"), "")
	mustExec(t, f.DB, `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)`)
	mustExec(t, f.DB, `INSERT INTO t VALUES (1, 'a@b.c')`)
	return f
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// runShell drives one session over input and returns what it wrote.
func runShell(t *testing.T, db handle, input string, tune func(*repl)) (out, errOut string, err error) {
	t.Helper()
	var o, e bytes.Buffer
	r := newREPL(db, strings.NewReader(input), &o, &e)
	if tune != nil {
		tune(r)
	}
	err = r.run(context.Background())
	return o.String(), e.String(), err
}

func interactive(r *repl) { r.interactive = true }

// TestPipedSessionRefreshesOnce pins the batch half of the freshness contract: a
// script is logically one read of one version, so a 200-statement pipe pays one
// pull, not 200.
func TestPipedSessionRefreshesOnce(t *testing.T) {
	f := seeded(t)
	var script strings.Builder
	for range 200 {
		script.WriteString("SELECT id FROM t;\n")
	}
	if _, _, err := runShell(t, f, script.String(), nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if refreshes, _, _ := f.counts(); refreshes != 1 {
		t.Fatalf("piped session refreshed %d times, want exactly 1", refreshes)
	}
}

// TestInteractiveSessionRefreshesPerStatement pins the other half: at a prompt,
// what you read is what the replica held when you pressed Enter.
func TestInteractiveSessionRefreshesPerStatement(t *testing.T) {
	f := seeded(t)
	const n = 5
	if _, _, err := runShell(t, f, strings.Repeat("SELECT id FROM t;\n", n), interactive); err != nil {
		t.Fatalf("run: %v", err)
	}
	if refreshes, _, _ := f.counts(); refreshes != n {
		t.Fatalf("interactive session refreshed %d times, want %d (one per statement)", refreshes, n)
	}
}

// TestRefreshSuppressedInsideTransaction is the transaction rule: publishing new
// state swaps the file under the session, so a hand-typed BEGIN … COMMIT must see
// one snapshot throughout — and the pull must resume right after it.
func TestRefreshSuppressedInsideTransaction(t *testing.T) {
	f := seeded(t)
	// Every pull "advances the replica" by one row, so a suppressed pull is
	// visible in the counts the session reads.
	next := 1
	f.onRefresh = func() {
		next++
		mustExec(t, f.DB, `INSERT INTO t VALUES (?, ?)`, next, "x")
	}

	out, _, err := runShell(t, f, strings.Join([]string{
		"SELECT count(*) FROM t;", // pull #1 → 2 rows
		"BEGIN;",                  // pull #2 → 3 rows, then the tx opens
		"SELECT count(*) FROM t;", // suppressed → still 3
		"SELECT count(*) FROM t;", // suppressed → still 3
		"COMMIT;",                 // suppressed
		"SELECT count(*) FROM t;", // pull #3 → 4 rows
		"",
	}, "\n"), interactive)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	got := strings.Fields(strings.NewReplacer("s3lite> ", "", "   ...> ", "").Replace(out))
	want := []string{"2", "3", "3", "4"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("counts across the transaction = %v, want %v (the snapshot must not move between BEGIN and COMMIT)", got, want)
	}
	if refreshes, _, _ := f.counts(); refreshes != 3 {
		t.Fatalf("refreshed %d times, want 3 (suppressed for the three in-transaction statements)", refreshes)
	}
}

// TestRollbackToSavepointStaysInTransaction guards the one prefix that reads like
// the end of a transaction but is not.
func TestRollbackToSavepointStaysInTransaction(t *testing.T) {
	r := newREPL(nil, nil, io.Discard, io.Discard)
	for _, stmt := range []string{"begin;", "SAVEPOINT s;", "  -- note\n  rollback TO s;"} {
		r.applyTx(stmt)
	}
	if r.txDepth != 1 {
		t.Fatalf("txDepth after ROLLBACK TO = %d, want 1 (it unwinds inside the transaction)", r.txDepth)
	}
	r.applyTx("/* done */ ROLLBACK;")
	if r.txDepth != 0 {
		t.Fatalf("txDepth after a bare ROLLBACK = %d, want 0", r.txDepth)
	}
}

// TestWriterTakesThePenOnAStatement is the write path OnDemandPromotion is built
// for: the session holds no lease until it has something to run.
func TestWriterTakesThePenOnAStatement(t *testing.T) {
	f := seeded(t)
	f.leader = false
	_, _, err := runShell(t, f, "SELECT id FROM t;\n", func(r *repl) { r.promote = true })
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, promotes, _ := f.counts(); promotes != 1 {
		t.Fatalf("promoted %d times, want 1", promotes)
	}
	if !f.IsLeader() {
		t.Fatal("session did not take the write lease")
	}
}

// TestFollowerNeverPromotesOrYields: --role=follower is read-only by construction,
// which is what makes the CLI's follower a follower rather than an eventual writer.
func TestFollowerNeverPromotesOrYields(t *testing.T) {
	f := seeded(t)
	f.leader = false
	_, _, err := runShell(t, f, "SELECT id FROM t;\n", func(r *repl) {
		r.interactive = true
		r.idleYield = time.Millisecond
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, promotes, yields := f.counts(); promotes != 0 || yields != 0 {
		t.Fatalf("follower promoted %d and yielded %d times, want 0 and 0", promotes, yields)
	}
}

// TestHeldLeaseIsReportedOnce keeps a session whose peer holds the lease readable:
// one line, not one per statement.
func TestHeldLeaseIsReportedOnce(t *testing.T) {
	f := seeded(t)
	f.leader, f.promotable = false, false
	_, errOut, err := runShell(t, f, strings.Repeat("SELECT id FROM t;\n", 3), func(r *repl) { r.promote = true })
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := strings.Count(errOut, "held by another instance"); n != 1 {
		t.Fatalf("reported the held lease %d times, want 1:\n%s", n, errOut)
	}
}

// TestIdleSessionYieldsAndTakesThePenBack is the REPL lifecycle the landed
// YieldLease + OnDemandPromotion slice exists for: a forgotten prompt releases the
// pen, and the next statement takes it back.
func TestIdleSessionYieldsAndTakesThePenBack(t *testing.T) {
	f := seeded(t)
	// The input blocks until the session has yielded, so the test drives the idle
	// window by observation rather than by sleeping.
	in := blockUntil(func() bool {
		_, _, yields := f.counts()
		return yields > 0
	}, "SELECT id FROM t;\n")

	var out, errOut bytes.Buffer
	r := newREPL(f, in, &out, &errOut)
	r.interactive = true
	r.promote = true
	r.idleYield = 5 * time.Millisecond
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	_, promotes, yields := f.counts()
	if yields != 1 {
		t.Fatalf("yielded %d times, want 1", yields)
	}
	if promotes != 1 {
		t.Fatalf("promoted %d times after the yield, want 1 (the next statement takes the pen back)", promotes)
	}
	if !f.IsLeader() {
		t.Fatal("session did not hold the lease again after writing")
	}
	if !strings.Contains(errOut.String(), "released the write lease") {
		t.Fatalf("the yield went unreported:\n%s", errOut.String())
	}
}

// TestIdleYieldWaitsForTheTransactionToEnd: a prompt sitting inside BEGIN is being
// typed at, not idle — handing the lease back there would fence the transaction.
func TestIdleYieldWaitsForTheTransactionToEnd(t *testing.T) {
	f := seeded(t)
	var out, errOut bytes.Buffer
	r := newREPL(f, io.MultiReader(strings.NewReader("BEGIN;\n"), blockFor(50*time.Millisecond)), &out, &errOut)
	r.interactive = true
	r.promote = true
	r.idleYield = 5 * time.Millisecond
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, _, yields := f.counts(); yields != 0 {
		t.Fatalf("yielded %d times inside a transaction, want 0", yields)
	}
}

// TestFollowerWriteNamesTheWriterFlag: the read-only error a follower gets is
// SQLite's, and it cannot say which flag makes the session writable.
func TestFollowerWriteNamesTheWriterFlag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	writable := newFake(t, path, "")
	mustExec(t, writable.DB, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)

	f := newFake(t, path, "?_pragma=query_only(1)")
	_, errOut, err := runShell(t, f, "INSERT INTO t VALUES (1);\n", nil)
	if !errors.Is(err, errStatementFailed) {
		t.Fatalf("run error = %v, want the script to stop", err)
	}
	if !strings.Contains(errOut, "--role=writer") {
		t.Fatalf("read-only error did not name --role=writer:\n%s", errOut)
	}
}

// TestScriptStopsAtFirstErrorAndPromptDoesNot pins the two error contracts: a pipe
// exits non-zero where sqlite3 would, a human retypes.
func TestScriptStopsAtFirstErrorAndPromptDoesNot(t *testing.T) {
	script := "SELECT 1;\nSELECT nope;\nSELECT 3;\n"

	out, _, err := runShell(t, seeded(t), script, nil)
	if !errors.Is(err, errStatementFailed) {
		t.Fatalf("piped run error = %v, want errStatementFailed", err)
	}
	if strings.Contains(out, "3") {
		t.Fatalf("piped session ran past its first error:\n%s", out)
	}

	out, _, err = runShell(t, seeded(t), script, interactive)
	if err != nil {
		t.Fatalf("interactive run: %v", err)
	}
	if !strings.Contains(out, "3") {
		t.Fatalf("interactive session stopped at an error:\n%s", out)
	}
}

// TestInterruptCancelsInFlightStatementThenExits pins the Ctrl-C ladder: the first
// interrupt kills the statement, one at an idle prompt ends the session (main then
// closes cleanly — see TestCleanExitKeepsEveryCommittedStatement).
func TestInterruptCancelsInFlightStatementThenExits(t *testing.T) {
	r := newREPL(seeded(t), blockFor(time.Hour), io.Discard, io.Discard)
	r.interactive = true

	if r.interruptStatement() {
		t.Fatal("interruptStatement reported a statement in flight at an idle prompt")
	}
	ctx, done := r.statementContext(context.Background())
	if !r.interruptStatement() {
		t.Fatal("interruptStatement did not find the registered statement")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("the in-flight statement's context was not cancelled")
	}
	done()

	r.requestExit()
	errc := make(chan error, 1)
	go func() { errc <- r.run(context.Background()) }()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("run after an interrupt at an idle prompt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an interrupt at an idle prompt did not end the session")
	}
}

// TestCleanExitKeepsEveryCommittedStatement is INVARIANTS.md #4 in user-facing
// form: a session ended by Ctrl-C at the prompt loses nothing it acked, which a
// restore into a fresh local file must show.
func TestCleanExitKeepsEveryCommittedStatement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	replica := "file://" + filepath.Join(dir, "replica")

	db, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(dir, "session.sqlite3"),
		RestoreFrom: replica,
		BackupTo:    replica,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	script := "CREATE TABLE t (id INTEGER PRIMARY KEY);\nINSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\nINSERT INTO t VALUES (3);\n"
	// The statements arrive, then the input blocks — the session ends the way a
	// human ends it, with an interrupt at an idle prompt.
	r := newREPL(db, io.MultiReader(strings.NewReader(script), blockUntil(func() bool {
		var n int
		return db.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n) == nil && n == 3
	}, "")), io.Discard, io.Discard)
	r.interactive = true
	go func() {
		for {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n); err == nil && n == 3 {
				r.requestExit()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	if err := r.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restored, err := s3lite.Open(ctx, s3lite.Config{
		LocalPath:   filepath.Join(dir, "restored.sqlite3"),
		RestoreFrom: replica,
	})
	if err != nil {
		t.Fatalf("reopen from the replica: %v", err)
	}
	defer restored.Close()
	var n int
	if err := restored.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count restored rows: %v", err)
	}
	if n != 3 {
		t.Fatalf("the replica holds %d of the 3 committed statements", n)
	}
}

func TestSplitStatement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		in       string
		stmt     string
		rest     string
		complete bool
	}{
		{"plain", "SELECT 1; SELECT 2;", "SELECT 1;", " SELECT 2;", true},
		{"incomplete", "SELECT\n1\n", "", "SELECT\n1\n", false},
		{"semicolon in a string", "INSERT INTO t VALUES (';');", "INSERT INTO t VALUES (';');", "", true},
		{"doubled quote", "SELECT 'it''s; fine';", "SELECT 'it''s; fine';", "", true},
		{"quoted identifier", `SELECT "a;b" FROM t;`, `SELECT "a;b" FROM t;`, "", true},
		{"bracket identifier", "SELECT [a;b] FROM t;", "SELECT [a;b] FROM t;", "", true},
		{"line comment", "SELECT 1 -- ;not here\n;", "SELECT 1 -- ;not here\n;", "", true},
		{"block comment", "SELECT /* ; */ 1;", "SELECT /* ; */ 1;", "", true},
		{"unterminated string", "SELECT 'oops;", "", "SELECT 'oops;", false},
		{"minus is not a comment", "SELECT 1-1;", "SELECT 1-1;", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, rest, complete := splitStatement(tc.in)
			if stmt != tc.stmt || rest != tc.rest || complete != tc.complete {
				t.Fatalf("splitStatement(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.in, stmt, rest, complete, tc.stmt, tc.rest, tc.complete)
			}
		})
	}
}

// blockUntil returns a reader that holds the session at its prompt until cond
// holds and only then yields s, so a test drives the idle window by observing the
// session rather than by sleeping for a fixed time.
func blockUntil(cond func() bool, s string) io.Reader {
	return io.MultiReader(readerFunc(func([]byte) (int, error) {
		for !cond() {
			time.Sleep(time.Millisecond)
		}
		return 0, io.EOF
	}), strings.NewReader(s))
}

// blockFor returns a reader that holds the session at its prompt for d.
func blockFor(d time.Duration) io.Reader {
	return readerFunc(func([]byte) (int, error) {
		time.Sleep(d)
		return 0, io.EOF
	})
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
