package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// handle is the slice of *s3lite.DB the shell drives. It is an interface so the
// cadence tests can count refreshes, promotions and yields around a plain
// *sql.DB with no bucket behind it; *s3lite.DB satisfies it as-is (the four SQL
// methods come from its embedded *sql.DB).
type handle interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Refresh(ctx context.Context) (bool, error)
	IsLeader() bool
	TryPromote(ctx context.Context) (bool, error)
	YieldLease(ctx context.Context) error
}

// errQuit unwinds the loop from a .quit/.exit dot-command.
var errQuit = errors.New("quit")

// errStatementFailed ends a piped session at its first error, the way sqlite3
// does. The loop has already printed the error, so main only turns this into a
// non-zero exit rather than reporting it a second time.
var errStatementFailed = errors.New("statement failed")

// maxStatementLine bounds one input line. The scanner's 64 KiB default is small
// for a machine-generated INSERT.
const maxStatementLine = 16 << 20

// repl is the shell: read a statement, maybe pull the replica, execute, render.
//
// Two contracts live here and nowhere else in the binary. Freshness: at an
// interactive prompt every statement is preceded by a Refresh, because human
// think-time dominates the pull and "what you read is what the replica had when
// you pressed Enter" is the least surprising rule; a piped script is logically
// one read of one version and refreshes once, before its first statement.
// Transactions: Refresh publishes by bumping the connector generation, so an
// in-flight connection re-dials against the new file — which must not happen
// under a hand-typed BEGIN … COMMIT. txDepth suppresses it.
type repl struct {
	db   handle
	in   io.Reader
	out  io.Writer
	errw io.Writer

	// interactive selects the freshness contract above; main sets it from whether
	// stdin is a terminal, and it also decides whether prompts are printed and
	// whether an error stops the session (a script bails, a human retypes).
	interactive bool
	// promote is set for --role=auto and --role=writer: a statement takes the pen
	// when this instance does not hold it, which is the write path that pairs with
	// OnDemandPromotion. A follower never promotes.
	promote bool
	// idleYield hands the lease back after this long at an idle prompt (0 disables).
	idleYield time.Duration

	mode    string
	headers bool

	// txDepth is explicit-transaction depth, tracked by statement-prefix
	// inspection — the subset a shell actually sees, not a SQL parser.
	txDepth int
	// refreshed records that the one-shot pull of a piped session has happened.
	refreshed bool
	// yielded disarms the idle timer after a hand-back, so an idle session ticks
	// once and then rests; the next statement re-arms it.
	yielded bool
	// warnedNoPen keeps "the lease is held elsewhere" to one line per contiguous
	// run of failed promotions rather than one per statement.
	warnedNoPen bool

	// interrupt carries a Ctrl-C at an idle prompt (or a second one) from the
	// signal watcher to the loop. Buffered so the watcher never blocks.
	interrupt chan struct{}
	// mu guards cancel, which the signal watcher reads off the loop goroutine.
	mu     sync.Mutex
	cancel context.CancelFunc
}

func newREPL(db handle, in io.Reader, out, errw io.Writer) *repl {
	return &repl{
		db:        db,
		in:        in,
		out:       out,
		errw:      errw,
		mode:      "list",
		interrupt: make(chan struct{}, 1),
	}
}

// run drives the loop until EOF, .quit, or an interrupt at an idle prompt. It
// returns the error that ended a non-interactive session (a script stops at its
// first error, as sqlite3 does); an interactive session prints and carries on.
func (r *repl) run(ctx context.Context) error {
	// The reader goroutine outlives nothing: cancelling on the way out releases it
	// from a send no one will receive.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lines := make(chan string)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(r.in)
		sc.Buffer(make([]byte, 0, 64*1024), maxStatementLine)
		for sc.Scan() {
			select {
			case lines <- sc.Text():
			case <-ctx.Done():
				return
			}
		}
	}()

	// A piped script is one read of one version: pull once, up front, so the
	// suppression rule cannot swallow the only refresh when the script opens with
	// BEGIN.
	if !r.interactive {
		r.refresh(ctx)
	}

	var pending string
	for {
		r.prompt(pending != "")
		var (
			line string
			ok   bool
		)
		select {
		case line, ok = <-lines:
			if !ok {
				return nil // EOF
			}
		case <-r.idleTimer():
			r.yieldIfIdle(ctx, pending != "")
			continue
		case <-r.interrupt:
			r.println("")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}

		// A dot-command is a whole line, only where a statement would start.
		if pending == "" && strings.HasPrefix(strings.TrimSpace(line), ".") {
			switch err := r.dot(ctx, strings.TrimSpace(line)); {
			case errors.Is(err, errQuit):
				return nil
			case err != nil:
				if stop := r.report(err); stop != nil {
					return stop
				}
			}
			continue
		}

		pending += line + "\n"
		for {
			stmt, rest, complete := splitStatement(pending)
			if !complete {
				break
			}
			pending = rest
			if strings.TrimSpace(strings.TrimSuffix(stmt, ";")) == "" {
				continue
			}
			if err := r.exec(ctx, stmt); err != nil {
				if stop := r.report(err); stop != nil {
					return stop
				}
			}
		}
		// Whitespace left after the last semicolon is not a pending statement: the
		// next line still starts one, and may be a dot-command.
		if strings.TrimSpace(pending) == "" {
			pending = ""
		}
	}
}

// report prints a statement error and decides whether it ends the session: an
// interactive user retypes, a script stops where sqlite3 would. The error is
// already on stderr here, so what it returns is only a non-zero exit.
func (r *repl) report(err error) error {
	fmt.Fprintf(r.errw, "Error: %v\n", err)
	if r.interactive {
		return nil
	}
	return errStatementFailed
}

func (r *repl) prompt(continuation bool) {
	if !r.interactive {
		return
	}
	if continuation {
		fmt.Fprint(r.out, "   ...> ")
		return
	}
	fmt.Fprint(r.out, "s3lite> ")
}

func (r *repl) println(s string) {
	if r.interactive {
		fmt.Fprintln(r.out, s)
	}
}

// exec runs one statement: take the pen if this session wants it, apply the
// freshness contract, then execute and render.
func (r *repl) exec(ctx context.Context, stmt string) error {
	r.beforeStatement(ctx)

	sctx, done := r.statementContext(ctx)
	defer done()

	// Everything goes through Query: a statement with no result set comes back
	// with zero columns and renders nothing, which keeps INSERT … RETURNING
	// printing its rows without the shell having to classify statements.
	rows, err := r.db.QueryContext(sctx, stmt)
	if err != nil {
		return r.sqlError(err)
	}
	defer rows.Close()
	if err := renderRows(r.out, rows, r.mode, r.headers); err != nil {
		return r.sqlError(err)
	}
	if err := rows.Err(); err != nil {
		return r.sqlError(err)
	}
	r.applyTx(stmt)
	r.yielded = false
	return nil
}

// beforeStatement is the pen-then-freshness order every database-touching input
// takes. Promotion first: it restores the replica's latest state itself, after
// which Refresh is the writer's no-op.
func (r *repl) beforeStatement(ctx context.Context) {
	if r.promote && !r.db.IsLeader() {
		switch ok, err := r.db.TryPromote(ctx); {
		case err != nil:
			fmt.Fprintf(r.errw, "s3lite: promotion failed, staying read-only: %v\n", err)
			r.warnedNoPen = true
		case !ok:
			if !r.warnedNoPen {
				fmt.Fprintln(r.errw, "s3lite: the write lease is held by another instance; this session stays read-only until it is released")
				r.warnedNoPen = true
			}
		default:
			r.warnedNoPen = false
		}
	}
	r.refresh(ctx)
}

// refresh applies the freshness contract, suppressed inside an explicit
// transaction. A failed pull is a warning, never fatal: the follower keeps
// serving the state it has.
func (r *repl) refresh(ctx context.Context) {
	if r.txDepth > 0 {
		return
	}
	if !r.interactive && r.refreshed {
		return
	}
	r.refreshed = true
	if _, err := r.db.Refresh(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(r.errw, "s3lite: refresh failed, serving current state: %v\n", err)
	}
}

// idleTimer arms the release-on-idle hand-back, or blocks forever when there is
// nothing to hand back (disabled, or already yielded).
func (r *repl) idleTimer() <-chan time.Time {
	if r.idleYield <= 0 || r.yielded || !r.promote {
		return nil
	}
	return time.After(r.idleYield)
}

// yieldIfIdle hands the lease back at a genuinely idle prompt. Mid-statement or
// mid-transaction it declines and lets the timer re-arm — the session is not
// idle, it is being typed at.
func (r *repl) yieldIfIdle(ctx context.Context, pending bool) {
	if pending || r.txDepth > 0 {
		return
	}
	err := r.db.YieldLease(ctx)
	r.yielded = true // either way, do not re-arm until the next statement
	if err == nil {
		fmt.Fprintln(r.errw, "s3lite: idle — released the write lease; the next statement takes it back")
	}
}

// statementContext scopes one statement so a Ctrl-C can cancel it. The returned
// func both unregisters and cancels.
func (r *repl) statementContext(ctx context.Context) (context.Context, func()) {
	sctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()
	return sctx, func() {
		r.mu.Lock()
		r.cancel = nil
		r.mu.Unlock()
		cancel()
	}
}

// interruptStatement cancels an in-flight statement and reports whether there
// was one. The signal watcher tries this first: a Ctrl-C at a busy prompt kills
// the statement, one at an idle prompt (or a second one) asks the loop to exit
// through a clean Close.
func (r *repl) interruptStatement() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel == nil {
		return false
	}
	r.cancel()
	r.cancel = nil
	return true
}

// requestExit is the signal watcher's fallback when no statement is running.
func (r *repl) requestExit() {
	select {
	case r.interrupt <- struct{}{}:
	default:
	}
}

// sqlError adds the one hint a shell user needs and the error itself cannot
// carry: which flag turns "readonly database" into a writable session.
func (r *repl) sqlError(err error) error {
	if !isReadOnlyErr(err) {
		return err
	}
	if r.promote {
		return fmt.Errorf("%w — the write lease is held by another instance; retry once it is released", err)
	}
	return fmt.Errorf("%w — this session is a read-only follower; open it with --role=writer to write", err)
}

func isReadOnlyErr(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "readonly") || strings.Contains(msg, "read-only")
}

// applyTx tracks explicit-transaction depth from the statement's leading
// keywords. It runs only after a statement succeeded, so a rejected COMMIT
// leaves the shell knowing it is still inside the transaction.
func (r *repl) applyTx(stmt string) {
	words := leadingWords(stmt, 2)
	if len(words) == 0 {
		return
	}
	switch strings.ToUpper(words[0]) {
	case "BEGIN":
		r.txDepth = 1
	case "COMMIT", "END":
		r.txDepth = 0
	case "ROLLBACK":
		// ROLLBACK TO <savepoint> unwinds inside the transaction; it does not end it.
		if len(words) < 2 || !strings.EqualFold(words[1], "TO") {
			r.txDepth = 0
		}
	}
}

// leadingWords returns up to n leading words of a statement with whitespace and
// leading comments stripped.
func leadingWords(stmt string, n int) []string {
	s := stmt
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return nil
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s[2:], "*/"); i >= 0 {
				s = s[2+i+2:]
				continue
			}
			return nil
		}
		break
	}
	// Trim the terminator so "BEGIN;" is the keyword BEGIN.
	f := strings.Fields(strings.TrimSuffix(strings.TrimRight(s, " \t\r\n"), ";"))
	if len(f) > n {
		f = f[:n]
	}
	return f
}

// splitStatement returns the first semicolon-terminated statement in s and the
// remainder. Quoted strings, quoted identifiers and comments are skipped so a
// semicolon inside them does not end the statement — enough to type SQL at,
// deliberately not a SQL parser.
func splitStatement(s string) (stmt, rest string, complete bool) {
	for i := 0; i < len(s); {
		switch c := s[i]; c {
		case '\'', '"', '`':
			i = skipQuoted(s, i, c)
		case '[':
			i = skipTo(s, i+1, "]")
		case '-':
			if i+1 < len(s) && s[i+1] == '-' {
				i = skipTo(s, i+2, "\n")
				continue
			}
			i++
		case '/':
			if i+1 < len(s) && s[i+1] == '*' {
				i = skipTo(s, i+2, "*/")
				continue
			}
			i++
		case ';':
			return s[:i+1], s[i+1:], true
		default:
			i++
		}
	}
	return "", s, false
}

// skipQuoted returns the index just past the string or identifier opened at i,
// honouring SQL's doubled-quote escape.
func skipQuoted(s string, i int, q byte) int {
	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			continue
		}
		if j+1 < len(s) && s[j+1] == q {
			j++
			continue
		}
		return j + 1
	}
	return len(s)
}

// skipTo returns the index just past the next occurrence of end at or after i,
// or the end of s when it never closes.
func skipTo(s string, i int, end string) int {
	if j := strings.Index(s[i:], end); j >= 0 {
		return i + j + len(end)
	}
	return len(s)
}
