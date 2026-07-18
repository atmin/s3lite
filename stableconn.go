package s3lite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"sync/atomic"
)

// The stable connector keeps a single *sql.DB valid for the whole life of an
// s3lite instance, even though a promote rebuilds the local file underneath it.
//
// Without this, a leased DB would have to swap its *sql.DB on every role change,
// and any caller that held the old handle (the obvious thing to do) would find
// it closed. Instead, Open hands back one *sql.DB built on stableConnector:
//
//   - Each physical connection is dialed against the current local file in the
//     current mode (read-write for the writer, read-only for a follower) and is
//     tagged with a generation number.
//   - A role change or file swap bumps the generation. Superseded connections are
//     rejected from the pool (ResetSession/IsValid return ErrBadConn/false), so
//     database/sql discards them and dials fresh ones.
//   - While the local file is being replaced (promote restore), connection
//     creation is gated so no connection ever observes a half-written database.
//
// The caller keeps one *sql.DB forever; the churn is invisible.

// sharedDriver returns the registered modernc SQLite driver. sql.OpenDB needs a
// driver.Connector, and the connector needs the underlying driver.Driver; the
// cleanest way to obtain it without importing driver internals is to open a
// throwaway handle and read its Driver().
var (
	driverOnce sync.Once
	driverRef  driver.Driver
	driverErr  error
)

func sharedDriver() (driver.Driver, error) {
	driverOnce.Do(func() {
		d, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			driverErr = err
			return
		}
		driverRef = d.Driver()
		_ = d.Close()
	})
	return driverRef, driverErr
}

// buildDSN renders the connection string with per-connection pragmas so they
// apply to every pooled connection. A follower pins query_only so it cannot
// mutate the file; a writer sets WAL journal mode.
func buildDSN(path string, readOnly bool) string {
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	if readOnly {
		return dsn + "&_pragma=query_only(1)"
	}
	return dsn + "&_pragma=journal_mode(WAL)"
}

type stableConnector struct {
	drv  driver.Driver
	path string

	gen atomic.Uint64 // bumped on every mode change / file swap

	// gate serializes connection creation against file swaps: Connect holds it
	// for reading, a swap holds it for writing across the file mutation.
	gate     sync.RWMutex
	readOnly bool // guarded by gate
}

func newStableConnector(drv driver.Driver, path string, readOnly bool) *stableConnector {
	c := &stableConnector{drv: drv, path: path, readOnly: readOnly}
	c.gen.Store(1)
	return c
}

func (c *stableConnector) Driver() driver.Driver { return c.drv }

// swapFiles blocks new connections, runs fn (which rebuilds the local file),
// flips the mode, and bumps the generation so pre-swap connections are dropped.
// fn runs with the gate held for writing; if fn fails the generation is still
// bumped so any connection opened before the aborted swap is discarded.
func (c *stableConnector) swapFiles(readOnly bool, fn func() error) error {
	c.gate.Lock()
	defer func() {
		c.readOnly = readOnly
		c.gen.Add(1)
		c.gate.Unlock()
	}()
	if fn != nil {
		return fn()
	}
	return nil
}

// setMode flips read-only/read-write without touching the file (used on demote,
// which keeps the file but must stop accepting writes immediately). Bumping the
// generation forces existing connections to be re-dialed in the new mode.
func (c *stableConnector) setMode(readOnly bool) {
	c.gate.Lock()
	c.readOnly = readOnly
	c.gen.Add(1)
	c.gate.Unlock()
}

func (c *stableConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if !c.rlock(ctx) {
		return nil, ctx.Err()
	}
	dsn := buildDSN(c.path, c.readOnly)
	gen := c.gen.Load()
	c.gate.RUnlock()

	conn, err := c.drv.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &genConn{Conn: conn, connector: c, gen: gen}, nil
}

// rlock acquires the gate for reading while honouring ctx, so a query with a
// deadline is not stuck behind a long-running swap. It returns false if ctx is
// cancelled first (the eventual lock is released in the background).
func (c *stableConnector) rlock(ctx context.Context) bool {
	locked := make(chan struct{})
	go func() {
		c.gate.RLock()
		close(locked)
	}()
	select {
	case <-locked:
		return true
	case <-ctx.Done():
		go func() {
			<-locked
			c.gate.RUnlock()
		}()
		return false
	}
}

// genConn wraps a driver connection with the generation it was dialed in. When
// the generation moves on (a swap replaced the file or changed the mode), the
// connection is stale and database/sql is told to discard it. Optional driver
// interfaces are forwarded so the wrapper does not degrade modernc's fast paths.
type genConn struct {
	driver.Conn
	connector *stableConnector
	gen       uint64
}

func (g *genConn) stale() bool { return g.connector.gen.Load() != g.gen }

// ResetSession runs before a pooled connection is reused.
func (g *genConn) ResetSession(ctx context.Context) error {
	if g.stale() {
		return driver.ErrBadConn
	}
	if rs, ok := g.Conn.(driver.SessionResetter); ok {
		return rs.ResetSession(ctx)
	}
	return nil
}

// IsValid runs before a connection is returned to the pool.
func (g *genConn) IsValid() bool {
	if g.stale() {
		return false
	}
	if v, ok := g.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (g *genConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if p, ok := g.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return g.Conn.Prepare(query)
}

func (g *genConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := g.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	return g.Conn.Begin()
}

func (g *genConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if q, ok := g.Conn.(driver.QueryerContext); ok {
		return q.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (g *genConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := g.Conn.(driver.ExecerContext); ok {
		return e.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (g *genConn) CheckNamedValue(nv *driver.NamedValue) error {
	if c, ok := g.Conn.(driver.NamedValueChecker); ok {
		return c.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

func (g *genConn) Ping(ctx context.Context) error {
	if p, ok := g.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}
