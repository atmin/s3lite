// Command s3lite is a sqlite3-familiar shell over an s3lite database: it opens a
// replica URL, restores the local copy, and reads statements from a prompt or a
// pipe. Semantics are the library's, unchanged — reads come from the local file, a
// writer streams the WAL to the replica, and the lease decides who writes.
//
//	s3lite --role=writer   s3://bucket/db   # take the lease, stream every second
//	s3lite --role=follower s3://bucket/db   # read-only, a fresh pull per statement
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/atmin/s3lite"
)

const usageHeader = `s3lite — a sqlite3-familiar shell over an S3-replicated SQLite database.

usage: s3lite [flags] <s3://bucket/path | file:///path>

Familiar, not compatible: it implements the dot-commands people actually type
(.tables .schema .mode .headers .dump .import .help .quit) and nothing beyond
them. Scripts that need the real shell should keep using sqlite3 against the
local file.

flags:
`

const usageFooter = `
Credentials fall back to $AWS_ENDPOINT_URL, $AWS_REGION, $AWS_ACCESS_KEY_ID and
$AWS_SECRET_ACCESS_KEY, and then to the AWS SDK's own chain (~/.aws/config, IAM
roles), exactly as the library documents.
`

// options is the parsed command line.
type options struct {
	replica   string
	local     string
	role      s3lite.Role
	mode      string
	headers   bool
	idleYield time.Duration
	key       []byte
	s3        s3lite.S3Config
}

func main() {
	err := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return
	case errors.Is(err, errStatementFailed):
		os.Exit(1) // the shell already printed it
	default:
		// The library prefixes its own errors; do not say it twice.
		fmt.Fprintf(os.Stderr, "s3lite: %s\n", strings.TrimPrefix(err.Error(), "s3lite: "))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, in io.Reader, out, errw io.Writer) error {
	opts, err := parseArgs(args, errw)
	if err != nil {
		return err
	}

	cfg := s3lite.Config{
		LocalPath:   opts.local,
		RestoreFrom: opts.replica,
		BackupTo:    opts.replica,
		Role:        opts.role,
		S3:          opts.s3,
		// A shell is idle far more than it writes: hold the lease only while
		// actively writing (see --idle-yield), and never promote a --role=follower
		// behind the user's back.
		OnDemandPromotion: true,
		EncryptionKey:     opts.key,
		Logger:            slog.New(slog.NewTextHandler(errw, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
	// The bar redraws one line, so it belongs on a terminal only; a redirected
	// stderr gets the restore's start/complete log lines instead.
	var bar *progressBar
	if isTerminal(errw) {
		bar = newProgressBar(errw)
		cfg.OnRestoreProgress = bar.tick
	}

	db, err := s3lite.Open(ctx, cfg)
	if bar != nil {
		bar.done()
	}
	if err != nil {
		return err
	}

	// A shell is one serial user, and a hand-typed BEGIN … COMMIT must land on the
	// connection that opened it — which a pool cannot promise. Pin it to one.
	db.SetMaxOpenConns(1)

	rp := newREPL(db, in, out, errw)
	rp.interactive = isTerminal(in)
	rp.promote = opts.role != s3lite.RoleFollower
	rp.idleYield = opts.idleYield
	rp.mode = opts.mode
	rp.headers = opts.headers

	stop := watchInterrupts(rp)
	defer stop()

	if rp.interactive {
		fmt.Fprintf(out, "s3lite shell — familiar, not sqlite3-compatible. %q for commands, %q to exit.\n", ".help", ".quit")
		fmt.Fprintf(out, "%s as %s, local copy %s\n", opts.replica, roleWord(db.IsLeader()), opts.local)
	}
	runErr := rp.run(ctx)

	// Sync before reading the tip, so the session reports a position that actually
	// reached the replica; Close does the same durable flush, but its teardown
	// takes the store — and the position with it.
	syncCtx, cancelSync := context.WithTimeout(ctx, s3lite.DefaultShutdownSyncTimeout)
	_ = db.Sync(syncCtx)
	cancelSync()
	st := db.ReplicationStatus()
	if err := db.Close(); err != nil { // bounded by Config.ShutdownSyncTimeout
		return fmt.Errorf("close: %w", err)
	}
	if st.Replicating {
		fmt.Fprintf(errw, "s3lite: flushed local TXID %d to %s and released the lease\n", st.LocalTXID, opts.replica)
	}
	return runErr
}

func roleWord(leader bool) string {
	if leader {
		return "writer"
	}
	return "follower"
}

// watchInterrupts wires Ctrl-C: the first one cancels the statement in flight; one
// at an idle prompt — or a second one — asks the loop to exit through a clean,
// durable Close.
func watchInterrupts(rp *repl) func() {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-sigC:
				if !rp.interruptStatement() {
					rp.requestExit()
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		signal.Stop(sigC)
		close(done)
	}
}

func parseArgs(args []string, errw io.Writer) (options, error) {
	fs := flag.NewFlagSet("s3lite", flag.ContinueOnError)
	fs.SetOutput(errw)
	fs.Usage = func() {
		fmt.Fprint(errw, usageHeader)
		fs.PrintDefaults()
		fmt.Fprint(errw, usageFooter)
	}

	role := fs.String("role", "auto", "how to coordinate: auto (write if the lease is free), writer (require it), follower (never take it)")
	local := fs.String("local", "", "path of the local database copy (default: a stable path under the user cache directory, keyed by the replica URL)")
	mode := fs.String("mode", "list", "output mode: list, table, csv, json, line")
	headers := fs.Bool("headers", false, "print column names in list and csv output")
	idleYield := fs.Duration("idle-yield", 30*time.Second, "release the write lease after this long at an idle prompt; 0 keeps it until exit")
	keyFile := fs.String("key-file", "", "file holding the client-side encryption key: 32 raw bytes or 64 hex characters")
	endpoint := fs.String("endpoint", "", "S3 endpoint URL for MinIO, R2, Scaleway (default $AWS_ENDPOINT_URL)")
	region := fs.String("region", "", "S3 region (default $AWS_REGION)")
	accessKey := fs.String("access-key-id", "", "S3 access key id (default $AWS_ACCESS_KEY_ID)")
	secretKey := fs.String("secret-access-key", "", "S3 secret access key (default $AWS_SECRET_ACCESS_KEY)")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return options{}, errors.New("exactly one replica URL is required")
	}
	u, err := replicaURL(fs.Arg(0))
	if err != nil {
		return options{}, err
	}

	opts := options{
		replica:   fs.Arg(0),
		local:     *local,
		mode:      *mode,
		headers:   *headers,
		idleYield: *idleYield,
		s3: s3lite.S3Config{
			Region:          orEnv(*region, "AWS_REGION"),
			Endpoint:        orEnv(*endpoint, "AWS_ENDPOINT_URL"),
			AccessKeyID:     orEnv(*accessKey, "AWS_ACCESS_KEY_ID"),
			SecretAccessKey: orEnv(*secretKey, "AWS_SECRET_ACCESS_KEY"),
		},
	}

	switch *role {
	case "auto":
		opts.role = s3lite.RoleAuto
	case "writer":
		opts.role = s3lite.RoleWriter
	case "follower":
		opts.role = s3lite.RoleFollower
	default:
		return options{}, fmt.Errorf("unknown --role %q (valid: auto, writer, follower)", *role)
	}
	if !validMode(opts.mode) {
		return options{}, fmt.Errorf("unknown --mode %q (valid: %s)", opts.mode, strings.Join(renderModes, ", "))
	}
	if *keyFile != "" {
		key, err := readKeyFile(*keyFile)
		if err != nil {
			return options{}, err
		}
		opts.key = key
	}
	if opts.local == "" {
		local, err := defaultLocalPath(u)
		if err != nil {
			return options{}, err
		}
		opts.local = local
	}
	return opts, nil
}

// replicaURL checks the one positional argument. A bare path is rejected rather
// than opened: an s3lite database without a replica is just a SQLite file, and
// sqlite3 already opens those.
func replicaURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid replica URL %q: %w", raw, err)
	}
	if u.Scheme != "s3" && u.Scheme != "file" {
		return nil, fmt.Errorf("the replica must be an s3:// or file:// URL, got %q", raw)
	}
	return u, nil
}

func orEnv(value, env string) string {
	if value != "" {
		return value
	}
	return os.Getenv(env)
}

// defaultLocalPath is where the local copy of a replica lives when --local is not
// given: a stable path under the user cache directory, keyed by the replica URL.
// Stability is the point — the same URL reopens the same file, which is what lets
// a clean restart resume in place instead of downloading the database again.
func defaultLocalPath(u *url.URL) (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	// Clean through an absolute path so a ".." in the URL cannot climb out of the
	// cache directory.
	rel := strings.TrimPrefix(path.Clean("/"+strings.Trim(u.Path, "/")), "/")
	if rel == "" {
		rel = "db"
	}
	return filepath.Join(cache, "s3lite", u.Host, filepath.FromSlash(rel)+".sqlite3"), nil
}

// readKeyFile loads Config.EncryptionKey from a file rather than the command line,
// where it would sit in the shell history and every process listing.
func readKeyFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if trimmed := strings.TrimSpace(string(raw)); len(trimmed) == 64 {
		if key, err := hex.DecodeString(trimmed); err == nil {
			return key, nil
		}
	}
	if len(raw) == 32 {
		return raw, nil
	}
	return nil, fmt.Errorf("%s: expected 32 raw bytes or 64 hex characters, got %d bytes", path, len(raw))
}

// isTerminal reports whether w is a character device — how the shell decides
// between the interactive and the piped contract, and whether to draw a bar.
func isTerminal(w any) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
