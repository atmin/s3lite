package s3lite

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// discardLogger is for restores a test drives itself as a verification step: restoreDB
// logs its start and completion, which is noise unless that is what is being asserted.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// captureLogger returns an INFO logger writing into a goroutine-safe buffer — s3lite
// logs lifecycle events from the lease loop as well as the caller's goroutine.
func captureLogger(buf *syncBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func TestLitestreamLoggerGatesInfoNotWarn(t *testing.T) {
	var buf bytes.Buffer
	app := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ls := litestreamLogger(app)

	// litestream's per-interval "replica sync" INFO must be dropped.
	ls.Info("replica sync", "txid", 42)
	if buf.Len() != 0 {
		t.Errorf("litestream INFO leaked to the log: %s", buf.String())
	}

	// A real replication problem (WARN+) must still surface.
	ls.Warn("replica sync error")
	if !strings.Contains(buf.String(), "replica sync error") {
		t.Errorf("litestream WARN was dropped: %q", buf.String())
	}

	// Gating must survive .With() — litestream chains LogKeySystem/LogKeyDB attrs.
	buf.Reset()
	ls.With("db", "meta.sqlite3").Info("replica sync")
	if buf.Len() != 0 {
		t.Errorf("gating lost after .With(): %s", buf.String())
	}

	// The application logger itself is untouched: s3lite's own INFO still logs.
	buf.Reset()
	app.Info("s3lite: promoted to writer")
	if !strings.Contains(buf.String(), "promoted to writer") {
		t.Errorf("application INFO was wrongly gated: %q", buf.String())
	}
}

// seedReplica creates a one-row replica at a fresh file:// URL and returns the URL. The
// clean Close flushes it, so a later restore from it has data to pull.
func seedReplica(t *testing.T, ctx context.Context) string {
	t.Helper()
	replicaURL := "file://" + t.TempDir()
	seed, err := Open(ctx, Config{
		LocalPath:  filepath.Join(t.TempDir(), "seed.sqlite3"),
		BackupTo:   replicaURL,
		Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.ExecContext(ctx, `INSERT INTO items (name) VALUES ('seeded')`); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	return replicaURL
}

func TestInitialOpenRestoreIsLogged(t *testing.T) {
	// The cold restore inside Open is the one lifecycle step that can take minutes, and
	// it runs before the handle is returned — so an application that blocks on Open must
	// be able to tell a restore from a stall. Pins the previously silent path.
	ctx := context.Background()
	replicaURL := seedReplica(t, ctx)

	logBuf := &syncBuffer{}
	db, err := Open(ctx, Config{
		LocalPath:   filepath.Join(t.TempDir(), "cold.sqlite3"), // absent → Open restores
		RestoreFrom: replicaURL,
		Logger:      captureLogger(logBuf),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// It really restored (not the empty-replica no-op), so the pair below describes work.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("cold Open restored %d rows, want 1", count)
	}

	out := logBuf.String()
	for _, want := range []string{
		"s3lite: restoring from replica", // the start — what a "restoring…" state hangs on
		"s3lite: restore complete",
		"bytes=", "elapsed=", // the completion reports the cost of the wait
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("initial Open restore did not log %q; log was:\n%s", want, out)
		}
	}
}

func TestEmptyReplicaRestoreLogsNothingToRestore(t *testing.T) {
	// A first deploy restores from an empty bucket: no file is written, so reporting a
	// completed restore would be a lie. It says so instead.
	ctx := context.Background()
	logBuf := &syncBuffer{}
	db, err := Open(ctx, Config{
		LocalPath:   filepath.Join(t.TempDir(), "fresh.sqlite3"),
		RestoreFrom: "file://" + t.TempDir(), // an empty replica
		Logger:      captureLogger(logBuf),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	out := logBuf.String()
	if !strings.Contains(out, "s3lite: replica is empty; nothing to restore") {
		t.Fatalf("empty-replica restore should say so; log was:\n%s", out)
	}
	if strings.Contains(out, "s3lite: restore complete") {
		t.Fatalf("nothing was restored, so no completion may be claimed; log was:\n%s", out)
	}
}

func TestSelfSuccessionPromoteLogsNoRestore(t *testing.T) {
	// The counterpart to TestTakeoverPromoteLogsRestore: a promotion that resumes in
	// place restores nothing, so it must not announce a restore — the log lines have to
	// track the decision, or a consumer surfacing "restoring…" would show it on every
	// crash restart. Mechanics are pinned by TestPromoteSelfSuccessionKeepsLocalTail.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaURL := "file://" + t.TempDir()
	localPath := filepath.Join(t.TempDir(), "node.sqlite3")

	w1, err := Open(ctx, Config{
		LocalPath: localPath, BackupTo: replicaURL, Role: RoleWriter,
		Owner: "node", LeaseTTL: time.Minute, Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('v1')`); err != nil {
		t.Fatal(err)
	}
	if err := w1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	simulateCrash(t, w1) // lease stays held at generation 1

	// The restart reuses the local file, so its Open restores nothing either; only this
	// instance's log is captured.
	logBuf := &syncBuffer{}
	w2, err := Open(ctx, Config{
		LocalPath: localPath, BackupTo: replicaURL, Role: RoleAuto,
		Owner: "node", LeaseTTL: time.Minute, Logger: captureLogger(logBuf),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	lock.expire()
	if promoted, err := w2.tryPromoteOnce(ctx); err != nil || !promoted {
		t.Fatalf("promote: promoted=%v err=%v", promoted, err)
	}

	out := logBuf.String()
	if !strings.Contains(out, "promoting in place (self-succession)") {
		t.Fatalf("expected the in-place promote decision on the captured logger; log was:\n%s", out)
	}
	if strings.Contains(out, "s3lite: restoring from replica") {
		t.Fatalf("a resume-in-place promotion must log no restore; log was:\n%s", out)
	}
}

func TestTakeoverPromoteLogsRestore(t *testing.T) {
	// A takeover promotion does restore the replica over the local file, and that wait
	// is as long as a cold start — so it logs the same start/complete pair as Open.
	lock := &fakeLock{}
	installFakeLeaser(t, lock)
	ctx := context.Background()
	replicaURL := "file://" + t.TempDir()
	localPath := filepath.Join(t.TempDir(), "node.sqlite3")

	w1, err := Open(ctx, Config{
		LocalPath: localPath, BackupTo: replicaURL, Role: RoleWriter,
		Owner: "node", LeaseTTL: time.Minute, Migrations: []string{itemsSchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.ExecContext(ctx, `INSERT INTO items (name) VALUES ('v1')`); err != nil {
		t.Fatal(err)
	}
	if err := w1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	simulateCrash(t, w1) // lease held at generation 1

	logBuf := &syncBuffer{}
	w2, err := Open(ctx, Config{
		LocalPath: localPath, BackupTo: replicaURL, Role: RoleAuto,
		Owner: "node", LeaseTTL: time.Minute, Logger: captureLogger(logBuf),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	lock.steal("other", time.Minute) // a successor took over (generation 2), then lapsed
	lock.expire()
	if promoted, err := w2.tryPromoteOnce(ctx); err != nil || !promoted {
		t.Fatalf("promote: promoted=%v err=%v", promoted, err)
	}

	out := logBuf.String()
	for _, want := range []string{"s3lite: restoring from replica", "s3lite: restore complete"} {
		if !strings.Contains(out, want) {
			t.Fatalf("takeover promote did not log %q; log was:\n%s", want, out)
		}
	}
}
