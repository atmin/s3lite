package s3lite

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

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
