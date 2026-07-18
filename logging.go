package s3lite

import (
	"context"
	"log/slog"
)

// minLevelHandler wraps a slog.Handler and drops records below min. It gates
// litestream's own logger: litestream emits a "replica sync" record at INFO on
// every sync interval (~1s), which is operational noise for an embedding
// application. Holding litestream to WARN+ keeps that chatter out of the log
// while still surfacing real replication problems (sync failures, retries).
// s3lite's own lifecycle events (promote/demote/restore) are logged directly
// through the application logger and are unaffected.
type minLevelHandler struct {
	slog.Handler
	min slog.Level
}

func (h minLevelHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.min && h.Handler.Enabled(ctx, l)
}

func (h minLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return minLevelHandler{Handler: h.Handler.WithAttrs(attrs), min: h.min}
}

func (h minLevelHandler) WithGroup(name string) slog.Handler {
	return minLevelHandler{Handler: h.Handler.WithGroup(name), min: h.min}
}

// litestreamLogger derives the logger handed to litestream from the application
// logger, gated to WARN+ so litestream's per-interval INFO chatter is dropped.
func litestreamLogger(appLogger *slog.Logger) *slog.Logger {
	return slog.New(minLevelHandler{Handler: appLogger.Handler(), min: slog.LevelWarn})
}
