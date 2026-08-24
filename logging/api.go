package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

// logger is replaced by setup. It starts as a stderr logger so call sites work
// in tests and in tools that never call Init.
var logger atomic.Pointer[slog.Logger]

// activeFile and activeSlack are what Close shuts down.
var (
	activeFile  atomic.Pointer[bufferedFile]
	activeSlack atomic.Pointer[slackSink]
)

func init() {
	logger.Store(stderrLogger())
}

func stderrLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// parseLevel maps a configured level name onto a slog.Level. The second return
// reports whether name was recognised, so a typo can be surfaced rather than
// running at the fallback.
func parseLevel(name string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug, true
	case "", "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return slog.LevelInfo, false
	}
}

func Debug(msg string, args ...any) { logger.Load().Debug(msg, args...) }
func Info(msg string, args ...any)  { logger.Load().Info(msg, args...) }
func Warn(msg string, args ...any)  { logger.Load().Warn(msg, args...) }
func Error(msg string, args ...any) { logger.Load().Error(msg, args...) }

func DebugContext(ctx context.Context, msg string, args ...any) {
	logger.Load().DebugContext(ctx, msg, args...)
}

func InfoContext(ctx context.Context, msg string, args ...any) {
	logger.Load().InfoContext(ctx, msg, args...)
}

func WarnContext(ctx context.Context, msg string, args ...any) {
	logger.Load().WarnContext(ctx, msg, args...)
}

func ErrorContext(ctx context.Context, msg string, args ...any) {
	logger.Load().ErrorContext(ctx, msg, args...)
}

// WithProcessAttrs adds args to every record this process logs from now on.
// A server distinguishes its concurrent work by the request id the handler
// attaches, while a one-shot tool has one unit of work per process and can
// name it once.
//
// Call it after Init, which replaces the logger, and before the work starts.
func WithProcessAttrs(args ...any) {
	logger.Store(logger.Load().With(args...))
	stdlogTarget.Store(stdlogTarget.Load().With(args...))
}

// Close flushes buffered records and stops the Slack worker. Call it before the
// process exits.
func Close() error {
	// Records emitted during and after shutdown reach stderr, since
	// bufferedFile.Write rejects them once closed.
	logger.Store(stderrLogger())
	stdlogTarget.Store(stderrLogger())

	if s := activeSlack.Swap(nil); s != nil {
		s.Close()
	}
	if f := activeFile.Swap(nil); f != nil {
		return f.Close()
	}
	return nil
}

// errorWriter adapts Error onto io.Writer, for libraries that accept only a
// *log.Logger.
type errorWriter struct{}

func (errorWriter) Write(p []byte) (int, error) {
	logger.Load().Error(strings.TrimRight(string(p), "\r\n"))
	return len(p), nil
}

// ErrorWriter returns a writer that records everything written to it at ERROR.
func ErrorWriter() io.Writer { return errorWriter{} }
