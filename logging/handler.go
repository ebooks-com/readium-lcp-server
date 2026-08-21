package logging

import (
	"context"
	"crypto/rand"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/readium/readium-lcp-server/config"
)

// forkHandler attaches the request correlation id, flushes the file at WARN and
// above so an abrupt stop can only lose buffered records below that level, and
// forwards to the Slack sink.
type forkHandler struct {
	inner slog.Handler
	file  *bufferedFile
	slack *slackSink
}

func (h *forkHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *forkHandler) Handle(ctx context.Context, r slog.Record) error {
	if id, found := RequestIDFromContext(ctx); found {
		r.AddAttrs(slog.String("req_id", id))
	}

	err := h.inner.Handle(ctx, r)

	if h.file != nil && r.Level >= slog.LevelWarn {
		h.file.Flush()
	}
	if h.slack != nil {
		h.slack.enqueue(r.Level, r.Message)
	}

	return err
}

func (h *forkHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &forkHandler{inner: h.inner.WithAttrs(attrs), file: h.file, slack: h.slack}
}

func (h *forkHandler) WithGroup(name string) slog.Handler {
	return &forkHandler{inner: h.inner.WithGroup(name), file: h.file, slack: h.slack}
}

// stdlogTarget carries no Slack sink. slack.go reports a failed post through
// the stdlib logger, and a sink configured down to info would post that report,
// fail again and report again.
var stdlogTarget atomic.Pointer[slog.Logger]

// stdlogWriter routes the stdlib log calls elsewhere in the codebase into the
// same file, since stderr is discarded when the server runs as a Windows
// service.
type stdlogWriter struct{}

func (stdlogWriter) Write(p []byte) (int, error) {
	stdlogTarget.Load().Info(strings.TrimRight(string(p), "\r\n"), "src", "stdlog")
	return len(p), nil
}

// setup builds the logger from configuration and installs it process-wide.
// serverName names the log file, so servers sharing a configuration file each
// rotate their own.
func setup(cfg config.Logging, serverName string) error {
	applyDefaults(&cfg)

	path := logFilePath(cfg.Directory, serverName)
	if path != "" {
		if err := probeLogPath(path); err != nil {
			return err
		}
	}

	file := newBufferedFile(cfg, path)

	var out io.Writer = os.Stderr
	if file != nil {
		out = io.MultiWriter(os.Stderr, file)
	}

	level, levelRecognised := parseLevel(cfg.Level)
	slackLevel, slackLevelRecognised := parseLevel(cfg.SlackLevel)

	inner := slog.NewTextHandler(out, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: newRedactor(resolveSalt(cfg.Salt)),
	})

	var sink *slackSink
	if cfg.SlackToken != "" && cfg.SlackChannelID != "" {
		sink = newSlackSink(slackLevel, cfg.SlackToken, cfg.SlackChannelID)
	}

	logger.Store(slog.New(&forkHandler{inner: inner, file: file, slack: sink}))
	stdlogTarget.Store(slog.New(&forkHandler{inner: inner, file: file}))

	// Each newBufferedFile starts a flush goroutine and holds a file handle, so
	// a second setup would leak both without closing what it replaces.
	if previous := activeFile.Swap(file); previous != nil {
		previous.Close()
	}
	if previous := activeSlack.Swap(sink); previous != nil {
		previous.Close()
	}

	log.SetFlags(0)
	log.SetOutput(stdlogWriter{})

	if !levelRecognised {
		Warn("unrecognised logging level, using info", "configured", cfg.Level)
	}
	if !slackLevelRecognised {
		Warn("unrecognised slack level, using info", "configured", cfg.SlackLevel)
	}
	if cfg.Salt == "" {
		Warn("logging salt is not configured; hashed identifiers will not correlate across restarts or between servers")
	}

	return nil
}

// probeLogPath reports whether the configured file can be created and appended
// to, so a bad path fails at start-up rather than on the first record.
// logFilePath returns the empty string when no directory is configured, which
// leaves the logger writing to stderr alone.
func logFilePath(directory, serverName string) string {
	if directory == "" {
		return ""
	}
	return filepath.Join(directory, serverName+".log")
}

func probeLogPath(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

func applyDefaults(cfg *config.Logging) {
	if cfg.Level == "" {
		cfg.Level = "info"
	}
	if cfg.MaxSizeMB == 0 {
		cfg.MaxSizeMB = 100
	}
	if cfg.MaxBackups == 0 {
		cfg.MaxBackups = 14
	}
	if cfg.MaxAgeDays == 0 {
		cfg.MaxAgeDays = 30
	}
	if cfg.SlackLevel == "" {
		cfg.SlackLevel = "error"
	}
}

func resolveSalt(configured string) []byte {
	if configured != "" {
		return []byte(configured)
	}

	salt := make([]byte, 32)
	// crypto/rand.Read fills salt entirely and never returns an error on go1.24.
	rand.Read(salt)
	return salt
}
