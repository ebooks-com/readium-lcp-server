package logging

import (
	"bytes"
	"context"
	stdlog "log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/readium/readium-lcp-server/config"
)

func TestHandlerAttachesRequestID(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(&forkHandler{inner: slog.NewTextHandler(&buf, nil)})

	l.InfoContext(WithRequestID(context.Background(), "abc123"), "batch started")

	if got := buf.String(); !strings.Contains(got, "req_id=abc123") {
		t.Errorf("output %q, want req_id=abc123", got)
	}
}

func TestHandlerOmitsRequestIDWhenAbsent(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(&forkHandler{inner: slog.NewTextHandler(&buf, nil)})

	l.Info("startup")

	if got := buf.String(); strings.Contains(got, "req_id=") {
		t.Errorf("output %q, want no req_id attribute", got)
	}
}

func TestSetupRedactsThroughTheConfiguredSalt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := setup(config.Logging{Directory: path, Salt: "fixed-salt", Level: "info"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	InfoContext(context.Background(), "license generated",
		"license_id", "abc-123", UserID("matthew@ebooks.com"))

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	body := readFileOrEmpty(t, path)
	if strings.Contains(body, "matthew") {
		t.Errorf("log contains the raw user id: %q", body)
	}
	if !strings.Contains(body, "user_id=u:") {
		t.Errorf("log %q, want a hashed user_id", body)
	}
	if !strings.Contains(body, "license_id=abc-123") {
		t.Errorf("log %q, want the license id readable", body)
	}
}

func TestSetupFlushesWarnImmediatelyButBuffersInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := setup(config.Logging{Directory: path, Salt: "s", Level: "info"}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer Close()

	Info("buffered record")
	if body := readFileOrEmpty(t, path); strings.Contains(body, "buffered record") {
		t.Error("INFO reached disk before a flush")
	}

	Warn("urgent record")
	body := readFileOrEmpty(t, path)
	if !strings.Contains(body, "urgent record") {
		t.Error("WARN did not reach disk immediately")
	}
	if !strings.Contains(body, "buffered record") {
		t.Error("the WARN flush did not carry the buffered INFO with it")
	}
}

func TestSetupCapturesStdlibLogCalls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := setup(config.Logging{Directory: path, Salt: "s", Level: "info"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	stdlog.Println("Database driver mssql")

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	body := readFileOrEmpty(t, path)
	if !strings.Contains(body, "Database driver mssql") {
		t.Errorf("log %q, want the stdlib message", body)
	}
	if !strings.Contains(body, "src=stdlog") {
		t.Errorf("log %q, want the stdlog source tag", body)
	}
}

func TestSetupWithoutADirectoryLeavesNoFile(t *testing.T) {
	if err := setup(config.Logging{Salt: "s"}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer Close()

	if activeFile.Load() != nil {
		t.Error("a file writer was created without a configured directory")
	}
}

func TestSetupClosesTheWriterItReplaces(t *testing.T) {
	first := filepath.Join(t.TempDir(), "first.log")
	if err := setup(config.Logging{Directory: first, Salt: "s"}); err != nil {
		t.Fatalf("first setup: %v", err)
	}
	replaced := activeFile.Load()

	second := filepath.Join(t.TempDir(), "second.log")
	if err := setup(config.Logging{Directory: second, Salt: "s"}); err != nil {
		t.Fatalf("second setup: %v", err)
	}
	defer Close()

	if _, err := replaced.Write([]byte("x")); err != errWriterClosed {
		t.Errorf("replaced writer returned %v, want errWriterClosed", err)
	}
}

func TestStdlibLogTargetHasNoSlackSink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := setup(config.Logging{Directory: path, Salt: "s"}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer Close()

	h, ok := stdlogTarget.Load().Handler().(*forkHandler)
	if !ok {
		t.Fatal("the stdlib log target is not a forkHandler")
	}
	if h.slack != nil {
		t.Error("a failed Slack post would be reported back into the Slack sink")
	}
}

func TestSetupRejectsAnUnusableDirectory(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	err := setup(config.Logging{Directory: filepath.Join(blocker, "test.log"), Salt: "s"})
	if err == nil {
		Close()
		t.Fatal("setup accepted a path whose parent is a regular file")
	}
}

func TestSetupWarnsOnAnUnrecognisedLevel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := setup(config.Logging{Directory: path, Salt: "s", Level: "wran"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if body := readFileOrEmpty(t, path); !strings.Contains(body, "unrecognised logging level") {
		t.Errorf("log %q, want a warning about the level", body)
	}
}

func TestSetupWritesToStderrAsWellAsTheFile(t *testing.T) {
	captured := captureStderr(t)

	path := filepath.Join(t.TempDir(), "test.log")
	if err := setup(config.Logging{Directory: path, Salt: "s", Level: "info"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	Warn("both sinks")

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if body := readFileOrEmpty(t, path); !strings.Contains(body, "both sinks") {
		t.Errorf("file holds %q, want the record", body)
	}
	if console := captured(); !strings.Contains(console, "both sinks") {
		t.Errorf("stderr holds %q, want the record", console)
	}
}

func captureStderr(t *testing.T) func() string {
	t.Helper()

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}

	previous := os.Stderr
	os.Stderr = write
	t.Cleanup(func() { os.Stderr = previous })

	return func() string {
		write.Close()
		var buf bytes.Buffer
		buf.ReadFrom(read)
		return buf.String()
	}
}
