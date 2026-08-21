package logging

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/readium/readium-lcp-server/config"
)

func TestPrintStillLogsAtInfo(t *testing.T) {
	buf := captureLogger(t, slog.LevelInfo)

	Print("upstream style message")

	got := buf.String()
	if !strings.Contains(got, "level=INFO") {
		t.Errorf("output %q, want level=INFO", got)
	}
	if !strings.Contains(got, "upstream style message") {
		t.Errorf("output %q, want the message", got)
	}
}

func TestInitInstallsTheConfiguredLogger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lcpserver.log")

	if err := Init(config.Logging{Directory: dir, Salt: "s", Level: "info"}, "lcpserver"); err != nil {
		t.Fatalf("init: %v", err)
	}

	Info("after init")

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if body := readFileOrEmpty(t, path); !strings.Contains(body, "after init") {
		t.Errorf("log %q, want the record Init's logger wrote", body)
	}
}

func TestInitRejectsAnUnusableDirectory(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	if err := Init(config.Logging{Directory: blocker}, "lcpserver"); err == nil {
		Close()
		t.Fatal("Init accepted a directory that is a regular file")
	}
}
