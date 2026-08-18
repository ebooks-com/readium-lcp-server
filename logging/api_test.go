package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func captureLogger(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	previous := logger.Load()
	logger.Store(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { logger.Store(previous) })

	return &buf
}

func TestLevelHelpersRecordTheirLevel(t *testing.T) {
	cases := []struct {
		emit  func()
		level string
	}{
		{func() { Debug("d") }, "DEBUG"},
		{func() { Info("i") }, "INFO"},
		{func() { Warn("w") }, "WARN"},
		{func() { Error("e") }, "ERROR"},
	}

	for _, c := range cases {
		buf := captureLogger(t, slog.LevelDebug)
		c.emit()
		if got := buf.String(); !strings.Contains(got, "level="+c.level) {
			t.Errorf("output %q, want level=%s", got, c.level)
		}
	}
}

func TestContextHelpersCarryAttributes(t *testing.T) {
	buf := captureLogger(t, slog.LevelInfo)

	InfoContext(context.Background(), "license generated", "license_id", "abc-123")

	got := buf.String()
	if !strings.Contains(got, `msg="license generated"`) {
		t.Errorf("output %q, want the message", got)
	}
	if !strings.Contains(got, "license_id=abc-123") {
		t.Errorf("output %q, want the attribute", got)
	}
}

func TestLevelFilteringSuppressesLowerRecords(t *testing.T) {
	buf := captureLogger(t, slog.LevelWarn)

	Info("suppressed")
	Warn("kept")

	got := buf.String()
	if strings.Contains(got, "suppressed") {
		t.Errorf("output %q, want the INFO record filtered out", got)
	}
	if !strings.Contains(got, "kept") {
		t.Errorf("output %q, want the WARN record", got)
	}
}

func TestLoggerIsUsableBeforeInit(t *testing.T) {
	if logger.Load() == nil {
		t.Error("logger is nil before Init; call sites would panic")
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]struct {
		level      slog.Level
		recognised bool
	}{
		"debug":   {slog.LevelDebug, true},
		"DEBUG":   {slog.LevelDebug, true},
		" info ":  {slog.LevelInfo, true},
		"warn":    {slog.LevelWarn, true},
		"warning": {slog.LevelWarn, true},
		"error":   {slog.LevelError, true},
		"":        {slog.LevelInfo, true},
		"junk":    {slog.LevelInfo, false},
		"wran":    {slog.LevelInfo, false},
	}

	for input, want := range cases {
		level, recognised := parseLevel(input)
		if level != want.level || recognised != want.recognised {
			t.Errorf("parseLevel(%q) = %v, %t; want %v, %t", input, level, recognised, want.level, want.recognised)
		}
	}
}

func TestCloseIsSafeWithNothingActive(t *testing.T) {
	previous := logger.Load()
	t.Cleanup(func() { logger.Store(previous) })

	if err := Close(); err != nil {
		t.Errorf("Close returned %v with no active writer or sink", err)
	}
}

func TestCloseReplacesTheLogger(t *testing.T) {
	previous := logger.Load()
	t.Cleanup(func() { logger.Store(previous) })

	installed := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	logger.Store(installed)

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if logger.Load() == installed {
		t.Error("Close left the logger writing to the closed file")
	}
}
