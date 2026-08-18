package logging

import (
	"log"
	"log/slog"
	"strings"
	"testing"
)

func TestErrorWriterRecordsAtErrorLevel(t *testing.T) {
	buf := captureLogger(t, slog.LevelInfo)

	log.New(ErrorWriter(), "", 0).Println("PANIC: boom")

	got := buf.String()
	if !strings.Contains(got, "level=ERROR") {
		t.Errorf("output %q, want level=ERROR", got)
	}
	if !strings.Contains(got, "PANIC: boom") {
		t.Errorf("output %q, want the panic message", got)
	}
}

func TestErrorWriterTrimsTheTrailingNewline(t *testing.T) {
	buf := captureLogger(t, slog.LevelInfo)

	log.New(ErrorWriter(), "", 0).Println("stack line")

	if got := buf.String(); strings.Contains(got, `\n"`) {
		t.Errorf("output %q, want the trailing newline trimmed from the message", got)
	}
}
