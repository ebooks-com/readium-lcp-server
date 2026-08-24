package logging

import (
	"log"
	"path/filepath"
	"strings"
	"testing"

	"github.com/readium/readium-lcp-server/config"
)

func TestWithProcessAttrsStampsEveryRecord(t *testing.T) {
	dir := t.TempDir()
	if err := Init(config.Logging{Directory: dir, Salt: "s", Level: "info"}, "tool"); err != nil {
		t.Fatalf("init: %v", err)
	}

	WithProcessAttrs("run_id", "abcd1234")
	Info("first")
	Warn("second")
	log.Println("through the stdlib logger")

	if err := Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	body := readFileOrEmpty(t, filepath.Join(dir, "tool.log"))
	if got := strings.Count(body, "run_id=abcd1234"); got != 3 {
		t.Errorf("got %d stamped records, want 3: %s", got, body)
	}
}

func TestNewCorrelationIDVaries(t *testing.T) {
	if NewCorrelationID() == NewCorrelationID() {
		t.Error("two correlation ids are identical")
	}
}
