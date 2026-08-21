package logging

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/readium/readium-lcp-server/config"
)

func TestBufferedFileHoldsWritesUntilFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	w := newBufferedFile(config.Logging{MaxSizeMB: 1, MaxBackups: 2, MaxAgeDays: 1}, path)
	defer w.Close()

	if _, err := w.Write([]byte("buffered line\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if body := readFileOrEmpty(t, path); strings.Contains(body, "buffered line") {
		t.Error("line reached disk before Flush")
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if body := readFileOrEmpty(t, path); !strings.Contains(body, "buffered line") {
		t.Error("line missing from disk after Flush")
	}
}

func TestBufferedFileFlushesOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	w := newBufferedFile(config.Logging{MaxSizeMB: 1, MaxBackups: 2, MaxAgeDays: 1}, path)

	if _, err := w.Write([]byte("closing line\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if body := readFileOrEmpty(t, path); !strings.Contains(body, "closing line") {
		t.Error("line missing from disk after Close")
	}
}

func TestBufferedFileRotatesPastMaxSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	w := newBufferedFile(config.Logging{MaxSizeMB: 1, MaxBackups: 3, MaxAgeDays: 1}, path)
	defer w.Close()

	line := strings.Repeat("x", 1024) + "\n"
	for i := 0; i < 2100; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) < 2 {
		t.Errorf("found %d files, want at least 2 after exceeding max_size_mb", len(entries))
	}
}

func TestBufferedFileIsNilWithoutAPath(t *testing.T) {
	if w := newBufferedFile(config.Logging{}, ""); w != nil {
		t.Error("newBufferedFile returned a writer when no path was given")
	}
}

func TestBufferedFileRecoversAfterInlineFlushFailure(t *testing.T) {
	sink := &failingSink{failing: true}
	w := newBufferedSink(sink)
	defer w.Close()

	if _, err := w.Write(bytes.Repeat([]byte("x"), writeBufferSize+1)); err == nil {
		t.Fatal("Write succeeded while the sink was failing")
	}

	sink.setFailing(false)

	if _, err := w.Write([]byte("recovered line\n")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}

	if got := sink.contents(); !strings.Contains(got, "recovered line") {
		t.Errorf("sink holds %q, want the line written after recovery", got)
	}
}

func TestBufferedFileClosesSinkWhenFinalFlushFails(t *testing.T) {
	sink := &failingSink{}
	w := newBufferedSink(sink)

	w.Write([]byte("pending line\n"))
	sink.setFailing(true)

	if err := w.Close(); err == nil {
		t.Error("Close returned nil, want the flush error")
	}
	if !sink.isClosed() {
		t.Error("Close left the sink open after the final flush failed")
	}
}

func readFileOrEmpty(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

type failingSink struct {
	mu      sync.Mutex
	failing bool
	closed  bool
	written []byte
}

func (s *failingSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failing {
		return 0, errors.New("sink unavailable")
	}
	s.written = append(s.written, p...)
	return len(p), nil
}

func (s *failingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *failingSink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *failingSink) setFailing(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = v
}

func (s *failingSink) contents() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.written)
}

func TestBufferedFileRecoversAfterSinkFailure(t *testing.T) {
	sink := &failingSink{failing: true}
	w := newBufferedSink(sink)
	defer w.Close()

	w.Write([]byte("lost line\n"))
	if err := w.Flush(); err == nil {
		t.Fatal("Flush succeeded while the sink was failing")
	}

	sink.setFailing(false)

	if _, err := w.Write([]byte("recovered line\n")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}

	if got := sink.contents(); !strings.Contains(got, "recovered line") {
		t.Errorf("sink holds %q, want the line written after recovery", got)
	}
}

func TestBufferedFileRejectsWritesAfterClose(t *testing.T) {
	sink := &failingSink{}
	w := newBufferedSink(sink)

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := w.Write([]byte("after close\n")); err != errWriterClosed {
		t.Errorf("Write after Close returned %v, want errWriterClosed", err)
	}
	if err := w.Flush(); err != nil {
		t.Errorf("Flush after Close returned %v, want nil", err)
	}
	if got := sink.contents(); strings.Contains(got, "after close") {
		t.Error("a write after Close reached the sink")
	}
}
