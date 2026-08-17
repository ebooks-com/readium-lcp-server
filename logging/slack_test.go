package logging

import (
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestSlackSinkIgnoresRecordsBelowItsLevel(t *testing.T) {
	posted := make(chan string, 8)
	s := newSlackSinkWithPoster(slog.LevelError, func(msg string) { posted <- msg })
	defer s.Close()

	s.enqueue(slog.LevelInfo, "routine")
	s.enqueue(slog.LevelWarn, "odd")

	select {
	case msg := <-posted:
		t.Errorf("posted %q, want nothing below ERROR", msg)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSlackSinkPostsRecordsAtItsLevel(t *testing.T) {
	posted := make(chan string, 8)
	s := newSlackSinkWithPoster(slog.LevelError, func(msg string) { posted <- msg })
	defer s.Close()

	s.enqueue(slog.LevelError, "database unreachable")

	select {
	case msg := <-posted:
		if msg != "database unreachable" {
			t.Errorf("posted %q, want %q", msg, "database unreachable")
		}
	case <-time.After(2 * time.Second):
		t.Error("nothing posted within the timeout")
	}
}

func TestSlackSinkDropsRatherThanBlocksWhenFull(t *testing.T) {
	release := make(chan struct{})
	s := newSlackSinkWithPoster(slog.LevelError, func(string) { <-release })
	defer func() { close(release); s.Close() }()

	done := make(chan struct{})
	go func() {
		for i := 0; i < slackQueueSize*4; i++ {
			s.enqueue(slog.LevelError, "flood")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("enqueue blocked once the queue filled, want it to drop")
	}
}

func TestSlackSinkEnqueueAfterCloseDoesNotPanic(t *testing.T) {
	s := newSlackSinkWithPoster(slog.LevelError, func(string) {})

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s.enqueue(slog.LevelError, "after close")
}

func TestSlackSinkCloseAbandonsTheQueue(t *testing.T) {
	const drainedPostDuration = 10 * time.Millisecond

	entered := make(chan struct{})
	release := make(chan struct{})

	var mu sync.Mutex
	posts := 0

	s := newSlackSinkWithPoster(slog.LevelError, func(string) {
		mu.Lock()
		posts++
		nth := posts
		mu.Unlock()

		if nth == 1 {
			close(entered)
			<-release
			return
		}
		time.Sleep(drainedPostDuration)
	})

	s.enqueue(slog.LevelError, "in flight")
	<-entered

	for i := 0; i < slackQueueSize; i++ {
		s.enqueue(slog.LevelError, "queued")
	}

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a post was in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	started := time.Now()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return once the in-flight post finished")
	}

	// Draining the queue would take slackQueueSize * drainedPostDuration.
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("Close took %v once the in-flight post finished", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if posts > slackQueueSize {
		t.Errorf("posted %d messages of %d queued", posts, slackQueueSize+1)
	}
}
