package logging

import (
	"log"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/slack-go/slack"
)

const (
	slackQueueSize = 256

	// slack-go builds an http.Client with no Timeout, and http.DefaultTransport
	// bounds only the dial and the TLS handshake. Close waits for post to
	// return, so an unanswered request would hold up shutdown.
	slackPostTimeout = 5 * time.Second
)

// slackSink delivers high-severity messages to Slack from a background
// goroutine. Delivery is a network call, so it must never happen on the path of
// an HTTP handler.
type slackSink struct {
	level     slog.Level
	queue     chan string
	stop      chan struct{}
	post      func(string)
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newSlackSink(level slog.Level, token, channelID string) *slackSink {
	client := slack.New(token, slack.OptionHTTPClient(&http.Client{Timeout: slackPostTimeout}))
	return newSlackSinkWithPoster(level, func(msg string) {
		if _, _, err := client.PostMessage(channelID, slack.MsgOptionText(msg, false)); err != nil {
			log.Printf("slack post failed: %v", err)
		}
	})
}

func newSlackSinkWithPoster(level slog.Level, post func(string)) *slackSink {
	s := &slackSink{
		level: level,
		queue: make(chan string, slackQueueSize),
		stop:  make(chan struct{}),
		post:  post,
	}

	s.wg.Add(1)
	go s.deliver()

	return s
}

func (s *slackSink) deliver() {
	defer s.wg.Done()

	for {
		select {
		case msg := <-s.queue:
			s.post(msg)
		case <-s.stop:
			return
		}
	}
}

// enqueue drops the message when the queue is full rather than waiting, so a
// slow or unreachable Slack cannot slow down request handling.
func (s *slackSink) enqueue(level slog.Level, msg string) {
	if level < s.level {
		return
	}

	select {
	case s.queue <- msg:
	default:
	}
}

// Close abandons whatever is still queued, so shutdown waits on at most one
// in-flight post.
func (s *slackSink) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.wg.Wait()
	})
	return nil
}
