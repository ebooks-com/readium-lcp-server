package logging

import (
	"bufio"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/readium/readium-lcp-server/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	writeBufferSize = 64 * 1024
	flushInterval   = time.Second
)

var errWriterClosed = errors.New("logging: writer is closed")

// bufferedFile buffers formatted records and flushes them to sink on a timer,
// on demand, and on Close.
type bufferedFile struct {
	mu       sync.Mutex
	buf      *bufio.Writer
	sink     io.WriteCloser
	stop     chan struct{}
	stopOnce sync.Once
	closed   bool
}

// newBufferedFile returns nil when no directory is configured, in which case
// the caller writes to stderr instead.
func newBufferedFile(cfg config.Logging) *bufferedFile {
	if cfg.Directory == "" {
		return nil
	}

	compress := true
	if cfg.Compress != nil {
		compress = *cfg.Compress
	}

	return newBufferedSink(&lumberjack.Logger{
		Filename:   cfg.Directory,
		MaxSize:    cfg.MaxSizeMB,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAgeDays,
		Compress:   compress,
		LocalTime:  true,
	})
}

func newBufferedSink(sink io.WriteCloser) *bufferedFile {
	f := &bufferedFile{
		buf:  bufio.NewWriterSize(sink, writeBufferSize),
		sink: sink,
		stop: make(chan struct{}),
	}

	go f.flushPeriodically()

	return f
}

func (f *bufferedFile) flushPeriodically() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.Flush()
		case <-f.stop:
			return
		}
	}
}

func (f *bufferedFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, errWriterClosed
	}

	n, err := f.buf.Write(p)
	if err != nil {
		// bufio caches this error and returns it from every later call until Reset.
		f.buf.Reset(f.sink)
	}
	return n, err
}

func (f *bufferedFile) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}

	err := f.buf.Flush()
	if err != nil {
		f.buf.Reset(f.sink)
	}
	return err
}

func (f *bufferedFile) Close() error {
	f.stopOnce.Do(func() { close(f.stop) })

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true

	err := f.buf.Flush()
	if closeErr := f.sink.Close(); err == nil {
		err = closeErr
	}
	return err
}
