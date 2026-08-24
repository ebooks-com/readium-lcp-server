package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

const (
	requestIDHeader = "X-Request-Id"

	// The Status Server exposes device register, return and renew to reading
	// apps without authentication, so the inbound header is public input and it
	// reaches the log file once per record.
	maxRequestIDLength = 64
)

type contextKey int

const requestIDKey contextKey = iota

// WithRequestID returns a context carrying id, which the handler attaches to
// every record logged while the request is in flight.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request id stored by WithRequestID.
func RequestIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(requestIDKey).(string)
	return id, ok && id != ""
}

// RequestIDMiddleware matches the negroni.HandlerFunc signature. It reuses an
// inbound X-Request-Id so a correlation id set by a caller survives across
// servers.
func RequestIDMiddleware(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	id := r.Header.Get(requestIDHeader)
	if !usableRequestID(id) {
		id = newRequestID()
	}

	w.Header().Set(requestIDHeader, id)
	next(w, r.WithContext(WithRequestID(r.Context(), id)))
}

func usableRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLength {
		return false
	}

	for _, c := range id {
		alphanumeric := c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		if !alphanumeric && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

// NewCorrelationID returns an identifier for one unit of work, in the form the
// request id middleware generates.
func NewCorrelationID() string { return newRequestID() }

func newRequestID() string {
	var b [4]byte
	// crypto/rand.Read fills b entirely and never returns an error on go1.24.
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
