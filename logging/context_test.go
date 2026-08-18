package logging

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRequestIDRoundTripsThroughContext(t *testing.T) {
	ctx := WithRequestID(context.Background(), "abc123")

	got, found := RequestIDFromContext(ctx)
	if !found {
		t.Fatal("RequestIDFromContext found nothing")
	}
	if got != "abc123" {
		t.Errorf("id = %q, want abc123", got)
	}
}

func TestRequestIDAbsentFromBareContext(t *testing.T) {
	if _, found := RequestIDFromContext(context.Background()); found {
		t.Error("RequestIDFromContext found an id in a bare context")
	}
}

func TestMiddlewareGeneratesAnID(t *testing.T) {
	var seen string
	next := func(w http.ResponseWriter, r *http.Request) {
		seen, _ = RequestIDFromContext(r.Context())
	}

	RequestIDMiddleware(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), next)

	if seen == "" {
		t.Error("middleware did not put an id in the request context")
	}
}

func TestMiddlewareHonoursInboundHeader(t *testing.T) {
	var seen string
	next := func(w http.ResponseWriter, r *http.Request) {
		seen, _ = RequestIDFromContext(r.Context())
	}

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(requestIDHeader, "upstream-id")
	RequestIDMiddleware(httptest.NewRecorder(), req, next)

	if seen != "upstream-id" {
		t.Errorf("id = %q, want upstream-id", seen)
	}
}

func TestMiddlewareReplacesUnusableInboundIDs(t *testing.T) {
	cases := []struct {
		name    string
		inbound string
		kept    bool
	}{
		{"alphanumeric", "abc123", true},
		{"hyphen and underscore", "req-1_2", true},
		{"at the length limit", strings.Repeat("a", maxRequestIDLength), true},
		{"over the length limit", strings.Repeat("a", maxRequestIDLength+1), false},
		{"space", "id with spaces", false},
		{"quote", `id"quoted`, false},
		{"non ascii", "idé", false},
		{"equals", "id=value", false},
		{"empty", "", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var seen string
			next := func(w http.ResponseWriter, r *http.Request) {
				seen, _ = RequestIDFromContext(r.Context())
			}

			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set(requestIDHeader, c.inbound)
			RequestIDMiddleware(httptest.NewRecorder(), req, next)

			if c.kept {
				if seen != c.inbound {
					t.Errorf("id = %q, want the inbound %q", seen, c.inbound)
				}
				return
			}
			if seen == c.inbound {
				t.Errorf("id = %q, want a generated id", seen)
			}
			if len(seen) != 8 {
				t.Errorf("id = %q, want 8 hex characters", seen)
			}
		})
	}
}

func TestMiddlewareEchoesTheIDInTheResponse(t *testing.T) {
	w := httptest.NewRecorder()

	RequestIDMiddleware(w, httptest.NewRequest("GET", "/", nil), func(http.ResponseWriter, *http.Request) {})

	if w.Header().Get(requestIDHeader) == "" {
		t.Error("middleware did not set the response header")
	}
}

func TestConcurrentRequestsGetDistinctIDs(t *testing.T) {
	const count = 50

	var mu sync.Mutex
	ids := make(map[string]bool)
	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next := func(w http.ResponseWriter, r *http.Request) {
				id, _ := RequestIDFromContext(r.Context())
				mu.Lock()
				ids[id] = true
				mu.Unlock()
			}
			RequestIDMiddleware(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), next)
		}()
	}
	wg.Wait()

	if len(ids) != count {
		t.Errorf("got %d distinct ids from %d requests", len(ids), count)
	}
}
