// Copyright 2026 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package encrypt

import (
	b64 "encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/logging"
)

var contentKey = []byte("a content key nobody may read")

func TestNotifyLCPServerKeepsTheContentKeyOutOfTheLogV1(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()
	records := captureLog(t)

	err := NotifyLCPServer(publicationWithKey(), false, "provider", server.URL, false, "user", "secret", true)

	if err != nil {
		t.Fatalf("NotifyLCPServer: %v", err)
	}
	assertKeyAbsent(t, records())
}

func TestNotifyLCPServerKeepsTheContentKeyOutOfTheLogV2(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()
	records := captureLog(t)

	err := NotifyLCPServer(publicationWithKey(), false, "provider", server.URL, true, "user", "secret", true)

	if err != nil {
		t.Fatalf("NotifyLCPServer: %v", err)
	}
	assertKeyAbsent(t, records())
}

func publicationWithKey() Publication {
	return Publication{
		UUID:          "812bbfe8-9a57-4b14-b8f3-4e0fc6e841c0",
		Title:         "Moby Dick",
		FileName:      "moby-dick.epub",
		ContentType:   "application/epub+zip",
		EncryptionKey: contentKey,
		Location:      "https://example.com/moby-dick.epub",
	}
}

// captureLog sends records to a log file and returns a function reading it
// back, since the notification body reaches the sink through the process-wide
// logger rather than a return value.
func captureLog(t *testing.T) func() string {
	t.Helper()

	directory := t.TempDir()
	if err := logging.Init(config.Logging{Directory: directory, Level: "debug", Salt: "salt"}, "test"); err != nil {
		t.Fatalf("logging.Init: %v", err)
	}
	t.Cleanup(func() { logging.Close() })

	return func() string {
		if err := logging.Close(); err != nil {
			t.Fatalf("logging.Close: %v", err)
		}
		written, err := os.ReadFile(filepath.Join(directory, "test.log"))
		if err != nil {
			t.Fatalf("reading the log: %v", err)
		}
		return string(written)
	}
}

func assertKeyAbsent(t *testing.T, records string) {
	t.Helper()

	if records == "" {
		t.Fatal("nothing was logged, so the assertion proves nothing")
	}
	if strings.Contains(records, b64.StdEncoding.EncodeToString(contentKey)) {
		t.Errorf("the content key was logged: %s", records)
	}
	if !strings.Contains(records, "[REDACTED]") {
		t.Errorf("the content key field was not redacted: %s", records)
	}
}
