package logging

import (
	"strings"
	"testing"
)

func TestRedactJSONBlanksTheV1ContentKey(t *testing.T) {
	body := []byte(`{"content-id":"abc","content-encryption-key":"c2VjcmV0","filename":"book.epub"}`)

	redacted := RedactJSON(body)

	if strings.Contains(redacted, "c2VjcmV0") {
		t.Errorf("content key survived redaction: %s", redacted)
	}
	if !strings.Contains(redacted, "book.epub") {
		t.Errorf("unredacted field was lost: %s", redacted)
	}
}

func TestRedactJSONBlanksTheV2EncryptionKey(t *testing.T) {
	body := []byte(`{"uuid":"abc","encryption_key":"c2VjcmV0","title":"Moby Dick"}`)

	redacted := RedactJSON(body)

	if strings.Contains(redacted, "c2VjcmV0") {
		t.Errorf("encryption key survived redaction: %s", redacted)
	}
	if !strings.Contains(redacted, "Moby Dick") {
		t.Errorf("unredacted field was lost: %s", redacted)
	}
}

func TestRedactJSONHashesUserID(t *testing.T) {
	redacted := RedactJSON([]byte(`{"user_id":"matthew@example.com"}`))

	if strings.Contains(redacted, "matthew@example.com") {
		t.Errorf("user id survived redaction: %s", redacted)
	}
	if !strings.Contains(redacted, `"u:`) {
		t.Errorf("user id was not hashed: %s", redacted)
	}
}

func TestRedactJSONReachesNestedValues(t *testing.T) {
	body := []byte(`{"publications":[{"title":"Moby Dick","encryption_key":"c2VjcmV0"}]}`)

	redacted := RedactJSON(body)

	if strings.Contains(redacted, "c2VjcmV0") {
		t.Errorf("nested encryption key survived redaction: %s", redacted)
	}
}

func TestRedactJSONRejectsUndecodableBody(t *testing.T) {
	redacted := RedactJSON([]byte("not json, possibly a content key"))

	if redacted != redactedValue {
		t.Errorf("got %q, want %q", redacted, redactedValue)
	}
}
