package logging

import (
	"log/slog"
	"strings"
	"testing"
)

func TestRedactorReplacesSecrets(t *testing.T) {
	redact := newRedactor([]byte("salt"))

	for _, key := range []string{
		"passphrase", "hashed_passphrase", "hint", "content_key",
		"user_key", "token", "secret", "password", "dsn", "private_key",
	} {
		got := redact(nil, slog.String(key, "sensitive"))
		if got.Value.String() != redactedValue {
			t.Errorf("key %q = %q, want %q", key, got.Value.String(), redactedValue)
		}
	}
}

func TestRedactorMatchesKeyVariants(t *testing.T) {
	redact := newRedactor([]byte("salt"))

	for _, key := range []string{"contentkey", "ContentKey", "CONTENT_KEY", "Content_Key"} {
		got := redact(nil, slog.String(key, "sensitive"))
		if got.Value.String() != redactedValue {
			t.Errorf("key %q = %q, want %q", key, got.Value.String(), redactedValue)
		}
	}
}

func TestRedactorHashesPersonalData(t *testing.T) {
	redact := newRedactor([]byte("salt"))

	user := redact(nil, slog.String("user_id", "matthew@ebooks.com")).Value.String()
	if !strings.HasPrefix(user, "u:") {
		t.Errorf("user_id = %q, want a u: prefix", user)
	}
	if strings.Contains(user, "matthew") {
		t.Errorf("user_id = %q, must not contain the raw value", user)
	}
	if len(user) != len("u:")+12 {
		t.Errorf("user_id = %q, want 12 hex characters after the prefix", user)
	}

	device := redact(nil, slog.String("device_name", "Matthew's iPad")).Value.String()
	if !strings.HasPrefix(device, "d:") {
		t.Errorf("device_name = %q, want a d: prefix", device)
	}
	if strings.Contains(device, "Matthew") {
		t.Errorf("device_name = %q, must not contain the raw value", device)
	}
}

func TestRedactorHashIsStableForOneSaltAndDiffersAcrossSalts(t *testing.T) {
	a := newRedactor([]byte("salt-a"))
	b := newRedactor([]byte("salt-b"))

	first := a(nil, slog.String("user_id", "u1")).Value.String()
	second := a(nil, slog.String("user_id", "u1")).Value.String()
	if first != second {
		t.Errorf("same salt gave %q then %q, want them equal", first, second)
	}

	other := b(nil, slog.String("user_id", "u1")).Value.String()
	if first == other {
		t.Errorf("different salts both gave %q, want them to differ", first)
	}
}

func TestRedactorLeavesOpaqueIdentifiersReadable(t *testing.T) {
	redact := newRedactor([]byte("salt"))

	for _, key := range []string{"license_id", "content_id", "device_id"} {
		got := redact(nil, slog.String(key, "abc-123")).Value.String()
		if got != "abc-123" {
			t.Errorf("key %q = %q, want it unchanged", key, got)
		}
	}
}

func TestPersonalDataConstructorsUseRedactedKeys(t *testing.T) {
	if got := UserID("x").Key; got != "user_id" {
		t.Errorf("UserID key = %q, want user_id", got)
	}
	if got := DeviceName("x").Key; got != "device_name" {
		t.Errorf("DeviceName key = %q, want device_name", got)
	}
}

func TestRedactorGivesDistinctValuesDistinctHashes(t *testing.T) {
	redact := newRedactor([]byte("salt"))

	first := redact(nil, slog.String("user_id", "u1")).Value.String()
	second := redact(nil, slog.String("user_id", "u2")).Value.String()

	if first == second {
		t.Errorf("both users hashed to %q, want distinct hashes", first)
	}
}

func TestRedactorHandlesNonStringValues(t *testing.T) {
	redact := newRedactor([]byte("salt"))

	secret := redact(nil, slog.Any("content_key", []byte{1, 2, 3})).Value.String()
	if secret != redactedValue {
		t.Errorf("content_key = %q, want %q", secret, redactedValue)
	}

	user := redact(nil, slog.Int("user_id", 42)).Value.String()
	if !strings.HasPrefix(user, "u:") {
		t.Errorf("user_id = %q, want a u: prefix", user)
	}
	if strings.Contains(user, "42") {
		t.Errorf("user_id = %q, must not contain the raw value", user)
	}
}
