package logging

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
)

const redactedValue = "[REDACTED]"

// redactedKeys are attribute keys whose values never reach a sink, in the
// normalised form produced by normaliseKey.
var redactedKeys = map[string]struct{}{
	"passphrase":       {},
	"hashedpassphrase": {},
	"hint":             {},
	"contentkey":       {},
	"userkey":          {},
	"token":            {},
	"secret":           {},
	"password":         {},
	"dsn":              {},
	"privatekey":       {},
}

// hashedKeys are attribute keys carrying personal data, mapped to the prefix
// their hashed value is given.
var hashedKeys = map[string]string{
	"userid":     "u:",
	"devicename": "d:",
}

// normaliseKey lowercases a key and strips underscores so that user_id, userId
// and UserID all resolve to one entry.
func normaliseKey(key string) string {
	return strings.ToLower(strings.ReplaceAll(key, "_", ""))
}

// newRedactor returns a slog ReplaceAttr function that blanks secret values and
// replaces personal data with a keyed hash. A keyed HMAC rather than a bare
// digest, because an identifier drawn from a small or guessable space is
// recoverable from an unkeyed hash by brute force.
func newRedactor(salt []byte) func([]string, slog.Attr) slog.Attr {
	return func(_ []string, a slog.Attr) slog.Attr {
		// Known limitation: slog skips ReplaceAttr for group values and recurses
		// into members by their leaf key, so a key nested in a slog.Group is
		// matched against neither redactedKeys nor hashedKeys.
		key := normaliseKey(a.Key)

		if _, found := redactedKeys[key]; found {
			return slog.String(a.Key, redactedValue)
		}

		if prefix, found := hashedKeys[key]; found {
			mac := hmac.New(sha256.New, salt)
			mac.Write([]byte(a.Value.String()))
			// 48 bits keeps collisions negligible at the distinct-user counts a
			// single log file holds; a collision would merge two customers' events.
			return slog.String(a.Key, prefix+hex.EncodeToString(mac.Sum(nil))[:12])
		}

		return a
	}
}
