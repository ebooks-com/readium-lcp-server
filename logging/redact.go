package logging

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
	"sync/atomic"
)

// activeSalt is the key RedactJSON hashes with, since it runs outside the
// handler that closes over the salt. setup replaces it.
var activeSalt atomic.Pointer[[]byte]

func init() {
	salt := randomSalt()
	activeSalt.Store(&salt)
}

const redactedValue = "[REDACTED]"

// redactedKeys are attribute keys whose values never reach a sink, in the
// normalised form produced by normaliseKey.
var redactedKeys = map[string]struct{}{
	"passphrase":           {},
	"hashedpassphrase":     {},
	"hint":                 {},
	"contentkey":           {},
	"contentencryptionkey": {},
	"encryptionkey":        {},
	"userkey":              {},
	"token":                {},
	"secret":               {},
	"password":             {},
	"dsn":                  {},
	"privatekey":           {},
}

// hashedKeys are attribute keys carrying personal data, mapped to the prefix
// their hashed value is given.
var hashedKeys = map[string]string{
	"userid":     "u:",
	"devicename": "d:",
}

// normaliseKey lowercases a key and strips underscores and hyphens so that
// user_id, userId, UserID and user-id all resolve to one entry.
func normaliseKey(key string) string {
	return strings.ToLower(keySeparators.Replace(key))
}

var keySeparators = strings.NewReplacer("_", "", "-", "")

// hashValue returns a keyed hash of value, truncated to 48 bits. Collisions
// stay negligible at the distinct-user counts a single log file holds, and a
// collision would merge two customers' events.
func hashValue(salt []byte, value string) string {
	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))[:12]
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
			return slog.String(a.Key, prefix+hashValue(salt, a.Value.String()))
		}

		return a
	}
}
