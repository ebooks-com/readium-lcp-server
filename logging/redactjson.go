package logging

import (
	"encoding/json"
	"fmt"
)

// RedactJSON returns body as indented JSON with the values of redactedKeys
// blanked and those of hashedKeys replaced by a keyed hash. slog applies
// newRedactor to attributes, so a marshalled request body passed as one
// attribute would otherwise be written whole.
//
// A body that cannot be decoded is reported as [REDACTED], since its fields
// cannot be examined.
func RedactJSON(body []byte) string {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return redactedValue
	}

	indented, err := json.MarshalIndent(redactValue(decoded), "", " ")
	if err != nil {
		return redactedValue
	}
	return string(indented)
}

func redactValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(typed))
		for key, member := range typed {
			normalised := normaliseKey(key)

			if _, found := redactedKeys[normalised]; found {
				redacted[key] = redactedValue
				continue
			}
			if prefix, found := hashedKeys[normalised]; found {
				redacted[key] = prefix + hashValue(*activeSalt.Load(), fmt.Sprint(member))
				continue
			}

			redacted[key] = redactValue(member)
		}
		return redacted

	case []any:
		redacted := make([]any, len(typed))
		for i, member := range typed {
			redacted[i] = redactValue(member)
		}
		return redacted
	}

	return value
}
