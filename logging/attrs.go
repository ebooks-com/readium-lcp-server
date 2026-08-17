package logging

import "log/slog"

// UserID names the attribute whose value the redactor replaces with a keyed
// hash. Use it rather than a literal key, since a misspelled key would write
// the identifier to disk unhashed.
func UserID(v string) slog.Attr { return slog.String("user_id", v) }

// DeviceName names the attribute whose value the redactor replaces with a keyed
// hash. Device names are user-supplied free text and routinely contain real
// names.
func DeviceName(v string) slog.Attr { return slog.String("device_name", v) }
