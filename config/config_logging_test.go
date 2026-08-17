package config

import (
	"testing"

	"gopkg.in/yaml.v2"
)

func TestLoggingConfigParsesNewKeys(t *testing.T) {
	const doc = `
logging:
    directory: "C:\\lcp\\logs\\lcp.log"
    level: "debug"
    max_size_mb: 50
    max_backups: 7
    max_age_days: 14
    compress: false
    salt: "s3cr3t"
    slack_token: "xoxb-1"
    slack_channel: "C123"
    slack_level: "warn"
`
	var c Configuration
	if err := yaml.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if c.Logging.Level != "debug" {
		t.Errorf("Level = %q, want debug", c.Logging.Level)
	}
	if c.Logging.MaxSizeMB != 50 {
		t.Errorf("MaxSizeMB = %d, want 50", c.Logging.MaxSizeMB)
	}
	if c.Logging.MaxBackups != 7 {
		t.Errorf("MaxBackups = %d, want 7", c.Logging.MaxBackups)
	}
	if c.Logging.MaxAgeDays != 14 {
		t.Errorf("MaxAgeDays = %d, want 14", c.Logging.MaxAgeDays)
	}
	if c.Logging.Compress == nil || *c.Logging.Compress {
		t.Errorf("Compress = %v, want explicit false", c.Logging.Compress)
	}
	if c.Logging.Salt != "s3cr3t" {
		t.Errorf("Salt = %q, want s3cr3t", c.Logging.Salt)
	}
	if c.Logging.SlackLevel != "warn" {
		t.Errorf("SlackLevel = %q, want warn", c.Logging.SlackLevel)
	}
}

func TestLoggingConfigOmittedCompressIsNil(t *testing.T) {
	var c Configuration
	if err := yaml.Unmarshal([]byte("logging:\n    directory: \"x.log\"\n"), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.Logging.Compress != nil {
		t.Errorf("Compress = %v, want nil when omitted", c.Logging.Compress)
	}
}
