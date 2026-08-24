// Copyright 2026 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/logging"
)

func TestInitLoggingWithoutAConfigFile(t *testing.T) {
	t.Chdir(t.TempDir())

	if err := initLogging(false); err != nil {
		t.Fatalf("initLogging: %v", err)
	}
}

func TestInitLoggingReadsTheLoggingSection(t *testing.T) {
	directory := t.TempDir()
	writeConfig(t, filepath.Join(directory, defaultConfigFile), "level: \"warn\"\n    salt: \"pepper\"\n")
	t.Chdir(directory)

	if err := initLogging(false); err != nil {
		t.Fatalf("initLogging: %v", err)
	}

	if config.Config.Logging.Salt != "pepper" {
		t.Errorf("got salt %q, want pepper", config.Config.Logging.Salt)
	}
}

func TestInitLoggingStampsRecordsWithARunID(t *testing.T) {
	directory := t.TempDir()
	writeConfig(t, filepath.Join(directory, defaultConfigFile), "level: \"info\"\n    directory: "+strconv.Quote(directory)+"\n")
	t.Chdir(directory)

	if err := initLogging(false); err != nil {
		t.Fatalf("initLogging: %v", err)
	}
	logging.Info("first")
	logging.Info("second")
	logging.Close()

	written, err := os.ReadFile(filepath.Join(directory, "lcpencrypt.log"))
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}

	records := strings.Split(strings.TrimSpace(string(written)), "\n")
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2: %s", len(records), written)
	}
	first := runID(t, records[0])
	if second := runID(t, records[1]); second != first {
		t.Errorf("run ids differ within one invocation: %s and %s", first, second)
	}
}

func runID(t *testing.T, record string) string {
	t.Helper()

	for _, field := range strings.Fields(record) {
		if id, found := strings.CutPrefix(field, "run_id="); found {
			return id
		}
	}
	t.Fatalf("no run_id in %s", record)
	return ""
}

func TestInitLoggingRejectsAnImposedMissingFile(t *testing.T) {
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "absent.yaml"))

	err := initLogging(false)

	if err == nil {
		t.Fatal("a missing imposed config file was accepted")
	}
	if !strings.Contains(err.Error(), configEnvVar) {
		t.Errorf("error does not name the variable: %v", err)
	}
}

func TestInitLoggingRejectsAMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.yaml")
	if err := os.WriteFile(path, []byte("logging: [unclosed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(configEnvVar, path)

	if err := initLogging(false); err == nil {
		t.Fatal("a malformed config file was accepted")
	}
}

func writeConfig(t *testing.T, path, loggingSection string) {
	t.Helper()

	if err := os.WriteFile(path, []byte("logging:\n    "+loggingSection), 0600); err != nil {
		t.Fatal(err)
	}
}
