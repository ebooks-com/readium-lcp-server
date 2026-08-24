// Copyright 2026 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package main

import (
	"fmt"
	"os"

	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/logging"
)

const (
	configEnvVar      = "READIUM_LCPENCRYPT_CONFIG"
	defaultConfigFile = "config.yaml"
)

// initLogging installs the logger described by the logging section of the same
// configuration file the servers read, so encryption runs share their log
// directory, rotation settings and hashing salt.
//
// verbose raises the level to debug, whatever the file says.
//
// The servers cannot run without a configuration file, so a missing one is
// fatal for them. lcpencrypt runs from any working directory, so an absent
// config.yaml leaves it logging to stderr at info. A file named through
// READIUM_LCPENCRYPT_CONFIG is expected to exist, and its absence is an error.
func initLogging(verbose bool) error {
	path, imposed := os.LookupEnv(configEnvVar)
	if path == "" {
		path, imposed = defaultConfigFile, false
	}

	loggingConfig := config.Logging{Level: levelFor(verbose)}

	if _, err := os.Stat(path); err != nil {
		if imposed {
			return fmt.Errorf("%s names %s: %w", configEnvVar, path, err)
		}
	} else {
		if err := readConfig(path); err != nil {
			return err
		}
		loggingConfig = config.Config.Logging
		if verbose {
			loggingConfig.Level = "debug"
		}
	}

	if err := logging.Init(loggingConfig, "lcpencrypt"); err != nil {
		return err
	}

	// One invocation encrypts one publication, so every record it writes belongs
	// to the same unit of work. The content id is not known until encryption
	// starts, and is reused when a publication is encrypted again.
	logging.WithProcessAttrs("run_id", logging.NewCorrelationID())
	return nil
}

func levelFor(verbose bool) string {
	if verbose {
		return "debug"
	}
	return "info"
}

// readConfig turns the panic config.ReadConfig raises on an unreadable or
// malformed file into an error, so encryption reports it the way it reports
// every other start-up failure.
func readConfig(path string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%v", recovered)
		}
	}()

	config.ReadConfig(path)
	return nil
}
