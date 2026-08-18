// Copyright 2023 European Digital Reading Lab. All rights reserved.
// Licensed to the Readium Foundation under one or more contributor license agreements.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package logging

import (
	"github.com/readium/readium-lcp-server/config"
)

// Init installs the process-wide logger described by the logging section of the
// configuration.
func Init(logging config.Logging) error {
	return setup(logging)
}

// Print logs message at INFO.
//
// Deprecated: retained so that code merged from readium/readium-lcp-server
// continues to compile and log. New call sites should use Info or InfoContext,
// which carry a level and structured attributes.
func Print(message string) {
	logger.Load().Info(message)
}
