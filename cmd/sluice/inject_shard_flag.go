// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/pipeline"
)

// parseInjectShardColumn parses the operator-supplied
// `--inject-shard-column NAME=VALUE` argument into a
// [pipeline.ShardColumnSpec]. Empty raw input returns a
// zero-value spec (Shape A disengaged — the default). The flag is
// single-valued per stream: each per-shard `sluice migrate` /
// `sluice sync start` carries exactly one (NAME, VALUE) pair.
//
// Refusals are operator-actionable:
//   - missing `=` separator (NAME=VALUE shape required),
//   - empty NAME ("=value"),
//   - empty VALUE ("name=").
//
// VALUE is taken as the string literal verbatim — the IR-pass
// targets a Varchar discriminator column today; future expansion
// to integer/UUID can layer typed parsing on top of this parser
// without changing call sites.
func parseInjectShardColumn(raw string) (pipeline.ShardColumnSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return pipeline.ShardColumnSpec{}, nil
	}
	eq := strings.IndexByte(raw, '=')
	if eq < 0 {
		return pipeline.ShardColumnSpec{}, fmt.Errorf(
			"--inject-shard-column: expected NAME=VALUE, got %q (missing '=')", raw,
		)
	}
	name := strings.TrimSpace(raw[:eq])
	value := strings.TrimSpace(raw[eq+1:])
	if name == "" {
		return pipeline.ShardColumnSpec{}, errors.New(
			"--inject-shard-column: NAME is empty (expected NAME=VALUE)",
		)
	}
	if value == "" {
		return pipeline.ShardColumnSpec{}, errors.New(
			"--inject-shard-column: VALUE is empty (expected NAME=VALUE)",
		)
	}
	return pipeline.ShardColumnSpec{Name: name, Value: value}, nil
}
