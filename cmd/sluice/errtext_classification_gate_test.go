// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestNoErrorTextClassification_CLI instantiates the audit-backlog C-1 gate
// over the CLI, which held the third instance of the slot-not-found text match.
//
// Coverage: string literals in `strings.Contains(x, "…")` and prose tokens in
// slice literals, in the non-test files of THIS directory only.
func TestNoErrorTextClassification_CLI(t *testing.T) {
	errclassgate.AssertNoErrorTextClassification(t, errclassgate.SQLStateTextConfig{
		Dir: ".",
		// Floor from the 5 calls measured 2026-08-08. Low, because the CLI
		// mostly formats rather than classifies; kept non-zero so a refactor
		// that empties this package still trips the anti-vacuity check.
		MinContainsCalls: 3,
		Allowed:          map[string]string{},
	})
}
