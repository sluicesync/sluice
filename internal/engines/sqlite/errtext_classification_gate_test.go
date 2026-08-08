// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestNoErrorTextClassification_SQLiteEngine instantiates the audit-backlog C-1
// gate over the SQLite/D1 engine.
//
// Added with the others rather than after the first finding here: per the
// errclassgate package doc, an engine without a gate file is itself the finding
// — that scoping gap is how the original setErr class recurred one engine at a
// time. SQLite's driver reports extended result codes rather than SQLSTATEs, so
// the SQLSTATE half of this gate is expected to stay quiet; the prose half is
// the part that applies.
func TestNoErrorTextClassification_SQLiteEngine(t *testing.T) {
	errclassgate.AssertNoErrorTextClassification(t, errclassgate.SQLStateTextConfig{
		Dir: ".",
		// Floor from the 11 calls measured 2026-08-08, minus slack.
		MinContainsCalls: 7,
		Allowed:          map[string]string{},
	})
}
