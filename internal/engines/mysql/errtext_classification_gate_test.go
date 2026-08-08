// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestNoErrorTextClassification_MySQLEngine instantiates the audit-backlog C-1
// gate over the MySQL family.
//
// The expectation here is different from Postgres's, and worth stating so a
// future reader does not mistake a quiet gate for a clean engine: this package
// legitimately matches prose in many places. MySQL and vttablet surface real
// conditions that carry NO distinct code — the disk-full family, the read-only
// and reparent shapes — so text is the only signal available and the sweep
// deliberately left it alone. What the gate still catches is the narrow case
// where a structural answer exists and was passed over.
func TestNoErrorTextClassification_MySQLEngine(t *testing.T) {
	errclassgate.AssertNoErrorTextClassification(t, errclassgate.SQLStateTextConfig{
		Dir: ".",
		// Floor from the 63 calls measured 2026-08-08, minus slack.
		MinContainsCalls: 45,
		Allowed:          map[string]string{},
	})
}
