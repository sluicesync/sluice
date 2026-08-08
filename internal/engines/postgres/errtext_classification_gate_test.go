// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestNoErrorTextClassification_PostgresEngine instantiates the audit-backlog
// C-1 gate over this engine.
//
// Postgres is where the class did the most damage, because pgx hands every
// caller a *pgconn.PgError with an exact SQLSTATE and the text is therefore
// never the only option. Five sites classified on prose anyway; three of them
// SWALLOWED the error they misread (a skipped TRUNCATE, a dropped-slot report,
// an empty-target probe).
//
// Coverage this gate reaches, stated so its name cannot be read as broader than
// the truth: string literals in `strings.Contains(x, "…")` and in slice
// literals, in the non-test files of THIS directory. It does not resolve
// constants, does not follow a literal through a variable, and says nothing
// about any other package — internal/pipeline and cmd/sluice have their own
// instantiations, and an engine added without one is itself a finding.
func TestNoErrorTextClassification_PostgresEngine(t *testing.T) {
	errclassgate.AssertNoErrorTextClassification(t, errclassgate.SQLStateTextConfig{
		Dir: ".",
		// Floor from the 16 calls measured 2026-08-08, minus slack. The gate is
		// worthless if a refactor stops it matching; this fails loudly instead.
		MinContainsCalls: 12,
		Allowed:          map[string]string{},
	})
}
