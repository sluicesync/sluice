// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"errors"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// view-creatability question, so the orchestrator's [ir.ViewEmitPreflighter]
// gate engages. The registry holds an Engine VALUE, so the assertion is on the
// value type — same shape as the index sibling next door.
var _ ir.ViewEmitPreflighter = Engine{}

// PreflightViews reports whether every view in s can actually be CREATED on a
// SQLite target, WITHOUT a connection and before any data moves (roadmap item
// 147).
//
// SQLite is the one engine that needs this. Its view emit is
// `CREATE VIEW IF NOT EXISTS` into a namespace it shares with tables, so a view
// whose name a table already holds is not refused at the view phase — it is
// silently not created. See [validateSQLiteViewNamespace] for the ground truth
// and for the pairs that are LOUD and therefore out of scope.
//
// This is also the only door that reaches the paths which create views without
// ever running CreateTablesWithoutConstraints — restore, chain replay, and a
// `sync` cold start onto a pre-existing target — and it is the only one that
// refuses BEFORE the copy rather than after it (views are the last phase).
func (Engine) PreflightViews(s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: PreflightViews: schema is nil")
	}
	return validateSQLiteViewNamespace(s.Tables, s.Views)
}
