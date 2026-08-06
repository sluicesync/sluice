// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"errors"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// table-name question, so the orchestrator's [ir.TableEmitPreflighter] gate
// engages. The registry holds an Engine VALUE, so the assertion is on the
// value type — same shape as the index and view siblings next door.
var _ ir.TableEmitPreflighter = Engine{}

// PreflightTables reports whether every table in s can actually be CREATED
// under its own name on a SQLite target, WITHOUT a connection and before any
// data moves (roadmap item 148).
//
// SQLite is the one engine that needs this. Its table emit is
// `CREATE TABLE IF NOT EXISTS` with a bare, never-qualified name into a
// case-folded namespace, so a table whose name another table already holds is
// not refused at the create phase — it is silently not created, and its rows
// are then written into the table that won the name. See
// [validateSQLiteTableNamespace] for the ground truth and for the pairs that
// are LOUD and therefore out of scope.
//
// # This door is not redundant with the writer's, and the reason is the copy
//
// The other two members of this class lose an object, so the writer phase that
// emits the object is a sufficient second door. This one loses ROWS, and the
// row write does not go through any schema phase: a run against a target whose
// tables already exist (`--resume`, `sync` cold start onto a pre-existing
// target, restore) never calls CreateTablesWithoutConstraints at all, and
// INSERTs straight into the folded name. This is the only door those paths
// pass through.
func (Engine) PreflightTables(s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: PreflightTables: schema is nil")
	}
	return validateSQLiteTableNamespace(s.Tables)
}
