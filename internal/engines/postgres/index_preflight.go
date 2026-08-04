// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// index-representability question, so the orchestrator's
// [ir.IndexEmitPreflighter] gate engages. The registry holds an Engine
// VALUE, so the assertion is on the value type.
var _ ir.IndexEmitPreflighter = Engine{}

// PreflightIndexes reports whether every index in s can be represented on a
// Postgres target, WITHOUT a connection and before any data moves (roadmap
// item 118).
//
// Postgres's one unrepresentable index attribute is a MySQL PREFIX LENGTH on
// a uniqueness-enforcing key: PG has no prefix-index feature, so the widened
// key ADMITS rows the source rejects — silently, at exit 0. On a NON-unique
// index the prefix is a size choice and is dropped with a WARN at the emit
// site rather than refused. This method therefore delegates to
// [refuseUnrepresentablePrefix], the refusal half of the same
// [checkIndexPrefixLength] every emit site calls, so the early answer and
// the late one cannot drift.
//
// # Which keys it reaches, and with which uniqueness verdict
//
// The four PG key-emitting sites and what this walk pairs with each:
//
//	inline PRIMARY KEY            table.PrimaryKey, enforcesUniqueness=true
//	inline UNIQUE (Bug 125 COPY)  drawn from table.Indexes, unique by selection
//	CREATE INDEX                  table.Indexes, enforcesUniqueness=idx.Unique
//	ADD CONSTRAINT … UNIQUE       ConstraintBacked entries of table.Indexes,
//	                              always enforcesUniqueness=true
//
// So the walk is table.PrimaryKey plus every table.Indexes entry, and the
// verdict is `idx.Unique || idx.ConstraintBacked` — the OR rather than
// idx.Unique alone because a ConstraintBacked index is emitted through
// [emitAddUniqueConstraint], which passes true unconditionally. No index is
// skipped: PG's index-build phase skips the inline COPY unique key and the
// ConstraintBacked entries, but both are emitted elsewhere and checked there
// too, so checking them here refuses nothing the run would have accepted.
func (Engine) PreflightIndexes(s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: PreflightIndexes: schema is nil")
	}
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		if pk := table.PrimaryKey; pk != nil {
			where := fmt.Sprintf("postgres: primary key on %s", table.Name)
			if err := refuseUnrepresentablePrefix(pk.Columns, where, true); err != nil {
				return err
			}
		}
		for _, idx := range table.Indexes {
			if idx == nil {
				continue
			}
			where := fmt.Sprintf("postgres: index %q on %s", idx.Name, table.Name)
			if err := refuseUnrepresentablePrefix(
				idx.Columns, where, idx.Unique || idx.ConstraintBacked,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
