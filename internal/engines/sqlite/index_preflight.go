// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

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
// SQLite target, WITHOUT a connection and before any data moves (roadmap
// item 118).
//
// This engine is the sharpest member of the class item 118 was filed for:
// [checkIndexPrefixLength]'s ONLY call site is [emitCreateIndex], and SQLite
// renders no secondary index inline at CREATE TABLE, so before this method
// existed there was no early path at all — every prefix refusal landed after
// the whole table had been copied.
//
// SQLite's one unrepresentable index attribute is a MySQL PREFIX LENGTH on a
// key that enforces uniqueness — a UNIQUE index or the PRIMARY KEY: SQLite has
// no prefix-index feature, so the widened key ADMITS rows the source rejects. A
// partial predicate is NOT a member — SQLite has real partial indexes and emits
// the `WHERE` verbatim. The method delegates to [refuseUnrepresentablePrefix],
// the refusal half of the check both emit sites call, so the early answer and
// the late one cannot drift.
//
// # Which keys it reaches, and with which uniqueness verdict
//
// The two SQLite key-emitting sites and what this walk pairs with each:
//
//	table-level PRIMARY KEY   table.PrimaryKey, always [primaryKeyKey]
//	CREATE INDEX              table.Indexes, kind from idx.Unique
//
// So the walk is table.PrimaryKey plus every table.Indexes entry — the latter
// exactly the set [SchemaWriter.CreateIndexes] walks, with no skip set on this
// engine.
//
// The PRIMARY KEY arm landed later than the rest (roadmap item 120): until the
// emitter refused a prefixed PK, walking it here would have refused a shape the
// run accepted — it accepted it by SILENTLY WIDENING the key, which is why the
// two had to move together and why the emitter moved first.
func (Engine) PreflightIndexes(s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: PreflightIndexes: schema is nil")
	}
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		if pk := table.PrimaryKey; pk != nil {
			where := "sqlite: primary key on " + table.Name
			if err := refuseUnrepresentablePrefix(pk.Columns, where, primaryKeyKey); err != nil {
				return err
			}
		}
		for _, idx := range table.Indexes {
			if idx == nil {
				continue
			}
			where := fmt.Sprintf("sqlite: index %q on table %s", idx.Name, table.Name)
			if err := refuseUnrepresentablePrefix(idx.Columns, where, indexKeyKind(idx.Unique)); err != nil {
				return err
			}
		}
	}
	return nil
}
