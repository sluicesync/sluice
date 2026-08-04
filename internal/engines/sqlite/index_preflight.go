// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"errors"

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
// UNIQUE index: SQLite has no prefix-index feature, so the widened key ADMITS
// rows the source rejects. A partial predicate is NOT a member — SQLite has
// real partial indexes and emits the `WHERE` verbatim. The method delegates
// to [refuseUnrepresentablePrefix], the refusal half of the same check
// [emitCreateIndex] calls, so the early answer and the late one cannot drift.
//
// # Which indexes it reaches
//
// Every entry of table.Indexes — exactly the set [SchemaWriter.CreateIndexes]
// walks, with no skip set on this engine.
//
// table.PrimaryKey is deliberately NOT walked, and the reason is a gap rather
// than a proof: SQLite's table-level PRIMARY KEY clause renders through
// [quoteIndexColumnList], which has never consulted the prefix length, so a
// prefixed composite PK is silently widened TODAY at emit time. Walking it
// here would make this preflight refuse a shape the run currently accepts,
// which is a different change from moving a refusal earlier. Filed rather
// than folded in; see roadmap item 118's follow-up note.
func (Engine) PreflightIndexes(s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: PreflightIndexes: schema is nil")
	}
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		for _, idx := range table.Indexes {
			if err := refuseUnrepresentablePrefix(idx, "sqlite: table "+table.Name); err != nil {
				return err
			}
		}
	}
	return nil
}
