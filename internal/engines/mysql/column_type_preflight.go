// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// column-type question, so the orchestrator's [ir.ColumnTypeEmitPreflighter]
// gate engages for every MySQL flavor — the registry holds Engine VALUES, so
// the assertion is on the value type.
var _ ir.ColumnTypeEmitPreflighter = Engine{}

// PreflightColumnTypes reports whether every column type in s can be rendered
// on a MySQL-family target, WITHOUT a connection and before any data moves.
//
// It delegates to [mysqlEmitter.emitColumnType] — the same method
// [mysqlEmitter.emitColumnDef] calls on the create path — through an emitter
// built the SAME way [Engine.OpenSchemaWriter] builds it
// ([newMySQLEmitterForFlavor] with this engine's `--mysql-sql-mode` and flavor).
// The flavor selects the cross-family collation remap and the MariaDB geometry
// spelling, so a preflight built from `stdEmitter` would be answering for a
// different target than the one the run is pointed at.
//
// UNGATED, said plainly: swapping this for `stdEmitter` fails NOTHING today,
// which was confirmed by mutation rather than assumed. No flavor currently
// changes whether emitColumnType RETURNS AN ERROR — the two flavor-dependent
// paths change the rendered bytes, not the verdict — so the flavor-matrix arm
// of TestPreflightColumnTypesAgreesWithTheTableEmitter passes either way. It is
// carried anyway because the first flavor-specific REFUSAL (a MariaDB type
// MySQL 8 renders, or the reverse) would otherwise be answered by the wrong
// emitter, silently, and the gate that exists then would be the wrong place to
// discover it.
//
// # Why a MySQL target needs this even though CheckCrossEngineSupportable exists
//
// That check is the one hand-coded door and it covers the PG → MySQL direction
// this engine is the target of, so on that pair it fires first and keeps owning
// the message. What it does NOT cover is every other source: a SQLite or
// flatfile source, or a MySQL → MySQL run over a hand-built or restored IR, can
// carry a type this emitter has no arm for (an `ir.Interval`, a `DOMAIN` with a
// nil base, an uncatalogued extension type) and reach `emitColumnType` at
// CREATE TABLE with tables already created ahead of it.
//
// # The retarget premise, and where it is checked
//
// On the restore lanes the preflight is handed the manifest's SOURCE-shaped
// schema, while [translate.RetargetForEngine] rewrites it for this target
// LATER. That is only safe because the sole emit-lane rule table
// (`retargetPGtoMySQL`) mirrors this emitter's own auto-emit arms — every type
// it rewrites, `emitColumnType` already accepts unrewritten. Left as a comment
// that would be a premise, so it is a test:
// TestPreflightColumnTypes_IsRetargetInvariant drives the real retarget over
// the whole IR type universe and fails if the two verdicts ever differ.
//
// # What it walks, and the one thing it skips
//
// Every column of every table. A nil column, or a column with a nil Type, is
// SKIPPED — a nil type is a corrupt IR rather than an unrepresentable one, and
// the emitter's own handling of it is deliberately left unchanged.
func (e Engine) PreflightColumnTypes(s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: PreflightColumnTypes: schema is nil")
	}
	emitter := newMySQLEmitterForFlavor(e.opts.sqlMode, e.Flavor)
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			if col == nil || col.Type == nil {
				continue
			}
			if _, err := emitter.emitColumnType(col.Type); err != nil {
				return fmt.Errorf("mysql: table %q column %q: %w", table.Name, col.Name, err)
			}
		}
	}
	return nil
}
