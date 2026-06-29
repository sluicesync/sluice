// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// SQLite-target affinity notice — surfaces, in `sluice schema preview`,
// every column whose IR type sluice maps to a SQLite storage affinity that
// differs from the column's nominal type, so an operator SEES the
// normalization before any DDL/data moves rather than discovering it only
// from a code comment.
//
// The headline is the Bug 162 value-fidelity case: an [ir.Decimal] is
// stored as TEXT affinity (NOT NUMERIC/DECIMAL) so the exact decimal text
// survives — SQLite's NUMERIC affinity would coerce e.g. `19.99` to a lossy
// binary REAL. The remaining notes (JSON/UUID/Enum/Set → TEXT, Char/Varchar
// → TEXT, Integer → INTEGER) are value-preserving downgrades that drop a
// declared length / width / sign SQLite cannot enforce.
//
// This MUST mirror internal/engines/sqlite/ddl_emit.go's emitColumnType —
// the two move together (a test pins the headline Decimal→TEXT mapping on
// both sides). The scan keys on the IR type + the TARGET engine name only
// (any source → SQLite), so the translate package stays free of an
// engine-package import (the IR-first tenet).
//
// Target-only: returns nil for a non-SQLite target or a nil schema.

import (
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// sqliteEngineName is the registry name SQLite self-registers under; the
// scan keys on it (string match, no engine-package import).
const sqliteEngineName = "sqlite"

// SQLiteAffinityNotice names one column whose IR type lands on a SQLite
// storage affinity that differs from the nominal type. SourceType is the
// engine-neutral IR rendering so the operator sees exactly what the column
// was; TargetType is the SQLite affinity keyword sluice emits.
type SQLiteAffinityNotice struct {
	// Table is the table the column lives in.
	Table string
	// Column is the affected column name.
	Column string
	// SourceType is the column's IR type as declared (String() form).
	SourceType string
	// TargetType is the SQLite affinity keyword sluice emits (TEXT / INTEGER).
	TargetType string
	// Note explains the normalization in operator-facing terms.
	Note string
}

// sqliteAffinityNote is the {affinity, explanation} pair for one IR type
// family that normalizes onto a SQLite affinity.
type sqliteAffinityNote struct {
	affinity string
	text     string
}

// sqliteAffinityNoteFor returns the affinity-normalization note for an IR
// type, or ok=false when the type emits a SQLite type that matches its
// nominal shape (no normalization to surface). The covered set mirrors the
// non-identity branches of internal/engines/sqlite/ddl_emit.go's
// emitColumnType.
func sqliteAffinityNoteFor(t ir.Type) (sqliteAffinityNote, bool) {
	switch t.(type) {
	case ir.Decimal:
		// The Bug 162 headline: TEXT affinity preserves the exact decimal.
		return sqliteAffinityNote{
			affinity: "TEXT",
			text: "stored as TEXT affinity to preserve the exact decimal value; " +
				"SQLite NUMERIC affinity would coerce to a lossy 15-digit REAL",
		}, true
	case ir.JSON, ir.UUID, ir.Enum, ir.Set:
		return sqliteAffinityNote{
			affinity: "TEXT",
			text:     "stored as TEXT; value preserved",
		}, true
	case ir.Char, ir.Varchar:
		return sqliteAffinityNote{
			affinity: "TEXT",
			text:     "SQLite does not enforce the declared length",
		}, true
	case ir.Integer:
		return sqliteAffinityNote{
			affinity: "INTEGER",
			text:     "width/sign not preserved; SQLite INTEGER is dynamic",
		}, true
	default:
		return sqliteAffinityNote{}, false
	}
}

// ScanSQLiteAffinityNotices walks schema and returns one
// [SQLiteAffinityNotice] per column whose IR type sluice normalizes to a
// different SQLite storage affinity. Target-only — returns nil for any
// non-SQLite target or a nil schema (the normalization only happens when
// emitting SQLite DDL).
//
// Results are sorted by (table, column) so rendering is stable.
func ScanSQLiteAffinityNotices(schema *ir.Schema, targetEngine string) []SQLiteAffinityNotice {
	if schema == nil || !strings.EqualFold(targetEngine, sqliteEngineName) {
		return nil
	}

	var out []SQLiteAffinityNotice
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil || col.Type == nil {
				continue
			}
			note, ok := sqliteAffinityNoteFor(col.Type)
			if !ok {
				continue
			}
			out = append(out, SQLiteAffinityNotice{
				Table:      tbl.Name,
				Column:     col.Name,
				SourceType: col.Type.String(),
				TargetType: note.affinity,
				Note:       note.text,
			})
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Column < out[j].Column
	})
	return out
}
