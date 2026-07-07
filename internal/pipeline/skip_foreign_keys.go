// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// --skip-foreign-keys (opt-in) implementation, shared by the migrate path
// and the sync cold-start path. The intent: transition a source that HAS
// foreign keys onto a target with limited/no FK support (Vitess/PlanetScale
// sharded keyspaces, or any target where the operator wants FKs off) WITHOUT
// first stripping FKs from the source — while preserving the referencing
// columns' index coverage so joins stay fast. On a MySQL target this is
// load-bearing: MySQL auto-creates an FK's backing index ONLY when the FK is
// created, so a naive skip would leave the referencing column unindexed.
//
// The transform runs once, on the finalized schema, before any DDL phase:
//
//  1. For each FK, if no existing target index (the copied index set + the
//     primary key) already covers its referencing column tuple as a LEFT
//     PREFIX, synthesize a deterministic non-unique btree index on that
//     tuple (in FK column order). Never a redundant index.
//  2. Strip every FK so the constraints phase (CreateConstraints) creates
//     none — the same surgical mechanism the multi-database deferral uses
//     (stripForeignKeys), which leaves UNIQUE/CHECK constraints intact.
//
// The synthesized indexes ride the normal, idempotent CreateIndexes phase
// (CREATE INDEX IF NOT EXISTS / detect-then-skip), so a --resume never
// double-creates them. Unset ⇒ this file is never entered and behaviour is
// byte-identical to before.

// synthFKIndexNameMaxLen is the ceiling for a synthesized backing-index
// name: the smaller of PostgreSQL's 63-byte NAMEDATALEN-1 limit and MySQL's
// 64-char limit, so the name is safe on either target without the PG
// writer's >63-byte loud refusal (ddl_emit.go validatePGIndexName) firing
// on sluice's OWN generated name. Engine-neutral by construction — the
// pipeline layer owns the name; the writer only validates it.
const synthFKIndexNameMaxLen = 63

// skippedForeignKey records one FK whose target-side creation was skipped
// under --skip-foreign-keys, plus how its referencing columns were kept
// indexed on the target (a synthesized backing index, or an existing index
// that already covered them).
type skippedForeignKey struct {
	Table           string
	Name            string   // FK constraint name; may be empty if unnamed at source
	Columns         []string // referencing (child) columns, in FK order
	ReferencedTable string
	IndexName       string // synthesized backing index name; empty when CoveredExisting
	CoveredExisting bool   // an existing index / PK already covered the columns
}

// skipForeignKeysReport is the per-schema result of applySkipForeignKeys,
// surfaced loudly at run start (a schema-altering opt-in must not be silent)
// and asserted on by the unit tests.
type skipForeignKeysReport struct {
	Skipped []skippedForeignKey
}

// applySkipForeignKeys implements --skip-foreign-keys on a finalized schema:
// it ensures each FK's referencing column tuple is indexed on the target
// (synthesizing a backing index only when no existing index already covers
// the tuple as a left-prefix), then strips every FK so the constraints phase
// creates none. It mutates schema in place and returns a report for
// end-of-run visibility. A nil / FK-less schema is a no-op.
func applySkipForeignKeys(schema *ir.Schema) skipForeignKeysReport {
	var rep skipForeignKeysReport
	if schema == nil {
		return rep
	}
	for _, t := range schema.Tables {
		if t == nil || len(t.ForeignKeys) == 0 {
			continue
		}
		for _, fk := range t.ForeignKeys {
			if fk == nil {
				continue
			}
			entry := skippedForeignKey{
				Table:           t.Name,
				Name:            fk.Name,
				Columns:         append([]string(nil), fk.Columns...),
				ReferencedTable: fk.ReferencedTable,
			}
			switch {
			case len(fk.Columns) == 0:
				// A column-less FK (malformed source metadata): nothing to
				// index. Record it as skipped; there is no tuple to cover.
				entry.CoveredExisting = true
			case fkColumnsCovered(t, fk.Columns):
				entry.CoveredExisting = true
			default:
				// Synthesize the backing index and append it to the table's
				// index set BEFORE examining the next FK, so a later FK on the
				// same columns sees this one and is not double-indexed.
				idx := newSyntheticFKIndex(t.Name, fk.Columns)
				t.Indexes = append(t.Indexes, idx)
				entry.IndexName = idx.Name
			}
			rep.Skipped = append(rep.Skipped, entry)
		}
		t.ForeignKeys = nil
	}
	return rep
}

// fkColumnsCovered reports whether the table already has an index (or a
// primary key) whose leading key columns match fkCols in order — a left-
// prefix cover. Both the copied secondary indexes and any index synthesized
// earlier in this pass are consulted (they have already been appended to
// t.Indexes), so a redundant index is never created.
func fkColumnsCovered(t *ir.Table, fkCols []string) bool {
	if t == nil {
		return false
	}
	if indexLeftPrefixCovers(t.PrimaryKey, fkCols) {
		return true
	}
	for _, idx := range t.Indexes {
		if indexLeftPrefixCovers(idx, fkCols) {
			return true
		}
	}
	return false
}

// indexLeftPrefixCovers reports whether idx's leading key columns are exactly
// fkCols, in order — i.e. idx can serve lookups on the FK's referencing
// columns.
//
// Correctness carve-outs:
//   - An expression/functional index entry carries Column=="" and never
//     matches a plain FK column.
//   - A PARTIAL index (Predicate != "") indexes only a subset of rows, so it
//     does NOT guarantee the columns are fully indexed for joins — it is not
//     treated as a cover, and a full backing index is synthesized. (A MySQL
//     prefix-LENGTH index still indexes every row's prefix and IS treated as
//     covering; only a WHERE-partial index is excluded.)
func indexLeftPrefixCovers(idx *ir.Index, fkCols []string) bool {
	if idx == nil || len(fkCols) == 0 || len(idx.Columns) < len(fkCols) {
		return false
	}
	if idx.Predicate != "" {
		return false
	}
	for i, col := range fkCols {
		if idx.Columns[i].Column != col {
			return false
		}
	}
	return true
}

// newSyntheticFKIndex builds a plain non-unique btree index over cols (in FK
// order) with a deterministic, target-safe name.
func newSyntheticFKIndex(table string, cols []string) *ir.Index {
	idxCols := make([]ir.IndexColumn, len(cols))
	for i, c := range cols {
		idxCols[i] = ir.IndexColumn{Column: c}
	}
	return &ir.Index{
		Name:    synthesizedFKIndexName(table, cols),
		Columns: idxCols,
		Kind:    ir.IndexKindBTree,
	}
}

// synthesizedFKIndexName derives a stable, target-safe name for a
// synthesized FK backing index.
//
// The readable form is "<table>_fk_<col>[_<col>...]": table-scoped (so it is
// unique within a PG schema, where index names are schema-scoped, not
// table-scoped) and recognized as already-table-scoped by the PG writer's
// pgIndexName (a leading "<table>_" is emitted verbatim, no double-prefix),
// so the name sluice generates is the name the target receives on BOTH
// engines. When that overflows the identifier limit it falls back to a
// deterministic hash of the same base — still fits, still stable across
// resume (CREATE INDEX IF NOT EXISTS / detect-then-skip idempotency depends
// on a stable name), and still unique per (table, columns) because the base
// carries both.
func synthesizedFKIndexName(table string, cols []string) string {
	base := table + "_fk_" + strings.Join(cols, "_")
	if len(base) <= synthFKIndexNameMaxLen {
		return base
	}
	sum := sha256.Sum256([]byte(base))
	return "fk_" + hex.EncodeToString(sum[:])[:24] // 3 + 24 = 27 bytes, safe on both engines
}

// logSkipForeignKeys emits the loud, structured end-of-transform summary for
// a --skip-foreign-keys run: one line per skipped FK (naming the table, the
// referencing columns, and the backing index that keeps them indexed) plus a
// single count line. No-op when nothing was skipped. Mirrors the shape of
// reportDegradedFKs so the two FK-policy surfaces read consistently.
func logSkipForeignKeys(ctx context.Context, rep skipForeignKeysReport) {
	if len(rep.Skipped) == 0 {
		return
	}
	synthesized := 0
	for _, s := range rep.Skipped {
		if s.IndexName != "" {
			synthesized++
			slog.InfoContext(
				ctx, "skip-foreign-keys: FK not created; synthesized backing index for its referencing columns",
				slog.String("table", s.Table),
				slog.String("constraint", s.Name),
				slog.Any("columns", s.Columns),
				slog.String("referenced_table", s.ReferencedTable),
				slog.String("backing_index", s.IndexName),
			)
			continue
		}
		slog.InfoContext(
			ctx, "skip-foreign-keys: FK not created; referencing columns already indexed",
			slog.String("table", s.Table),
			slog.String("constraint", s.Name),
			slog.Any("columns", s.Columns),
			slog.String("referenced_table", s.ReferencedTable),
		)
	}
	slog.InfoContext(
		ctx, "skip-foreign-keys: summary",
		slog.Int("foreign_keys_skipped", len(rep.Skipped)),
		slog.Int("backing_indexes_synthesized", synthesized),
		slog.Int("already_covered", len(rep.Skipped)-synthesized),
	)
}
