// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Skipped-index definition-drift advisory (audit 2026-07-15 MED-D0-8)
//
// The index-build paths (direct [SchemaWriter.buildTableIndexes] and the
// ADR-0148 fallback's [SchemaWriter.routeIndexJobToFallback]) are
// idempotent via detect-then-skip — but the detection is by NAME only,
// so a pre-existing index that merely SHARES the intended index's name
// is silently accepted as "already built". For a UNIQUE index that
// silently changes which duplicate writes the target accepts; for any
// index it changes the query plans the operator thinks they migrated.
//
// This file adds the definition compare behind that skip: when a
// same-name index is found, its catalog definition (key columns in
// order, per-column prefix length and direction, uniqueness) is checked
// against the definition sluice would have built, and a divergence gets
// a loud WARN naming both definitions — deliberately a WARN and not a
// refusal, because a differing definition can be an intentional operator
// customization (a wider covering index, a tuned prefix) that detect-
// then-skip exists to respect. The compare is ADVISORY end to end: a
// probe failure logs at DEBUG and never fails a build the existence
// probe already green-lit.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// indexCatalogColumn is one key part of an index definition, normalized
// for the intended-vs-catalog compare: name lower-cased (the
// information_schema ci-collation semantic foldCatalogPair documents),
// expression key parts matched positionally only (expr=true, name
// empty) — MySQL normalizes expression text, so comparing it would
// false-flag byte-different-but-equal spellings.
type indexCatalogColumn struct {
	name    string
	expr    bool
	subPart int // prefix length; 0 = whole column
	desc    bool
}

// indexCatalogDef is a comparable index definition: uniqueness plus the
// ordered key parts. INCLUDE columns, predicates, and access methods
// beyond the FULLTEXT/SPATIAL normalization don't exist on MySQL, so
// this is the complete definition surface the target catalog can hold.
type indexCatalogDef struct {
	unique  bool
	columns []indexCatalogColumn
}

// intendedIndexCatalogDef derives the catalog definition the writer's
// own DDL would produce for idx — mirroring [emitAddIndexClause]'s
// rules, not the raw IR: FULLTEXT/SPATIAL indexes drop UNIQUE and the
// per-column prefix at emit time (Error 1089), so the intended side
// drops them here too.
func intendedIndexCatalogDef(idx *ir.Index) indexCatalogDef {
	keyed := idx.Kind != ir.IndexKindFullText && idx.Kind != ir.IndexKindSpatial
	def := indexCatalogDef{unique: idx.Unique && keyed}
	for _, c := range idx.Columns {
		col := indexCatalogColumn{
			name: strings.ToLower(c.Column),
			expr: c.Expression != "",
			desc: c.Desc,
		}
		if keyed && !col.expr && c.Length > 0 {
			col.subPart = c.Length
		}
		def.columns = append(def.columns, col)
	}
	return def
}

// indexCatalogDefsEqual reports whether the two definitions match part
// for part.
func indexCatalogDefsEqual(a, b indexCatalogDef) bool {
	if a.unique != b.unique || len(a.columns) != len(b.columns) {
		return false
	}
	for i := range a.columns {
		if a.columns[i] != b.columns[i] {
			return false
		}
	}
	return true
}

// formatIndexCatalogDef renders a definition for the drift WARN —
// compact DDL-ish shape: `UNIQUE (a, b(10) DESC, (<expression>))`.
func formatIndexCatalogDef(d indexCatalogDef) string {
	parts := make([]string, len(d.columns))
	for i, c := range d.columns {
		s := c.name
		if c.expr {
			s = "(<expression>)"
		}
		if c.subPart > 0 {
			s += fmt.Sprintf("(%d)", c.subPart)
		}
		if c.desc {
			s += " DESC"
		}
		parts[i] = s
	}
	prefix := ""
	if d.unique {
		prefix = "UNIQUE "
	}
	return prefix + "(" + strings.Join(parts, ", ") + ")"
}

// probeIndexCatalogDefs reads the catalog definitions of the named
// indexes on one table in ONE information_schema query (keys are
// lower-cased index names). No chunking: MySQL caps a table at 64
// indexes, far under the placeholder budget. FULLTEXT/SPATIAL rows are
// normalized to match the intended side (their SUB_PART/COLLATION
// catalog noise — e.g. the SUB_PART 32 a SPATIAL index reports — is not
// part of any buildable definition).
func probeIndexCatalogDefs(ctx context.Context, db *sql.DB, schema, table string, names []string) (map[string]indexCatalogDef, error) {
	q := "SELECT index_name, non_unique, column_name, sub_part, collation, index_type" +
		" FROM information_schema.statistics" +
		" WHERE table_schema = ? AND table_name = ? AND index_name IN (" + sqlPlaceholders(len(names)) + ")" +
		" ORDER BY index_name, seq_in_index"
	args := make([]any, 0, 2+len(names))
	args = append(args, schema, table)
	for _, n := range names {
		args = append(args, n)
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]indexCatalogDef{}
	for rows.Next() {
		var (
			name, indexType    string
			nonUnique          int64
			colName, collation sql.NullString
			subPart            sql.NullInt64
		)
		if err := rows.Scan(&name, &nonUnique, &colName, &subPart, &collation, &indexType); err != nil {
			return nil, err
		}
		unkeyed := strings.EqualFold(indexType, "FULLTEXT") || strings.EqualFold(indexType, "SPATIAL")
		col := indexCatalogColumn{
			name: strings.ToLower(colName.String),
			expr: !colName.Valid,
			desc: !unkeyed && collation.String == "D",
		}
		if !unkeyed && !col.expr && subPart.Valid {
			col.subPart = int(subPart.Int64)
		}
		key := strings.ToLower(name)
		def := out[key]
		def.unique = nonUnique == 0 && !unkeyed
		def.columns = append(def.columns, col)
		out[key] = def
	}
	return out, rows.Err()
}

// warnOnSkippedIndexDefinitionDrift runs the MED-D0-8 advisory for one
// table's index job: for every index the existence probe found (and the
// build will therefore skip), compare the catalog definition against
// the intended one and WARN on divergence. Never fails the build — any
// probe error downgrades to a DEBUG line and the skip proceeds exactly
// as before this check existed.
func (w *SchemaWriter) warnOnSkippedIndexDefinitionDrift(ctx context.Context, job indexBuildJob, existing map[catalogPair]struct{}) {
	var skipped []*ir.Index
	for _, idx := range job.idxs {
		if _, ok := existing[foldCatalogPair(job.tableName, idx.Name)]; ok {
			skipped = append(skipped, idx)
		}
	}
	if len(skipped) == 0 {
		return
	}
	names := make([]string, len(skipped))
	for i, idx := range skipped {
		names[i] = idx.Name
	}
	defs, err := probeIndexCatalogDefs(ctx, w.db, w.schema, job.tableName, names)
	if err != nil {
		slog.DebugContext(ctx, "mysql: index definition-drift probe failed; skipping the advisory compare",
			slog.String("table", job.tableName), slog.String("err", err.Error()))
		return
	}
	for _, idx := range skipped {
		got, ok := defs[strings.ToLower(idx.Name)]
		if !ok {
			// The existence probe saw it, the definition probe didn't
			// (dropped in between?) — the build decision stays with the
			// existence probe; nothing to compare.
			continue
		}
		want := intendedIndexCatalogDef(idx)
		if indexCatalogDefsEqual(want, got) {
			continue
		}
		msg := "mysql: an index with this name already exists with a DIFFERENT definition — the build skips it, leaving the existing index in place; drop/rename it first if the source definition is the one you want (audit MED-D0-8)"
		if want.unique != got.unique {
			msg = "mysql: an index with this name already exists with DIFFERENT UNIQUENESS — the build skips it, so the EXISTING definition decides which duplicate writes the target accepts or refuses, silently diverging from the source; drop/rename it first if the source definition is the one you want (audit MED-D0-8)"
		}
		slog.WarnContext(ctx, msg,
			slog.String("table", job.tableName),
			slog.String("index", idx.Name),
			slog.String("existing_definition", formatIndexCatalogDef(got)),
			slog.String("intended_definition", formatIndexCatalogDef(want)))
	}
}
