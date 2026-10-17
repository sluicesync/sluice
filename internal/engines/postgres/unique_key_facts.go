// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// UniqueKeyFacts implements [ir.UniqueKeyFactReader] for the stream's
// unique-key door (UNIQUE-KEY-STOPPED-WINDOW-DOOR). Three facts the schema
// read does not carry, each of which decides whether a unique index can
// merge rows:
//
//   - whether the index is enforced: `indisvalid AND indisready` — an
//     interrupted CREATE / DROP INDEX CONCURRENTLY leaves one that is not;
//   - NULLS NOT DISTINCT for every unique index (the schema read keeps it
//     for a constraint-owned index only, where it drives a WARN);
//   - a per-part INDEX collation that differs from the column's own
//     (`(k COLLATE "x")`), which the schema read never reads (pg_index
//     indcollation; the writer's checkIndexColumnCollation notes it). An
//     expression part reports none: its collation derives from what it
//     reads, and the expression text is what the door compares.
//
// Scoped to the reader's schema and table scope, unique indexes only.
func (r *SchemaReader) UniqueKeyFacts(ctx context.Context) (map[string]map[string]ir.UniqueKeyFact, error) {
	version, err := serverVersionNum(ctx, r.db)
	if err != nil {
		return nil, fmt.Errorf("postgres: unique-key facts: %w", err)
	}
	nullsNotDistinctExpr, _ := uniqueConstraintAttrExprs(version)
	q := `
		SELECT cl.relname, i.relname,
		       ix.indisvalid AND ix.indisready,
		       ` + nullsNotDistinctExpr + `,
		       COALESCE(CASE WHEN k.attnum <> 0 AND u.coll <> 0 AND u.coll IS DISTINCT FROM a.attcollation
		                     THEN c.collname END, '')
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_class      i  ON i.oid  = ix.indexrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL pg_catalog.unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
		LEFT JOIN LATERAL pg_catalog.unnest(ix.indcollation) WITH ORDINALITY AS u(coll, ord) ON u.ord = k.ord
		LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum
		LEFT JOIN pg_collation c ON c.oid = u.coll
		WHERE  n.nspname = $1
		  AND  cl.relkind = 'r'
		  AND  ix.indisunique
		  AND  k.ord <= ix.indnkeyatts
		ORDER  BY cl.relname, i.relname, k.ord`
	rows, err := r.catalogQuery(ctx, q, r.schema)
	if err != nil {
		return nil, fmt.Errorf("postgres: unique-key facts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]map[string]ir.UniqueKeyFact{}
	for rows.Next() {
		var table, index, coll string
		var enforcing, nnd bool
		if err := rows.Scan(&table, &index, &enforcing, &nnd, &coll); err != nil {
			return nil, fmt.Errorf("postgres: unique-key facts: %w", err)
		}
		if r.tableScope != nil && !r.tableScope(table) {
			continue
		}
		if out[table] == nil {
			out[table] = map[string]ir.UniqueKeyFact{}
		}
		f := out[table][index]
		f.Enforcing, f.NullsNotDistinct = enforcing, nnd
		f.PartCollations = append(f.PartCollations, coll)
		out[table][index] = f
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: unique-key facts: %w", err)
	}
	return out, nil
}
