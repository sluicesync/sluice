// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
)

// LargeObjectCensus reports whether the source database holds
// pg_largeobject large objects, and which BASE-TABLE columns in the
// reader's active namespace are typed `oid` or `lo` (the lo extension's
// domain over oid) — the column types that conventionally REFERENCE
// large objects.
//
// Used by the pipeline's advisory `warnLargeObjects` preflight (roadmap
// item 68c): large-object CONTENTS live in pg_largeobject, outside any
// user table, and sluice does not copy them — a skip that was
// previously silent. The census powers a WARN, never a refusal of its
// own: only the operator knows whether the referenced blobs matter.
// Note the suspect columns themselves are unsupported column types the
// schema read refuses loudly, which is why the pipeline runs this
// census BEFORE ReadSchema (Bug 205) — the WARN is that refusal's
// large-object context.
//
// loCount is the number of large objects on the source
// (pg_largeobject_metadata rows — one per lo, world-readable unlike
// the pg_largeobject data pages). suspects maps table name → the
// oid/lo-typed column names, in attnum order; tables outside the
// reader's namespace never appear, and scoping to the migration's
// filtered table set is the pipeline's job.
func (r *SchemaReader) LargeObjectCensus(ctx context.Context) (loCount int64, suspects map[string][]string, err error) {
	if err := r.db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_largeobject_metadata`).Scan(&loCount); err != nil {
		return 0, nil, err
	}

	const q = `
		SELECT c.relname, a.attname
		FROM   pg_attribute a
		JOIN   pg_class     c ON c.oid = a.attrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		JOIN   pg_type      t ON t.oid = a.atttypid
		WHERE  n.nspname = $1
		  AND  c.relkind = 'r'
		  AND  a.attnum > 0
		  AND  NOT a.attisdropped
		  AND  t.typname IN ('oid', 'lo')
		ORDER  BY c.relname, a.attnum`
	rows, err := r.catalogQuery(ctx, q, r.schema)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = rows.Close() }()
	suspects = map[string][]string{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return 0, nil, err
		}
		suspects[table] = append(suspects[table], column)
	}
	return loCount, suspects, rows.Err()
}
