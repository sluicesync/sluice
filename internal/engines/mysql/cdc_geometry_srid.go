// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"

	"sluicesync.dev/sluice/internal/ir"
)

// resolveGeometryColumnSRIDs fills in the declared SRID for every geometry
// column in tbl, from whichever spatial catalog this server exposes.
//
// # Why this exists (Bug 236, a regression v0.118.0 shipped)
//
// [loadTableSchema] is the CDC lane's schema source — the binlog reader reaches
// it through its schemaLoader, and the change applier calls it directly for the
// TARGET shape. Its query never selected `srs_id`, so every geometry column it
// produced carried `ir.Geometry.SRID` 0, no matter what the column declared.
// The full [SchemaReader.populateColumns] path DOES read it, which is why
// `migrate` was correct in every direction and the asymmetry stayed hidden.
//
// v0.118.0 then added [ir.CheckGeometryRowSRID], which refuses a row whose EWKB
// names a different SRID than its column declares. Against a column this loader
// reported as 0, a MySQL source declaring `POINT SRID 4326` had every CDC row
// refused on arrival — a configuration that worked on v0.117.0.
//
// The write half is older and quieter: the applier stamps geometry from the
// same cached schema, so a target column declaring an SRID got SRID 0 written
// at it — Error 3643 against a declared column (loud), and silently accepted
// against an undeclared one.
//
// # Why a second query rather than adding srs_id to the first
//
// The two servers keep this in different places and `loadTableSchema` does not
// know the flavor:
//
//   - MySQL 8.0 — `information_schema.st_geometry_columns.srs_id`.
//   - MariaDB — no `srs_id` column anywhere in `information_schema.columns`
//     (selecting it is Error 1054, the wall item 73 documents); the value lives
//     in `information_schema.geometry_columns.srid`.
//
// Trying both and keeping whichever answers costs one round-trip on tables that
// actually have geometry, and none on tables that do not. It also mirrors the
// Postgres CDC lane's resolver, which had the identical defect for the
// identical reason — pgoutput cannot carry an SRID either.
//
// # What happens when neither catalog answers
//
// The columns are left at [ir.GeometrySRIDUnknown], which DISABLES the per-row
// check for them. Same asymmetry argument as the Postgres side: refusing would
// wedge a running stream over metadata sluice merely could not read, while
// skipping restores pre-v0.118.0 behaviour for that one table. A column the
// catalog DOES describe keeps the check — including when it answers 0, which is
// the genuinely-undeclared case the guard exists for.
func resolveGeometryColumnSRIDs(ctx context.Context, db *sql.DB, tbl *tableSchema) {
	if !tableHasGeometryColumn(tbl) {
		return
	}

	srids, ok := geometrySRIDsFromCatalog(ctx, db, tbl.Schema, tbl.Name)
	if !ok {
		// Neither catalog is readable. Mark every geometry column unknown so
		// the per-row check cannot fire on a value we have nothing to compare
		// against.
		for _, col := range tbl.Columns {
			if g, isGeom := col.Type.(ir.Geometry); isGeom {
				g.SRID = ir.GeometrySRIDUnknown
				col.Type = g
			}
		}
		return
	}

	for _, col := range tbl.Columns {
		g, isGeom := col.Type.(ir.Geometry)
		if !isGeom {
			continue
		}
		srid, found := srids[col.Name]
		if !found {
			// The table has geometry but this column is not registered. Treat
			// it as unknown rather than as "declares 0" — the two are
			// different, and only the second may refuse a row.
			g.SRID = ir.GeometrySRIDUnknown
			col.Type = g
			continue
		}
		g.SRID = srid
		col.Type = g
	}
}

// tableHasGeometryColumn reports whether tbl carries at least one geometry
// column, so a table without any costs no catalog round-trip.
func tableHasGeometryColumn(tbl *tableSchema) bool {
	for _, col := range tbl.Columns {
		if _, ok := col.Type.(ir.Geometry); ok {
			return true
		}
	}
	return false
}

// geometrySRIDsFromCatalog returns column name → declared SRID for schema.table
// from whichever spatial catalog answers, and whether any of them did.
//
// ok=false means neither view could be read (an old server, a role without
// SELECT on it) and the caller must treat every geometry column as unknown. A
// successful read that simply lists no rows returns ok=true with an empty map:
// that is the catalog saying "these columns are not registered", which is a
// different answer from "I could not ask".
func geometrySRIDsFromCatalog(ctx context.Context, db *sql.DB, schema, table string) (map[string]int, bool) {
	queries := []string{
		// MySQL 8.0.
		`SELECT column_name, srs_id FROM information_schema.st_geometry_columns
		 WHERE table_schema = ? AND table_name = ?`,
		// MariaDB.
		`SELECT g_geometry_column, srid FROM information_schema.geometry_columns
		 WHERE g_table_schema = ? AND g_table_name = ?`,
	}
	for _, q := range queries {
		out, err := scanGeometrySRIDs(ctx, db, q, schema, table)
		if err != nil {
			continue // Wrong server for this catalog; try the other spelling.
		}
		return out, true
	}
	return nil, false
}

// scanGeometrySRIDs runs one catalog query and collects its rows.
func scanGeometrySRIDs(ctx context.Context, db *sql.DB, query, schema, table string) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]int{}
	for rows.Next() {
		var (
			col  string
			srid sql.NullInt64
		)
		if err := rows.Scan(&col, &srid); err != nil {
			return nil, err
		}
		// A NULL srs_id is MySQL's spelling of "no SRID declared", which is
		// the same thing as 0 and is exactly the case the guard exists for.
		out[col] = int(srid.Int64)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// containSRIDSentinel returns cols with [ir.GeometrySRIDUnknown] mapped back to
// 0, COPYING any column it has to change and sharing the rest.
//
// The sentinel is a read-side value: the binlog decoder consults it to tell
// "this column declares 0" from "no catalog established what it declares", and
// only the first may refuse a row. Every OTHER consumer treats
// ir.Geometry.SRID as a number to render or compare — [maybeSnapshotSchemaB1]
// projects these columns into a SchemaSnapshot that can reach schema-forward
// DDL on the target, where -1 would emit `SRID -1`, and MySQL's own writer
// would stamp `uint32(-1)` = 4294967295 onto the wire.
//
// The copy is the load-bearing part. These *ir.Column pointers are the CDC
// reader's LIVE schema cache, shared with the decode path; zeroing the sentinel
// in place would silently re-arm the per-row guard against a column nothing had
// read, which is the exact defect (Bug 236) this file exists to fix. Columns
// without the sentinel are passed through by pointer, so the common case
// allocates nothing but the slice.
func containSRIDSentinel(cols []*ir.Column) []*ir.Column {
	out := make([]*ir.Column, len(cols))
	for i, col := range cols {
		g, isGeom := col.Type.(ir.Geometry)
		if !isGeom || g.SRID != ir.GeometrySRIDUnknown {
			out[i] = col
			continue
		}
		g.SRID = 0
		clone := *col
		clone.Type = g
		out[i] = &clone
	}
	return out
}

// errTableNotFound is the sentinel [loadTableSchema] wraps when
// information_schema returns no columns for the requested table — which is how
// a missing table presents there, since the catalog answers zero rows rather
// than raising errno 1146.
//
// It exists so the unmapped-table probe can tell "absent" from "the lookup
// failed" without matching the message (audit backlog C-1).
var errTableNotFound = errors.New("mysql: table not found")
