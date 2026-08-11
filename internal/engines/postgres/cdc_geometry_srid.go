// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// resolveGeometryColumnSRIDs fills in the declared SRID for every geometry /
// geography column in entry, by reading the source catalog.
//
// # Why the CDC lane needs its own resolution
//
// [ir.CheckGeometryRowSRID] refuses a row whose EWKB names a different SRID
// than its column declares, because the decoder strips the framing and the
// writer re-frames from the COLUMN — so a mismatch is a silent re-stamp
// (audit 2026-08-05 C-14). That check needs the column's declared SRID, and on
// this lane it was not available: PostGIS's `geometry` OID is dynamic and
// pgoutput's RelationMessage carries only the OID and typmod, so
// [buildRelationCacheEntry] projects a bare `ir.Geometry{}`.
//
// A bare `ir.Geometry{}` has SRID 0, and 0 is ALSO the legitimate value for an
// unconstrained column. That collision is the whole defect: the check read
// "nobody ever told us" as "the column declares 0" and refused every row of
// every `geometry(Point,4326)` column, wedging the stream on the first spatial
// DML. It was caught by the PostGIS integration job, and never shipped — the
// C-14 fix and this resolution belong to the same unreleased delta.
//
// So the SRID is READ rather than defaulted, from the same
// geometry_columns/geography_columns views the cold-start
// [SchemaReader.readGeometryColumnInfo] uses, keeping the two lanes on one
// source of truth.
//
// # What happens when the catalog cannot answer
//
// A lookup failure leaves the columns unresolved and the per-row check
// DISABLED for that relation, with a warning naming the table. This is a
// deliberate choice against the project's usual loud-failure default, and the
// reason is that the two failure modes are not symmetric here: refusing would
// wedge a running stream over metadata sluice merely could not READ (PostGIS
// not installed for a non-spatial table, a role without SELECT on the views),
// while skipping restores exactly the pre-C-14 behaviour for that one relation
// — a narrow, named, logged regression in protection rather than an outage.
// The check is a guard against a rare shape, not the thing keeping the stream
// correct.
//
// # The staleness caveat, stated because it is real
//
// This reads the LIVE catalog at the moment the RelationMessage arrives, not
// the historic catalog at the WAL record's LSN. If a column's SRID changed
// between the record and now, the comparison uses the new value. The
// consequence is bounded — the check can only produce a loud refusal, never a
// silent apply — and it is the same posture as this reader's other catalog
// lookups (identity keys, column attnums), which cannot be historic either.
func (r *CDCReader) resolveGeometryColumnSRIDs(ctx context.Context, entry *relationCacheEntry) error {
	if r.db == nil {
		return nil
	}
	if !entryHasGeometryColumn(entry) {
		return nil
	}

	srids, err := geometryColumnSRIDsFromCatalog(ctx, r.db, entry.Schema, entry.Name)
	if err != nil {
		slog.WarnContext(
			ctx, "postgres: cdc: could not read declared geometry SRIDs; per-row SRID mismatches on this table will not be detected",
			slog.String("schema", entry.Schema),
			slog.String("table", entry.Name),
			slog.String("err", err.Error()),
			slog.String("hint", "grant SELECT on geometry_columns/geography_columns, or ignore this if the table's geometry columns are unconstrained"),
		)
		return nil
	}

	for i := range entry.Columns {
		g, ok := entry.Columns[i].Type.(ir.Geometry)
		if !ok {
			continue
		}
		srid, found := srids[entry.Columns[i].Name]
		if !found {
			// Not registered in either view, so nothing established what this
			// column declares. An unconstrained `geometry` column DOES appear
			// there with srid 0, so absence is not "declares 0" — it is the
			// unknown case, and the type already carries
			// [ir.GeometrySRIDUnknown] from [buildRelationCacheEntry]. Leave
			// it that way.
			continue
		}
		g.SRID = srid
		entry.Columns[i].Type = g
	}
	return nil
}

// entryHasGeometryColumn reports whether the relation carries at least one
// geometry/geography column, so a relation with none costs no catalog query.
func entryHasGeometryColumn(entry *relationCacheEntry) bool {
	for i := range entry.Columns {
		if _, ok := entry.Columns[i].Type.(ir.Geometry); ok {
			return true
		}
	}
	return false
}

// geometryColumnSRIDsFromCatalog returns column name → declared SRID for
// schema.table, from both PostGIS registration views.
//
// Both views are consulted because [ir.Geometry] covers both: a `geography`
// column is an ir.Geometry with IsGeography set, and geography_columns is a
// separate view. A missing view (PostGIS not installed) is not an error — it
// simply contributes no rows, which is the same answer as a non-spatial table.
func geometryColumnSRIDsFromCatalog(ctx context.Context, db *sql.DB, schema, table string) (map[string]int, error) {
	out := map[string]int{}
	queries := []struct {
		what        string
		sql         string
		isGeography bool
	}{
		// COALESCE per Bug 240: geography_columns does no null-handling of
		// its own (a bare `geography` column measured srid = 0 on PostGIS
		// 3.4, but that is accessor behaviour, not a view guarantee — see
		// [SchemaReader.readGeographyColumnInfo]).
		{"geometry_columns", `SELECT f_geometry_column, COALESCE(srid, 0) FROM geometry_columns WHERE f_table_schema = $1 AND f_table_name = $2`, false},
		{"geography_columns", `SELECT f_geography_column, COALESCE(srid, 0) FROM geography_columns WHERE f_table_schema = $1 AND f_table_name = $2`, true},
	}
	for _, q := range queries {
		if err := scanGeometrySRIDsInto(ctx, db, q.sql, schema, table, q.isGeography, out); err != nil {
			if isUndefinedRelationErr(err) {
				continue // PostGIS not installed; nothing to learn here.
			}
			return nil, fmt.Errorf("read %s: %w", q.what, err)
		}
	}
	return out, nil
}

// scanGeometrySRIDsInto runs one registration-view query and merges its rows
// into out. Split from the caller so the rows handle has a single scope and a
// plain defer. Geography rows get [effectiveGeographySRID], same as every
// other reader of that view: the raw 0 a bare column reports is not what the
// column declares, and comparing rows (which physically carry 4326) against
// it would wedge the CDC lane on the first spatial DML.
func scanGeometrySRIDsInto(ctx context.Context, db *sql.DB, query, schema, table string, isGeography bool, out map[string]int) error {
	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			col  string
			srid int64
		)
		if err := rows.Scan(&col, &srid); err != nil {
			return err
		}
		sridInt := int(srid)
		if isGeography {
			sridInt = effectiveGeographySRID(sridInt)
		}
		out[col] = sridInt
	}
	return rows.Err()
}
