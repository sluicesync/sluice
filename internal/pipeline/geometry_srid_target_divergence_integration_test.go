//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The WRITE-side counterpart to C-14's read-side guard, measured rather
// than assumed (Bug 236 arm B, 2026-08-10).
//
// # The shape, and what actually protects against it
//
// sluice carries a geometry SRID per COLUMN, never per row: every reader
// strips the value's own framing and every writer re-frames from the
// column ([ir.CheckGeometryRowSRID]). C-14 closed the READ side — a row
// whose SRID differs from its source column's is refused. The write side
// has the mirror exposure: the SRID a row LANDS with is whatever the
// TARGET column declares, so a target column declaring a different SRID
// than the source's re-stamps every row.
//
// Nothing downstream catches that. Measured on real servers: an
// undeclared MySQL 8.4 `POINT` column stores ST_SRID 4326 without
// complaint (`E6100000…` prefix intact), and an unconstrained PostGIS
// `geometry` column does the same. So the target is perfectly capable of
// holding the source's SRID — a re-stamp to 0 is avoidable loss, not a
// target limitation, and it exits 0.
//
// What stops it reaching an operator is the EXISTING-TARGET SHAPE GATE
// (migrate_existing_tables.go), which compares the source-derived
// intended column against the pre-existing target column and refuses the
// run. It catches the SRID divergence only because [ir.Geometry.String]
// renders the SRID — incidental, and unpinned until this file and
// internal/ir/diff/column_shape_geometry_test.go.
//
// # What this file reaches, stated rather than implied
//
//   - Both entry points of the shared gate: `sync` cold start
//     (existingTablesGate via the Streamer) and `migrate`
//     (Migrator.phasePlanExistingTables). They are thin delegates onto
//     one plan(), but "one core, two callers" is precisely the shape
//     that has shipped half-wired here before.
//   - MySQL only. The gate is engine-neutral (it compares ir.Column
//     shapes), and the comparison itself is pinned per geometry family
//     in internal/ir/diff; this file's job is to prove the comparison is
//     CONSULTED before any data moves, which one engine establishes.
//
// It does NOT reach — and this is the residual the measurement found —
// a target whose geometry column is ALTERed AFTER cold start. Nothing
// re-runs the shape gate on a warm resume, so a target-side SRID change
// mid-stream silently re-stamps subsequent CDC rows (measured: cold-copy
// row landed 4326, CDC rows after the ALTER landed 0, one column holding
// both, no ERROR and no WARN). That is not geometry-specific — no column
// property is re-checked after cold start — and it is recorded as a
// named wart on the MySQL writer's geometry arm rather than fixed here.
package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// geomSRIDSourceDDL seeds two tables with the identical declared-SRID
// column: one for the refusal cells, one for the control.
const geomSRIDSourceDDL = `
	CREATE TABLE geo_divergent (
		id  BIGINT NOT NULL,
		loc POINT  NOT NULL SRID 4326,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	CREATE TABLE geo_control (
		id  BIGINT NOT NULL,
		loc POINT  NOT NULL SRID 4326,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	INSERT INTO geo_divergent (id, loc) VALUES (1, ST_GeomFromText('POINT(1 2)', 4326));
	INSERT INTO geo_control   (id, loc) VALUES (1, ST_GeomFromText('POINT(1 2)', 4326));
`

// geomSRIDUndeclaredTargetDDL is the hand-built target the operator
// could plausibly have: same column, no SRID attribute. MySQL reports
// SRS_ID NULL for it in st_geometry_columns, which sluice's readers
// render as SRID 0 — a DECLARED 0, distinct from "never read"
// ([ir.GeometrySRIDUnknown]).
const geomSRIDUndeclaredTargetDDL = `
	CREATE TABLE geo_divergent (
		id  BIGINT NOT NULL,
		loc POINT  NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// TestGeometrySRIDTargetDivergence_RefusedBeforeAnyDataMoves pins the
// gate at BOTH its entry points, plus the control that proves it is not
// simply refusing geometry.
func TestGeometrySRIDTargetDivergence_RefusedBeforeAnyDataMoves(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlogWithRowImage(t, "FULL")
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, geomSRIDSourceDDL)
	applyMySQLDDL(t, targetDSN, geomSRIDUndeclaredTargetDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	// Premise check, read off the server rather than assumed: the
	// pre-built target column really does declare nothing, and the
	// source really does declare 4326. Without this the refusal below
	// could be passing for an unrelated reason.
	if got := mysqlDeclaredSRID(t, sourceDSN, "geo_divergent"); got != "4326" {
		t.Fatalf("premise: source geo_divergent.loc srs_id = %q; want 4326", got)
	}
	if got := mysqlDeclaredSRID(t, targetDSN, "geo_divergent"); got != "NULL" {
		t.Fatalf("premise: target geo_divergent.loc srs_id = %q; want NULL (undeclared)", got)
	}

	assertRefusal := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("run over an SRID-divergent target SUCCEEDED; every row's SRID would be "+
				"re-stamped to the target column's 0 (landed ST_SRID now %d)",
				readMySQLSRID(t, context.Background(), targetDSN, "geo_divergent"))
		}
		// The operator has to be able to see WHICH property diverged and
		// on which side; a refusal naming only "shape differs" is not
		// actionable for an attribute this easy to overlook.
		for _, want := range []string{"loc", "4326"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not name %q: %v", want, err)
			}
		}
		if pollRowCountMySQL(targetDSN, "geo_divergent") != 0 {
			t.Errorf("target geo_divergent holds %d rows after the refusal; the gate must fire "+
				"before any data moves", pollRowCountMySQL(targetDSN, "geo_divergent"))
		}
	}

	t.Run("migrate", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		mig := &Migrator{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			Filter:      migcore.TableFilter{Include: []string{"geo_divergent"}},
			MigrationID: "geom-srid-divergent-migrate",
		}
		assertRefusal(t, mig.Run(ctx))
	})

	t.Run("sync-cold-start", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		streamer := &Streamer{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			Filter:   migcore.TableFilter{Include: []string{"geo_divergent"}},
			StreamID: "geom-srid-divergent-sync",
		}
		assertRefusal(t, streamer.Run(ctx))
	})

	// The control, and it is the load-bearing half: a target sluice
	// creates ITSELF carries the source's SRID, so both the cold-copy row
	// and a CDC row land at 4326. Without this the refusals above are
	// consistent with "sluice cannot migrate spatial columns at all".
	t.Run("control-sluice-created-target-carries-the-srid", func(t *testing.T) {
		streamer := &Streamer{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			Filter:   migcore.TableFilter{Include: []string{"geo_control"}},
			StreamID: "geom-srid-control-sync",
		}
		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()
		runErr := make(chan error, 1)
		go func() { runErr <- streamer.Run(streamCtx) }()

		if !waitForExactRowCountMySQL(targetDSN, "geo_control", 1, 120*time.Second) {
			select {
			case err := <-runErr:
				t.Fatalf("control cold copy never landed the seed row; Streamer.Run: %v", err)
			default:
			}
			t.Fatal("control cold copy never landed the seed row")
		}
		if got := mysqlDeclaredSRID(t, targetDSN, "geo_control"); got != "4326" {
			t.Errorf("sluice-created target column declares srs_id %q; want 4326 — a target sluice "+
				"builds itself must carry the source's SRID, or every CDC row re-stamps", got)
		}
		if got := readMySQLSRID(t, streamCtx, targetDSN, "geo_control"); got != 4326 {
			t.Errorf("cold-copy row landed at ST_SRID %d; want 4326", got)
		}

		applyMySQLDDL(t, sourceDSN,
			`INSERT INTO geo_control (id, loc) VALUES (2, ST_GeomFromText('POINT(3 4)', 4326));`)
		if !waitForExactRowCountMySQL(targetDSN, "geo_control", 2, 90*time.Second) {
			select {
			case err := <-runErr:
				t.Fatalf("control CDC row never landed; Streamer.Run: %v", err)
			default:
			}
			t.Fatal("control CDC row never landed")
		}
		if got := readMySQLSRIDForID(t, streamCtx, targetDSN, "geo_control", 2); got != 4326 {
			t.Errorf("CDC-applied row landed at ST_SRID %d; want 4326 — the applier stamps the "+
				"TARGET column's declared SRID, so this is the cell that proves cold start "+
				"declared it", got)
		}

		streamCancel()
		select {
		case <-runErr:
		case <-time.After(15 * time.Second):
			t.Error("Streamer.Run did not return after ctx cancel")
		}
	})
}

// mysqlDeclaredSRID reads what the COLUMN declares, as the server's own
// spatial catalog reports it — "NULL" for a column with no SRID
// attribute, which is MySQL's spelling of undeclared. Distinct from
// [readMySQLSRID], which reads what a stored VALUE carries; the whole
// defect class lives in the gap between those two answers.
func mysqlDeclaredSRID(t *testing.T, dsn, table string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var srid sql.NullString
	err = db.QueryRowContext(
		ctx, `
		SELECT srs_id FROM information_schema.st_geometry_columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'loc'`,
		table,
	).Scan(&srid)
	if err != nil {
		t.Fatalf("st_geometry_columns probe for %s: %v", table, err)
	}
	if !srid.Valid {
		return "NULL"
	}
	return srid.String
}

// readMySQLSRIDForID is [readMySQLSRID] for a caller-chosen row, so the
// cold-copy row and the CDC row can be told apart.
func readMySQLSRIDForID(t *testing.T, ctx context.Context, dsn, table string, id int64) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var srid int
	if err := db.QueryRowContext(ctx, "SELECT ST_SRID(loc) FROM "+table+" WHERE id = ?", id).Scan(&srid); err != nil {
		t.Fatalf("ST_SRID(%s.loc) id=%d: %v", table, id, err)
	}
	return srid
}
