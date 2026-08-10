//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 237(a) — a PRECISION-LESS Postgres temporal against the explicit
// precision a MySQL target declares for it.
//
// MySQL has no "engine-default 6" bare form (bare means 0 and would
// silently truncate fractional seconds), so its writer materializes a
// precision-unspecified source temporal as the explicit maximum. The
// catalog then reads back that 6, while the expected side still said
// "unspecified" — one phantom mismatch per bare temporal column.
//
// # Why this is not the LOW it was filed as
//
// The filing graded it as an advisory-surface problem in `schema diff`.
// It is not only that: the ADR-0166 migrate pre-create gate compares the
// SAME rendering, through the SAME [translate.RetargetForShapeCompare],
// so a second `sluice migrate` over a target the first one created
// REFUSED with `1 pre-existing target table(s) differ … column "ts_bare":
// want DateTime(unspecified), target has DateTime(6)`. Measured on real
// containers before the fix. That is a loud failure on a working
// configuration — a resumed or re-run migration of any schema with a
// bare `TIMESTAMP` in it — and TestMigrateTwice_… below is the half that
// pins it, because the diff assertions alone would not have.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// The MySQL CATALOG's own read-back of a schema `sluice migrate` just
// created. Nothing in the diff path produces it, so a symmetric error in
// the expected-side builder cannot make the two agree — which a unit test
// over hand-written `want` types cannot promise, since the same hand
// writes both sides.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pgTemporalMatrixDDL carries the temporal family × shape matrix rather
// than the single reported column, per the Bug 74 lesson: the precision
// axis and the TIME-ZONE axis are independent, and a fixture varying only
// the first would have gone green while `timetz` stayed broken.
//
// Every PG temporal type sluice can read, × {bare, explicit 0, explicit
// mid, explicit max} where the type takes a precision, × {zoned,
// unzoned} where it takes a zone. `d` and the explicit columns are the
// controls: they were never wrong, and a fix that started rewriting them
// would be inventing drift instead of removing it.
const pgTemporalMatrixDDL = `
	CREATE TABLE temporal (
		id      INT PRIMARY KEY,
		t_bare  TIME,
		t_p0    TIME(0),
		t_p3    TIME(3),
		t_p6    TIME(6),
		ttz     TIME WITH TIME ZONE,
		ttz_p3  TIME(3) WITH TIME ZONE,
		ts_bare TIMESTAMP,
		ts_p0   TIMESTAMP(0),
		ts_p3   TIMESTAMP(3),
		ts_p6   TIMESTAMP(6),
		tstz    TIMESTAMPTZ,
		tstz_p3 TIMESTAMPTZ(3),
		d       DATE
	);
`

// TestSchemaDiffAfterMigrate_PostgresToMySQL_TemporalPrecisionMatrix is
// the CLEAN half over the whole matrix at once, plus one genuine ALTER
// per axis so the fix cannot have bought silence by blinding the compare.
func TestSchemaDiffAfterMigrate_PostgresToMySQL_TemporalPrecisionMatrix(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, pgTemporalMatrixDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: pgEng, Target: mysqlEng, SourceDSN: pgSource, TargetDSN: mysqlTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	assertNoDrift(t, runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget))

	// DIRTY. Each is a change an operator could really make, and each is
	// one the materialization could have swallowed: precisions 1-5 must
	// stay distinct from the materialized 6, an explicitly-declared
	// precision must stay distinct from a different one, and the TIME/
	// DATETIME family boundary must survive the timetz flag drop.
	for _, tc := range []struct {
		name   string
		alter  string
		column string
	}{
		{
			"a bare source temporal's target precision narrowed",
			"ALTER TABLE `temporal` MODIFY COLUMN `ts_bare` DATETIME(3) NULL",
			"ts_bare",
		},
		{
			"an explicitly-declared precision widened on the target",
			"ALTER TABLE `temporal` MODIFY COLUMN `t_p3` TIME(6) NULL",
			"t_p3",
		},
		{
			"a bare TIME column re-typed to a different temporal family",
			"ALTER TABLE `temporal` MODIFY COLUMN `t_bare` DATETIME(6) NULL",
			"t_bare",
		},
		{
			"a zoned source column's target precision narrowed",
			"ALTER TABLE `temporal` MODIFY COLUMN `tstz` TIMESTAMP(3) NULL",
			"tstz",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applyMySQLDDL(t, mysqlTarget, tc.alter)
			diff := runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
			td := findTableDiff(*diff, "temporal")
			if td == nil {
				t.Fatalf("no table diff after %q — a REAL column-type change went unreported, which is the "+
					"failure mode the temporal materialization must not have introduced", tc.alter)
			}
			var found bool
			for _, cd := range td.ColumnsMismatched {
				if cd.Name == tc.column {
					found = true
					t.Logf("reported: %s expected=%s actual=%s", cd.Name, cd.ExpectedType, cd.ActualType)
				}
			}
			if !found {
				t.Errorf("column %q is NOT in columns_mismatched after %q. `sluice schema diff` would tell the "+
					"operator this target is in sync while its column type has genuinely changed.\nreported: %+v",
					tc.column, tc.alter, td.ColumnsMismatched)
			}
		})
	}
}

// TestMigrateTwice_PostgresToMySQL_BareTemporalDoesNotRefuse is the half
// that makes Bug 237(a) more than an advisory-surface defect, and it goes
// through `Migrator.Run` rather than through the diff because that is
// where the cost was: the ADR-0166 pre-create gate compares the same
// rendering and REFUSED the whole run — before any data moved, on a
// target sluice itself had created moments earlier.
//
// The re-run is the ordinary shape (a resumed migration, a re-issued
// command, a second shard landing into the first's tables), so this was
// reachable without doing anything unusual.
//
// Both directions here too. A re-run over a MATCHING target must proceed;
// a re-run over a target whose column was genuinely altered must still
// refuse, or the fix would have disarmed the gate rather than corrected
// its expected side.
func TestMigrateTwice_PostgresToMySQL_BareTemporalDoesNotRefuse(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, pgTemporalMatrixDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Distinct MigrationIDs on purpose. An auto-derived id is a function
	// of the source/target pair, so a bare re-run trips the
	// "migration_id … is already complete" guard and never reaches the
	// pre-create shape gate this test is about — which would make it pass
	// for the wrong reason if the ids ever converged.
	first := &Migrator{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "temporal-rerun-1",
	}
	if err := first.Run(ctx); err != nil {
		t.Fatalf("first Migrator.Run: %v", err)
	}

	second := &Migrator{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "temporal-rerun-2",
	}
	if err := second.Run(ctx); err != nil {
		t.Fatalf("a SECOND migrate over the target the first one created refused: %v\n\n"+
			"This is Bug 237(a)'s real cost: the pre-create shape gate compares the same rendering "+
			"`schema diff` does, so a bare PG temporal made every re-run and every resume refuse before any "+
			"data moved", err)
	}

	// The gate must still refuse a target that is genuinely different.
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `temporal` MODIFY COLUMN `ts_bare` DATETIME(3) NULL")
	third := &Migrator{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "temporal-rerun-3",
	}
	if err := third.Run(ctx); err == nil {
		t.Fatal("the pre-create shape gate ACCEPTED a target whose column precision was genuinely altered — " +
			"the temporal materialization has disarmed the gate rather than corrected its expected side, and " +
			"a copy into a narrower column silently truncates fractional seconds")
	}
}
