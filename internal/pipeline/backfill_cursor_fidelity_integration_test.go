//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Backfill resume-cursor fidelity matrix (audit 2026-07-15 CRITICAL-2
// / HIGH-1). backfill_integration_test.go's resume pin exercises only
// INT PKs; this file re-derives the family matrix the Bug-74 lesson
// demands — the interrupt-then-resume shape for EVERY PK family
// [migcore.IsOrderablePKType] admits whose driver/store round-trip
// differs, on real MySQL AND Postgres:
//
//   - int64 above 2^53 (odd-spaced, so any float64 pass collapses
//     neighbours — the HIGH-1 drift);
//   - BINARY(16)/BYTEA whose bytes are invalid UTF-8, containing the
//     observed 0x9F8041FE10 (the CRITICAL-2 mangling);
//   - composite (int, string);
//   - temporal (DATETIME(6)/TIMESTAMP with microseconds);
//   - string (multibyte UTF-8).
//
// Plus the legacy-cursor contract: a pre-envelope plain-integer cursor
// still resumes (live control tables keep reading), and provably
// mangled legacy cursors (U+FFFD string, float-shaped integer) refuse
// with SLUICE-E-BACKFILL-CORRUPT-CURSOR and heal via --restart.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestBackfillCursorFidelity_MySQL(t *testing.T) {
	srcDSN, _, cleanup := startMySQL(t)
	defer cleanup()
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	db, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	runBackfillCursorFidelityScenarios(t, db, eng, srcDSN, false)
}

func TestBackfillCursorFidelity_PG(t *testing.T) {
	srcDSN, _, cleanup := startPostgres(t)
	defer cleanup()
	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	db, err := sql.Open("pgx", srcDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	runBackfillCursorFidelityScenarios(t, db, eng, srcDSN, true)
}

// bfFidelityExpr is the double-apply detector shared by every
// scenario: applied once a row is old+1; any replay past the guard
// would land 2*old+2 and the exactness assertion catches it.
const bfFidelityExpr = "COALESCE(new_col, 0) + old_col + 1"

// bfBinIDHex returns row i's 16-byte binary PK as hex: a first byte in
// the invalid-UTF-8 range (0x80+i), the audit's observed mangled tail
// 0x9F8041FE10, and a padding suffix keeping ids distinct and ordered.
func bfBinIDHex(i int) string {
	return fmt.Sprintf("%02x9f8041fe10%020x", 0x80+i, i)
}

// runBackfillInterruptResume creates a table whose PK column(s) come
// from pkDDL, seeds n rows via rowLit (which must yield ids that sort
// ascending in insert order), kills the first walk after 3 chunks of
// batch 10, resumes, and asserts every row holds exactly old_col + 1 —
// the "no row skipped, no row double-applied" contract a corrupted
// cursor breaks.
func runBackfillInterruptResume(t *testing.T, db *sql.DB, eng ir.Engine, dsn, table, pkDDL string, rowLit func(i int) string, n int) {
	t.Helper()
	ctx := context.Background()
	mustExecBF(t, db, fmt.Sprintf("CREATE TABLE %s (%s, old_col INT NOT NULL, new_col INT NULL)", table, pkDDL))
	values := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		values = append(values, rowLit(i))
	}
	mustExecBF(t, db, fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(values, ", ")))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	b1 := newIntgBackfiller(eng, dsn, table, bfFidelityExpr, "new_col IS NULL", 10)
	b1.Progress = &cancelAfterNChunksSink{after: 3, cancel: cancel}
	if _, err := b1.Run(runCtx); err == nil {
		t.Fatal("cancelled run returned nil error; want context cancellation")
	}

	var done int64
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table + " WHERE new_col IS NOT NULL").Scan(&done); err != nil {
		t.Fatalf("count done: %v", err)
	}
	if done == 0 || done == int64(n) {
		t.Fatalf("done after kill = %d; want a partial run (the cancel lever failed)", done)
	}

	b2 := newIntgBackfiller(eng, dsn, table, bfFidelityExpr, "new_col IS NULL", 10)
	res2, err := b2.Run(ctx)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if !res2.Resumed {
		t.Error("resume run did not pick up the persisted cursor")
	}
	assertBackfillExactOnce(t, db, table, n)
}

// assertBackfillExactOnce asserts all n rows hold exactly old_col + 1:
// a skipped PK range leaves NULLs, a replayed one lands 2*old+2.
func assertBackfillExactOnce(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	var total, wrong int64
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != int64(n) {
		t.Fatalf("row count = %d; want %d", total, n)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table + " WHERE new_col IS NULL OR new_col <> old_col + 1").Scan(&wrong); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if wrong != 0 {
		t.Errorf("%d row(s) of %s not backfilled exactly once (skipped or double-applied)", wrong, table)
	}
}

// overwriteBackfillProgress rewrites the stored progress JSON for a
// backfill spec — the lever that plants pre-envelope (legacy) cursor
// shapes under the real store.
func overwriteBackfillProgress(t *testing.T, db *sql.DB, isPG bool, table, expr, where, progressJSON string) {
	t.Helper()
	migID := BackfillMigrationID(table, []ir.BackfillSet{{Column: "new_col", Expr: expr}}, where)
	stmt := "UPDATE sluice_migrate_table_progress SET progress = ? WHERE migration_id = ? AND table_name = ?"
	if isPG {
		stmt = "UPDATE sluice_migrate_table_progress SET progress = $1 WHERE migration_id = $2 AND table_name = $3"
	}
	res, err := db.Exec(stmt, progressJSON, migID, table)
	if err != nil {
		t.Fatalf("rewrite progress row: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rewrite progress row: %d rows affected; want 1 (id %s)", n, migID)
	}
}

func runBackfillCursorFidelityScenarios(t *testing.T, db *sql.DB, eng ir.Engine, dsn string, isPG bool) {
	ctx := context.Background()

	t.Run("bigint_pk_above_2_53", func(t *testing.T) {
		runBackfillInterruptResume(t, db, eng, dsn, "bf_bigpk", "id BIGINT PRIMARY KEY",
			func(i int) string {
				return fmt.Sprintf("(%d, %d, NULL)", 9007199254740993+2*int64(i), i)
			}, 60)
	})

	t.Run("binary16_pk_invalid_utf8", func(t *testing.T) {
		pkDDL := "id BINARY(16) PRIMARY KEY"
		lit := func(i int) string { return fmt.Sprintf("(X'%s', %d, NULL)", bfBinIDHex(i), i) }
		if isPG {
			pkDDL = "id BYTEA PRIMARY KEY"
			lit = func(i int) string { return fmt.Sprintf("('\\x%s'::bytea, %d, NULL)", bfBinIDHex(i), i) }
		}
		runBackfillInterruptResume(t, db, eng, dsn, "bf_binpk", pkDDL, lit, 60)
	})

	t.Run("composite_int_string_pk", func(t *testing.T) {
		runBackfillInterruptResume(t, db, eng, dsn, "bf_comppk", "a INT, b VARCHAR(16), PRIMARY KEY (a, b)",
			func(i int) string {
				return fmt.Sprintf("(%d, 'k%02d', %d, NULL)", (i-1)/10+1, (i-1)%10, i)
			}, 60)
	})

	t.Run("temporal_pk_microseconds", func(t *testing.T) {
		pkDDL := "id DATETIME(6) PRIMARY KEY"
		if isPG {
			pkDDL = "id TIMESTAMP PRIMARY KEY"
		}
		base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		runBackfillInterruptResume(t, db, eng, dsn, "bf_timepk", pkDDL,
			func(i int) string {
				ts := base.Add(time.Duration(i)*time.Second + time.Duration(i)*time.Microsecond)
				return fmt.Sprintf("('%s', %d, NULL)", ts.Format("2006-01-02 15:04:05.000000"), i)
			}, 60)
	})

	t.Run("string_pk_multibyte", func(t *testing.T) {
		runBackfillInterruptResume(t, db, eng, dsn, "bf_strpk", "id VARCHAR(32) PRIMARY KEY",
			func(i int) string {
				return fmt.Sprintf("('café-%03d', %d, NULL)", i, i)
			}, 60)
	})

	t.Run("legacy_plain_int_cursor_still_resumes", func(t *testing.T) {
		table := "bf_legacyint"
		mustExecBF(t, db, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, old_col INT NOT NULL, new_col INT NULL)", table))
		seedBackfillRows(t, db, table, 60)

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		b1 := newIntgBackfiller(eng, dsn, table, bfFidelityExpr, "new_col IS NULL", 10)
		b1.Progress = &cancelAfterNChunksSink{after: 3, cancel: cancel}
		if _, err := b1.Run(runCtx); err == nil {
			t.Fatal("cancelled run returned nil error; want context cancellation")
		}
		// Rewind the stored cursor to the pre-envelope plain-number
		// shape (a live soak's control table). The guard makes the
		// replayed 11..30 range a no-op; the resume must read the bare
		// 10 exactly and finish the table.
		overwriteBackfillProgress(t, db, isPG, table, bfFidelityExpr, "new_col IS NULL",
			`{"state":"in_progress","last_pk":[10],"rows_copied":10}`)

		b2 := newIntgBackfiller(eng, dsn, table, bfFidelityExpr, "new_col IS NULL", 10)
		res2, err := b2.Run(ctx)
		if err != nil {
			t.Fatalf("resume from legacy cursor: %v", err)
		}
		if !res2.Resumed {
			t.Error("legacy plain-int cursor was not resumed from")
		}
		assertBackfillExactOnce(t, db, table, 60)
	})

	t.Run("legacy_mangled_cursor_refuses_then_restart_heals", func(t *testing.T) {
		cases := []struct {
			name     string
			table    string
			pkDDL    string
			rowLit   func(i int) string
			poisoned string
		}{
			{
				name:  "u_fffd_string_over_binary",
				table: "bf_legacybin", pkDDL: "id BINARY(16) PRIMARY KEY",
				rowLit: func(i int) string { return fmt.Sprintf("(X'%s', %d, NULL)", bfBinIDHex(i), i) },
				// The observed CRITICAL-2 shape: invalid UTF-8 replaced
				// with U+FFFD at the pre-envelope Marshal.
				poisoned: `{"state":"in_progress","last_pk":["��A�\u0010"],"rows_copied":30}`,
			},
			{
				name:  "float_shaped_bigint",
				table: "bf_legacyfloat", pkDDL: "id BIGINT PRIMARY KEY",
				rowLit: func(i int) string {
					return fmt.Sprintf("(%d, %d, NULL)", 1750000000000000123+int64(i), i)
				},
				// An old binary re-persisting its drifted float64 cursor.
				poisoned: `{"state":"in_progress","last_pk":[1.75e+18],"rows_copied":30}`,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				pkDDL := tc.pkDDL
				rowLit := tc.rowLit
				if isPG && tc.name == "u_fffd_string_over_binary" {
					pkDDL = "id BYTEA PRIMARY KEY"
					rowLit = func(i int) string { return fmt.Sprintf("('\\x%s'::bytea, %d, NULL)", bfBinIDHex(i), i) }
				}
				mustExecBF(t, db, fmt.Sprintf("CREATE TABLE %s (%s, old_col INT NOT NULL, new_col INT NULL)", tc.table, pkDDL))
				values := make([]string, 0, 60)
				for i := 1; i <= 60; i++ {
					values = append(values, rowLit(i))
				}
				mustExecBF(t, db, fmt.Sprintf("INSERT INTO %s VALUES %s", tc.table, strings.Join(values, ", ")))

				runCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				b1 := newIntgBackfiller(eng, dsn, tc.table, bfFidelityExpr, "new_col IS NULL", 10)
				b1.Progress = &cancelAfterNChunksSink{after: 3, cancel: cancel}
				if _, err := b1.Run(runCtx); err == nil {
					t.Fatal("cancelled run returned nil error; want context cancellation")
				}
				overwriteBackfillProgress(t, db, isPG, tc.table, bfFidelityExpr, "new_col IS NULL", tc.poisoned)

				b2 := newIntgBackfiller(eng, dsn, tc.table, bfFidelityExpr, "new_col IS NULL", 10)
				_, err := b2.Run(ctx)
				if err == nil {
					t.Fatal("resume from a mangled legacy cursor returned nil; want SLUICE-E-BACKFILL-CORRUPT-CURSOR")
				}
				var coded *sluicecode.CodedError
				if !errors.As(err, &coded) || coded.Code != sluicecode.CodeBackfillCorruptCursor {
					t.Fatalf("err = %v; want code %s", err, sluicecode.CodeBackfillCorruptCursor)
				}
				if !strings.Contains(coded.Hint, "--restart") {
					t.Errorf("hint %q missing --restart", coded.Hint)
				}

				b3 := newIntgBackfiller(eng, dsn, tc.table, bfFidelityExpr, "new_col IS NULL", 10)
				b3.Restart = true
				if _, err := b3.Run(ctx); err != nil {
					t.Fatalf("--restart heal run: %v", err)
				}
				assertBackfillExactOnce(t, db, tc.table, 60)
			})
		}
	})
}
