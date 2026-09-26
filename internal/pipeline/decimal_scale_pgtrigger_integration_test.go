//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"
)

// TestSync_DecimalScale_PGTriggerToMySQL pins GC-37 (c) review finding 1 on
// the managed-Postgres lane: the postgres-trigger reader hands every
// NON-integer numeric to the applier as json.Number (a named string type the
// first cut's guard did not recognise), so 31 fractional digits reached
// MySQL and were ROUNDED at exit 0 (reproduced by the reviewer end to end).
// It also pins finding 4: a target column whose name differs only in case
// from the source's (MySQL matches column names case-insensitively) used to
// get no descriptor at all, so the guard never ran there either.
//
// Independent expected value: the source's own v::text, compared to the
// target's CAST(v AS CHAR) with trailing fractional zeros trimmed.
func TestSync_DecimalScale_PGTriggerToMySQL(t *testing.T) {
	pgSrc, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	root, _, myCleanup := startMySQL(t)
	defer myCleanup()
	tgt := dsNewDatabase(t, root, "trig_ds")

	applyPGDDL(t, pgSrc, `
		CREATE TABLE tds_exact (id INT PRIMARY KEY, v NUMERIC);
		CREATE TABLE tds_over  (id INT PRIMARY KEY, v NUMERIC);
		CREATE TABLE tds_case  (id INT PRIMARY KEY, v NUMERIC);
	`)
	trigEng, _ := engines.Get(pgtrigger.EngineName)
	myEng, _ := engines.Get("mysql")
	ctx := context.Background()
	if _, err := pgtrigger.Setup(ctx, pgSrc, pgtrigger.SetupOptions{
		Tables: []string{"tds_exact", "tds_over", "tds_case"}, Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	migCtx, migCancel := context.WithTimeout(ctx, 3*time.Minute)
	defer migCancel()
	if err := (&Migrator{
		Source: trigEng, Target: myEng, SourceDSN: pgSrc, TargetDSN: tgt,
		Filter: mustNewFilter(t, nil, []string{pgtrigger.ChangeLogTable, pgtrigger.ChangeLogMetaTable}),
	}).Run(migCtx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	// Finding 4's shape: the target column's name now differs in case.
	applyMySQLDDL(t, tgt, "ALTER TABLE tds_case RENAME COLUMN v TO V")

	// run opens a reader anchored "from now" and an applier, applies the
	// given source DML, and returns the applier's result (nil after the
	// wait if it is still running).
	run := func(dml string, wait func() bool) error {
		reader, err := trigEng.OpenCDCReader(ctx, pgSrc)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		actx, cancel := context.WithCancel(ctx)
		defer cancel()
		out, err := reader.StreamChanges(actx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		applier, err := myEng.OpenChangeApplier(ctx, tgt)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		if err := applier.EnsureControlTable(ctx); err != nil {
			t.Fatalf("EnsureControlTable: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- applier.Apply(actx, "trig-ds", out) }()
		applyPGDDL(t, pgSrc, dml)
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-done:
				return err
			default:
			}
			if wait != nil && wait() {
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}
		return nil
	}
	landed := func(table string, id int) func() bool {
		return func() bool {
			db, _ := sql.Open("mysql", tgt)
			defer func() { _ = db.Close() }()
			var n int
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE id = %d", table, id)).Scan(&n)
			return n == 1
		}
	}

	// Exact cells: at-scale, trailing zeros, integer, and the case-folded
	// column with an in-scale value (typed prep must not break it).
	exact := map[int]string{1: "0.123456789012345678901234567890", 2: "1.50000000000000000000000000000000000", 3: "42"}
	if err := run(`INSERT INTO tds_exact VALUES (1, 0.123456789012345678901234567890), (2, 1.50000000000000000000000000000000000), (3, 42);
		INSERT INTO tds_case VALUES (1, 2.5);`, landed("tds_case", 1)); err != nil {
		t.Fatalf("exact cells: applier returned %v", err)
	}
	db, err := sql.Open("mysql", tgt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	pg, err := sql.Open("pgx", pgSrc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pg.Close() }()
	for id := range exact {
		var src, got string
		if err := pg.QueryRow("SELECT v::text FROM tds_exact WHERE id = $1", id).Scan(&src); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT CAST(v AS CHAR) FROM tds_exact WHERE id = ?", id).Scan(&got); err != nil {
			t.Fatalf("id=%d not on target: %v", id, err)
		}
		if dsTrimFrac(got) != dsTrimFrac(src) {
			t.Errorf("tds_exact id=%d: target [%s], source [%s]", id, got, src)
		}
	}

	// Over-scale through json.Number: must refuse, and must not land.
	err = run(`INSERT INTO tds_over VALUES (1, 0.1234567890123456789012345678901);`, nil)
	if err == nil || !strings.Contains(err.Error(), "DECIMAL-SCALE-EXCEEDED") {
		t.Errorf("pgtrigger over-scale: applier returned %v; want the DECIMAL-SCALE-EXCEEDED refusal", err)
	}
	if landed("tds_over", 1)() {
		var got string
		_ = db.QueryRow("SELECT CAST(v AS CHAR) FROM tds_over WHERE id = 1").Scan(&got)
		t.Errorf("pgtrigger over-scale row landed as [%s]", got)
	}

	// Over-scale into the case-folded column: must refuse too.
	err = run(`INSERT INTO tds_case VALUES (2, 0.1234567890123456789012345678901);`, nil)
	if err == nil || !strings.Contains(err.Error(), "DECIMAL-SCALE-EXCEEDED") {
		t.Errorf("case-mismatched column over-scale: applier returned %v; want the DECIMAL-SCALE-EXCEEDED refusal", err)
	}
	if landed("tds_case", 2)() {
		t.Error("case-mismatched column: the over-scale row landed")
	}
}
