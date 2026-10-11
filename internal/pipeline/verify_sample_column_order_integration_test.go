//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// VERIFY-SAMPLE-COLUMN-ORDER, end to end on real MySQL and Postgres.
//
// `sluice verify --depth sample` used to hash each side's columns in that
// side's OWN declared order, so it compared columns by POSITION. A target
// whose `b` and `c` held each other's values, declared in swapped order —
// what a RENAME swap leaves behind — hashed identically to its source and
// reported `OK … clean`. These pins hold the fix (columns hashed by NAME in
// one order both sides share, [planSampleColumns]) to its four shapes:
//
//   - swap:    same names, values under the wrong names  → every row MISMATCH
//   - reorder: same names, same values, other declaration → clean
//   - missing: a source column the target lacks          → COLUMN-MISMATCH
//   - extra:   a target column the source lacks          → clean, named
//
// Independent expected value: each fixture's ground truth is the DDL and
// DML written here — the swap target is constructed to hold the source's
// values under exchanged names, so "every sampled row differs" is known
// before verify runs, not derived from anything verify reads.

package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// colOrderRows is the fixture's row count; well under the sample size, so
// every row is sampled and a swap must flag every one of them.
const colOrderRows = 40

// colOrderSeed renders INSERTs for `table` whose columns are listed in
// cols, with value(col, i) for row i.
func colOrderSeed(table string, cols []string, value func(col string, i int) string) string {
	var sb strings.Builder
	for i := 1; i <= colOrderRows; i++ {
		vals := make([]string, len(cols))
		for j, c := range cols {
			vals[j] = value(c, i)
		}
		fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES (%s);\n", table, strings.Join(cols, ", "), strings.Join(vals, ", "))
	}
	return sb.String()
}

// colOrderValue is the source's value for (col, row): the column's own
// name is in it, so a value under the wrong name is a different value.
func colOrderValue(col string, i int) string {
	if col == "id" {
		return fmt.Sprint(i)
	}
	return fmt.Sprintf("'%s-%d'", col, i)
}

// colOrderCase is one shape: the target's DDL and the value it stores.
type colOrderCase struct {
	table     string
	tgtCols   []string // target declaration order
	tgtValue  func(col string, i int) string
	wantClean bool
	wantRows  int // SampleMismatch expected when !wantClean (0 = column mismatch)
	missing   []string
	extra     []string
}

func colOrderCases() []colOrderCase {
	return []colOrderCase{
		{
			// The bug's own shape: target declares (id, b, c) and holds the
			// source's POSITIONAL values, so its `b` holds the source's `c`.
			table:   "kd_swap",
			tgtCols: []string{"id", "b", "c"},
			tgtValue: func(col string, i int) string {
				switch col {
				case "b":
					return colOrderValue("c", i)
				case "c":
					return colOrderValue("b", i)
				}
				return colOrderValue(col, i)
			},
			wantRows: colOrderRows,
		},
		{
			// A genuine reorder: same values under the same names.
			table: "kd_reorder", tgtCols: []string{"id", "b", "c"},
			tgtValue: colOrderValue, wantClean: true,
		},
		{
			table: "kd_missing", tgtCols: []string{"id", "b"},
			tgtValue: colOrderValue, missing: []string{"c"},
		},
		{
			table: "kd_extra", tgtCols: []string{"id", "c", "b", "x"},
			tgtValue: colOrderValue, wantClean: true, extra: []string{"x"},
		},
	}
}

// runColOrderMatrix seeds every case on the (src, tgt) pair through apply,
// then runs the real Verifier per table and grades it.
func runColOrderMatrix(t *testing.T, engineName, srcDSN, tgtDSN string, apply func(t *testing.T, dsn, sql string), textType string) {
	t.Helper()
	srcCols := []string{"id", "c", "b"} // source declaration order
	for _, tc := range colOrderCases() {
		ddl := func(cols []string) string {
			defs := make([]string, len(cols))
			for i, c := range cols {
				defs[i] = c + " " + textType
				if c == "id" {
					defs[i] = "id BIGINT PRIMARY KEY"
				}
			}
			return fmt.Sprintf("CREATE TABLE %s (%s);", tc.table, strings.Join(defs, ", "))
		}
		apply(t, srcDSN, ddl(srcCols))
		apply(t, srcDSN, colOrderSeed(tc.table, srcCols, colOrderValue))
		apply(t, tgtDSN, ddl(tc.tgtCols))
		apply(t, tgtDSN, colOrderSeed(tc.table, tc.tgtCols, tc.tgtValue))
	}

	eng, ok := engines.Get(engineName)
	if !ok {
		t.Fatalf("engine %q not registered", engineName)
	}
	for _, tc := range colOrderCases() {
		t.Run(tc.table, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			var buf bytes.Buffer
			v := &Verifier{
				Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN,
				Depth: VerifyDepthSample, Filter: migcore.TableFilter{Include: []string{tc.table}},
				Out: &buf,
			}
			r, err := v.Run(ctx)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(r.Tables) != 1 {
				t.Fatalf("tables = %d; want 1\n%s", len(r.Tables), buf.String())
			}
			tr := r.Tables[0]
			if r.HasUnverified() {
				t.Fatalf("table unverified (%s)\n%s", tr.Reason, buf.String())
			}
			if tr.CountMismatch {
				t.Fatalf("fixture error: counts differ\n%s", buf.String())
			}
			if got := r.HasMismatch(); got == tc.wantClean {
				t.Errorf("HasMismatch = %v; want %v\n%s", got, !tc.wantClean, buf.String())
			}
			if tr.SampleMismatch != tc.wantRows {
				t.Errorf("SampleMismatch = %d; want %d\n%s", tr.SampleMismatch, tc.wantRows, buf.String())
			}
			if strings.Join(tr.ColumnsMissingOnTarget, ",") != strings.Join(tc.missing, ",") {
				t.Errorf("missing = %v; want %v", tr.ColumnsMissingOnTarget, tc.missing)
			}
			if strings.Join(tr.ColumnsExtraOnTarget, ",") != strings.Join(tc.extra, ",") {
				t.Errorf("extra = %v; want %v", tr.ColumnsExtraOnTarget, tc.extra)
			}
			if tc.wantClean && tr.SampleSize != colOrderRows {
				t.Errorf("SampleSize = %d; want every row (%d) sampled", tr.SampleSize, colOrderRows)
			}
		})
	}

	// Count depth cannot see a swap — the documented limit, pinned so the
	// docs saying so stay true (the swap is exactly what sample is for).
	t.Run("count_depth_is_blind_to_the_swap", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		var buf bytes.Buffer
		v := &Verifier{
			Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN,
			Depth: VerifyDepthCount, Filter: migcore.TableFilter{Include: []string{"kd_swap"}},
			Out: &buf,
		}
		r, err := v.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if r.HasMismatch() {
			t.Errorf("count depth flagged the swap — the docs say it cannot; update them\n%s", buf.String())
		}
	})
}

func TestVerify_SampleColumnOrder_Postgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgres(t)
	defer cleanup()
	runColOrderMatrix(t, "postgres", srcDSN, tgtDSN, applyPGDDL, "TEXT")
}

func TestVerify_SampleColumnOrder_MySQL(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQL(t)
	defer cleanup()
	runColOrderMatrix(t, "mysql", srcDSN, tgtDSN, applyMySQLDDL, "VARCHAR(64)")
}
