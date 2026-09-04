// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 265, and the reason this test drives the whole staging entry point
// rather than stageD1Table.
//
// TestStageD1Table_TextByteBracket grades the per-table function with the
// in-scope flag handed to it directly. A mutation that hard-coded the flag
// to false AT THE CALL SITE — inside stageD1ClientToLocalFile, which is the
// only place it is computed — left that test fully green. The refusal was
// disarmed for every table and nothing failed.
//
// That is the narrow-gate shape this repo keeps paying for: the pin graded
// the function and not the wiring. This grades the wiring, by staging a
// database in which exactly one table is mangled and asserting the outcome
// turns on whether that table is in scope.
func TestStageD1ClientToLocalFile_MangleRefusalFollowsTheTableFilter(t *testing.T) {
	srcPath := buildScopeSource(t)
	srcDB, err := sql.Open("sqlite", srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	t.Cleanup(func() { _ = srcDB.Close() })

	for _, tc := range []struct {
		name        string
		inScope     func(string) bool
		wantRefusal bool
	}{
		{
			// The operator excluded the mangled table. Staging still copies
			// it — the staged file is a faithful whole-database replica —
			// but the value can never reach the target, so refusing would
			// fail a run that was going to be correct, and no flag could
			// get past it because staging runs before the filter is read.
			name:    "the mangled table is OUT of scope: the run proceeds",
			inScope: func(table string) bool { return table == "keep" },
		},
		{
			name:        "the mangled table is IN scope: the run refuses",
			inScope:     func(table string) bool { return table == "mangled" },
			wantRefusal: true,
		},
		{
			// nil is "no filter", which must behave as everything-in-scope
			// rather than as nothing-in-scope. Getting this backwards is
			// how the refusal would silently stop existing for every
			// ordinary run, which is the majority of runs.
			name:        "no filter at all means every table is in scope",
			inScope:     nil,
			wantRefusal: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := startMockD1(t, underReportingBytesFor(srcDB, "mangled"))
			dest := filepath.Join(t.TempDir(), "stage.db")
			err := stageD1ClientToLocalFile(context.Background(), mock, dest, tc.inScope, nil)

			var coded *sluicecode.CodedError
			gotRefusal := errors.As(err, &coded) && coded.Code == sluicecode.CodeD1TextMangled
			if gotRefusal != tc.wantRefusal {
				t.Fatalf("refused=%v, want %v; err = %v", gotRefusal, tc.wantRefusal, err)
			}
			if tc.wantRefusal {
				if !strings.Contains(err.Error(), "mangled") {
					t.Fatalf("the refusal must name the offending table: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("staging should have completed: %v", err)
			}
			// Scoping the REFUSAL must not have scoped the COPY. A later
			// phase reading the staged file would otherwise find a table
			// that is silently empty.
			assertStagedRowCount(t, dest, "mangled", 1)
			assertStagedRowCount(t, dest, "keep", 1)
		})
	}
}

func buildScopeSource(t *testing.T) string {
	t.Helper()
	return seedDB(
		t,
		`CREATE TABLE keep (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO keep (id,v) VALUES (1,'fine')`,
		`CREATE TABLE mangled (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO mangled (id,v) VALUES (1,'fine')`,
	)
}

// underReportingBytesFor serves the source honestly EXCEPT for one table's
// byte-sum bracket, which comes back short — the shape a server-side U+FFFD
// rewrite produces, where the client receives more text bytes than the
// source stores.
func underReportingBytesFor(db *sql.DB, table string) d1Handler {
	honest := execD1Handler(db)
	return func(sqlStr string, params []string) (int, []byte) {
		if isD1CountQuery(sqlStr) && strings.Contains(sqlStr, `"`+table+`"`) {
			return http.StatusOK, d1OK([]map[string]any{{"n": "1", "b": "1"}})
		}
		return honest(sqlStr, params)
	}
}

func assertStagedRowCount(t *testing.T, path, table string, want int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open staged: %v", err)
	}
	defer func() { _ = db.Close() }()
	var got int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM "`+table+`"`).Scan(&got); err != nil {
		t.Fatalf("count %s in the staged file: %v", table, err)
	}
	if got != want {
		t.Fatalf("staged %s has %d rows, want %d — the table was skipped, not merely exempted from the refusal",
			table, got, want)
	}
}
