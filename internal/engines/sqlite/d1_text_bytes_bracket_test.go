// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The LA-4 byte bracket, pinned WITHOUT live credentials.
//
// The live pin (TestD1Verify_MangledTextIsRefusedNotCopied) is behind the
// d1verify tag and is not in any CI shard, and every canned mock in this
// package answers the byte sum with the -1 "cannot weigh text" sentinel,
// which SKIPS the comparison. So the whole contract — the sum the server
// reports against the bytes the decoder saw — had no gate that runs on a
// PR. These cells are that gate.
//
// The concurrency cell is the one that matters most. A COUNT(*) is blind
// to an UPDATE, so counting rows alone would let the byte bracket refuse a
// healthy live database whenever a write landed in an already-delivered
// page — a false refusal whose message accuses the operator's data of
// corruption and tells them to exclude the table.

// bracketMock serves one page of rows and answers the bracket with the
// counts and byte sums it is told to, before and after.
func bracketMock(t *testing.T, rows []map[string]any, beforeN, afterN int, beforeB, afterB int64) *D1RowReader {
	t.Helper()
	call := 0
	h := func(sql string, _ []string) (int, []byte) {
		if isD1CountQuery(sql) {
			call++
			n, b := beforeN, beforeB
			if call > 1 {
				n, b = afterN, afterB
			}
			return http.StatusOK, d1OK([]map[string]any{{
				"n": strconv.Itoa(n),
				"b": strconv.FormatInt(b, 10),
			}})
		}
		if isD1WidthProbe(sql) {
			return http.StatusOK, d1WidthProbeAnswer(0)
		}
		return http.StatusOK, d1OK(rows)
	}
	return &D1RowReader{client: startMockD1(t, h)}
}

func TestD1RowReader_TextByteBracket(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "c", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	// One row whose text cell is delivered as 5 bytes.
	rows := []map[string]any{dataRow(table, map[string]cell{
		"id": ival("1"),
		"c":  tval("plain"),
	})}

	for _, tc := range []struct {
		name            string
		beforeN, afterN int
		beforeB, afterB int64
		wantRefusal     bool
		why             string
	}{
		{
			// The source says it stores exactly what arrived.
			name: "agreeing sums pass", beforeN: 1, afterN: 1, beforeB: 5, afterB: 5,
			wantRefusal: false, why: "clean read",
		},
		{
			// The source stores 3 bytes; 5 arrived. That is the mangle.
			name: "delivered longer than stored refuses", beforeN: 1, afterN: 1, beforeB: 3, afterB: 3,
			wantRefusal: true, why: "LA-4: U+FFFD is 3 bytes replacing 1",
		},
		{
			// THE FALSE-REFUSAL CELL. Row count identical, byte sums moved
			// together: a concurrent UPDATE lengthened a text cell. The
			// bracket must abstain exactly as the count bracket does for a
			// moving table, or a live D1 migration becomes unrunnable.
			name:    "concurrent update moves both byte sums: no refusal",
			beforeN: 1, afterN: 1, beforeB: 5, afterB: 40,
			wantRefusal: false, why: "quiescence needs BOTH readings to hold still",
		},
		{
			// A moving row count is already the count bracket's business.
			name:    "moving row count leaves it to the count bracket",
			beforeN: 1, afterN: 2, beforeB: 5, afterB: 99,
			wantRefusal: false, why: "not quiescent",
		},
		{
			// The sentinel a transport that cannot weigh text reports.
			name: "sentinel disarms the comparison", beforeN: 1, afterN: 1, beforeB: -1, afterB: -1,
			wantRefusal: false, why: "-1 means no byte evidence",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := bracketMock(t, rows, tc.beforeN, tc.afterN, tc.beforeB, tc.afterB)
			out, err := r.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			for range out {
			}
			readErr := r.Err()

			var coded *sluicecode.CodedError
			gotRefusal := errors.As(readErr, &coded) && coded.Code == sluicecode.CodeD1TextMangled
			if gotRefusal != tc.wantRefusal {
				t.Fatalf("refused=%v, want %v (%s); err = %v", gotRefusal, tc.wantRefusal, tc.why, readErr)
			}
			if !tc.wantRefusal && readErr != nil && gotRefusal {
				t.Fatalf("unexpected mangle refusal: %v", readErr)
			}
		})
	}
}

// TestStageD1Table_TextByteBracket is HIGH-1 of the v0.140.0 pre-tag review:
// fetchPages has TWO consumers and the byte bracket first reached only the
// reader's. `--infer-types` against D1 engages staging AUTOMATICALLY and then
// swaps the source to the staged file, so a mangled cell would have been
// written into that file as valid UTF-8 with nothing downstream able to tell
// — a silent loss at exit 0 on the path an operator is most likely to take.
//
// The sibling was enumerated in the doc rows of the other two D1 codes and
// not in this one's, which is exactly how it was missed.
func TestStageD1Table_TextByteBracket(t *testing.T) {
	table := &ir.Table{
		Name: "items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "label", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	rows := []map[string]any{dataRow(table, map[string]cell{
		"id":    ival("1"),
		"label": tval("plain"), // 5 bytes delivered
	})}

	for _, tc := range []struct {
		name        string
		srcBytes    int64
		inScope     bool
		wantRefusal bool
	}{
		{"agreeing sums stage cleanly", 5, true, false},
		{"delivered longer than stored refuses before the file is trusted", 3, true, true},
		{
			// Bug 265, found by the v0.140.0 regression cycle against the
			// refusal v0.140.0 itself added. Staging copies the WHOLE
			// database by design, and it runs before the table filter is
			// consulted — so refusing for any table meant one mangled table
			// in a schema the operator had excluded failed the entire run,
			// with no flag able to get past it. An out-of-scope mangle
			// cannot reach the target, so it warns and the run proceeds.
			"a mangled table OUT OF SCOPE warns instead of failing the run", 3, false, false,
		},
		{
			// The other direction, which is the one that would quietly
			// disarm the refusal: out-of-scope must not become a blanket
			// skip of the comparison.
			"an out-of-scope table with agreeing sums is still clean", 5, false, false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := bracketMock(t, rows, 1, 1, tc.srcBytes, tc.srcBytes)
			db := openStageDest(t, `CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)`)
			_, err := stageD1Table(context.Background(), rr, db, table, tc.inScope, slog.Default())

			var coded *sluicecode.CodedError
			gotRefusal := errors.As(err, &coded) && coded.Code == sluicecode.CodeD1TextMangled
			if gotRefusal != tc.wantRefusal {
				t.Fatalf("staging refused=%v, want %v; err = %v", gotRefusal, tc.wantRefusal, err)
			}
			if !tc.wantRefusal && err != nil {
				t.Fatalf("clean staging failed: %v", err)
			}
			if !tc.wantRefusal {
				// Scoping the REFUSAL must not have scoped the COPY: the
				// staged file is a faithful whole-database replica, and a
				// later phase reading it would find a silently empty table.
				var staged int
				if qerr := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM items").Scan(&staged); qerr != nil {
					t.Fatalf("count staged rows: %v", qerr)
				}
				if staged != 1 {
					t.Fatalf("staged %d rows, want 1 — the table was skipped, not just exempted from the refusal", staged)
				}
			}
		})
	}
}
