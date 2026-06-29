// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestBuildMultiRowInsertSQL pins the ADR-0139 multi-row INSERT shape: one
// VALUES group per row, args flattened row-major, the shared
// AS new ON DUPLICATE KEY UPDATE clause appended once, and a single-row call
// byte-identical to the legacy single-row builder.
func TestBuildMultiRowInsertSQL(t *testing.T) {
	t.Run("three rows: one group per row, row-major args, single AS new clause", func(t *testing.T) {
		rows := []ir.Row{
			{"id": int64(7), "email": "a@x", "active": true},
			{"id": int64(8), "email": "b@x", "active": false},
			{"id": int64(9), "email": "c@x", "active": true},
		}
		gotSQL, gotArgs, err := buildMultiRowInsertSQL("src", "users", rows, []string{"id"}, nil)
		if err != nil {
			t.Fatalf("buildMultiRowInsertSQL: %v", err)
		}
		wantSQL := "INSERT INTO `src`.`users` (`active`, `email`, `id`) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?) " +
			"AS new ON DUPLICATE KEY UPDATE `active` = new.`active`, `email` = new.`email`"
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
		// Sorted column order is active, email, id; args are row-major.
		wantArgs := []any{
			true, "a@x", int64(7),
			false, "b@x", int64(8),
			true, "c@x", int64(9),
		}
		if !reflect.DeepEqual(gotArgs, wantArgs) {
			t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
		}
	})

	t.Run("no-PK table: full-row SET-list repeated per VALUES group", func(t *testing.T) {
		rows := []ir.Row{
			{"id": int64(1), "name": "x"},
			{"id": int64(2), "name": "y"},
		}
		gotSQL, gotArgs, err := buildMultiRowInsertSQL("src", "conns", rows, nil, nil)
		if err != nil {
			t.Fatalf("buildMultiRowInsertSQL: %v", err)
		}
		wantSQL := "INSERT INTO `src`.`conns` (`id`, `name`) VALUES (?, ?), (?, ?) " +
			"AS new ON DUPLICATE KEY UPDATE `id` = new.`id`, `name` = new.`name`"
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
		wantArgs := []any{int64(1), "x", int64(2), "y"}
		if !reflect.DeepEqual(gotArgs, wantArgs) {
			t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
		}
	})

	t.Run("empty rows is a loud error", func(t *testing.T) {
		if _, _, err := buildMultiRowInsertSQL("src", "users", nil, []string{"id"}, nil); err == nil {
			t.Fatal("want error for zero rows, got nil")
		}
	})
}

// TestBuildMultiRowInsertSQL_SingleRowEquivalence is the load-bearing
// equivalence pin: for N == 1 the multi-row builder must produce SQL + args
// byte-identical to buildInsertSQL across the full upsert/keyless/PK matrix, so
// the serial per-change path's output never changes (buildInsertSQL delegates
// to it). Mirrors the TestBuildInsertSQL case set.
func TestBuildMultiRowInsertSQL_SingleRowEquivalence(t *testing.T) {
	cases := []struct {
		name  string
		row   ir.Row
		pk    []string
		table string
	}{
		{"single-col PK", ir.Row{"id": int64(7), "email": "a@x", "active": true}, []string{"id"}, "users"},
		{"no PK", ir.Row{"id": int64(42), "name": "conn-a"}, nil, "connections"},
		{"single-col no PK", ir.Row{"payload": "hello"}, nil, "events"},
		{"all columns PK", ir.Row{"a_id": int64(1), "b_id": int64(2)}, []string{"a_id", "b_id"}, "join_table"},
		{"composite PK", ir.Row{"a": int64(1), "b": int64(2), "data": "x"}, []string{"a", "b"}, "composite"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantSQL, wantArgs, werr := buildInsertSQL("src", c.table, c.row, c.pk, nil)
			gotSQL, gotArgs, gerr := buildMultiRowInsertSQL("src", c.table, []ir.Row{c.row}, c.pk, nil)
			if werr != nil || gerr != nil {
				t.Fatalf("build error: single=%v multi=%v", werr, gerr)
			}
			if gotSQL != wantSQL {
				t.Errorf("SQL diverges:\n single: %q\n  multi: %q", wantSQL, gotSQL)
			}
			if !reflect.DeepEqual(gotArgs, wantArgs) {
				t.Errorf("args diverge:\n single: %#v\n  multi: %#v", wantArgs, gotArgs)
			}
		})
	}
}

// TestInsertRun_ShouldFlushBefore pins the coalescing decision (the part of the
// state machine that runs without a live connection): an empty run never
// flushes, a same-shape within-caps row appends, and a table switch / schema
// switch / column-shape change / placeholder-cap / byte-cap overflow each force
// a flush before the new row. The non-insert and keyless flush boundaries and
// apply-order preservation are pinned by the integration differential.
func TestInsertRun_ShouldFlushBefore(t *testing.T) {
	base := func() insertRun {
		return insertRun{
			schema: "db",
			table:  "t",
			cols:   []string{"a", "b"},
			rows:   []ir.Row{{"a": 1, "b": 2}},
			args:   2,
			bytes:  100,
		}
	}
	cases := []struct {
		name     string
		run      insertRun
		schema   string
		table    string
		cols     []string
		addArgs  int
		addBytes int64
		want     bool
	}{
		{"empty run never flushes", insertRun{}, "db", "t", []string{"a", "b"}, 2, 100, false},
		{"same shape within caps appends", base(), "db", "t", []string{"a", "b"}, 2, 100, false},
		{"table switch flushes", base(), "db", "other", []string{"a", "b"}, 2, 100, true},
		{"schema switch flushes", base(), "other", "t", []string{"a", "b"}, 2, 100, true},
		{"column-shape change flushes", base(), "db", "t", []string{"a", "b", "c"}, 3, 100, true},
		{"column-reorder counts as shape change", base(), "db", "t", []string{"b", "a"}, 2, 100, true},
		{
			name: "placeholder cap flushes",
			run: insertRun{
				schema: "db", table: "t", cols: []string{"a", "b"},
				rows: []ir.Row{{"a": 1}}, args: maxCoalescedPlaceholders - 1, bytes: 100,
			},
			schema: "db", table: "t", cols: []string{"a", "b"}, addArgs: 2, addBytes: 100, want: true,
		},
		{
			name: "byte cap flushes",
			run: insertRun{
				schema: "db", table: "t", cols: []string{"a", "b"},
				rows: []ir.Row{{"a": 1}}, args: 2, bytes: maxCoalescedStatementBytes,
			},
			schema: "db", table: "t", cols: []string{"a", "b"}, addArgs: 2, addBytes: 1, want: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			run := c.run
			if got := run.shouldFlushBefore(c.schema, c.table, c.cols, c.addArgs, c.addBytes); got != c.want {
				t.Errorf("shouldFlushBefore = %v; want %v", got, c.want)
			}
		})
	}
}
