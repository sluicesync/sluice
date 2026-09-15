// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// AN OUT-OF-SCOPE TABLE'S ROWS EVENT CANNOT KILL THE STREAM, WHATEVER IS
// WRONG WITH IT — AND AN IN-SCOPE ONE STILL REFUSES EXACTLY AS BEFORE.
//
// Audit 2026-09-15 A0915-ARCH-MEDIUM-3: the Bug 246 / A0909-AQ-M-2 fixes gated two
// refusals on the sync's table scope and left every other stream-killing
// refusal on the row path ungated — the TABLE_MAP shape guard, the
// TINYINT(1) range refusal on both CDC lanes, the partial-image belt. The
// sharpest case: an excluded legacy table whose TINYINT(1) held 2..127
// passed the (in-scope-only) preflight and then halted the sync at its
// first row, under a remedy that could not mention --exclude-table
// because the flag did not work.
//
// The fix asks the scope question ONCE per rows event, ahead of every
// per-row refusal. This matrix pins it the way the AST roster
// (TestStreamKillingRefusalsInDispatchAreScopeGated) cannot: behaviourally,
// per refusal family, in both directions. Every family the audit named,
// plus the two the dispatcher also carries (arity, generated PK), ×
// {excluded → dropped whole and the stream lives; included → the refusal
// is unchanged; nil predicate → refuses, the fail-loud direction}.
func TestDispatchRows_OutOfScopeTableNeverKillsTheStream(t *testing.T) {
	ctx := context.Background()

	// Column shapes the cases prime app.users with.
	plainID := func() []*ir.Column { return []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}} }
	writeRows := func(rows ...[]any) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: hdr(replication.WRITE_ROWS_EVENTv2),
			Event:  &replication.RowsEvent{TableID: 7, Rows: rows},
		}
	}

	cases := []struct {
		name  string
		prime func(tbl *tableSchema)
		event *replication.BinlogEvent
		// wantCode is the coded refusal an in-scope row must still
		// produce; wantSubstr pins the uncoded decode floors.
		wantCode   sluicecode.Code
		wantSubstr string
	}{
		{
			name: "TINYINT(1) out of range (SLUICE-E-VALUE-TINYINT1-RANGE)",
			prime: func(tbl *tableSchema) {
				tbl.Columns = []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "flag", Type: ir.Boolean{}}}
			},
			event:    writeRows([]any{int64(1), int8(2)}),
			wantCode: sluicecode.CodeValueTinyint1Range,
		},
		{
			name:  "partial row image (SLUICE-E-CDC-ROW-IMAGE-PARTIAL)",
			prime: func(*tableSchema) {},
			event: &replication.BinlogEvent{
				Header: hdr(replication.WRITE_ROWS_EVENTv2),
				Event:  &replication.RowsEvent{TableID: 7, Rows: [][]any{{nil}}, SkippedColumns: [][]int{{0}}},
			},
			wantCode: sluicecode.CodeCDCRowImagePartial,
		},
		{
			name: "TABLE_MAP family mismatch on replay (SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH)",
			prime: func(tbl *tableSchema) {
				tbl.Columns = plainID()
				tbl.DataTypes = []string{"int"}
			},
			event: &replication.BinlogEvent{
				Header: hdr(replication.WRITE_ROWS_EVENTv2),
				Event: &replication.RowsEvent{
					TableID: 7,
					Table: &replication.TableMapEvent{
						ColumnCount: 1,
						ColumnType:  []byte{gomysql.MYSQL_TYPE_VARCHAR},
						ColumnMeta:  []uint16{0},
					},
					Rows: [][]any{{"x"}},
				},
			},
			wantCode: sluicecode.CodeCDCSchemaReplayMismatch,
		},
		{
			name:       "column-count mismatch (the uncoded decode floor)",
			prime:      func(*tableSchema) {},
			event:      writeRows([]any{int64(1), int64(2)}),
			wantSubstr: "row has 2 values; schema has 1 columns",
		},
		{
			name: "generated column in the PRIMARY KEY (SLUICE-E-CDC-GENERATED-PRIMARY-KEY)",
			prime: func(tbl *tableSchema) {
				tbl.Columns = []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}, GeneratedExpr: "1+1"}}
			},
			event: &replication.BinlogEvent{
				Header: hdr(replication.UPDATE_ROWS_EVENTv2),
				Event:  &replication.RowsEvent{TableID: 7, Rows: [][]any{{int64(1)}, {int64(1)}}},
			},
			wantCode: sluicecode.CodeCDCGeneratedPrimaryKey,
		},
	}

	// newReader primes a staging reader with the case's app.users shape
	// and a second, always-in-scope table app.orders (table_id 8), which
	// is how the excluded arm proves the stream is still alive.
	newReader := func(t *testing.T, prime func(*tableSchema)) *CDCReader {
		t.Helper()
		r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
		prime(r.schemaCache["app.users"])
		r.tableMap[8] = "app.orders"
		r.schemaCache["app.orders"] = &tableSchema{
			Schema: "app", Name: "orders", Columns: plainID(), PrimaryKey: []string{"id"},
		}
		return r
	}
	ordersRow := &replication.BinlogEvent{
		Header: hdr(replication.WRITE_ROWS_EVENTv2),
		Event:  &replication.RowsEvent{TableID: 8, Rows: [][]any{{int64(42)}}},
	}
	rowChanges := func(out chan ir.Change) []ir.Change {
		close(out)
		var rows []ir.Change
		for c := range out {
			switch c.(type) {
			case ir.Insert, ir.Update, ir.Delete:
				rows = append(rows, c)
			}
		}
		return rows
	}
	assertRefused := func(t *testing.T, err error, wantCode sluicecode.Code, wantSubstr, arm string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: the refusal did not fire — the scope gate must only ever exempt EXCLUDED tables", arm)
		}
		if wantCode != "" {
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != wantCode {
				t.Fatalf("%s: want %s; got %T: %v", arm, wantCode, err, err)
			}
		}
		if wantSubstr != "" && !strings.Contains(err.Error(), wantSubstr) {
			t.Fatalf("%s: refusal does not say %q: %v", arm, wantSubstr, err)
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("excluded table is dropped whole and the stream lives", func(t *testing.T) {
				r := newReader(t, tc.prime)
				r.SetCDCScopePredicate(func(_, table string) bool { return table != "users" })
				out := make(chan ir.Change, 8)
				dispatchOne(t, r, mysqlGTIDEvent(t, 6), out, "GTID :6")
				dispatchOne(t, r, queryEvent("BEGIN"), out, "BEGIN")
				if err := r.dispatch(ctx, tc.event, out); err != nil {
					t.Fatalf("a rows event for a table the sync EXCLUDES killed the stream: %v\n\n"+
						"The table filter runs one stage downstream, so this table's rows would never reach the "+
						"target — refusing over its shape refuses a working configuration, and excluding the table "+
						"(which the operator already did) changes nothing the reader sees (audit 2026-09-15 A0915-ARCH-MEDIUM-3).", err)
				}
				dispatchOne(t, r, ordersRow, out, "in-scope orders row")
				rows := rowChanges(out)
				if len(rows) != 1 {
					t.Fatalf("emitted %d row changes; want exactly 1 (the in-scope orders row) — the excluded "+
						"event must emit NOTHING and the next in-scope row must still flow", len(rows))
				}
				if ins, ok := rows[0].(ir.Insert); !ok || ins.Table != "orders" {
					t.Fatalf("the surviving change is %T %v; want the orders Insert", rows[0], rows[0])
				}
			})
			t.Run("included table still refuses", func(t *testing.T) {
				r := newReader(t, tc.prime)
				r.SetCDCScopePredicate(func(string, string) bool { return true })
				out := make(chan ir.Change, 8)
				dispatchOne(t, r, mysqlGTIDEvent(t, 6), out, "GTID :6")
				dispatchOne(t, r, queryEvent("BEGIN"), out, "BEGIN")
				assertRefused(t, r.dispatch(ctx, tc.event, out), tc.wantCode, tc.wantSubstr, "included")
				if rows := rowChanges(out); len(rows) != 0 {
					t.Fatalf("a refused event emitted %d row changes; want 0", len(rows))
				}
			})
			t.Run("nil predicate refuses (the fail-loud direction)", func(t *testing.T) {
				r := newReader(t, tc.prime)
				out := make(chan ir.Change, 8)
				dispatchOne(t, r, mysqlGTIDEvent(t, 6), out, "GTID :6")
				dispatchOne(t, r, queryEvent("BEGIN"), out, "BEGIN")
				assertRefused(t, r.dispatch(ctx, tc.event, out), tc.wantCode, tc.wantSubstr, "nil predicate")
			})
		})
	}
}

// TestVStreamDispatch_OutOfScopeTableNeverKillsTheStream is the same
// matrix on the two VStream lanes — the standalone reader's dispatchRow
// and the cold-start snapshot stream's hand-mirrored dispatchCDCRow. The
// tail request's rules end in `Match: "/.*/"`, so every table in the
// keyspace reaches both, exactly as every table in the database reaches
// the binlog lane. Two refusal families are driven here: the partial-image
// belt (a coded refusal ahead of decode) and the TINYINT(1) range refusal
// (inside decodeVStreamRow, the deepest per-value site).
func TestVStreamDispatch_OutOfScopeTableNeverKillsTheStream(t *testing.T) {
	usersFields := []*query.Field{
		{Name: "id", Type: query.Type_INT64},
		{Name: "flag", Type: query.Type_INT8, ColumnType: "tinyint(1)"},
	}
	ordersFields := []*query.Field{{Name: "id", Type: query.Type_INT64}}
	fieldEv := func(table string, fields []*query.Field) *binlogdata.VEvent {
		return &binlogdata.VEvent{
			Type:       binlogdata.VEventType_FIELD,
			FieldEvent: &binlogdata.FieldEvent{TableName: table, Keyspace: "main", Shard: "-", Fields: fields},
		}
	}
	rowEv := func(table string, rc *binlogdata.RowChange) *binlogdata.VEvent {
		return &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: table, Keyspace: "main", Shard: "-",
				RowChanges: []*binlogdata.RowChange{rc},
			},
		}
	}
	cases := []struct {
		name     string
		event    *binlogdata.VEvent
		wantCode sluicecode.Code
	}{
		{
			name: "partial row image (SLUICE-E-CDC-ROW-IMAGE-PARTIAL)",
			event: rowEv("users", &binlogdata.RowChange{
				Before:      makeRowOmitting([]*string{strptr("7"), nil}),
				After:       makeRowOmitting([]*string{strptr("7"), nil}),
				DataColumns: makeColBitmap([]bool{true, false}),
			}),
			wantCode: sluicecode.CodeCDCRowImagePartial,
		},
		{
			name:     "TINYINT(1) out of range (SLUICE-E-VALUE-TINYINT1-RANGE)",
			event:    rowEv("users", &binlogdata.RowChange{After: makeRowOmitting([]*string{strptr("7"), strptr("2")})}),
			wantCode: sluicecode.CodeValueTinyint1Range,
		},
	}
	ordersRow := rowEv("orders", &binlogdata.RowChange{After: makeRowOmitting([]*string{strptr("42")})})
	excludeUsers := func(_, table string) bool { return table != "users" }

	// Each lane is driven through ONE function so the two hand-mirrored
	// dispatchers are graded by the same assertions.
	type lane struct {
		name string
		// run dispatches the FIELD events, the case event, and the
		// in-scope orders row, returning the case event's error and the
		// row changes emitted overall.
		run func(t *testing.T, allowed func(schema, table string) bool, ev *binlogdata.VEvent) ([]ir.Change, error)
	}
	lanes := []lane{
		{"standalone reader dispatchRow", func(t *testing.T, allowed func(string, string) bool, ev *binlogdata.VEvent) ([]ir.Change, error) {
			t.Helper()
			r := &vstreamCDCReader{keyspace: "main", shards: []string{"-"}, fields: map[string][]*query.Field{}, scopeAllowed: allowed}
			out := make(chan ir.Change, 8)
			ctx := context.Background()
			for _, fe := range []*binlogdata.VEvent{fieldEv("users", usersFields), fieldEv("orders", ordersFields)} {
				if err := r.dispatch(ctx, fe, out); err != nil {
					t.Fatalf("field dispatch: %v", err)
				}
			}
			caseErr := r.dispatch(ctx, ev, out)
			if err := r.dispatch(ctx, ordersRow, out); err != nil {
				t.Fatalf("in-scope orders row: %v", err)
			}
			close(out)
			return drainChannel(out), caseErr
		}},
		{"cold-start snapshot dispatchCDCRow", func(t *testing.T, allowed func(string, string) bool, ev *binlogdata.VEvent) ([]ir.Change, error) {
			t.Helper()
			s := &vstreamSnapshotStream{
				fields: map[string][]*query.Field{
					fieldCacheKey("-", "users"):  usersFields,
					fieldCacheKey("-", "orders"): ordersFields,
				},
				scopeAllowed: allowed,
			}
			out := make(chan ir.Change, 8)
			ctx := context.Background()
			caseErr := s.dispatchCDCRow(ctx, ev, out)
			if err := s.dispatchCDCRow(ctx, ordersRow, out); err != nil {
				t.Fatalf("in-scope orders row: %v", err)
			}
			close(out)
			return drainChannel(out), caseErr
		}},
	}

	for _, ln := range lanes {
		for _, tc := range cases {
			t.Run(ln.name+"/"+tc.name, func(t *testing.T) {
				t.Run("excluded table is dropped whole and the stream lives", func(t *testing.T) {
					got, err := ln.run(t, excludeUsers, tc.event)
					if err != nil {
						t.Fatalf("a rows event for a table the sync EXCLUDES killed the stream: %v (audit 2026-09-15 A0915-ARCH-MEDIUM-3)", err)
					}
					if len(got) != 1 {
						t.Fatalf("emitted %d changes; want exactly 1 (the in-scope orders row)", len(got))
					}
					if ins, ok := got[0].(ir.Insert); !ok || ins.Table != "orders" {
						t.Fatalf("the surviving change is %T %v; want the orders Insert", got[0], got[0])
					}
				})
				t.Run("included table still refuses", func(t *testing.T) {
					got, err := ln.run(t, func(string, string) bool { return true }, tc.event)
					ce, ok := sluicecode.FromError(err)
					if err == nil || !ok || ce.Code != tc.wantCode {
						t.Fatalf("want %s; got %T: %v", tc.wantCode, err, err)
					}
					if len(got) != 1 {
						t.Fatalf("emitted %d changes; want 1 (only the orders row; the refused event emits nothing)", len(got))
					}
				})
				t.Run("nil predicate refuses (the fail-loud direction)", func(t *testing.T) {
					_, err := ln.run(t, nil, tc.event)
					ce, ok := sluicecode.FromError(err)
					if err == nil || !ok || ce.Code != tc.wantCode {
						t.Fatalf("want %s; got %T: %v", tc.wantCode, err, err)
					}
				})
			})
		}
	}
}
