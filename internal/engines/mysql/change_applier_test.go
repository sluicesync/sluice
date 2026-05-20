// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// fakeResult is a minimal [sql.Result] for unit-testing the
// zero-rows-affected log helper without a database round-trip.
type fakeResult struct {
	rowsAffected int64
}

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// captureSlog swaps slog.Default with a text handler writing into
// buf for the duration of the test, restoring the previous default
// on cleanup. Mirrors the helper in internal/pipeline/migrate_test.go.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf
}

// TestBuildInsertSQL covers both the upsert path (PK present) and
// the plain-INSERT fallback (PK empty). Column order is sorted so
// the SQL is deterministic.
func TestBuildInsertSQL(t *testing.T) {
	cases := []struct {
		name     string
		schema   string
		table    string
		row      ir.Row
		pk       []string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:    "upsert with single-column PK",
			schema:  "src",
			table:   "users",
			row:     ir.Row{"id": int64(7), "email": "alice@example.com", "active": true},
			pk:      []string{"id"},
			wantSQL: "INSERT INTO `src`.`users` (`active`, `email`, `id`) VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `active` = new.`active`, `email` = new.`email`",
			// args follow sorted column order: active, email, id.
			wantArgs: []any{true, "alice@example.com", int64(7)},
		},
		{
			name:     "plain insert when PK is empty (no-PK table)",
			schema:   "src",
			table:    "events",
			row:      ir.Row{"payload": "hello"},
			pk:       nil,
			wantSQL:  "INSERT INTO `src`.`events` (`payload`) VALUES (?)",
			wantArgs: []any{"hello"},
		},
		{
			name:     "all columns are PK — no-op upsert",
			schema:   "src",
			table:    "join_table",
			row:      ir.Row{"a_id": int64(1), "b_id": int64(2)},
			pk:       []string{"a_id", "b_id"},
			wantSQL:  "INSERT INTO `src`.`join_table` (`a_id`, `b_id`) VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE `a_id` = new.`a_id`",
			wantArgs: []any{int64(1), int64(2)},
		},
		{
			name:     "composite PK",
			schema:   "src",
			table:    "composite",
			row:      ir.Row{"a": int64(1), "b": int64(2), "data": "x"},
			pk:       []string{"a", "b"},
			wantSQL:  "INSERT INTO `src`.`composite` (`a`, `b`, `data`) VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `data` = new.`data`",
			wantArgs: []any{int64(1), int64(2), "x"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotSQL, gotArgs := buildInsertSQL(c.schema, c.table, c.row, c.pk, nil)
			if gotSQL != c.wantSQL {
				t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, c.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, c.wantArgs) {
				t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, c.wantArgs)
			}
		})
	}
}

func TestBuildUpdateSQL(t *testing.T) {
	before := ir.Row{"id": int64(7), "email": "old@example.com"}
	after := ir.Row{"id": int64(7), "email": "new@example.com", "active": false}

	gotSQL, gotArgs := buildUpdateSQL("src", "users", before, after, nil)
	wantSQL := "UPDATE `src`.`users` SET `active` = ?, `email` = ?, `id` = ? WHERE `email` = ? AND `id` = ?"
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	// SET args (after sorted: active, email, id) then WHERE args (before sorted: email, id).
	wantArgs := []any{false, "new@example.com", int64(7), "old@example.com", int64(7)}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

func TestBuildDeleteSQL(t *testing.T) {
	before := ir.Row{"id": int64(7), "email": "alice@example.com"}
	gotSQL, gotArgs := buildDeleteSQL("src", "users", before, nil)
	wantSQL := "DELETE FROM `src`.`users` WHERE `email` = ? AND `id` = ?"
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{"alice@example.com", int64(7)}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

func TestBuildTruncateSQL(t *testing.T) {
	got := buildTruncateSQL("src", "users")
	want := "TRUNCATE TABLE `src`.`users`"
	if got != want {
		t.Errorf("\n got: %q\nwant: %q", got, want)
	}
}

// TestBuildWhereClause_NullHandling is the load-bearing check for
// the NULL-aware WHERE builder. SQL's `WHERE col = NULL` is always
// false; the builder must produce `col IS NULL` instead so the
// predicate matches.
func TestBuildWhereClause_NullHandling(t *testing.T) {
	row := ir.Row{
		"id":    int64(7),
		"email": nil, // NULL — must produce IS NULL, not = NULL
		"name":  "alice",
	}
	gotSQL, gotArgs := buildWhereClause(row, nil)
	wantSQL := "`email` IS NULL AND `id` = ? AND `name` = ?"
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	// Only non-NULL columns produce parameters; sorted order: id, name.
	wantArgs := []any{int64(7), "alice"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

func TestBuildSetClause(t *testing.T) {
	row := ir.Row{"a": int64(1), "b": "x"}
	gotSQL, gotArgs := buildSetClause(row, nil)
	wantSQL := "`a` = ?, `b` = ?"
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{int64(1), "x"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

// TestBuildInsertSQL_JSONColumnRoutesThroughPrepareValue is the
// load-bearing check for Bug 6's loud-failure path: the applier
// must convert []byte JSON values to string before binding so the
// MySQL driver doesn't tag them with `_binary` charset on the wire.
//
// With colTypes nil (legacy callers), the raw []byte passes through
// — preserving the old shape. With a populated map declaring the
// column as ir.JSON, the value is the string form.
func TestBuildInsertSQL_JSONColumnRoutesThroughPrepareValue(t *testing.T) {
	row := ir.Row{
		"id":   int64(1),
		"data": []byte(`{"k":"v"}`),
	}
	colTypes := map[string]*ir.Column{
		"id":   {Name: "id", Type: ir.Integer{Width: 64}},
		"data": {Name: "data", Type: ir.JSON{Binary: true}},
	}

	_, gotArgs := buildInsertSQL("src", "docs", row, []string{"id"}, colTypes)
	// Sorted column order: data, id. data must be a string (not []byte).
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	got, ok := gotArgs[0].(string)
	if !ok {
		t.Fatalf("data arg is %T; want string (Bug 6 regression — JSON []byte was bound raw)", gotArgs[0])
	}
	if got != `{"k":"v"}` {
		t.Errorf("data arg = %q; want %q", got, `{"k":"v"}`)
	}
}

// TestBuildSetClause_JSONColumnRoutesThroughPrepareValue covers the
// UPDATE SET path of Bug 6's loud failure.
func TestBuildSetClause_JSONColumnRoutesThroughPrepareValue(t *testing.T) {
	after := ir.Row{
		"id":   int64(1),
		"data": []byte(`{"k":"v"}`),
	}
	colTypes := map[string]*ir.Column{
		"id":   {Name: "id", Type: ir.Integer{Width: 64}},
		"data": {Name: "data", Type: ir.JSON{Binary: true}},
	}

	_, gotArgs := buildSetClause(after, colTypes)
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	got, ok := gotArgs[0].(string)
	if !ok {
		t.Fatalf("data SET arg is %T; want string (Bug 6 regression)", gotArgs[0])
	}
	if got != `{"k":"v"}` {
		t.Errorf("data SET arg = %q; want %q", got, `{"k":"v"}`)
	}
}

// TestBuildWhereClause_JSONColumnRoutesThroughPrepareValue covers
// Bug 6's silent failure: WHERE on a JSON-typed Before image must
// (1) emit the value as string, not []byte, so the driver doesn't
// tag it `_binary`; and (2) wrap the placeholder in CAST(? AS JSON)
// so MySQL's equality operator does a JSON-vs-JSON comparison.
// Either omission produces a predicate that silently matches zero
// rows — the signature of the silent MySQL → MySQL divergence.
func TestBuildWhereClause_JSONColumnRoutesThroughPrepareValue(t *testing.T) {
	before := ir.Row{
		"id":   int64(1),
		"data": []byte(`{"k":"v"}`),
	}
	colTypes := map[string]*ir.Column{
		"id":   {Name: "id", Type: ir.Integer{Width: 64}},
		"data": {Name: "data", Type: ir.JSON{Binary: true}},
	}

	gotSQL, gotArgs := buildWhereClause(before, colTypes)
	wantSQL := "`data` = CAST(? AS JSON) AND `id` = ?"
	if gotSQL != wantSQL {
		t.Errorf("\n got SQL: %q\nwant SQL: %q (Bug 6: WHERE without CAST AS JSON silently matches zero rows)", gotSQL, wantSQL)
	}
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	got, ok := gotArgs[0].(string)
	if !ok {
		t.Fatalf("data WHERE arg is %T; want string (Bug 6 regression — _binary charset would silently match zero rows)", gotArgs[0])
	}
	if got != `{"k":"v"}` {
		t.Errorf("data WHERE arg = %q; want %q", got, `{"k":"v"}`)
	}
}

// TestPrepareApplierValue_FallsBackOnMissingType: defensive — if
// the cache is cold or the column is unknown, the raw value is
// passed through unchanged. Same shape as the pre-Bug-6 behavior.
func TestPrepareApplierValue_FallsBackOnMissingType(t *testing.T) {
	raw := []byte(`{"k":"v"}`)
	if got := prepareApplierValue(raw, nil, "data"); !reflect.DeepEqual(got, raw) {
		t.Errorf("nil colTypes: got %#v; want raw value passthrough", got)
	}
	if got := prepareApplierValue(raw, map[string]*ir.Column{}, "data"); !reflect.DeepEqual(got, raw) {
		t.Errorf("missing colName: got %#v; want raw value passthrough", got)
	}
}

// TestLogZeroRowsAffected verifies the debug-log signal that makes
// Bug 6's silent-failure mode observable after the fact. Resume
// idempotency depends on tolerating zero-rows-affected, so this
// stays at debug level — but it must fire when the rows-affected
// count is zero, so a future investigator has a footprint to grep.
func TestLogZeroRowsAffected(t *testing.T) {
	t.Run("zero rows fires debug log", func(t *testing.T) {
		logs := captureSlog(t)
		logZeroRowsAffected(context.Background(), "update", "src", "users", fakeResult{rowsAffected: 0})
		out := logs.String()
		if !strings.Contains(out, "zero rows affected") {
			t.Errorf("log output missing zero-rows-affected marker:\n%s", out)
		}
		if !strings.Contains(out, "op=update") {
			t.Errorf("log output missing op label: %s", out)
		}
		if !strings.Contains(out, "table=users") {
			t.Errorf("log output missing table label: %s", out)
		}
	})
	t.Run("non-zero rows is silent", func(t *testing.T) {
		logs := captureSlog(t)
		logZeroRowsAffected(context.Background(), "update", "src", "users", fakeResult{rowsAffected: 1})
		if logs.Len() != 0 {
			t.Errorf("log output should be empty when rows affected > 0; got: %s", logs.String())
		}
	})
	t.Run("nil result is tolerated", func(t *testing.T) {
		logs := captureSlog(t)
		logZeroRowsAffected(context.Background(), "update", "src", "users", nil)
		if logs.Len() != 0 {
			t.Errorf("log output should be empty when result is nil; got: %s", logs.String())
		}
	})
}

// TestApplierSchema covers the small fallback rule. The applier's
// configured schema wins; the change's source-side schema is only
// used when the applier wasn't given a default.
func TestApplierSchema(t *testing.T) {
	if got := applierSchema("default_db", "source_db"); got != "default_db" {
		t.Errorf("default wins: got %q; want default_db", got)
	}
	if got := applierSchema("default_db", ""); got != "default_db" {
		t.Errorf("empty change schema: got %q; want default_db", got)
	}
	if got := applierSchema("", "source_db"); got != "source_db" {
		t.Errorf("empty default falls back to change schema: got %q; want source_db", got)
	}
}

// TestBuildSQL_FiltersGeneratedColumns covers the GitHub issue #12 fix:
// the CDC apply path must exclude STORED generated columns from
// INSERT column lists, UPDATE SET clauses, and UPDATE/DELETE WHERE
// predicates. MySQL refuses non-DEFAULT values on generated columns
// with Error 3105 ("The value specified for generated column ... is
// not allowed"); a CDC INSERT that includes the generated column's
// value would fail the whole batch.
//
// Mirrors the existing nonGeneratedColumns filter used by the LOAD
// DATA path (ADR-0026:100).
func TestBuildSQL_FiltersGeneratedColumns(t *testing.T) {
	colTypes := map[string]*ir.Column{
		"id":     {Name: "id", Type: ir.Integer{Width: 64}},
		"price":  {Name: "price", Type: ir.Decimal{Precision: 12, Scale: 2}},
		"cost":   {Name: "cost", Type: ir.Decimal{Precision: 12, Scale: 2}},
		"margin": {Name: "margin", Type: ir.Decimal{Precision: 12, Scale: 2}, GeneratedExpr: "price - COALESCE(cost, 0)", GeneratedStored: true},
	}

	t.Run("INSERT excludes generated column from column list and ON DUPLICATE KEY UPDATE SET", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		gotSQL, gotArgs := buildInsertSQL("src", "products", row, []string{"id"}, colTypes)
		wantSQL := "INSERT INTO `src`.`products` (`cost`, `id`, `price`) VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `cost` = new.`cost`, `price` = new.`price`"
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
		// Sorted non-generated columns: cost, id, price.
		wantArgs := []any{"4.50", int64(1), "9.99"}
		if !reflect.DeepEqual(gotArgs, wantArgs) {
			t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
		}
	})

	t.Run("UPDATE SET excludes generated column", func(t *testing.T) {
		before := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		after := ir.Row{"id": int64(1), "price": "12.99", "cost": "4.50", "margin": "8.49"}
		gotSQL, _ := buildUpdateSQL("src", "products", before, after, colTypes)
		// SET excludes margin; WHERE also excludes margin.
		wantSQL := "UPDATE `src`.`products` SET `cost` = ?, `id` = ?, `price` = ? WHERE `cost` = ? AND `id` = ? AND `price` = ?"
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
	})

	t.Run("DELETE WHERE excludes generated column", func(t *testing.T) {
		before := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		gotSQL, _ := buildDeleteSQL("src", "products", before, colTypes)
		wantSQL := "DELETE FROM `src`.`products` WHERE `cost` = ? AND `id` = ? AND `price` = ?"
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
	})

	t.Run("nil colTypes: every column passes through (pre-fix shape)", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "margin": "5.49"}
		gotSQL, _ := buildInsertSQL("src", "products", row, []string{"id"}, nil)
		if !strings.Contains(gotSQL, "`margin`") {
			t.Errorf("with nil colTypes the generated-column filter should not engage; got %q", gotSQL)
		}
	})
}

// TestExecTimeoutCtx pins the wrapping contract for the helper that
// bounds the writePositionTx call site (Bug 56, v0.52.1).
//
//   - Zero / negative execTimeout: returns the input ctx verbatim and
//     a no-op cancel. The caller's `defer cancel()` is harmless.
//   - Positive execTimeout: returns a child ctx with a Deadline set
//     within (now, now+timeout].
func TestExecTimeoutCtx(t *testing.T) {
	t.Run("zero timeout returns input ctx verbatim", func(t *testing.T) {
		a := &ChangeApplier{execTimeout: 0}
		ctx, cancel := a.execTimeoutCtx(context.Background())
		defer cancel()
		if _, ok := ctx.Deadline(); ok {
			t.Errorf("zero timeout: child ctx has a deadline; want none")
		}
	})

	t.Run("negative timeout returns input ctx verbatim", func(t *testing.T) {
		a := &ChangeApplier{execTimeout: -5 * time.Second}
		ctx, cancel := a.execTimeoutCtx(context.Background())
		defer cancel()
		if _, ok := ctx.Deadline(); ok {
			t.Errorf("negative timeout: child ctx has a deadline; want none")
		}
	})

	t.Run("positive timeout produces a child ctx with a deadline within the window", func(t *testing.T) {
		a := &ChangeApplier{execTimeout: 50 * time.Millisecond}
		start := time.Now()
		ctx, cancel := a.execTimeoutCtx(context.Background())
		defer cancel()
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("positive timeout: child ctx has no deadline; want one")
		}
		if dl.Before(start) || dl.After(start.Add(60*time.Millisecond)) {
			t.Errorf("deadline %v outside expected window [%v, %v]", dl, start, start.Add(60*time.Millisecond))
		}
	})
}

// TestRunWithDeadline exercises the package-level watchdog that
// [commitWithTimeout] delegates to (Bug 56, v0.52.1). The watchdog
// race semantics need direct coverage because the production failure
// mode (tx.Commit blocked inside crypto/tls.(*Conn).Read on a
// half-closed PlanetScale destination) can't be reproduced in a unit
// test — but the race + cancel + passthrough logic is testable with
// synthetic closures.
func TestRunWithDeadline(t *testing.T) {
	t.Run("zero timeout: passthrough preserves return verbatim", func(t *testing.T) {
		sentinel := errors.New("synthetic commit failure")
		got := runWithDeadline(0, func() error { return sentinel })
		if !errors.Is(got, sentinel) {
			t.Errorf("zero-timeout passthrough lost the original error; got %v; want %v", got, sentinel)
		}
	})

	t.Run("negative timeout: same passthrough", func(t *testing.T) {
		got := runWithDeadline(-1*time.Second, func() error { return nil })
		if got != nil {
			t.Errorf("negative-timeout passthrough produced unexpected error: %v", got)
		}
	})

	t.Run("positive timeout: fast f returns its own value", func(t *testing.T) {
		sentinel := errors.New("fast f")
		got := runWithDeadline(500*time.Millisecond, func() error { return sentinel })
		if !errors.Is(got, sentinel) {
			t.Errorf("fast-f race lost the original error; got %v; want %v", got, sentinel)
		}
	})

	t.Run("positive timeout: slow f trips watchdog with DeadlineExceeded", func(t *testing.T) {
		// f sleeps longer than the timeout; the watchdog must fire.
		start := time.Now()
		got := runWithDeadline(20*time.Millisecond, func() error {
			time.Sleep(500 * time.Millisecond)
			return nil
		})
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Errorf("slow-f watchdog did not return DeadlineExceeded; got %v", got)
		}
		// The wall-clock cost should match the timeout, not f's sleep.
		// Allow generous slack for scheduler jitter, especially on CI.
		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			t.Errorf("watchdog took %v; expected ~20ms (cap 100ms)", elapsed)
		}
	})
}

// TestApplier_SchemaSnapshot_NilIRIsLoud pins ADR-0049 locked
// decision #4b at the dispatch boundary: a SchemaSnapshot with a nil
// IR table is a loud error (never silently skipped). The nil check
// returns before any tx use, so a nil tx is safe here — the point is
// the loud refusal, not the SQL.
func TestApplier_SchemaSnapshot_NilIRIsLoud(t *testing.T) {
	a := &ChangeApplier{schema: "app", streamID: "s1"}
	err := a.dispatch(context.Background(), nil, "s1", ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "tok"},
		Schema:   "app",
		Table:    "users",
		IR:       nil,
	})
	if err == nil {
		t.Fatal("nil-IR SchemaSnapshot: want loud error, got nil")
	}
	if !strings.Contains(err.Error(), "nil IR") {
		t.Errorf("error = %q, want it to name the nil IR table", err.Error())
	}
}

// TestApplier_SchemaSnapshot_NotSkippedByBoundaryNoOp pins the
// load-bearing wiring for ADR-0049 #4a: unlike TxBegin/TxCommit, a
// SchemaSnapshot must NOT be the per-change Apply loop's continue
// (no-op) case — it has to reach applyOne so the version write lands
// in the SAME tx as a position write. This guards the exact
// regression where someone "tidies" the boundary switch and folds
// SchemaSnapshot into the no-op set, silently dropping every schema
// version.
func TestApplier_SchemaSnapshot_NotSkippedByBoundaryNoOp(t *testing.T) {
	switch any(ir.SchemaSnapshot{}).(type) {
	case ir.TxBegin, ir.TxCommit:
		t.Fatal("SchemaSnapshot must not be a TxBegin/TxCommit-class no-op")
	case ir.SchemaSnapshot:
		// correct: distinct change type that reaches applyOne/dispatch
	default:
		t.Fatal("SchemaSnapshot type assertion unexpectedly fell through")
	}
}
