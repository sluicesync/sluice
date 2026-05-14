// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

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
// buf for the duration of the test. Mirrors the helper in
// internal/pipeline/migrate_test.go.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf
}

// TestBuildInsertSQL covers both the upsert path (PK present) and
// the plain-INSERT fallback (PK empty), plus the all-PK DO NOTHING
// edge case. Column order is sorted so the SQL is deterministic.
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
			name:     "upsert with single-column PK",
			schema:   "public",
			table:    "users",
			row:      ir.Row{"id": int64(7), "email": "alice@example.com", "active": true},
			pk:       []string{"id"},
			wantSQL:  `INSERT INTO "public"."users" ("active", "email", "id") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "active" = EXCLUDED."active", "email" = EXCLUDED."email"`,
			wantArgs: []any{true, "alice@example.com", int64(7)},
		},
		{
			name:     "plain insert when PK is empty (no-PK table)",
			schema:   "public",
			table:    "events",
			row:      ir.Row{"payload": "hello"},
			pk:       nil,
			wantSQL:  `INSERT INTO "public"."events" ("payload") VALUES ($1)`,
			wantArgs: []any{"hello"},
		},
		{
			name:     "all columns are PK — DO NOTHING on conflict",
			schema:   "public",
			table:    "join_table",
			row:      ir.Row{"a_id": int64(1), "b_id": int64(2)},
			pk:       []string{"a_id", "b_id"},
			wantSQL:  `INSERT INTO "public"."join_table" ("a_id", "b_id") VALUES ($1, $2) ON CONFLICT ("a_id", "b_id") DO NOTHING`,
			wantArgs: []any{int64(1), int64(2)},
		},
		{
			name:     "composite PK",
			schema:   "public",
			table:    "composite",
			row:      ir.Row{"a": int64(1), "b": int64(2), "data": "x"},
			pk:       []string{"a", "b"},
			wantSQL:  `INSERT INTO "public"."composite" ("a", "b", "data") VALUES ($1, $2, $3) ON CONFLICT ("a", "b") DO UPDATE SET "data" = EXCLUDED."data"`,
			wantArgs: []any{int64(1), int64(2), "x"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotSQL, gotArgs, err := buildInsertSQL(c.schema, c.table, c.row, c.pk, nil, nil)
			if err != nil {
				t.Fatalf("buildInsertSQL: %v", err)
			}
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

	gotSQL, gotArgs, err := buildUpdateSQL("public", "users", before, after, nil, nil)
	if err != nil {
		t.Fatalf("buildUpdateSQL: %v", err)
	}
	// SET uses $1..$3 (3 cols), WHERE continues at $4..$5 (2 cols).
	wantSQL := `UPDATE "public"."users" SET "active" = $1, "email" = $2, "id" = $3 WHERE "email" = $4 AND "id" = $5`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{false, "new@example.com", int64(7), "old@example.com", int64(7)}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

func TestBuildDeleteSQL(t *testing.T) {
	before := ir.Row{"id": int64(7), "email": "alice@example.com"}
	gotSQL, gotArgs, err := buildDeleteSQL("public", "users", before, nil, nil)
	if err != nil {
		t.Fatalf("buildDeleteSQL: %v", err)
	}
	wantSQL := `DELETE FROM "public"."users" WHERE "email" = $1 AND "id" = $2`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{"alice@example.com", int64(7)}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

func TestBuildTruncateSQL(t *testing.T) {
	got := buildTruncateSQL("public", "users")
	want := `TRUNCATE TABLE "public"."users"`
	if got != want {
		t.Errorf("\n got: %q\nwant: %q", got, want)
	}
}

// TestBuildWhereClause_NullHandling is the load-bearing check for
// the NULL-aware WHERE builder. The placeholder counter must skip
// NULL columns (no parameter is emitted for them).
func TestBuildWhereClause_NullHandling(t *testing.T) {
	row := ir.Row{
		"id":    int64(7),
		"email": nil, // NULL — must produce IS NULL, not = $N
		"name":  "alice",
	}
	gotSQL, gotArgs, err := buildWhereClause(row, 1, nil, nil)
	if err != nil {
		t.Fatalf("buildWhereClause: %v", err)
	}
	// sorted order: email, id, name. email goes to IS NULL (no
	// param). id is $1, name is $2.
	wantSQL := `"email" IS NULL AND "id" = $1 AND "name" = $2`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{int64(7), "alice"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

// TestBuildWhereClause_StartIdx confirms the placeholder offset is
// honoured — UPDATE SET + WHERE share a numbering sequence.
func TestBuildWhereClause_StartIdx(t *testing.T) {
	row := ir.Row{"id": int64(1)}
	gotSQL, _, err := buildWhereClause(row, 5, nil, nil)
	if err != nil {
		t.Fatalf("buildWhereClause: %v", err)
	}
	wantSQL := `"id" = $5`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
}

func TestBuildSetClause(t *testing.T) {
	row := ir.Row{"a": int64(1), "b": "x"}
	gotSQL, gotArgs, err := buildSetClause(row, 1, nil, nil)
	if err != nil {
		t.Fatalf("buildSetClause: %v", err)
	}
	wantSQL := `"a" = $1, "b" = $2`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{int64(1), "x"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
}

// TestBuildInsertSQL_RoutesThroughPrepareValue is the PG-side
// mirror of the MySQL Bug-6 unit test. PG doesn't have the same
// `_binary` charset failure mode (pgx inspects type metadata), but
// the structural fix is symmetric: the applier must route every
// bound value through prepareValue. The most-visible signal is on
// ir.Array, where the IR's []any must be converted to a typed slice
// before pgx accepts it.
func TestBuildInsertSQL_RoutesThroughPrepareValue(t *testing.T) {
	row := ir.Row{
		"id":   int64(1),
		"tags": []any{"a", "b"},
	}
	colTypes := map[string]ir.Type{
		"id":   ir.Integer{Width: 64},
		"tags": ir.Array{Element: ir.Text{Size: ir.TextLong}},
	}

	_, gotArgs, err := buildInsertSQL("public", "docs", row, []string{"id"}, colTypes, nil)
	if err != nil {
		t.Fatalf("buildInsertSQL: %v", err)
	}
	// Sorted order: id, tags. tags must be a typed []string after
	// prepareValue runs; the raw []any would fail pgx serialization.
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	if _, ok := gotArgs[1].([]string); !ok {
		t.Errorf("tags arg is %T; want []string (Bug 6 mirror — array []any wasn't routed through prepareValue)", gotArgs[1])
	}
}

// TestBuildWhereClause_RoutesThroughPrepareValue covers the WHERE
// path of the PG mirror of the Bug-6 fix.
func TestBuildWhereClause_RoutesThroughPrepareValue(t *testing.T) {
	before := ir.Row{
		"id":   int64(1),
		"tags": []any{"a", "b"},
	}
	colTypes := map[string]ir.Type{
		"id":   ir.Integer{Width: 64},
		"tags": ir.Array{Element: ir.Text{Size: ir.TextLong}},
	}

	_, gotArgs, err := buildWhereClause(before, 1, colTypes, nil)
	if err != nil {
		t.Fatalf("buildWhereClause: %v", err)
	}
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	if _, ok := gotArgs[1].([]string); !ok {
		t.Errorf("tags WHERE arg is %T; want []string", gotArgs[1])
	}
}

// TestPrepareApplierValue_FallsBackOnMissingType: defensive —
// matches the MySQL applier's behavior. Cache cold or column
// unknown → raw value passes through.
func TestPrepareApplierValue_FallsBackOnMissingType(t *testing.T) {
	raw := []byte("anything")
	got, err := prepareApplierValue(raw, nil, "data")
	if err != nil {
		t.Fatalf("nil colTypes: unexpected err: %v", err)
	}
	if !reflect.DeepEqual(got, raw) {
		t.Errorf("nil colTypes: got %#v; want raw value passthrough", got)
	}
	got, err = prepareApplierValue(raw, map[string]ir.Type{}, "data")
	if err != nil {
		t.Fatalf("missing colName: unexpected err: %v", err)
	}
	if !reflect.DeepEqual(got, raw) {
		t.Errorf("missing colName: got %#v; want raw value passthrough", got)
	}
}

// TestLogZeroRowsAffected mirrors the MySQL applier's same test:
// debug-log fires when rows affected = 0, silent otherwise.
func TestLogZeroRowsAffected(t *testing.T) {
	t.Run("zero rows fires debug log", func(t *testing.T) {
		logs := captureSlog(t)
		logZeroRowsAffected(context.Background(), "update", "public", "users", fakeResult{rowsAffected: 0})
		out := logs.String()
		if !strings.Contains(out, "zero rows affected") {
			t.Errorf("log output missing zero-rows-affected marker:\n%s", out)
		}
		if !strings.Contains(out, "op=update") {
			t.Errorf("log output missing op label: %s", out)
		}
	})
	t.Run("non-zero rows is silent", func(t *testing.T) {
		logs := captureSlog(t)
		logZeroRowsAffected(context.Background(), "update", "public", "users", fakeResult{rowsAffected: 1})
		if logs.Len() != 0 {
			t.Errorf("log output should be empty; got: %s", logs.String())
		}
	})
	t.Run("nil result is tolerated", func(t *testing.T) {
		logs := captureSlog(t)
		logZeroRowsAffected(context.Background(), "update", "public", "users", nil)
		if logs.Len() != 0 {
			t.Errorf("log output should be empty; got: %s", logs.String())
		}
	})
}

func TestApplierSchema(t *testing.T) {
	if got := applierSchema("public", "myschema"); got != "public" {
		t.Errorf("default wins: got %q; want public", got)
	}
	if got := applierSchema("public", ""); got != "public" {
		t.Errorf("empty change schema: got %q; want public", got)
	}
	if got := applierSchema("", "myschema"); got != "myschema" {
		t.Errorf("empty default falls back to change schema: got %q; want myschema", got)
	}
}

// TestBuildSQL_FiltersGeneratedColumns covers the GitHub issue #12 fix
// on the PG side: the CDC apply path must exclude GENERATED ALWAYS AS
// (...) STORED columns from INSERT column lists, UPDATE SET clauses,
// and UPDATE/DELETE WHERE predicates. PG rejects non-DEFAULT values
// on generated columns with SQLSTATE 428C9 ("cannot insert a
// non-DEFAULT value into column"); a CDC INSERT that includes the
// generated column's value would fail the whole batch.
func TestBuildSQL_FiltersGeneratedColumns(t *testing.T) {
	colTypes := map[string]ir.Type{
		"id":     ir.Integer{Width: 64},
		"price":  ir.Decimal{Precision: 12, Scale: 2},
		"cost":   ir.Decimal{Precision: 12, Scale: 2},
		"margin": ir.Decimal{Precision: 12, Scale: 2},
	}
	generated := map[string]bool{"margin": true}

	t.Run("INSERT excludes generated column from column list and ON CONFLICT DO UPDATE SET", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		gotSQL, _, err := buildInsertSQL("public", "products", row, []string{"id"}, colTypes, generated)
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		// Sorted non-generated columns: cost, id, price.
		wantSQL := `INSERT INTO "public"."products" ("cost", "id", "price") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "cost" = EXCLUDED."cost", "price" = EXCLUDED."price"`
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
	})

	t.Run("UPDATE SET and WHERE both exclude generated column", func(t *testing.T) {
		before := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		after := ir.Row{"id": int64(1), "price": "12.99", "cost": "4.50", "margin": "8.49"}
		gotSQL, _, err := buildUpdateSQL("public", "products", before, after, colTypes, generated)
		if err != nil {
			t.Fatalf("buildUpdateSQL: %v", err)
		}
		wantSQL := `UPDATE "public"."products" SET "cost" = $1, "id" = $2, "price" = $3 WHERE "cost" = $4 AND "id" = $5 AND "price" = $6`
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
	})

	t.Run("DELETE WHERE excludes generated column", func(t *testing.T) {
		before := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		gotSQL, _, err := buildDeleteSQL("public", "products", before, colTypes, generated)
		if err != nil {
			t.Fatalf("buildDeleteSQL: %v", err)
		}
		wantSQL := `DELETE FROM "public"."products" WHERE "cost" = $1 AND "id" = $2 AND "price" = $3`
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
	})

	t.Run("nil generated map: every column passes through (pre-fix shape)", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "margin": "5.49"}
		gotSQL, _, err := buildInsertSQL("public", "products", row, []string{"id"}, colTypes, nil)
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		if !strings.Contains(gotSQL, `"margin"`) {
			t.Errorf("with nil generated map the filter should not engage; got %q", gotSQL)
		}
	})
}

// TestExecTimeoutCtx pins the wrapping contract for the helper that
// bounds the writePositionTx call site (Bug 56, v0.52.1). Mirror of
// the MySQL test; engines duplicate the helper so the per-engine
// coverage stays local.
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

// TestRunWithDeadline mirrors the MySQL test. The PG engine has its
// own copy of runWithDeadline so the watchdog stays local to the
// package; the tests stay symmetric for the same reason.
func TestRunWithDeadline(t *testing.T) {
	t.Run("zero timeout: passthrough preserves return verbatim", func(t *testing.T) {
		sentinel := errors.New("synthetic commit failure")
		got := runWithDeadline(0, func() error { return sentinel })
		if !errors.Is(got, sentinel) {
			t.Errorf("zero-timeout passthrough lost the original error; got %v; want %v", got, sentinel)
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
		start := time.Now()
		got := runWithDeadline(20*time.Millisecond, func() error {
			time.Sleep(500 * time.Millisecond)
			return nil
		})
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Errorf("slow-f watchdog did not return DeadlineExceeded; got %v", got)
		}
		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			t.Errorf("watchdog took %v; expected ~20ms (cap 100ms)", elapsed)
		}
	})
}
