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

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
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

// TestApplier_RoutedSchema_BackCompatClass pins the ADR-0074 Phase 1b
// namespace-routing class on the Postgres applier (part B). Mirror of
// the MySQL pin — the reviewer re-derives the same back-compat matrix
// on BOTH engines. Routing OFF must be byte-identical (bound schema
// always); routing ON qualifies ONLY across DIFFERING non-empty
// namespaces.
//
// The cross-engine single-database trap is the third OFF row: a
// namespaced source (e.g. another PG, or a MySQL source whose binlog
// reader stamps the source db) whose Change.Schema differs from the
// bound target schema must stay bound when routing is OFF.
func TestApplier_RoutedSchema_BackCompatClass(t *testing.T) {
	cases := []struct {
		name         string
		bound        string
		routing      bool
		changeSchema string
		want         string
	}{
		{"off: empty change schema -> bound", "public", false, "", "public"},
		{"off: change schema == bound -> bound", "public", false, "public", "public"},
		{"off: differing schema -> bound (NO over-qualify)", "public", false, "app_db", "public"},
		{"on: empty change schema -> bound (bare)", "public", true, "", "public"},
		{"on: change schema == bound -> bound (bare)", "public", true, "public", "public"},
		{"on: differing non-empty schema -> qualified", "public", true, "app_db", "app_db"},
		{"off: unbound applier -> change schema fallback", "", false, "src", "src"},
		{"on: unbound applier -> change schema fallback", "", true, "src", "src"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			a := &ChangeApplier{schema: c.bound, multiDBRouting: c.routing}
			if got := a.routedSchema(c.changeSchema); got != c.want {
				t.Errorf("routedSchema(%q) with bound=%q routing=%v = %q; want %q",
					c.changeSchema, c.bound, c.routing, got, c.want)
			}
		})
	}
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
			gotSQL, gotArgs, err := buildInsertSQL(c.schema, c.table, c.row, c.pk, nil)
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

// TestBuildSQL_VerbatimTypeCasts pins the v0.92.3 Bug 97 wire-encoding
// closure. INSERT VALUES, UPDATE SET, and WHERE-equality must emit
// `$N::<verbatim-type>` casts for ir.VerbatimType columns so pgx
// binds the value as text instead of falling through to bytea
// encoding (which produced `\x…` hex literals on the wire and PG
// rejected with `invalid input syntax for type money` / `pg_lsn`).
// Every verbatim-eligible family is exercised — money / pg_lsn
// (the cycle-observed failures) plus xml / tsvector / int4range
// (cycle-observed pass; the cast hardens them too) plus pg_snapshot
// / txid_snapshot / multirange (likely-affected families not
// exercised in the cycle but structurally identical).
func TestBuildSQL_VerbatimTypeCasts(t *testing.T) {
	colTypes := map[string]*ir.Column{
		"id":    {Name: "id", Type: ir.Integer{Width: 64}},
		"price": {Name: "price", Type: ir.VerbatimType{Definition: "money"}},
		"lsn":   {Name: "lsn", Type: ir.VerbatimType{Definition: "pg_lsn"}},
		"doc":   {Name: "doc", Type: ir.VerbatimType{Definition: "xml"}},
		"r":     {Name: "r", Type: ir.VerbatimType{Definition: "int4range"}},
		"v":     {Name: "v", Type: ir.VerbatimType{Definition: "tsvector"}},
	}

	t.Run("INSERT VALUES placeholders carry the cast", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "price": "$99.99", "lsn": "0/16B3748", "doc": "<a/>", "r": "[1,10)", "v": "'foo':1"}
		gotSQL, _, err := buildInsertSQL("public", "t", row, []string{"id"}, colTypes)
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		for _, want := range []string{
			`"doc", "id", "lsn", "price", "r", "v"`,
			`$1::xml`,
			`$2`,
			`$3::pg_lsn`,
			`$4::money`,
			`$5::int4range`,
			`$6::tsvector`,
		} {
			if !strings.Contains(gotSQL, want) {
				t.Errorf("buildInsertSQL output missing %q\nfull SQL: %s", want, gotSQL)
			}
		}
	})

	t.Run("UPDATE SET clause carries the cast", func(t *testing.T) {
		before := ir.Row{"id": int64(1)}
		after := ir.Row{"id": int64(1), "price": "$50.00", "lsn": "0/100"}
		gotSQL, _, err := buildUpdateSQL("public", "t", before, after, colTypes)
		if err != nil {
			t.Fatalf("buildUpdateSQL: %v", err)
		}
		for _, want := range []string{
			`"lsn" = $2::pg_lsn`,
			`"price" = $3::money`,
		} {
			if !strings.Contains(gotSQL, want) {
				t.Errorf("buildUpdateSQL output missing %q\nfull SQL: %s", want, gotSQL)
			}
		}
	})

	t.Run("WHERE equality predicate casts both sides", func(t *testing.T) {
		before := ir.Row{"id": int64(1), "price": "$50.00"}
		gotSQL, _, err := buildDeleteSQL("public", "t", before, colTypes)
		if err != nil {
			t.Fatalf("buildDeleteSQL: %v", err)
		}
		// Money column matched via canonical text form.
		if !strings.Contains(gotSQL, `"price"::text = $2::money::text`) {
			t.Errorf("WHERE predicate missing cast on both sides; got: %s", gotSQL)
		}
	})

	t.Run("non-verbatim columns keep bare $N", func(t *testing.T) {
		row := ir.Row{"plain": "x"}
		colTypesBare := map[string]*ir.Column{"plain": {Name: "plain", Type: ir.Text{Size: ir.TextLong}}}
		gotSQL, _, err := buildInsertSQL("public", "t", row, nil, colTypesBare)
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		if !strings.Contains(gotSQL, `VALUES ($1)`) || strings.Contains(gotSQL, `::`) {
			t.Errorf("plain text column should use bare $1 placeholder; got: %s", gotSQL)
		}
	})
}

// TestPrepareApplierValue_VerbatimTypeBytesBecomeString pins the v0.92.4
// Bug 97 wire-encoding REDO. v0.92.3 added explicit `$N::TYPE` casts in
// the apply SQL but the cycle subagent found the fix didn't actually
// close the bug for money/pg_lsn: PG received the value as bytea
// (because pgx binds Go `[]byte` as bytea) and the implicit
// `bytea → TYPE` cast fails through the `\x…` text form. prepareApplierValue
// must convert `[]byte` to `string` for ir.VerbatimType columns so pgx
// binds as text and PG's `text::TYPE` parse sees the canonical form.
func TestPrepareApplierValue_VerbatimTypeBytesBecomeString(t *testing.T) {
	colTypes := map[string]*ir.Column{
		"price": {Name: "price", Type: ir.VerbatimType{Definition: "money"}},
		"lsn":   {Name: "lsn", Type: ir.VerbatimType{Definition: "pg_lsn"}},
		"doc":   {Name: "doc", Type: ir.VerbatimType{Definition: "xml"}},
		"plain": {Name: "plain", Type: ir.Text{Size: ir.TextLong}},
	}

	cases := []struct {
		name string
		col  string
		in   any
		want any
	}{
		{
			name: "money bytes → string",
			col:  "price",
			in:   []byte("$99.99"),
			want: "$99.99",
		},
		{
			name: "pg_lsn bytes → string",
			col:  "lsn",
			in:   []byte("0/3000000"),
			want: "0/3000000",
		},
		{
			name: "xml bytes → string (uniform across families)",
			col:  "doc",
			in:   []byte("<a/>"),
			want: "<a/>",
		},
		{
			name: "money string → string (idempotent)",
			col:  "price",
			in:   "$50.00",
			want: "$50.00",
		},
		{
			name: "plain text bytes preserved as bytes (not a verbatim column)",
			col:  "plain",
			in:   []byte("hello"),
			want: []byte("hello"),
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareApplierValue(c.in, colTypes, c.col)
			if err != nil {
				t.Fatalf("prepareApplierValue: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v (%T); want %#v (%T)", got, got, c.want, c.want)
			}
		})
	}
}

func TestBuildUpdateSQL(t *testing.T) {
	before := ir.Row{"id": int64(7), "email": "old@example.com"}
	after := ir.Row{"id": int64(7), "email": "new@example.com", "active": false}

	gotSQL, gotArgs, err := buildUpdateSQL("public", "users", before, after, nil)
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
	gotSQL, gotArgs, err := buildDeleteSQL("public", "users", before, nil)
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
	// Bug 98 (v0.92.0) — TRUNCATE flag plumbing. Each combination of
	// CASCADE / RESTART IDENTITY produces the correct PG clause order
	// (RESTART IDENTITY before CASCADE per PG's grammar).
	cases := []struct {
		name            string
		cascade         bool
		restartIdentity bool
		want            string
	}{
		{"plain", false, false, `TRUNCATE TABLE "public"."users"`},
		{"cascade", true, false, `TRUNCATE TABLE "public"."users" CASCADE`},
		{"restart_identity", false, true, `TRUNCATE TABLE "public"."users" RESTART IDENTITY`},
		{"both", true, true, `TRUNCATE TABLE "public"."users" RESTART IDENTITY CASCADE`},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := buildTruncateSQL("public", "users", c.cascade, c.restartIdentity)
			if got != c.want {
				t.Errorf("\n got: %q\nwant: %q", got, c.want)
			}
		})
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
	gotSQL, gotArgs, err := buildWhereClause(row, 1, nil)
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
	gotSQL, _, err := buildWhereClause(row, 5, nil)
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
	gotSQL, gotArgs, err := buildSetClause(row, 1, nil)
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
	colTypes := map[string]*ir.Column{
		"id":   {Name: "id", Type: ir.Integer{Width: 64}},
		"tags": {Name: "tags", Type: ir.Array{Element: ir.Text{Size: ir.TextLong}}},
	}

	_, gotArgs, err := buildInsertSQL("public", "docs", row, []string{"id"}, colTypes)
	if err != nil {
		t.Fatalf("buildInsertSQL: %v", err)
	}
	// Sorted order: id, tags. tags must be a pgtype.Array[*pgtype.Text]
	// after prepareValue runs; the raw []any would fail pgx
	// serialization. Bug 70/74: convertArray returns pgtype.Array[*T]
	// (pointer elements for NULL survival, explicit Dims for multi-dim
	// fidelity); the string-shaped families use a *pgtype.Text leaf so
	// pgx's array codec preserves dimensions for every element OID
	// (a bare *string silently flattens ≥2-D for uuid/inet/cidr/...).
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	if _, ok := gotArgs[1].(pgtype.Array[*pgtype.Text]); !ok {
		t.Errorf("tags arg is %T; want pgtype.Array[*pgtype.Text] (Bug 6 mirror — array []any wasn't routed through prepareValue)", gotArgs[1])
	}
}

// TestBuildWhereClause_RoutesThroughPrepareValue covers the WHERE
// path of the PG mirror of the Bug-6 fix.
func TestBuildWhereClause_RoutesThroughPrepareValue(t *testing.T) {
	before := ir.Row{
		"id":   int64(1),
		"tags": []any{"a", "b"},
	}
	colTypes := map[string]*ir.Column{
		"id":   {Name: "id", Type: ir.Integer{Width: 64}},
		"tags": {Name: "tags", Type: ir.Array{Element: ir.Text{Size: ir.TextLong}}},
	}

	_, gotArgs, err := buildWhereClause(before, 1, colTypes)
	if err != nil {
		t.Fatalf("buildWhereClause: %v", err)
	}
	if len(gotArgs) != 2 {
		t.Fatalf("args length = %d; want 2", len(gotArgs))
	}
	if _, ok := gotArgs[1].(pgtype.Array[*pgtype.Text]); !ok {
		t.Errorf("tags WHERE arg is %T; want pgtype.Array[*pgtype.Text]", gotArgs[1])
	}
}

// TestBuildWhereClause_JSONCastUnderReplicaIdentityFull pins the apply
// WHERE clause's type-aware predicate for PG's `json` type. Under
// REPLICA IDENTITY FULL the OldTuple carries every column, so the
// applier emits a predicate for each. PG's `json` (text-backed) has
// NO `=` operator — a bare `col = $N` against it errors with
// `42883 could not identify an equality operator for type json` and
// every UPDATE/DELETE apply against a target with a `json` column
// silently breaks. The fix casts both sides to text for json columns
// only; `jsonb` is unaffected (it has a native `=` operator with
// semantic equality). Mirrors pgcopydb PR #28.
func TestBuildWhereClause_JSONCastUnderReplicaIdentityFull(t *testing.T) {
	before := ir.Row{
		"id":       int64(7),
		"doc_text": `{"a":1}`, // PG json (text-backed) — needs ::text cast
		"doc_bin":  `{"b":2}`, // PG jsonb — uses native `=`
		"label":    "hello",   // plain text — uses native `=`
	}
	colTypes := map[string]*ir.Column{
		"id":       {Name: "id", Type: ir.Integer{Width: 64}},
		"doc_text": {Name: "doc_text", Type: ir.JSON{Binary: false}},
		"doc_bin":  {Name: "doc_bin", Type: ir.JSON{Binary: true}},
		"label":    {Name: "label", Type: ir.Text{Size: ir.TextLong}},
	}

	gotSQL, _, err := buildWhereClause(before, 1, colTypes)
	if err != nil {
		t.Fatalf("buildWhereClause: %v", err)
	}
	// The json column MUST have the ::text cast on both sides.
	if !strings.Contains(gotSQL, `"doc_text"::text = $`) {
		t.Errorf("json column predicate missing ::text cast on LHS; sql=%q", gotSQL)
	}
	if !strings.Contains(gotSQL, `::text`) || !strings.Contains(gotSQL, `::text = $`) {
		t.Errorf("json column predicate missing ::text cast; sql=%q", gotSQL)
	}
	// The `jsonb` column MUST NOT carry the ::text cast — it has a
	// native `=` operator and casting would lose semantic equality.
	if strings.Contains(gotSQL, `"doc_bin"::text`) {
		t.Errorf("jsonb column must not get the ::text cast; sql=%q", gotSQL)
	}
	// Non-json columns must still use the plain `col = $N` form.
	if !strings.Contains(gotSQL, `"id" = $`) || !strings.Contains(gotSQL, `"label" = $`) {
		t.Errorf("non-json column predicates regressed; sql=%q", gotSQL)
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
	got, err = prepareApplierValue(raw, map[string]*ir.Column{}, "data")
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
	colTypes := map[string]*ir.Column{
		"id":     {Name: "id", Type: ir.Integer{Width: 64}},
		"price":  {Name: "price", Type: ir.Decimal{Precision: 12, Scale: 2}},
		"cost":   {Name: "cost", Type: ir.Decimal{Precision: 12, Scale: 2}},
		"margin": {Name: "margin", Type: ir.Decimal{Precision: 12, Scale: 2}, GeneratedExpr: "price - COALESCE(cost, 0)", GeneratedStored: true},
	}

	t.Run("INSERT excludes generated column from column list and ON CONFLICT DO UPDATE SET", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"}
		gotSQL, _, err := buildInsertSQL("public", "products", row, []string{"id"}, colTypes)
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
		gotSQL, _, err := buildUpdateSQL("public", "products", before, after, colTypes)
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
		gotSQL, _, err := buildDeleteSQL("public", "products", before, colTypes)
		if err != nil {
			t.Fatalf("buildDeleteSQL: %v", err)
		}
		wantSQL := `DELETE FROM "public"."products" WHERE "cost" = $1 AND "id" = $2 AND "price" = $3`
		if gotSQL != wantSQL {
			t.Errorf("\n got SQL: %q\nwant SQL: %q", gotSQL, wantSQL)
		}
	})

	t.Run("nil colTypes: every column passes through (pre-fix shape)", func(t *testing.T) {
		row := ir.Row{"id": int64(1), "margin": "5.49"}
		gotSQL, _, err := buildInsertSQL("public", "products", row, []string{"id"}, nil)
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		if !strings.Contains(gotSQL, `"margin"`) {
			t.Errorf("with nil colTypes the generated-column filter should not engage; got %q", gotSQL)
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
