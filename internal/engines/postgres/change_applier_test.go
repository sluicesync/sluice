package postgres

import (
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

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
			gotSQL, gotArgs := buildInsertSQL(c.schema, c.table, c.row, c.pk)
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

	gotSQL, gotArgs := buildUpdateSQL("public", "users", before, after)
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
	gotSQL, gotArgs := buildDeleteSQL("public", "users", before)
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
	gotSQL, gotArgs := buildWhereClause(row, 1)
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
	gotSQL, _ := buildWhereClause(row, 5)
	wantSQL := `"id" = $5`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
}

func TestBuildSetClause(t *testing.T) {
	row := ir.Row{"a": int64(1), "b": "x"}
	gotSQL, gotArgs := buildSetClause(row, 1)
	wantSQL := `"a" = $1, "b" = $2`
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{int64(1), "x"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
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
