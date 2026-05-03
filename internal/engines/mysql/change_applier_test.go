package mysql

import (
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

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

	gotSQL, gotArgs := buildUpdateSQL("src", "users", before, after)
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
	gotSQL, gotArgs := buildDeleteSQL("src", "users", before)
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
	gotSQL, gotArgs := buildWhereClause(row)
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
	gotSQL, gotArgs := buildSetClause(row)
	wantSQL := "`a` = ?, `b` = ?"
	if gotSQL != wantSQL {
		t.Errorf("\n got: %q\nwant: %q", gotSQL, wantSQL)
	}
	wantArgs := []any{int64(1), "x"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("\n got args: %#v\nwant args: %#v", gotArgs, wantArgs)
	}
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
