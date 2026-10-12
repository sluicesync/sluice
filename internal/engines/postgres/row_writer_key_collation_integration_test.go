//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-ROWWRITER: the key-collation guard on the idempotent copy
// writer (row_writer_batch.go — a resumed or chunk-resumed copy, add-table,
// a VStream COPY onto Postgres, restore), on a real Postgres 16 with a
// nondeterministic ICU key on the target.
//
// The independent expected value is the target's rows read back by
// encode(convert_to(k,'UTF8'),'hex') after the write.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowWriterKeyCollationGuard_Postgres is the copy writer's matrix on
// Postgres: a nondeterministic ICU key on the target, the source's key
// declared "C", the database default, or another named deterministic
// collation. Every merge through the conflict arbiter — a PRIMARY KEY, a
// table whose every column is its key (whose DO NOTHING would drop the
// row), a NOT NULL UNIQUE key standing in for a missing primary key —
// refuses KEY-COLLATION-MERGE with the target untouched; a merge inside one
// statement is refused by Postgres itself; a merge on a non-arbiter UNIQUE
// key raises unique_violation; a copy with no merge lands byte-exact; a
// re-sent row passes; a MySQL-family source key refuses
// KEY-COLLATION-GUARD-UNSUPPORTED before anything is written.
func TestRowWriterKeyCollationGuard_Postgres(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "krw_pg")
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);`)

	write := func(t *testing.T, table *ir.Table, rows ...ir.Row) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		rw, err := Engine{}.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		ch := make(chan ir.Row, len(rows))
		for _, r := range rows {
			ch <- r
		}
		close(ch)
		return rw.(*RowWriter).WriteRowsIdempotent(ctx, table, ch)
	}
	wantMerge := func(t *testing.T, err error, table, idCol, before string) {
		t.Helper()
		got := kcgPGRows(t, dsn, table, idCol)
		if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), "KEY-COLLATION-MERGE") {
			t.Fatalf("err = %v; want %s (the target holds %q)", err, ir.ErrKeyCollationMerge, got)
		}
		if got != before {
			t.Fatalf("the refusal changed the target: %s → %s", before, got)
		}
	}
	threeCols := func(key ir.Type) []*ir.Column {
		return []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: key},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		}
	}
	pkOn := func(col string) *ir.Index { return &ir.Index{Columns: []ir.IndexColumn{{Column: col}}} }

	for _, src := range []struct {
		name string
		typ  ir.Type
	}{
		{"source C", kcgPGText("C", ir.CollationDeterministic)},
		{"source default", kcgPGText("", ir.CollationDeterministic)},
		{"source ucs_basic", kcgPGText("ucs_basic", ir.CollationDeterministic)},
	} {
		src := src
		prefix := strings.ToLower(strings.TrimPrefix(src.name, "source "))
		t.Run(src.name, func(t *testing.T) {
			t.Run("pk merge against an earlier batch", func(t *testing.T) {
				table := "kd_" + prefix
				applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int); INSERT INTO "+table+" VALUES (1,'a',1);")
				before := kcgPGRows(t, dsn, table, "id")
				err := write(t, &ir.Table{Name: table, Columns: threeCols(src.typ), PrimaryKey: pkOn("k")},
					ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				wantMerge(t, err, table, "id", before)
			})
			t.Run("pk merge inside one statement", func(t *testing.T) {
				// Postgres refuses an ON CONFLICT that would touch a row the
				// same command inserted (cardinality_violation), before the
				// guard's WHERE is consulted: loud without the guard.
				table := "ks_" + prefix
				applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int);")
				err := write(t, &ir.Table{Name: table, Columns: threeCols(src.typ), PrimaryKey: pkOn("k")},
					ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
					ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				if err == nil {
					t.Fatalf("two source rows the target key equates landed in one statement: target %s", kcgPGRows(t, dsn, table, "id"))
				}
				if got := kcgPGRows(t, dsn, table, "id"); got != "" {
					t.Fatalf("the refused statement left %s on the target", got)
				}
			})
			t.Run("every column is the key", func(t *testing.T) {
				table := "ak_" + prefix
				applyPGApplier(t, dsn, "CREATE TABLE "+table+" (k text COLLATE ci NOT NULL PRIMARY KEY); INSERT INTO "+table+" VALUES ('a');")
				before := kcgPGRows(t, dsn, table, "k")
				tbl := &ir.Table{Name: table, Columns: []*ir.Column{{Name: "k", Type: src.typ}}, PrimaryKey: pkOn("k")}
				if err := write(t, tbl, ir.Row{"k": "a"}); err != nil {
					t.Fatalf("an identical re-send refused: %v", err)
				}
				// DO NOTHING would silently drop the incoming 'A'.
				wantMerge(t, write(t, tbl, ir.Row{"k": "A"}), table, "k", before)
			})
			t.Run("unique key and no primary key", func(t *testing.T) {
				table := "nk_" + prefix
				applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id int NOT NULL, k text COLLATE ci NOT NULL, d int, CONSTRAINT "+table+"_uk UNIQUE (k)); INSERT INTO "+table+" VALUES (1,'a',1);")
				before := kcgPGRows(t, dsn, table, "id")
				cols := threeCols(src.typ)
				tbl := &ir.Table{Name: table, Columns: cols, Indexes: []*ir.Index{{Name: table + "_uk", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}}}
				err := write(t, tbl, ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				wantMerge(t, err, table, "id", before)
			})
			t.Run("a unique key beside the primary key is not the arbiter, and raises", func(t *testing.T) {
				table := "ku_" + prefix
				applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id int NOT NULL PRIMARY KEY, k text COLLATE ci NOT NULL UNIQUE, d int); INSERT INTO "+table+" VALUES (1,'a',1);")
				before := kcgPGRows(t, dsn, table, "id")
				err := write(t, &ir.Table{Name: table, Columns: threeCols(src.typ), PrimaryKey: pkOn("id")},
					ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				if err == nil || !strings.Contains(err.Error(), "23505") {
					t.Fatalf("err = %v; want a unique_violation (23505)", err)
				}
				if got := kcgPGRows(t, dsn, table, "id"); got != before {
					t.Fatalf("the refusal changed the target: %s → %s", before, got)
				}
			})
			t.Run("a copy with no merge lands byte-exact, and a re-sent row passes", func(t *testing.T) {
				table := "kx_" + prefix
				applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int);")
				tbl := &ir.Table{Name: table, Columns: threeCols(src.typ), PrimaryKey: pkOn("k")}
				rows := []ir.Row{
					{"id": int64(1), "k": "A", "d": int64(1)},
					{"id": int64(2), "k": "b", "d": int64(2)},
					{"id": int64(3), "k": "ß", "d": int64(3)},
					{"id": int64(4), "k": "é", "d": int64(4)},
					{"id": int64(5), "k": "Z z", "d": nil},
				}
				if err := write(t, tbl, rows...); err != nil {
					t.Fatalf("a copy with no merge refused: %v", err)
				}
				const want = "1:41,2:62,3:c39f,4:c3a9,5:5a207a"
				if got := kcgPGRows(t, dsn, table, "id"); got != want {
					t.Fatalf("target %s; want %s", got, want)
				}
				rows[0]["d"] = int64(10)
				if err := write(t, tbl, rows[:2]...); err != nil {
					t.Fatalf("re-sending rows already copied refused: %v", err)
				}
				if got := kcgPGRows(t, dsn, table, "id"); got != want {
					t.Fatalf("after the re-send the target is %s; want %s", got, want)
				}
				db, err := sql.Open("pgx", dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				var d int
				if err := db.QueryRow("SELECT d FROM " + table + " WHERE id = 1").Scan(&d); err != nil || d != 10 {
					t.Fatalf("the re-sent row's value is %d (%v); want 10 (the upsert did not refresh it)", d, err)
				}
			})
		})
	}

	t.Run("a MySQL-family source key cannot be guarded and refuses before writing", func(t *testing.T) {
		applyPGApplier(t, dsn, "CREATE TABLE km (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int);")
		src := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
		err := write(t, &ir.Table{Name: "km", Columns: threeCols(src), PrimaryKey: pkOn("k")},
			ir.Row{"id": int64(1), "k": "a", "d": int64(1)})
		if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), "KEY-COLLATION-GUARD-UNSUPPORTED") {
			t.Fatalf("err = %v; want KEY-COLLATION-GUARD-UNSUPPORTED", err)
		}
		if got := kcgPGRows(t, dsn, "km", "id"); got != "" {
			t.Fatalf("the refusal wrote %s", got)
		}
	})

	t.Run("agreeing collations carry no guard", func(t *testing.T) {
		applyPGApplier(t, dsn, "CREATE TABLE kg (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int); INSERT INTO kg VALUES (1,'a',1);")
		tbl := &ir.Table{Name: "kg", Columns: threeCols(kcgPGText("ci", ir.CollationNonDeterministic)), PrimaryKey: pkOn("k")}
		if err := write(t, tbl, ir.Row{"id": int64(1), "k": "A", "d": int64(2)}); err != nil {
			t.Fatalf("an unarmed table refused: %v", err)
		}
		if got, want := kcgPGRows(t, dsn, "kg", "id"), "1:61"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})
}
