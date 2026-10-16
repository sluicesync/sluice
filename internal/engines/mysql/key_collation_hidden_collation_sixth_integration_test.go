//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// kchcNewDB returns a constructor of fresh databases on the shared server,
// named prefix_1, prefix_2, ….
func kchcNewDB(prefix string) func(t *testing.T) string {
	n := 0
	return func(t *testing.T) string {
		t.Helper()
		n++
		dsn, cleanup := newSharedDB(t, fmt.Sprintf("%s_%d", prefix, n))
		t.Cleanup(cleanup)
		return dsn
	}
}

// kchcWrite writes rows through the idempotent copy writer of eng.
func kchcWrite(t *testing.T, eng Engine, dsn string, table *ir.Table, rows ...ir.Row) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rw, err := eng.OpenRowWriter(ctx, dsn)
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

// kchcWantMerge fails unless err is the KEY-COLLATION-MERGE refusal.
func kchcWantMerge(t *testing.T, err error, dsn string) {
	t.Helper()
	if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(fmtErr(err), keyCollationMergeMarker) {
		t.Fatalf("err = %v; want KEY-COLLATION-MERGE (target %s)", err, kcgRows(t, dsn, "kd", "id"))
	}
}

// kchcReadTable reads the source table kd through the reader.
func kchcReadTable(ctx context.Context, t *testing.T, dsn string) *ir.Table {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		t.Fatal(err)
	}
	for _, tb := range schema.Tables {
		if tb.Name == "kd" {
			return tb
		}
	}
	t.Fatal("the reader returned no table kd")
	return nil
}

// kchcCreate creates table on a fresh target through sluice's schema
// writer: the table, then its indexes (the pinned key).
func kchcCreate(ctx context.Context, t *testing.T, dsn string, table *ir.Table) error {
	t.Helper()
	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIf(sw)
	one := &ir.Schema{Tables: []*ir.Table{table}}
	if err := sw.CreateTablesWithoutConstraints(ctx, one); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return sw.CreateIndexes(ctx, one)
}

// kchcDB opens dsn for a direct catalog read, closed at the test's end.
func kchcDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestFunctionalKeyHiddenCollation_SixthReview pins the sixth review's two
// real-server findings against the class fix, each with the source's own
// rows as the independent expected value.
//
// F1 (HIGH, silent): an equal hidden collation fixes how the key COMPARES,
// not how its value is COMPUTED — the expression evaluates under the target
// COLUMN's collation. Source `k utf8mb4_bin`, key `if(k = 'a', 'same', k)`:
// 'a' → 'same', 'A' → 'A' on the source. A target whose column was moved to
// `utf8mb4_0900_ai_ci` (a --type-override drops the collation; an operator
// MODIFY) computes 'same' for 'A' too, and with the key pinned `_bin` the
// guard compared equal hidden collations and armed nothing: (2,'A') replaced
// (1,'a') at exit 0 (measured on ab4346fa). The column-divergence cell must
// refuse; the aligned control must land both rows.
//
// F2 (MEDIUM, loud): an ALTER … MODIFY that changes the column's CHARACTER
// SET leaves the hidden column on the old set, and the writer's
// `(trim(k)) COLLATE utf8mb3_general_ci` over the utf8mb4 value was ERROR
// 1253 at the index build, after the copy. The reader now leaves such a
// part's collation unknown: the index builds bare, and — the target key
// comparing under utf8mb4's default where the source compared under
// utf8mb3_general_ci ('ss' and 'ß' apart) — the guard refuses the merge.
func TestFunctionalKeyHiddenCollation_SixthReview(t *testing.T) {
	newDB := kchcNewDB("kchc6")

	t.Run("F1: the target column diverges under a read-equal key", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		src := newDB(t)
		applyDDL(t, src, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 "+
			"COLLATE utf8mb4_bin NOT NULL, f INT, UNIQUE KEY fk ((if(k = 'a', 'same', k))))")
		rows := [2]ir.Row{{"id": int64(1), "k": "a", "f": int64(1)}, {"id": int64(2), "k": "A", "f": int64(2)}}
		for _, r := range rows {
			applyDDL(t, src, fmt.Sprintf("INSERT INTO kd VALUES (%d, '%s', %d)", r["id"], r["k"], r["f"]))
		}
		want := kcgRows(t, src, "kd", "id")
		if strings.Count(want, ",") != 1 {
			t.Fatalf("the source key did not keep the pair apart (%s); the cell measures nothing", want)
		}
		table := kchcReadTable(ctx, t, src)
		if got := table.Indexes[0].Columns[0].HiddenCollation; got != "utf8mb4_bin" {
			t.Fatalf("hidden collation %q; want utf8mb4_bin", got)
		}
		for _, tc := range []struct {
			name, alter string
			refuses     bool
		}{
			{"control: aligned column, both rows land", "", false},
			{"column moved to utf8mb4_0900_ai_ci: refuses", "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tgt := newDB(t)
				if err := kchcCreate(ctx, t, tgt, table); err != nil {
					t.Fatalf("create the pinned key: %v", err)
				}
				if tc.alter != "" {
					applyDDL(t, tgt, tc.alter)
				}
				if err := kchcWrite(t, Engine{}, tgt, table, rows[0]); err != nil {
					t.Fatal(err)
				}
				before := kcgRows(t, tgt, "kd", "id")
				err := kchcWrite(t, Engine{}, tgt, table, rows[1])
				if !tc.refuses {
					if err != nil {
						t.Fatalf("the aligned target refused: %v", err)
					}
					if got := kcgRows(t, tgt, "kd", "id"); got != want {
						t.Fatalf("target %s; the source holds %s", got, want)
					}
					return
				}
				kchcWantMerge(t, err, tgt)
				if got := kcgRows(t, tgt, "kd", "id"); got != before {
					t.Fatalf("the refusal changed the target: %s → %s", before, got)
				}
			})
		}
	})

	// Seventh review, finding 1 (HIGH, silent): the same divergence one
	// step removed. g is declared alike on both sides, but the target
	// RECOMPUTES it from k under k's target collation, so the key over g
	// merged with equal hidden collations because only g was compared.
	t.Run("F1 through a generated column", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		src := newDB(t)
		applyDDL(t, src, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 "+
			"COLLATE utf8mb4_bin NOT NULL, f INT, g VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci "+
			"AS (if(k = 'a', 'same', k)), UNIQUE KEY fk ((upper(g))))")
		rows := [2]ir.Row{{"id": int64(1), "k": "a", "f": int64(1)}, {"id": int64(2), "k": "A", "f": int64(2)}}
		for _, r := range rows {
			applyDDL(t, src, fmt.Sprintf("INSERT INTO kd (id, k, f) VALUES (%d, '%s', %d)", r["id"], r["k"], r["f"]))
		}
		want := kcgRows(t, src, "kd", "id")
		if strings.Count(want, ",") != 1 {
			t.Fatalf("the source key did not keep the pair apart (%s); the cell measures nothing", want)
		}
		table := kchcReadTable(ctx, t, src)
		if got := table.Indexes[0].Columns[0].HiddenCollation; got != "utf8mb4_0900_ai_ci" {
			t.Fatalf("hidden collation %q; want utf8mb4_0900_ai_ci", got)
		}
		for _, tc := range []struct {
			name, alter string
			refuses     bool
		}{
			{"control: aligned base column, both rows land", "", false},
			{"base column moved to utf8mb4_0900_ai_ci: refuses", "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tgt := newDB(t)
				if err := kchcCreate(ctx, t, tgt, table); err != nil {
					t.Fatalf("create the pinned key: %v", err)
				}
				if tc.alter != "" {
					applyDDL(t, tgt, tc.alter)
				}
				if err := kchcWrite(t, Engine{}, tgt, table, rows[0]); err != nil {
					t.Fatal(err)
				}
				before := kcgRows(t, tgt, "kd", "id")
				err := kchcWrite(t, Engine{}, tgt, table, rows[1])
				if !tc.refuses {
					if err != nil {
						t.Fatalf("the aligned target refused: %v", err)
					}
					if got := kcgRows(t, tgt, "kd", "id"); got != want {
						t.Fatalf("target %s; the source holds %s", got, want)
					}
					return
				}
				kchcWantMerge(t, err, tgt)
				if got := kcgRows(t, tgt, "kd", "id"); got != before {
					t.Fatalf("the refusal changed the target: %s → %s", before, got)
				}
			})
		}
	})

	// Seventh review, finding 2 (MEDIUM, silent): a column that is a string
	// on the target alone. On the source k is VARBINARY, so `k = 'a'` is a
	// byte comparison and 'A' computes 'other'; on a target where k became a
	// `_ci` VARCHAR (a --type-override; here a MODIFY) 'A' computes 'same'.
	// k carries no source collation, so the plan never armed it.
	t.Run("a column that is a string on the target alone", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		src := newDB(t)
		applyDDL(t, src, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARBINARY(20) NOT NULL, f INT, "+
			"UNIQUE KEY fk ((if(k = 'a', 'same', 'other'))))")
		rows := [2]ir.Row{{"id": int64(1), "k": []byte("a"), "f": int64(1)}, {"id": int64(2), "k": []byte("A"), "f": int64(2)}}
		for _, r := range rows {
			applyDDL(t, src, fmt.Sprintf("INSERT INTO kd VALUES (%d, '%s', %d)", r["id"], r["k"], r["f"]))
		}
		want := kcgRows(t, src, "kd", "id")
		if strings.Count(want, ",") != 1 {
			t.Fatalf("the source key did not keep the pair apart (%s); the cell measures nothing", want)
		}
		table := kchcReadTable(ctx, t, src)
		for _, tc := range []struct {
			name, alter string
			refuses     bool
		}{
			{"control: VARBINARY on both sides, both rows land", "", false},
			{"k a utf8mb4_0900_ai_ci VARCHAR on the target: refuses", "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tgt := newDB(t)
				if err := kchcCreate(ctx, t, tgt, table); err != nil {
					t.Fatalf("create the pinned key: %v", err)
				}
				if tc.alter != "" {
					applyDDL(t, tgt, tc.alter)
				}
				if err := kchcWrite(t, Engine{}, tgt, table, rows[0]); err != nil {
					t.Fatal(err)
				}
				before := kcgRows(t, tgt, "kd", "id")
				err := kchcWrite(t, Engine{}, tgt, table, rows[1])
				if !tc.refuses {
					if err != nil {
						t.Fatalf("the aligned target refused: %v", err)
					}
					if got := kcgRows(t, tgt, "kd", "id"); got != want {
						t.Fatalf("target %s; the source holds %s", got, want)
					}
					return
				}
				kchcWantMerge(t, err, tgt)
				if got := kcgRows(t, tgt, "kd", "id"); got != before {
					t.Fatalf("the refusal changed the target: %s → %s", before, got)
				}
			})
		}
	})

	t.Run("F2: S1 with a charset change", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		src := newDB(t)
		applyDDL(t, src, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb3 "+
			"COLLATE utf8mb3_general_ci NOT NULL, f INT, UNIQUE KEY fk ((trim(k))))")
		applyDDL(t, src, "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL")
		rows := [2]ir.Row{{"id": int64(1), "k": "ss", "f": int64(1)}, {"id": int64(2), "k": "ß", "f": int64(2)}}
		for _, r := range rows {
			applyDDL(t, src, fmt.Sprintf("INSERT INTO kd VALUES (%d, '%s', %d)", r["id"], r["k"], r["f"]))
		}
		want := kcgRows(t, src, "kd", "id")
		if strings.Count(want, ",") != 1 {
			t.Fatalf("the source key did not keep the pair apart (%s); the cell measures nothing", want)
		}
		// The premise, read off the catalog: the hidden column kept utf8mb3.
		hidden, ok := functionalKeyCollations(ctx, kchcDB(t, src), FlavorVanilla, kchcScalar(t, src, "SELECT DATABASE()"), "kd")
		if !ok || len(hidden) != 1 || !strings.HasPrefix(hidden[0].collation, "utf8mb3_") {
			t.Fatalf("the hidden column is not on utf8mb3 (%+v, %v); the cell measures nothing", hidden, ok)
		}
		table := kchcReadTable(ctx, t, src)
		if got := table.Indexes[0].Columns[0].HiddenCollation; got != "" {
			t.Fatalf("the reader recorded hidden collation %q, which is not of the expression's charset (utf8mb4) — "+
				"the writer's COLLATE would be ERROR 1253", got)
		}
		tgt := newDB(t)
		if err := kchcCreate(ctx, t, tgt, table); err != nil {
			t.Fatalf("the index build failed (the F2 shape is ERROR 1253): %v", err)
		}
		if err := kchcWrite(t, Engine{}, tgt, table, rows[0]); err != nil {
			t.Fatal(err)
		}
		before := kcgRows(t, tgt, "kd", "id")
		kchcWantMerge(t, kchcWrite(t, Engine{}, tgt, table, rows[1]), tgt)
		if got := kcgRows(t, tgt, "kd", "id"); got != before {
			t.Fatalf("the refusal changed the target: %s → %s", before, got)
		}
	})
}
