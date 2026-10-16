//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// FUNCTIONAL-CAST-HIDDEN-COLLATION, the class fix, on the real server. A
// MySQL functional key part compares under its hidden column's collation,
// which the expression text does not record: a charset-less CAST and a
// literal take the creating session's collation_connection, and an ALTER …
// MODIFY of the column leaves the hidden column on its OLD collation. The
// reader now reads that collation into the IR, the writer pins it as
// `((expr) COLLATE <it>)`, and the guard compares it against the target's
// hidden column.
//
// Each shape is created on a real source under the session that decides it;
// the source's own rows are the independent expected value (both rows are
// held there, which the cell asserts first). On a target sluice's writer
// creates, both rows must land, the emitted COLLATE accepted by the server.
// On a STALE target built from the expression text alone (what every
// released version created), the guard must refuse KEY-COLLATION-MERGE with
// the target unchanged — and so must the Vitess flavor, which cannot read a
// hidden column.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestFunctionalKeyHiddenCollation_ClassFix(t *testing.T) {
	const ci = "k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"
	shapes := []struct {
		name, session string
		// create builds the source table kd; alter, when set, runs after it.
		create, alter string
		// stale is the target key as the expression text alone rebuilds it.
		stale      string
		wantHidden string
		rows       [2]ir.Row
	}{
		{
			name: "CAST from a utf8mb4_bin session", session: "utf8mb4_bin",
			create: "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " + ci + ", f INT, UNIQUE KEY fk ((cast(k as char(10)))))",
			stale:  "UNIQUE KEY fk ((cast(k as char(10) charset utf8mb4)))", wantHidden: "utf8mb4_bin",
			rows: [2]ir.Row{{"id": int64(1), "k": "a", "f": int64(1)}, {"id": int64(2), "k": "A", "f": int64(2)}},
		},
		{
			name: "CAST from a utf8mb4_general_ci session", session: "utf8mb4_general_ci",
			create: "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " + ci + ", f INT, UNIQUE KEY fk ((cast(k as char(10)))))",
			stale:  "UNIQUE KEY fk ((cast(k as char(10) charset utf8mb4)))", wantHidden: "utf8mb4_general_ci",
			rows: [2]ir.Row{{"id": int64(1), "k": "ss", "f": int64(1)}, {"id": int64(2), "k": "ß", "f": int64(2)}},
		},
		{
			name:   "S1: the column MODIFYed after the key (trim)",
			create: "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, f INT, UNIQUE KEY fk ((trim(k))))",
			alter:  "ALTER TABLE kd MODIFY " + ci,
			stale:  "UNIQUE KEY fk ((trim(k)))", wantHidden: "utf8mb4_bin",
			rows: [2]ir.Row{{"id": int64(1), "k": "e", "f": int64(1)}, {"id": int64(2), "k": "é", "f": int64(2)}},
		},
		{
			name: "S2: string literals under an INT column (if)", session: "utf8mb4_bin",
			create: "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " + ci + ", f INT, UNIQUE KEY fk ((if(f, 'A', 'a'))))",
			stale:  "UNIQUE KEY fk ((if(f, _utf8mb4'A', _utf8mb4'a')))", wantHidden: "utf8mb4_bin",
			rows: [2]ir.Row{{"id": int64(1), "k": "x", "f": int64(1)}, {"id": int64(2), "k": "y", "f": int64(0)}},
		},
		{
			name: "nested: lower over a session CAST", session: "utf8mb4_bin",
			create: "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " + ci + ", f INT, UNIQUE KEY fk ((lower(cast(k as char(10))))))",
			stale:  "UNIQUE KEY fk ((lower(cast(k as char(10) charset utf8mb4))))", wantHidden: "utf8mb4_bin",
			rows: [2]ir.Row{{"id": int64(1), "k": "e", "f": int64(1)}, {"id": int64(2), "k": "é", "f": int64(2)}},
		},
	}
	newDB := kchcNewDB("kchc")
	write, wantMerge := kchcWrite, kchcWantMerge
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			src := newDB(t)
			sessionDSN := src
			if s.session != "" {
				sessionDSN += "&collation=" + s.session
			}
			applyDDL(t, sessionDSN, s.create)
			if s.alter != "" {
				applyDDL(t, src, s.alter)
			}
			for _, r := range s.rows {
				applyDDL(t, src, fmt.Sprintf("INSERT INTO kd VALUES (%d, '%s', %d)", r["id"], r["k"], r["f"]))
			}
			want := kcgRows(t, src, "kd", "id")
			if strings.Count(want, ",") != 1 {
				t.Fatalf("the source key did not keep the pair apart (%s); the cell measures nothing", want)
			}
			sr, err := Engine{}.OpenSchemaReader(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := sr.ReadSchema(ctx)
			closeIf(sr)
			if err != nil {
				t.Fatal(err)
			}
			var table *ir.Table
			for _, tb := range schema.Tables {
				if tb.Name == "kd" {
					table = tb
				}
			}
			if table == nil || len(table.Indexes) != 1 || table.Indexes[0].Columns[0].HiddenCollation != s.wantHidden {
				t.Fatalf("the reader did not record the source key's hidden collation %q: %+v", s.wantHidden, table)
			}

			for _, flavor := range []struct {
				name string
				eng  Engine
			}{{"mysql", Engine{}}, {"vitess flavor (hidden column unknown)", Engine{Flavor: FlavorVitess}}} {
				t.Run(flavor.name+"/target created by sluice: both rows land", func(t *testing.T) {
					tgt := newDB(t)
					sw, err := Engine{}.OpenSchemaWriter(ctx, tgt)
					if err != nil {
						t.Fatal(err)
					}
					one := &ir.Schema{Tables: []*ir.Table{table}}
					if err := sw.CreateTablesWithoutConstraints(ctx, one); err != nil {
						t.Fatalf("create table: %v", err)
					}
					if err := sw.CreateIndexes(ctx, one); err != nil {
						t.Fatalf("create the pinned key: %v", err)
					}
					closeIf(sw)
					t.Logf("target key: %s", kchcScalar(t, tgt, "SELECT expression FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'kd' AND index_name = 'fk'"))
					if err := write(t, flavor.eng, tgt, table, s.rows[0]); err != nil {
						t.Fatal(err)
					}
					if err := write(t, flavor.eng, tgt, table, s.rows[1]); err != nil {
						t.Fatalf("the second source row refused on a target that compares as the source: %v", err)
					}
					if got := kcgRows(t, tgt, "kd", "id"); got != want {
						t.Fatalf("target %s; the source holds %s", got, want)
					}
				})
				t.Run(flavor.name+"/stale target: refuses", func(t *testing.T) {
					tgt := newDB(t)
					applyDDL(t, tgt, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, "+ci+", f INT, "+s.stale+")")
					if err := write(t, flavor.eng, tgt, table, s.rows[0]); err != nil {
						t.Fatal(err)
					}
					before := kcgRows(t, tgt, "kd", "id")
					wantMerge(t, write(t, flavor.eng, tgt, table, s.rows[1]), tgt)
					if got := kcgRows(t, tgt, "kd", "id"); got != before {
						t.Fatalf("the refusal changed the target: %s → %s", before, got)
					}
				})
			}
		})
	}
}

func kchcScalar(t *testing.T, dsn, q string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v sql.NullString
	if err := db.QueryRow(q).Scan(&v); err != nil {
		return "<" + err.Error() + ">"
	}
	return v.String
}
