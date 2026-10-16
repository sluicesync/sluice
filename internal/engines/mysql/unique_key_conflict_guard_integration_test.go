//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SOURCE-DROPPED-UNIQUE-KEY-MERGE, the MySQL-family conflict-identity guard
// (unique_key_conflict_guard.go), on real servers: MySQL 8.0 and MariaDB
// 11.4 (the VALUES() spelling), through both apply doors — the serial Apply
// and the batched ApplyBatch (the coalesced multi-row upsert the lanes use
// too) — and through the idempotent copy writer.
//
// The independent expected value is the target's own rows read back after
// each run (id, key and value), compared with what the source holds: a
// refusal must leave the target exactly as it was; a pass must leave it as
// the source would.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ukcRows reads query's single text column, sorted, joined by ",".
func ukcRows(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

const ukcKdRows = "SELECT CONCAT_WS(':', COALESCE(CAST(id AS CHAR), 'NULL'), k, COALESCE(v, 'NULL')) FROM kd"

func TestUniqueKeyConflictGuard_MySQLFamily(t *testing.T) {
	for _, srv := range kcgServers(t) {
		for _, batched := range []bool{false, true} {
			door := "serial"
			if batched {
				door = "batched"
			}
			t.Run(srv.name+"/"+door, func(t *testing.T) { ukcRunServer(t, srv, batched) })
		}
	}
}

func ukcRunServer(t *testing.T, srv kcgServer, batched bool) {
	n := 0
	cell := func() (string, string) {
		n++
		db := fmt.Sprintf("ukc_%s_%d_%t", strings.NewReplacer("-", "", ".", "").Replace(srv.name), n, batched)
		dsn, cleanup := srv.newDB(t, db)
		t.Cleanup(cleanup)
		return db, dsn
	}
	ins := func(db string, pos int, id any, k string, v any) ir.Insert {
		return ir.Insert{Position: kcgPos(pos), Schema: db, Table: "kd", Row: ir.Row{"id": id, "k": k, "v": v}}
	}
	wantRefusal := func(t *testing.T, err error, dsn, before string) {
		t.Helper()
		if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
			t.Fatalf("err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
		}
		if !strings.Contains(err.Error(), "UNIQUE-KEY-CONFLICT-OTHER-ROW") || !strings.Contains(err.Error(), "kd_k (k)") {
			t.Fatalf("the refusal does not carry the marker and the key: %v", err)
		}
		if got := ukcRows(t, dsn, ukcKdRows); got != before {
			t.Fatalf("the refusal changed the target: %s -> %s", before, got)
		}
	}

	t.Run("primary key: the measured reproduction", func(t *testing.T) {
		db, dsn := cell()
		applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a','one')")
		before := ukcRows(t, dsn, ukcKdRows)
		// The source dropped kd_k and inserted (99,'a'): before the guard the
		// target ended 1:a:ninetynine, row 99 absent, exit 0.
		wantRefusal(t, kcgApply(t, srv.eng, dsn, batched, ins(db, 1, int64(99), "a", "ninetynine")), dsn, before)
	})

	t.Run("primary key: a re-delivery and an update through the key converge", func(t *testing.T) {
		db, dsn := cell()
		applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a','one')")
		if err := kcgApply(
			t, srv.eng, dsn, batched,
			ins(db, 1, int64(1), "a", "one"),
			ins(db, 2, int64(1), "a", "uno"),
		); err != nil {
			t.Fatalf("a re-delivery of the same row refused: %v", err)
		}
		if got, want := ukcRows(t, dsn, ukcKdRows), "1:a:uno"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})

	t.Run("primary key: an UPDATE of k and a reuse of the old k in one transaction", func(t *testing.T) {
		db, dsn := cell()
		applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a','one')")
		if err := kcgApply(
			t, srv.eng, dsn, batched,
			ir.Update{
				Position: kcgPos(1), Schema: db, Table: "kd",
				Before: ir.Row{"id": int64(1), "k": "a", "v": "one"},
				After:  ir.Row{"id": int64(1), "k": "b", "v": "one"},
			},
			ins(db, 2, int64(99), "a", "ninetynine"),
		); err != nil {
			t.Fatalf("a key moved and reused in order refused: %v", err)
		}
		if got, want := ukcRows(t, dsn, ukcKdRows), "1:b:one,99:a:ninetynine"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})

	t.Run("no primary key, two unique keys: a row disagreeing on the other key refuses", func(t *testing.T) {
		db, dsn := cell()
		applyDDL(t, dsn, "CREATE TABLE kd (id INT, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_id (id), UNIQUE KEY kd_k (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a','one'),(2,'b',NULL),(3,'c','three')")
		before := ukcRows(t, dsn, ukcKdRows)
		// The source dropped kd_k; before the guard the id itself was
		// rewritten: 99:a:ninetynine.
		err := kcgApply(t, srv.eng, dsn, batched, ins(db, 1, int64(99), "a", "ninetynine"))
		if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
			t.Fatalf("err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
		}
		// The hang column (id) NULL on the incoming row: the raise must still
		// fire (a CONCAT of the value itself would be NULL and swallow it).
		err = kcgApply(t, srv.eng, dsn, batched, ins(db, 2, nil, "c", "three"))
		if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
			t.Fatalf("NULL hang: err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
		}
		if got := ukcRows(t, dsn, ukcKdRows); got != before {
			t.Fatalf("the refusals changed the target: %s -> %s", before, got)
		}
		// The same rows, re-sent at a newer image: they agree on every key.
		if err := kcgApply(
			t, srv.eng, dsn, batched,
			ins(db, 3, int64(1), "a", "uno"),
			ins(db, 4, int64(2), "b", nil),
		); err != nil {
			t.Fatalf("a re-send agreeing on every unique key refused: %v", err)
		}
		if got, want := ukcRows(t, dsn, ukcKdRows), "1:a:uno,2:b:NULL,3:c:three"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})

	t.Run("no primary key, one unique key: the newer-image re-send still converges", func(t *testing.T) {
		db, dsn := cell()
		// The documented contract the guard must not break (see the file
		// comment of unique_key_conflict_guard.go): this table is not guarded.
		applyDDL(t, dsn, "CREATE TABLE kd (id INT, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a','one')")
		if err := kcgApply(t, srv.eng, dsn, batched, ins(db, 1, int64(1), "a", "uno")); err != nil {
			t.Fatalf("a newer-image re-send refused: %v", err)
		}
		if got, want := ukcRows(t, dsn, ukcKdRows), "1:a:uno"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})
}

// TestUniqueKeyConflictGuard_CopyWriter pins the idempotent copy writer: a
// target unique key the copy's source table declares keeps today's absorb
// (a stale row refreshed — the VStream catchup re-emission shape); one it
// does not declare refuses a row that would overwrite another.
func TestUniqueKeyConflictGuard_CopyWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	source := func(declared bool) *ir.Table {
		tbl := &ir.Table{
			Name: "kd",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "k", Type: ir.Varchar{Length: 20}},
				{Name: "v", Type: ir.Varchar{Length: 20}, Nullable: true},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}
		if declared {
			tbl.Indexes = []*ir.Index{{Name: "kd_k", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}}
		}
		return tbl
	}
	for _, declared := range []bool{true, false} {
		t.Run(fmt.Sprintf("declared=%t", declared), func(t *testing.T) {
			dsn, cleanup := newSharedDB(t, fmt.Sprintf("ukc_copy_%t", declared))
			defer cleanup()
			applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))")
			applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a','one')")
			before := ukcRows(t, dsn, ukcKdRows)
			w, err := Engine{}.OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIf(w)
			rows := make(chan ir.Row, 1)
			rows <- ir.Row{"id": int64(99), "k": "a", "v": "ninetynine"}
			close(rows)
			err = w.(ir.IdempotentRowWriter).WriteRowsIdempotent(ctx, source(declared), rows)
			if declared {
				if err != nil {
					t.Fatalf("a conflict through a key the source declares refused: %v", err)
				}
				return
			}
			if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) || !strings.Contains(err.Error(), "DROP INDEX `kd_k`") {
				t.Fatalf("err = %v; want %s naming the key", err, ir.ErrUniqueKeyConflictOtherRow)
			}
			if got := ukcRows(t, dsn, ukcKdRows); got != before {
				t.Fatalf("the refusal changed the target: %s -> %s", before, got)
			}
		})
	}
}
