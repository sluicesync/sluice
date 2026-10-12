//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

// TestVStream_KeyCollationGuard_ShardedTarget pins the key-collation
// backstop through vtgate on a SHARDED keyspace (key_collation_guard.go):
// the guard is evaluated by the shard's mysqld inside the upsert vtgate
// routes, and its raise must surface as KEY-COLLATION-MERGE with nothing
// written, on all three apply paths (serial, batch, lanes). Two key shapes:
// a primary key that is itself the vindex column, and a unique key beside a
// hash-vindexed primary key. An identical re-apply must pass.
func TestVStream_KeyCollationGuard_ShardedTarget(t *testing.T) {
	vt := bootVTTestServer(t, "test,ctl", "2,1")
	defer vt.terminate()
	dsn := vt.dsn("test")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range []string{
		"CREATE TABLE kd (id BIGINT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, d INT, PRIMARY KEY (k)) ENGINE=InnoDB",
		"ALTER VSCHEMA ON kd ADD VINDEX unicode_loose_md5(k) USING unicode_loose_md5",
		"CREATE TABLE ku (id BIGINT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, d INT, UNIQUE KEY uk (k)) ENGINE=InnoDB",
		"ALTER VSCHEMA ON ku ADD VINDEX hash(id) USING hash",
		"INSERT INTO kd (id, k, d) VALUES (1, 'a', 1)",
		"INSERT INTO ku (id, k, d) VALUES (1, 'a', 1)",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	rows := func(table string) string {
		t.Helper()
		r, err := db.QueryContext(ctx, "SELECT CONCAT(id, ':', HEX(k), ':', d) FROM "+table)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		var out []string
		for r.Next() {
			var s string
			if err := r.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	eng, err := Engine{Flavor: FlavorVitess}.WithControlKeyspace("ctl")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	pos := func() ir.Position {
		n++
		return ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf(`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-%d"}`, n)}
	}
	source := func(table string) ir.SchemaSnapshot {
		return ir.SchemaSnapshot{Schema: "test", Table: table, Position: pos(), IR: &ir.Table{Schema: "test", Name: table, Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "k", Type: ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		}}}
	}
	for _, path := range []string{"serial", "batch", "lanes"} {
		for _, table := range []string{"kd", "ku"} {
			path, table := path, table
			t.Run(path+"/"+table, func(t *testing.T) {
				opened, err := eng.OpenChangeApplier(ctx, dsn)
				if err != nil {
					t.Fatalf("OpenChangeApplier: %v", err)
				}
				a := opened.(*ChangeApplier)
				defer func() { _ = a.Close() }()
				if err := a.EnsureControlTable(ctx); err != nil {
					t.Fatalf("EnsureControlTable: %v", err)
				}
				apply := func(changes ...ir.Change) error {
					ch := make(chan ir.Change, len(changes))
					for _, c := range changes {
						ch <- c
					}
					close(ch)
					switch path {
					case "serial":
						return a.Apply(ctx, "kcg-vt-"+path+table, ch)
					case "lanes":
						a.SetApplyConcurrency(2)
					}
					return a.ApplyBatch(ctx, "kcg-vt-"+path+table, ch, 50)
				}
				before := rows(table)
				if err := apply(source(table), ir.Insert{Position: pos(), Schema: "test", Table: table, Row: ir.Row{"id": int64(1), "k": "a", "d": int64(1)}}); err != nil {
					t.Fatalf("an identical re-apply refused: %v", err)
				}
				err = apply(source(table), ir.Insert{Position: pos(), Schema: "test", Table: table, Row: ir.Row{"id": int64(99), "k": "A", "d": int64(99)}})
				if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), "KEY-COLLATION-MERGE") {
					t.Fatalf("err = %v; want %s", err, ir.ErrKeyCollationMerge)
				}
				if got := rows(table); got != before {
					t.Fatalf("the refusal changed the target: %s → %s", before, got)
				}
			})
		}
	}
}
