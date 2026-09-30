//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

// TestVStream_ShardedTarget_VindexMoveRefusesLoudly pins, on real vtgate, the
// premise that keeps a vindex-moving change from silently corrupting a
// PRE-VINDEXED sharded target (GC-41 (e)): vtgate refuses to assign a
// primary-vindex column — Error 1235 VT12001 — for the UPDATE sluice's
// serial path sends AND for the row-alias ON DUPLICATE KEY UPDATE its batch
// and lane paths send. (The VALUES() spelling of the latter is ACCEPTED and
// duplicates the row across shards; TestUpsertSpelling_VitessFamilyNeverUsesValuesFunc
// keeps sluice off it.)
//
// Keyspace "test" has two shards and two tables: t_pk (vindex hash(id), the
// primary key) and t_np (primary key id, vindex hash(cust)). For each apply
// path — serial Apply, the serial batch loop, the lanes — a primary-key
// change on t_pk and a cust change on t_np must fail with 1235 and the
// SHARDED-TARGET-VINDEX-UPDATE marker, leave BOTH physical shards exactly as
// they were (read beneath vtgate from vt_test_-80 / vt_test_80-), and leave
// the stream's position where it was.
//
// The scope subtests pin what docs/managed-services.md states about the
// same target for a change that does NOT move the vindex: on t_pk the
// batched path applies a non-key update, and the per-change path refuses the
// same update because its UPDATE re-states the unchanged vindex column — the
// OVER-refusal GC-41 (e) leaves open. When that is fixed the serial scope arm
// goes red on purpose: update the doc with it.
func TestVStream_ShardedTarget_VindexMoveRefusesLoudly(t *testing.T) {
	vt := bootVTTestServer(t, "test,ctl", "2,1")
	defer vt.terminate()
	dsn := vt.dsn("test")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range []string{
		`CREATE TABLE t_pk (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB`,
		`ALTER VSCHEMA ON t_pk ADD VINDEX hash(id) USING hash`,
		`CREATE TABLE t_np (id BIGINT NOT NULL PRIMARY KEY, cust BIGINT NOT NULL, v VARCHAR(32) NOT NULL) ENGINE=InnoDB`,
		`ALTER VSCHEMA ON t_np ADD VINDEX hash(cust) USING hash`,
		`INSERT INTO t_pk (id, v) VALUES (1, 'a')`,
		`INSERT INTO t_np (id, cust, v) VALUES (10, 1, 'a')`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	shards := func() string {
		t.Helper()
		var b strings.Builder
		for _, shard := range []string{"vt_test_-80", "vt_test_80-"} {
			b.WriteString(shard + ":\n")
			b.WriteString(mysqldExec(ctx, t, vt, "SELECT 't_pk', id, v FROM `"+shard+"`.t_pk ORDER BY id; SELECT 't_np', id, cust, v FROM `"+shard+"`.t_np ORDER BY id"))
		}
		return b.String()
	}
	before := shards()
	if !strings.Contains(before, "t_pk\t1\ta") || !strings.Contains(before, "t_np\t10\t1\ta") {
		t.Fatalf("premise: the seed rows are not on the physical shards:\n%s", before)
	}

	eng, err := Engine{Flavor: FlavorVitess}.WithControlKeyspace("ctl")
	if err != nil {
		t.Fatalf("WithControlKeyspace: %v", err)
	}
	pos := func(n int) ir.Position {
		return ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf(`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-%d"}`, n)}
	}
	moves := map[string]ir.Change{
		"t_pk primary-key change": ir.Update{
			Schema: "test", Table: "t_pk", Position: pos(2),
			Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(2), "v": "a"},
		},
		"t_np vindex change": ir.Update{
			Schema: "test", Table: "t_np", Position: pos(2),
			Before: ir.Row{"id": int64(10), "cust": int64(1), "v": "a"}, After: ir.Row{"id": int64(10), "cust": int64(4), "v": "moved"},
		},
	}
	paths := map[string]func(a *ChangeApplier, stream string, ch <-chan ir.Change) error{
		"serial": func(a *ChangeApplier, stream string, ch <-chan ir.Change) error { return a.Apply(ctx, stream, ch) },
		"batch": func(a *ChangeApplier, stream string, ch <-chan ir.Change) error {
			return a.ApplyBatch(ctx, stream, ch, 50)
		},
		"lanes": func(a *ChangeApplier, stream string, ch <-chan ir.Change) error {
			a.SetApplyConcurrency(2)
			return a.ApplyBatch(ctx, stream, ch, 50)
		},
	}
	open := func(t *testing.T, stream string) *ChangeApplier {
		t.Helper()
		opened, err := eng.OpenChangeApplier(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		a := opened.(*ChangeApplier)
		t.Cleanup(func() { _ = a.Close() })
		if err := a.EnsureControlTable(ctx); err != nil {
			t.Fatalf("EnsureControlTable: %v", err)
		}
		if err := a.WritePosition(ctx, stream, pos(1)); err != nil {
			t.Fatalf("baseline position: %v", err)
		}
		return a
	}
	oneTx := func(c ir.Change) <-chan ir.Change {
		ch := make(chan ir.Change, 3)
		ch <- ir.TxBegin{Position: pos(2)}
		ch <- c
		ch <- ir.TxCommit{Position: pos(2)}
		close(ch)
		return ch
	}
	// Scope first, while the seed row is still where it was: a non-key update
	// of t_pk's v, which moves nothing.
	keep := ir.Update{
		Schema: "test", Table: "t_pk", Position: pos(2),
		Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(1), "v": "b"},
	}
	t.Run("scope/serial over-refuses an unchanged vindex column", func(t *testing.T) {
		a := open(t, "vindex-scope-serial")
		if err := a.Apply(ctx, "vindex-scope-serial", oneTx(keep)); err == nil || !strings.Contains(err.Error(), shardedTargetVindexUpdateMarker) {
			t.Fatalf("serial non-key update = %v; the documented over-refusal (GC-41 (e)) did not fire — if it was fixed, update docs/managed-services.md", err)
		}
	})
	t.Run("scope/batch applies a non-key update", func(t *testing.T) {
		a := open(t, "vindex-scope-batch")
		if err := a.ApplyBatch(ctx, "vindex-scope-batch", oneTx(keep), 50); err != nil {
			t.Fatalf("batched non-key update on a primary-key-vindexed table: %v", err)
		}
		var v string
		if err := db.QueryRowContext(ctx, `SELECT v FROM t_pk WHERE id = 1`).Scan(&v); err != nil || v != "b" {
			t.Fatalf("t_pk id=1 v = %q (err %v); want b", v, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE t_pk SET v = 'a' WHERE id = 1`); err != nil {
			t.Fatalf("restore the seed: %v", err)
		}
	})

	for pathName, apply := range paths {
		for moveName, move := range moves {
			t.Run(pathName+"/"+moveName, func(t *testing.T) {
				stream := "vindex-" + pathName + "-" + strings.Fields(moveName)[0]
				a := open(t, stream)
				err := apply(a, stream, oneTx(move))
				if err == nil || !strings.Contains(err.Error(), "1235") || !strings.Contains(err.Error(), shardedTargetVindexUpdateMarker) {
					t.Fatalf("apply = %v; want vtgate's 1235 VT12001 refusal carrying %s", err, shardedTargetVindexUpdateMarker)
				}
				if after := shards(); after != before {
					t.Fatalf("the refused change altered the physical shards — a cross-shard move landed:\nbefore:\n%s\nafter:\n%s", before, after)
				}
				got, ok, err := a.ReadPosition(ctx, stream)
				if err != nil || !ok || got.Token != pos(1).Token {
					t.Fatalf("position after the refusal = %q (ok=%v, err=%v); want it unmoved at %q", got.Token, ok, err, pos(1).Token)
				}
			})
		}
	}
}
