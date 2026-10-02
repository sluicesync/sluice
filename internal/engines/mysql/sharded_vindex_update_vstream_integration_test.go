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
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVStream_ShardedTarget_VindexMoveRefusesLoudly pins, on real vtgate, what
// a PRE-VINDEXED sharded target does with each change shape sluice sends it
// (GC-41 (e)).
//
// Keyspace "test" has two shards and three tables: t_pk (vindex hash(id), the
// primary key), t_np (primary key id, vindex hash(cust)) and t_comp (primary
// key (tenant, id), vindex hash(tenant)).
//
// The refusal arms pin the premise that keeps a vindex-moving change from
// silently corrupting the target: vtgate refuses to assign a primary-vindex
// column — Error 1235 VT12001 — for the UPDATE sluice's serial path sends AND
// for the row-alias ON DUPLICATE KEY UPDATE its batch and lane paths send.
// (The VALUES() spelling of the latter is ACCEPTED and duplicates the row
// across shards; TestUpsertSpelling_VitessFamilyNeverUsesValuesFunc keeps
// sluice off it.) For each apply path — serial Apply, the serial batch loop,
// the lanes — a primary-key change on t_pk and a cust change on t_np must fail
// with 1235 and the SHARDED-TARGET-VINDEX-UPDATE marker, leave BOTH physical
// shards exactly as they were (read beneath vtgate from vt_test_-80 /
// vt_test_80-), and leave the stream's position where it was.
//
// The apply arms pin the fix for the over-refusal: a change that does not move
// the vindex applies on every path. Before GC-41 (e) the per-change UPDATE
// re-stated the unchanged vindex column and vtgate refused it, so these were
// red on the serial path, on a partial after-image (which the batch loop hands
// to the serial path), and on an in-shard primary-key change. Each must apply,
// leave the row on exactly one physical shard, and advance the position.
//
// The preflight arm pins part (a): t_np routes on a column outside its primary
// key, which no upsert spelling can serve, so it is refused before anything is
// written under SLUICE-E-TARGET-SHARD-KEY-NOT-IN-UPSERT-KEY, while t_pk and
// t_comp pass.
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
	exec := func(t *testing.T, stmt string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, stmt := range []string{
		`CREATE TABLE t_pk (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32) NOT NULL, w VARCHAR(32) NULL) ENGINE=InnoDB`,
		`ALTER VSCHEMA ON t_pk ADD VINDEX hash(id) USING hash`,
		`CREATE TABLE t_np (id BIGINT NOT NULL PRIMARY KEY, cust BIGINT NOT NULL, v VARCHAR(32) NOT NULL) ENGINE=InnoDB`,
		`ALTER VSCHEMA ON t_np ADD VINDEX hash(cust) USING hash`,
		`CREATE TABLE t_comp (tenant BIGINT NOT NULL, id BIGINT NOT NULL, v VARCHAR(32) NOT NULL, PRIMARY KEY (tenant, id)) ENGINE=InnoDB`,
		`ALTER VSCHEMA ON t_comp ADD VINDEX hash(tenant) USING hash`,
		`INSERT INTO t_pk (id, v, w) VALUES (1, 'a', 'x')`,
		`INSERT INTO t_np (id, cust, v) VALUES (10, 1, 'a')`,
		`INSERT INTO t_comp (tenant, id, v) VALUES (1, 1, 'a')`,
	} {
		exec(t, stmt)
	}
	shards := func() string {
		t.Helper()
		var b strings.Builder
		for _, shard := range []string{"vt_test_-80", "vt_test_80-"} {
			b.WriteString(shard + ":\n")
			b.WriteString(mysqldExec(ctx, t, vt, "SELECT 't_pk', id, v, w FROM `"+shard+"`.t_pk ORDER BY id; "+
				"SELECT 't_np', id, cust, v FROM `"+shard+"`.t_np ORDER BY id; "+
				"SELECT 't_comp', tenant, id, v FROM `"+shard+"`.t_comp ORDER BY tenant, id"))
		}
		return b.String()
	}
	before := shards()
	for _, seed := range []string{"t_pk\t1\ta\tx", "t_np\t10\t1\ta", "t_comp\t1\t1\ta"} {
		if !strings.Contains(before, seed) {
			t.Fatalf("premise: seed row %q is not on the physical shards:\n%s", seed, before)
		}
	}
	restoreSeed := func(t *testing.T) {
		t.Helper()
		exec(t, `UPDATE t_pk SET v = 'a', w = 'x' WHERE id = 1`)
		exec(t, `DELETE FROM t_comp WHERE tenant = 1`)
		exec(t, `INSERT INTO t_comp (tenant, id, v) VALUES (1, 1, 'a')`)
		if got := shards(); got != before {
			t.Fatalf("could not restore the seed:\nwant:\n%s\ngot:\n%s", before, got)
		}
	}

	eng, err := Engine{Flavor: FlavorVitess}.WithControlKeyspace("ctl")
	if err != nil {
		t.Fatalf("WithControlKeyspace: %v", err)
	}
	pos := func(n int) ir.Position {
		return ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf(`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-%d"}`, n)}
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

	// The apply arms: no change here moves a vindex value.
	applies := []struct {
		name   string
		change ir.Update
		// want is the row each must leave, read through vtgate, plus the
		// physical-shard dump line that proves it sits on one shard.
		query, want, shardLine string
	}{
		{
			name: "t_pk non-key update",
			change: ir.Update{
				Schema: "test", Table: "t_pk", Position: pos(2),
				Before: ir.Row{"id": int64(1), "v": "a", "w": "x"}, After: ir.Row{"id": int64(1), "v": "b", "w": "x"},
			},
			query: `SELECT CONCAT(v, '/', w) FROM t_pk WHERE id = 1`, want: "b/x", shardLine: "t_pk\t1\tb\tx",
		},
		{
			// The default-on ADD COLUMN backfill's shape: the key plus the
			// columns it sets. The batch loop hands it to the serial path.
			name: "t_pk partial after-image",
			change: ir.Update{
				Schema: "test", Table: "t_pk", Position: pos(2),
				Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "w": "y"},
			},
			query: `SELECT CONCAT(v, '/', w) FROM t_pk WHERE id = 1`, want: "a/y", shardLine: "t_pk\t1\ta\ty",
		},
		{
			// A primary-key change that keeps the vindex column: legal on
			// vtgate (an in-shard update), and every path sends it serially.
			name: "t_comp in-shard key change",
			change: ir.Update{
				Schema: "test", Table: "t_comp", Position: pos(2),
				Before: ir.Row{"tenant": int64(1), "id": int64(1), "v": "a"}, After: ir.Row{"tenant": int64(1), "id": int64(2), "v": "a"},
			},
			query: `SELECT CONCAT(id, '/', v) FROM t_comp WHERE tenant = 1`, want: "2/a", shardLine: "t_comp\t1\t2\ta",
		},
	}
	for pathName, apply := range paths {
		for _, c := range applies {
			t.Run("applies/"+pathName+"/"+c.name, func(t *testing.T) {
				defer restoreSeed(t)
				stream := "vindex-ok-" + pathName + "-" + strings.ReplaceAll(c.name, " ", "-")
				a := open(t, stream)
				if err := apply(a, stream, oneTx(c.change)); err != nil {
					t.Fatalf("apply = %v; a change that moves no vindex value must apply on a pre-vindexed sharded target", err)
				}
				var got string
				if err := db.QueryRowContext(ctx, c.query).Scan(&got); err != nil || got != c.want {
					t.Fatalf("%s = %q (err %v); want %q", c.query, got, err, c.want)
				}
				if n := strings.Count(shards(), c.shardLine); n != 1 {
					t.Fatalf("row %q is on %d physical shards, want exactly 1:\n%s", c.shardLine, n, shards())
				}
				got2, ok, err := a.ReadPosition(ctx, stream)
				if err != nil || !ok || got2.Token != pos(2).Token {
					t.Fatalf("position = %q (ok=%v, err=%v); want %q", got2.Token, ok, err, pos(2).Token)
				}
			})
		}
	}

	moves := map[string]ir.Change{
		"t_pk primary-key change": ir.Update{
			Schema: "test", Table: "t_pk", Position: pos(2),
			Before: ir.Row{"id": int64(1), "v": "a", "w": "x"}, After: ir.Row{"id": int64(2), "v": "a", "w": "x"},
		},
		"t_np vindex change": ir.Update{
			Schema: "test", Table: "t_np", Position: pos(2),
			Before: ir.Row{"id": int64(10), "cust": int64(1), "v": "a"}, After: ir.Row{"id": int64(10), "cust": int64(4), "v": "moved"},
		},
	}
	for pathName, apply := range paths {
		for moveName, move := range moves {
			t.Run("refuses/"+pathName+"/"+moveName, func(t *testing.T) {
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

	t.Run("preflight refuses a vindex outside the primary key", func(t *testing.T) {
		rw, err := eng.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		err = migcore.PreflightShardKeyUpsert(ctx, &ir.Schema{Tables: []*ir.Table{{Name: "t_pk"}, {Name: "t_comp"}, {Name: "t_np"}}}, rw)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeTargetShardKeyNotInUpsertKey {
			t.Fatalf("preflight = %v; want %s for t_np", err, sluicecode.CodeTargetShardKeyNotInUpsertKey)
		}
		if !strings.Contains(err.Error(), "test.t_np") || !strings.Contains(err.Error(), "(cust)") {
			t.Fatalf("the refusal does not name the table and the vindex column: %v", err)
		}
		if err := migcore.PreflightShardKeyUpsert(ctx, &ir.Schema{Tables: []*ir.Table{{Name: "t_pk"}, {Name: "t_comp"}}}, rw); err != nil {
			t.Fatalf("tables whose vindex is inside the primary key were refused: %v", err)
		}
		if after := shards(); after != before {
			t.Fatalf("the preflight wrote:\n%s", after)
		}
	})
}
