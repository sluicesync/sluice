// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_StaleFullBeforeImage_AppliesByKey is the MySQL applier
// half of the Bug 339 pin ([appliershared.KeyPredicate]); the Postgres test of
// the same name carries the full story. An UPDATE or DELETE whose full
// before-image no longer matches the target row's NON-KEY columns must still
// reach the row by its primary key.
//
// The MySQL batch path already applied a keyed UPDATE with a complete
// after-image as an upsert by primary key (ADR-0140), which is why the filed
// single-stream MySQL cells were exact. Every other UPDATE and DELETE takes the
// per-change builder, and these are the paths driven here:
//   - per-change Apply;
//   - the batch loop's serial fall-back, reached by a partial after-image
//     (audit 2026-08-05 C-10) and by a keyless table;
//   - a Shape A stream on the lanes: its rows are routed before the shard
//     column is stamped, so a primary key that includes the discriminator is
//     not routable and every row change is applied alone, as a barrier —
//     which is why the filed Shape A MySQL cells lost rows. The pin also holds
//     the discriminator in the WHERE: another shard's row with the same id is
//     left alone.
//
// The control is a keyless table, whose whole-row WHERE is its only address
// (GC-42): a stale image must still match nothing.
func TestChangeApplier_StaleFullBeforeImage_AppliesByKey(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	for i, path := range []struct {
		name    string
		batch   int
		lanes   int
		shapeA  bool
		partial bool
	}{
		{name: "per-change"},
		{name: "batch-serial-fallback", batch: 50, lanes: 1, partial: true},
		{name: "shapeA-lanes-barrier", batch: 50, lanes: concurrentLanesW, shapeA: true},
	} {
		t.Run(path.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			p := fmt.Sprintf("kp%d", i)
			shardCol, shardPK := "", ""
			if path.shapeA {
				shardCol, shardPK = ", src_shard VARCHAR(8) NOT NULL DEFAULT 'lo'", ", src_shard"
			}
			applyMySQLApplier(t, dsn, fmt.Sprintf(`
				CREATE TABLE %[1]s_one (
					id BIGINT NOT NULL, t VARCHAR(20), n DECIMAL(10,3), dt DATETIME(6),
					b TINYINT(1), by_ VARBINARY(4), f DOUBLE, z VARCHAR(20)%[2]s,
					PRIMARY KEY (id%[3]s)) ENGINE=InnoDB;
				CREATE TABLE %[1]s_keyless (id INT, v VARCHAR(20)%[2]s) ENGINE=InnoDB;
				INSERT INTO %[1]s_keyless (id, v) VALUES (1, 'target');
			`, p, shardCol, shardPK))
			for id := 1; id <= 3; id++ {
				cols, vals := "", ""
				if path.shapeA {
					cols, vals = ", src_shard", ", 'lo'"
				}
				applyMySQLApplier(t, dsn, fmt.Sprintf(`INSERT INTO %s_one (id, t, n, dt, b, by_, f, z%s)
					VALUES (%d, 'target-t', 1.500, '2024-01-02 03:04:05', 1, 0x01, 0.5, NULL%s)`, p, cols, id, vals))
			}
			if path.shapeA {
				// Another shard's rows with the same keys: the stale image
				// must not reach them.
				applyMySQLApplier(t, dsn, fmt.Sprintf(`INSERT INTO %s_one (id, t, src_shard) VALUES (1, 'other-shard', 'hi'), (2, 'other-shard', 'hi')`, p))
			}

			stale := func(id int64) ir.Row {
				return ir.Row{
					"id": id, "t": "stale-t", "n": "9.999", "dt": time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
					"b": false, "by_": []byte{0xFF}, "f": -1.25, "z": "was-null",
				}
			}
			after := ir.Row{
				"id": int64(1), "t": "after-t", "n": "2.250", "dt": time.Date(2025, 5, 6, 7, 8, 9, 0, time.UTC),
				"b": false, "by_": []byte{0x02}, "f": 2.5, "z": nil,
			}
			if path.partial {
				delete(after, "z") // an omitted column: "preserve the target's value"
			}
			mp := func(n int) ir.Position { return ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("kp-%06d", n)} }
			events := []ir.Change{
				ir.Update{Position: mp(1), Schema: "target_db", Table: p + "_one", Before: stale(1), After: after},
				ir.Delete{Position: mp(2), Schema: "target_db", Table: p + "_one", Before: stale(2)},
				ir.Update{
					Position: mp(3), Schema: "target_db", Table: p + "_keyless",
					Before: ir.Row{"id": int64(1), "v": "stale"},
					After:  ir.Row{"id": int64(1), "v": "must-not-land"},
				},
			}
			a := openConcurrentApplier(t, ctx, dsn, path.lanes)
			defer closeApplier(a)
			if path.shapeA {
				a.(*ChangeApplier).SetShardColumn("src_shard", "lo")
			}
			if err := a.EnsureControlTable(ctx); err != nil {
				t.Fatalf("EnsureControlTable: %v", err)
			}
			ch := make(chan ir.Change, len(events))
			for _, e := range events {
				ch <- e
			}
			close(ch)
			streamID := "kp-" + path.name
			if path.batch == 0 {
				if err := a.Apply(ctx, streamID, ch); err != nil {
					t.Fatalf("Apply: %v", err)
				}
			} else if err := a.(ir.BatchedChangeApplier).ApplyBatch(ctx, streamID, ch, path.batch); err != nil {
				t.Fatalf("ApplyBatch: %v", err)
			}

			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			got := func(q string) string {
				t.Helper()
				var s sql.NullString
				if err := db.QueryRowContext(ctx, q).Scan(&s); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				if !s.Valid {
					return "<NULL>"
				}
				return s.String
			}
			lo := ""
			if path.shapeA {
				lo = " AND src_shard = 'lo'"
			}
			wantZ := "<NULL>"
			if g, w := got(`SELECT CONCAT_WS('|', t, CAST(n AS CHAR), DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s'), CAST(b AS CHAR),
					LOWER(HEX(by_)), CAST(f AS CHAR), COALESCE(z, '<NULL>')) FROM `+p+`_one WHERE id = 1`+lo),
				"after-t|2.250|2025-05-06 07:08:09|0|02|2.5|"+wantZ; g != w {
				t.Errorf("UPDATE with a stale full before-image: target row id=1 is %q, want the after-image %q — "+
					"the WHERE compared non-key columns and matched no row (Bug 339)", g, w)
			}
			if g := got(`SELECT CAST(COUNT(*) AS CHAR) FROM ` + p + `_one WHERE id = 2` + lo); g != "0" {
				t.Errorf("DELETE with a stale full before-image left id=2 on the target (Bug 339)")
			}
			if path.shapeA {
				if g := got(`SELECT GROUP_CONCAT(CONCAT(id, ':', t) ORDER BY id) FROM ` + p + `_one WHERE src_shard = 'hi'`); g != "1:other-shard,2:other-shard" {
					t.Errorf("another shard's rows were touched by this shard's key-scoped writes: %q", g)
				}
			}
			if g := got(`SELECT v FROM ` + p + `_keyless WHERE id = 1`); g != "target" {
				t.Errorf("keyless control: a stale whole-row image overwrote the row (v = %q) — a keyless "+
					"table's whole-row WHERE is its only address and must keep matching nothing", g)
			}
		})
	}
}
