//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-42's multi-row guard on the MySQL applier — the backstop the Postgres
// fix owes its sibling (see checkKeyScopedResult for why MySQL cannot reach
// the Postgres mechanism). The live shapes are a before-image that does not
// carry a target key: one narrowed to a column the target does not hold
// unique (a Postgres source under REPLICA IDENTITY USING INDEX), and a
// key-narrowed one against a keyless target. Every MySQL apply path that
// reaches the serial UPDATE/DELETE: per-change, the batch loop's serial arm (a
// before-image without the PRIMARY KEY is not coalescable), and the lanes
// (not routable → barrier).

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestGC42_MySQLMultiRowMatchRefused(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	paths := []struct {
		name         string
		lanes, batch int
	}{
		{"pc", 0, 1},
		{"b", 0, 100},
		{"l", concurrentLanesW, 100},
	}
	const pkTable = "CREATE TABLE %[1]s (id INT PRIMARY KEY, u VARCHAR(8) NOT NULL, v VARCHAR(8)); INSERT INTO %[1]s VALUES (1,'x','a'),(2,'x','b');"
	cells := []struct {
		name, ddl string
		row       func(table string) ir.Change
		refused   bool
	}{
		{"upd", pkTable, func(table string) ir.Change {
			return ir.Update{Schema: "target_db", Table: table, Before: ir.Row{"u": "x"}, After: ir.Row{"u": "x", "v": "z"}}
		}, true},
		{"del", pkTable, func(table string) ir.Change {
			return ir.Delete{Schema: "target_db", Table: table, Before: ir.Row{"u": "x"}}
		}, true},
		// Keyed by a NOT NULL unique index alone, as Postgres counts it.
		{"uniq", "CREATE TABLE %[1]s (k VARCHAR(8) NOT NULL UNIQUE, u VARCHAR(8) NOT NULL, v VARCHAR(8)); INSERT INTO %[1]s VALUES ('1','x','a'),('2','x','b');", func(table string) ir.Change {
			return ir.Delete{Schema: "target_db", Table: table, Before: ir.Row{"u": "x"}}
		}, true},
		// A keyless target fed a key-narrowed before-image is not exempt.
		{"kl_narrow", "CREATE TABLE %[1]s (id INT, v VARCHAR(8)); INSERT INTO %[1]s VALUES (2,'a'),(2,'b');", func(table string) ir.Change {
			return ir.Delete{Schema: "target_db", Table: table, Before: ir.Row{"id": int64(2)}}
		}, true},
		// A keyless table addressed by its whole row is (ADR-0089 caveat).
		{"kl_whole", "CREATE TABLE %[1]s (id INT, v VARCHAR(8)); INSERT INTO %[1]s VALUES (1,'a'),(1,'a');", func(table string) ir.Change {
			return ir.Delete{Schema: "target_db", Table: table, Before: ir.Row{"id": int64(1), "v": "a"}}
		}, false},
		// Zero rows stays tolerated (ADR-0010 resume idempotency).
		{"zero", pkTable, func(table string) ir.Change {
			return ir.Delete{Schema: "target_db", Table: table, Before: ir.Row{"u": "absent"}}
		}, false},
	}
	for _, p := range paths {
		for _, c := range cells {
			t.Run(p.name+"/"+c.name, func(t *testing.T) {
				table := fmt.Sprintf("gc42_%s_%s", p.name, c.name)
				applyMySQLApplier(t, dsn, fmt.Sprintf(c.ddl, table))
				cols := "id, v"
				if c.name == "uniq" {
					cols = "k, v"
				}
				state := fmt.Sprintf("SELECT GROUP_CONCAT(CONCAT_WS(',', %[1]s) ORDER BY %[1]s) FROM %[2]s", cols, table)
				before, _ := queryScalarString(t, dsn, state)

				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()
				a := openConcurrentApplier(t, ctx, dsn, p.lanes)
				defer func() { _ = a.(*ChangeApplier).Close() }()
				if err := a.EnsureControlTable(ctx); err != nil {
					t.Fatalf("EnsureControlTable: %v", err)
				}
				ch := make(chan ir.Change, 1)
				switch v := c.row(table).(type) {
				case ir.Update:
					v.Position = ir.Position{Engine: engineNameMySQL, Token: table + "-1"}
					ch <- v
				case ir.Delete:
					v.Position = ir.Position{Engine: engineNameMySQL, Token: table + "-1"}
					ch <- v
				}
				close(ch)
				var err error
				if p.batch <= 1 {
					err = a.Apply(ctx, table, ch)
				} else {
					err = a.(ir.BatchedChangeApplier).ApplyBatch(ctx, table, ch, p.batch)
				}

				if !c.refused {
					if err != nil {
						t.Fatalf("want the write applied; got %v", err)
					}
					return
				}
				if err == nil || !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) {
					t.Fatalf("want %s; got %v", appliershared.KeyScopedWriteMultiMatchMarker, err)
				}
				if !ir.IsTerminal(err) {
					t.Errorf("the refusal is not terminal: %v", err)
				}
				if coded, ok := sluicecode.FromError(err); !ok || coded.Code != sluicecode.CodeCDCKeyMatchedMultipleRows {
					t.Errorf("the refusal is not coded %s: %v", sluicecode.CodeCDCKeyMatchedMultipleRows, err)
				}
				if !strings.Contains(err.Error(), table) {
					t.Errorf("the refusal does not name the table: %v", err)
				}
				if after, _ := queryScalarString(t, dsn, state); after != before {
					t.Errorf("target = %q; want it untouched at %q", after, before)
				}
			})
		}
	}
}
