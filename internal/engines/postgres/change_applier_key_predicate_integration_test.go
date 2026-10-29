// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_StaleFullBeforeImage_AppliesByKey is the applier half of
// the Bug 339 pin ([appliershared.KeyPredicate]). A full before-image whose
// NON-KEY columns no longer match the target row — what a VStream source
// produces once the ADD COLUMN backfill has written ahead of the stream, and
// what a trigger source's full capture mode or a replayed chain produces after
// any divergence — must still reach the row by its primary key: an UPDATE
// lands its after-image and a DELETE removes the row. Before the fix both
// matched no row and were dropped at exit 0.
//
// Every apply path the Postgres applier has is driven: per-change Apply, the
// pipelined batch loop, and the concurrent lanes (which dispatch through the
// pipelined builder). The stale columns span the value families a WHERE binds
// differently — text, numeric, timestamptz, boolean, bytea, float8, a NULL
// against a value — and the keys span a single bigint and a composite
// (text, int).
//
// The control is a keyless table: there the whole-row WHERE is the only
// address the row has (GC-42), so a stale image must still match nothing, and
// does.
func TestChangeApplier_StaleFullBeforeImage_AppliesByKey(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	for _, path := range []struct {
		name  string
		batch int
		lanes int
	}{
		{"per-change", 0, 1},
		{"pipelined-batch", 50, 1},
		{"lanes", 50, concurrentLanesW},
	} {
		t.Run(path.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			p := "kp_" + strings.ReplaceAll(path.name, "-", "_")
			applyPGApplier(t, dsn, fmt.Sprintf(`
				CREATE TABLE %[1]s_one (
					id BIGINT PRIMARY KEY, t TEXT, n NUMERIC(10,3), ts TIMESTAMPTZ,
					b BOOLEAN, by BYTEA, f DOUBLE PRECISION, z TEXT);
				INSERT INTO %[1]s_one VALUES
					(1, 'target-t', 1.500, '2024-01-02 03:04:05+00', true, '\x01', 0.5, NULL),
					(2, 'target-t', 1.500, '2024-01-02 03:04:05+00', true, '\x01', 0.5, NULL),
					(3, 'target-t', 1.500, '2024-01-02 03:04:05+00', true, '\x01', 0.5, NULL);
				CREATE TABLE %[1]s_comp (k TEXT, n INT, v TEXT, PRIMARY KEY (k, n));
				INSERT INTO %[1]s_comp VALUES ('a', 1, 'target'), ('a', 2, 'target');
				CREATE TABLE %[1]s_keyless (id INT, v TEXT);
				INSERT INTO %[1]s_keyless VALUES (1, 'target');
			`, p))

			stale := ir.Row{
				"t": "stale-t", "n": "9.999", "ts": time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
				"b": false, "by": []byte{0xFF}, "f": -1.25, "z": "was-null",
			}
			with := func(r ir.Row, kv ...any) ir.Row {
				out := ir.Row{}
				for k, v := range r {
					out[k] = v
				}
				for i := 0; i < len(kv); i += 2 {
					out[kv[i].(string)] = kv[i+1]
				}
				return out
			}
			after := ir.Row{
				"id": int64(1), "t": "after-t", "n": "2.250", "ts": time.Date(2025, 5, 6, 7, 8, 9, 0, time.UTC),
				"b": false, "by": []byte{0x02}, "f": 2.5, "z": nil,
			}
			events := []ir.Change{
				ir.Update{Position: cpos("1"), Schema: "public", Table: p + "_one", Before: with(stale, "id", int64(1)), After: after},
				ir.Delete{Position: cpos("2"), Schema: "public", Table: p + "_one", Before: with(stale, "id", int64(2))},
				ir.Update{
					Position: cpos("3"), Schema: "public", Table: p + "_comp",
					Before: ir.Row{"k": "a", "n": int64(1), "v": "stale"},
					After:  ir.Row{"k": "a", "n": int64(1), "v": "after"},
				},
				ir.Delete{Position: cpos("4"), Schema: "public", Table: p + "_comp", Before: ir.Row{"k": "a", "n": int64(2), "v": "stale"}},
				ir.Update{
					Position: cpos("5"), Schema: "public", Table: p + "_keyless",
					Before: ir.Row{"id": int64(1), "v": "stale"},
					After:  ir.Row{"id": int64(1), "v": "must-not-land"},
				},
			}
			a := openConcurrentApplier(t, ctx, dsn, path.lanes)
			defer func() { _ = a.Close() }()
			if path.batch == 0 {
				if err := a.EnsureControlTable(ctx); err != nil {
					t.Fatalf("EnsureControlTable: %v", err)
				}
				ch := make(chan ir.Change, len(events))
				for _, e := range events {
					ch <- e
				}
				close(ch)
				if err := a.Apply(ctx, "kp-"+path.name, ch); err != nil {
					t.Fatalf("Apply: %v", err)
				}
			} else {
				pumpConcurrentChanges(t, ctx, a, "kp-"+path.name, events, path.batch)
			}

			db, err := sql.Open("pgx", dsn)
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
			if g, w := got(`SELECT concat_ws('|', t, n::text, to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
					b::text, encode(by, 'hex'), f::text, coalesce(z, '<NULL>')) FROM `+p+`_one WHERE id = 1`),
				"after-t|2.250|2025-05-06 07:08:09|false|02|2.5|<NULL>"; g != w {
				t.Errorf("UPDATE with a stale full before-image: target row id=1 is %q, want the after-image %q — "+
					"the WHERE compared non-key columns and matched no row (Bug 339)", g, w)
			}
			if g := got(`SELECT count(*)::text FROM ` + p + `_one WHERE id = 2`); g != "0" {
				t.Errorf("DELETE with a stale full before-image left id=2 on the target (Bug 339)")
			}
			if g := got(`SELECT count(*)::text FROM ` + p + `_one`); g != "2" {
				t.Errorf("the key-scoped writes touched rows they did not name: %s rows remain, want 2", g)
			}
			if g := got(`SELECT v FROM ` + p + `_comp WHERE k = 'a' AND n = 1`); g != "after" {
				t.Errorf("composite-key UPDATE with a stale before-image: v = %q, want \"after\" (Bug 339)", g)
			}
			if g := got(`SELECT count(*)::text FROM ` + p + `_comp WHERE k = 'a' AND n = 2`); g != "0" {
				t.Errorf("composite-key DELETE with a stale before-image left the row (Bug 339)")
			}
			if g := got(`SELECT v FROM ` + p + `_keyless WHERE id = 1`); g != "target" {
				t.Errorf("keyless control: a stale whole-row image overwrote the row (v = %q) — a keyless "+
					"table's whole-row WHERE is its only address and must keep matching nothing", g)
			}
		})
	}
}
