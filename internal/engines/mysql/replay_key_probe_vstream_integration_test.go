//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_ProbeReplayKey_Vitess pins the F-E1 replay-door surfaces on a
// vtgate-fronted target, unsharded and sharded. Both probes read
// information_schema through vtgate scoped to the session's DATABASE(), and
// the premise that vtgate answers that for the keyspace —
// rather than for some underlying vttablet database, or not at all — is a
// fact about Vitess, not about this code, so it is asserted here rather than
// assumed (the CLAUDE.md premise-naming rule). A probe that silently read
// "absent" for every table would make both doors inert on PlanetScale and
// Vitess: "absent" is judged on the recorded schema alone, and "empty" is
// never refused.
func TestVStream_ProbeReplayKey_Vitess(t *testing.T) {
	for _, shards := range []int{1, 2} {
		t.Run(map[int]string{1: "unsharded", 2: "sharded"}[shards], func(t *testing.T) {
			dsn, _, _, cleanup := startVTTestServerWithShards(t, shards)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			for _, ddl := range []string{
				"CREATE TABLE no_key (id BIGINT NOT NULL, v TEXT)",
				"CREATE TABLE pk (id BIGINT NOT NULL PRIMARY KEY, v TEXT)",
				"CREATE TABLE nullable_unique (id BIGINT NOT NULL PRIMARY KEY, u BIGINT NULL, v TEXT, UNIQUE KEY (u))",
				"CREATE TABLE surrogate (sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, id BIGINT NOT NULL, v TEXT)",
			} {
				if _, err := db.ExecContext(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			if shards > 1 {
				// A sharded table is readable and writable through vtgate only
				// once it has a primary vindex — a real sharded restore or
				// broker target has one on every table.
				for _, tbl := range []string{"no_key", "pk", "nullable_unique", "surrogate"} {
					applyVTTestSQL(t, dsn, `ALTER VSCHEMA ON test.`+tbl+` ADD VINDEX hash(id) USING hash`)
				}
			}
			// A sharded keyspace's vschema picks up a new table
			// asynchronously ("table pk not found" for a moment).
			deadline := time.Now().Add(60 * time.Second)
			for {
				_, err := db.ExecContext(ctx, "INSERT INTO pk (id, v) VALUES (1, 'x')")
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal(err)
				}
				time.Sleep(500 * time.Millisecond)
			}

			rw, err := (Engine{Flavor: FlavorVitess}).OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rw.(*RowWriter).Close() }()
			prober := rw.(ir.ReplayKeyProber)
			checker := rw.(ir.TableEmptyChecker)
			cases := []struct {
				table              string
				recorded           []string
				exists, keyed, emp bool
			}{
				{"no_key", []string{"id", "v"}, true, false, true},
				{"pk", []string{"id", "v"}, true, true, false},
				// The nullable UNIQUE does not count, but the PRIMARY KEY is
				// supplied, so the table is keyed.
				{"nullable_unique", []string{"id", "u", "v"}, true, true, true},
				{"surrogate", []string{"id", "v"}, true, false, true},
				{"absent", []string{"id"}, false, false, true},
			}
			for _, c := range cases {
				recorded := &ir.Table{Name: c.table}
				for _, n := range c.recorded {
					recorded.Columns = append(recorded.Columns, &ir.Column{Name: n, Type: ir.Integer{Width: 64}})
				}
				exists, keyed, err := prober.ProbeReplayKey(ctx, recorded)
				if err != nil || exists != c.exists || keyed != c.keyed {
					t.Errorf("%s: ProbeReplayKey = (%v, %v, %v); want (%v, %v, nil)", c.table, exists, keyed, err, c.exists, c.keyed)
				}
				empty, err := checker.IsTableEmpty(ctx, recorded)
				if err != nil || empty != c.emp {
					t.Errorf("%s: IsTableEmpty = (%v, %v); want (%v, nil)", c.table, empty, err, c.emp)
				}
			}
		})
	}
}
