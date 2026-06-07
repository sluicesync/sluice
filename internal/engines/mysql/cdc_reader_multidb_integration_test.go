//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the ADR-0074 Phase 1b multi-database binlog CDC
// reader. The MySQL binlog is server-wide, so a single reader scoped to
// a SELECTED database set must:
//
//   - emit row events from EVERY database in the set, each tagged with
//     its SOURCE database in ir.Change.Schema (read per-event from the
//     TABLE_MAP_EVENT metadata), and
//   - DROP events from a database OUTSIDE the set (same per-event drop
//     mechanism the single-database path uses, just a wider allow set).
//
// This is the part-A pin for Phase 1b.1: the reader sourcing
// change.Schema per source database and honouring the multi-database
// allow-set.

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReader_MultiDatabaseScope boots two in-scope databases plus one
// out-of-scope database on the shared server-wide binlog, opens ONE
// reader scoped to the two, writes to all three, and asserts the emitted
// changes carry the right per-source-database change.Schema and that the
// out-of-scope database's events are dropped.
func TestCDCReader_MultiDatabaseScope(t *testing.T) {
	// Three databases on the same server (one server-wide binlog).
	const (
		dbA   = "md_app_a"
		dbB   = "md_app_b"
		dbOut = "md_out_of_scope"
	)
	dsnA, _ := newSharedDB(t, dbA)
	dsnB, _ := newSharedDB(t, dbB)
	dsnOut, _ := newSharedDB(t, dbOut)

	const seed = `
		CREATE TABLE widgets (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			name  VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsnA, seed)
	applyMySQL(t, dsnB, seed)
	applyMySQL(t, dsnOut, seed)

	eng := Engine{Flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// The reader's DSN binds it to dbA, but the multi-database scope
	// widens the allow-set to {dbA, dbB}. dbOut is deliberately excluded.
	rdr, err := eng.OpenCDCReader(ctx, dsnA)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	scoper, ok := rdr.(ir.CDCDatabaseScoper)
	if !ok {
		t.Fatalf("reader %T does not implement ir.CDCDatabaseScoper", rdr)
	}
	selected := map[string]struct{}{dbA: {}, dbB: {}}
	scoper.SetCDCDatabaseScope(func(db string) bool {
		_, in := selected[db]
		return in
	})

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Let the syncer register at "now" before generating events.
	time.Sleep(300 * time.Millisecond)

	// Write to BOTH in-scope databases and the out-of-scope one. The
	// server-wide binlog carries all three; the reader must keep the two
	// in-scope and drop the out-of-scope one.
	applyMySQL(t, dsnA, `INSERT INTO widgets (name) VALUES ('a1'), ('a2');`)
	applyMySQL(t, dsnB, `INSERT INTO widgets (name) VALUES ('b1');`)
	applyMySQL(t, dsnOut, `INSERT INTO widgets (name) VALUES ('out1'), ('out2');`)
	// A trailing in-scope write acts as a fence: once we've seen it, every
	// earlier in-scope event has flowed, so any out-of-scope event that
	// was going to (wrongly) arrive would already have shown up.
	applyMySQL(t, dsnB, `INSERT INTO widgets (name) VALUES ('b2');`)

	// Expect exactly 4 in-scope inserts: a1, a2 (dbA) + b1, b2 (dbB).
	got := drainChanges(t, ctx, changes, 4, 30*time.Second)
	if len(got) != 4 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d in-scope changes; want 4 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d in-scope changes; want 4", len(got))
	}

	// Tally per-source-database and assert every change is an Insert
	// carrying the correct source database in change.Schema. An
	// out-of-scope row showing up here is the silent-leak this pins
	// against.
	perDB := map[string]int{}
	names := map[string]bool{}
	for _, c := range got {
		ins, isInsert := c.(ir.Insert)
		if !isInsert {
			t.Fatalf("unexpected change type %T; want ir.Insert", c)
		}
		if ins.Schema == dbOut {
			t.Fatalf("out-of-scope database %q leaked an event into the stream: %+v", dbOut, ins)
		}
		if ins.Schema != dbA && ins.Schema != dbB {
			t.Fatalf("change.Schema = %q; want one of {%q, %q}", ins.Schema, dbA, dbB)
		}
		if ins.Table != "widgets" {
			t.Errorf("change.Table = %q; want widgets", ins.Table)
		}
		perDB[ins.Schema]++
		if name, ok := ins.Row["name"].(string); ok {
			names[name] = true
		}
	}

	if perDB[dbA] != 2 {
		t.Errorf("dbA (%q) change count = %d; want 2", dbA, perDB[dbA])
	}
	if perDB[dbB] != 2 {
		t.Errorf("dbB (%q) change count = %d; want 2", dbB, perDB[dbB])
	}
	for _, want := range []string{"a1", "a2", "b1", "b2"} {
		if !names[want] {
			t.Errorf("missing expected in-scope row name %q", want)
		}
	}
	for _, leaked := range []string{"out1", "out2"} {
		if names[leaked] {
			t.Errorf("out-of-scope row name %q leaked into the stream", leaked)
		}
	}
}
