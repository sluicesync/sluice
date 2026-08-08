//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The change-kind half of the laneapply decode-type-stability premise
// (2026-08-07 invariant sweep). `laneapply.LaneFor`'s safety argument cites
// "the decode path guarantees a given column yields the same Go type across
// Insert/Update/Delete", and every test that could have held it built its
// rows by hand from one `int64` variable — proof that the router is
// kind-agnostic given identical inputs, and no evidence about any real
// reader. This file asks a real Postgres.

package postgres

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// pkTypeCell is one primary-key column type the pgoutput reader must decode
// identically for an INSERT, an UPDATE and a DELETE.
type pkTypeCell struct {
	name string
	// ddlType is the PG column type of the primary key.
	ddlType string
	// literal is a SQL literal of that type used as the row's key.
	literal string
	// replicaIdentity picks which DELETE/UPDATE-before arm the row takes:
	// FULL carries the whole before-image through decodeTuple, DEFAULT
	// carries key columns only — and on an UPDATE whose key did not change,
	// pgoutput omits the old tuple entirely and the reader synthesises the
	// before-image from the after-image (synthesizeKeyOnlyBefore). Three
	// different routes to the same value; all three must agree on its type.
	replicaIdentity string
}

// pkTypeMatrix is the class, not a representative. The PG value decoder
// dispatches on the IR column TYPE, so one family proves nothing about the
// others — the Bug 74 lesson applied to the routing key rather than to a
// value codec.
func pkTypeMatrix() []pkTypeCell {
	families := []struct{ name, ddlType, literal string }{
		{"bigint", "BIGINT", "42"},
		{"integer", "INTEGER", "42"},
		{"smallint", "SMALLINT", "42"},
		{"text", "TEXT", "'k-42'"},
		{"varchar", "VARCHAR(64)", "'k-42'"},
		{"uuid", "UUID", "'6f9619ff-8b86-d011-b42d-00c04fc964ff'"},
		{"numeric", "NUMERIC(20,4)", "42.5000"},
		{"bytea", "BYTEA", `'\x0102ff'::bytea`},
		{"timestamptz", "TIMESTAMPTZ", "'2026-08-07 12:34:56.123456+02'"},
		{"date", "DATE", "'2026-08-07'"},
		{"boolean", "BOOLEAN", "true"},
	}
	var out []pkTypeCell
	for _, f := range families {
		for _, ri := range []string{"FULL", "DEFAULT"} {
			out = append(out, pkTypeCell{
				name:            f.name + "/RI-" + ri,
				ddlType:         f.ddlType,
				literal:         f.literal,
				replicaIdentity: ri,
			})
		}
	}
	return out
}

// tableFor renders the per-cell table name.
func (c pkTypeCell) tableFor(i int) string { return fmt.Sprintf("pk_cell_%02d", i) }

// TestCDCDecodeTypeStableAcrossChangeKinds is the named test
// `laneapply.Router.LaneFor`'s doc comment now cites.
//
// WHAT IT ASSERTS. For every primary-key column family × replica identity,
// against a real Postgres and the real pgoutput reader: the Go type the
// decoder places in the key column is IDENTICAL for Insert.Row,
// Update.After, Update.Before and Delete.Before — and, bound to that, that
// `laneapply.Router.LaneFor` returns the same lane for all four. The second
// half is what makes this a routing test rather than a decoder test: two
// facts pinned separately would leave the argument unpinned, which is the
// shape this whole sweep exists to close.
//
// SCOPE, stated at the gate rather than implied. It reaches the POSTGRES
// pgoutput reader only. The other producers that can feed a concurrent-lane
// applier are the MySQL binlog reader, the VStream reader, pgtrigger,
// sqlite-trigger, and the backup change-chunk reader; each decodes both
// images through one function invoked before the kind switch (code-read
// 2026-08-07), and the chunk reader's separate hazard — a type fold ACROSS
// PROVENANCE rather than across kinds — is gated by
// TestCanonicalKeyValue_SurvivesTheBackupRoundTrip in internal/pipeline/blobcodec.
// Those five are UNGATED for this property. Naming them is deliberate: a
// gate whose coverage is narrower than its name is worse than no gate.
func TestCDCDecodeTypeStableAcrossChangeKinds(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	cells := pkTypeMatrix()
	ddl := ""
	for i, c := range cells {
		tbl := c.tableFor(i)
		ddl += fmt.Sprintf(
			"CREATE TABLE %s (pk %s PRIMARY KEY, payload TEXT);\nALTER TABLE %s REPLICA IDENTITY %s;\n",
			tbl, c.ddlType, tbl, c.replicaIdentity,
		)
	}
	applyPGSQL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	// Let the pump reach steady state before the first write, the same
	// settle the other readers in this package take.
	time.Sleep(500 * time.Millisecond)

	dml := ""
	for i, c := range cells {
		tbl := c.tableFor(i)
		dml += fmt.Sprintf("INSERT INTO %s (pk, payload) VALUES (%s, 'one');\n", tbl, c.literal)
		dml += fmt.Sprintf("UPDATE %s SET payload = 'two' WHERE pk = %s;\n", tbl, c.literal)
		dml += fmt.Sprintf("DELETE FROM %s WHERE pk = %s;\n", tbl, c.literal)
	}
	applyPGSQL(t, dsn, dml)

	want := len(cells) * 3
	got := drainChanges(t, ctx, changes, want, 3*time.Minute)
	if len(got) != want {
		t.Fatalf("drained %d row changes; want %d — the matrix did not complete, so any all-clear below is vacuous", len(got), want)
	}

	// Collect, per table, the key value each change kind carried.
	type observed struct {
		label string
		val   any
	}
	byTable := map[string][]observed{}
	for _, c := range got {
		switch v := c.(type) {
		case ir.Insert:
			byTable[v.Table] = append(byTable[v.Table], observed{"Insert.Row", v.Row["pk"]})
		case ir.Update:
			byTable[v.Table] = append(byTable[v.Table],
				observed{"Update.After", v.After["pk"]},
				observed{"Update.Before", v.Before["pk"]})
		case ir.Delete:
			byTable[v.Table] = append(byTable[v.Table], observed{"Delete.Before", v.Before["pk"]})
		default:
			t.Fatalf("unexpected change kind %T in a DML-only stream", c)
		}
	}

	router := laneapply.NewRouter(8)
	graded := 0
	for i, c := range cells {
		tbl := c.tableFor(i)
		obs := byTable[tbl]
		t.Run(c.name, func(t *testing.T) {
			// Anti-vacuity per cell: four observations (insert, update
			// after, update before, delete before) or the cell proves
			// nothing about its own family.
			if len(obs) != 4 {
				t.Fatalf("observed %d key values for %s; want 4 (Insert.Row, Update.After, Update.Before, Delete.Before): %+v",
					len(obs), tbl, obs)
			}
			wantType := reflect.TypeOf(obs[0].val)
			wantLane := router.LaneFor("public."+tbl, []any{obs[0].val})
			for _, o := range obs[1:] {
				// The type assertion is the CANARY and the lane assertion
				// is the consequence, and they are deliberately separate:
				// laneapply aliases the sized integer widths onto one tag,
				// so a kind-dependent int32/int64 split would move the type
				// and NOT the lane. That is survivable today and it is
				// still a reader regression — fail on it, then say whether
				// it reached routing.
				if gotType := reflect.TypeOf(o.val); gotType != wantType {
					t.Errorf("%s decoded the %s primary key as %v, but %s decoded it as %v — "+
						"laneapply.LaneFor's stability premise says a column's Go kind does not depend on the change kind",
						o.label, c.ddlType, gotType, obs[0].label, wantType)
				}
				if gotLane := router.LaneFor("public."+tbl, []any{o.val}); gotLane != wantLane {
					t.Errorf("%s routes to lane %d but %s routes to lane %d for the same row (%s key, value %v vs %v) — "+
						"the row's INSERT and its DELETE would run on two concurrent lanes and can commit out of source order",
						o.label, gotLane, obs[0].label, wantLane, c.ddlType, o.val, obs[0].val)
				}
			}
			t.Logf("PROVEN %s: %s decodes to %v across all four images, lane %d",
				c.name, c.ddlType, wantType, wantLane)
		})
		graded++
	}
	// Anti-vacuity floor for the matrix as a whole.
	if graded != len(cells) || graded < 20 {
		t.Fatalf("graded %d cells; want every one of the %d in the matrix (floor 20)", graded, len(cells))
	}
}
