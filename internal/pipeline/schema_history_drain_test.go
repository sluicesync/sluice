// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// drainTable is kd(id int, k text COLLATE coll) with the key on pk ("k" or
// "id"), the shape a Postgres Relation projects.
func drainTable(coll string, det ir.CollationDeterminism, pk string) *ir.Table {
	return &ir.Table{
		Schema: "public", Name: "kd",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "k", Type: ir.Text{Collation: coll, Determinism: det}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: pk}}},
	}
}

func drainSnap(t *ir.Table) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{Position: ir.Position{Token: "0/0"}, Schema: t.Schema, Table: t.Name, IR: t}
}

// TestSchemaHistoryDrain_SameAnchorKeyCollationRefuses pins
// KEY-COLLATION-SAME-ANCHOR deterministically (the integration cell depends
// on whether the reader keeps up). Two versions of one table at one anchor
// that disagree on a KEY column's collation refuse — in one drain call or
// across a window's chunks, on a primary key or on a UNIQUE key only the
// recorded schema knows — while an identical re-emit, a non-key collation
// change, and versions at distinct anchors still record as before.
func TestSchemaHistoryDrain_SameAnchorKeyCollationRefuses(t *testing.T) {
	ci := func(pk string) *ir.Table { return drainTable("ci", ir.CollationNonDeterministic, pk) }
	c := func(pk string) *ir.Table { return drainTable("C", ir.CollationDeterministic, pk) }
	uniqueK := &ir.Schema{Tables: []*ir.Table{{
		Schema: "public", Name: "kd",
		Indexes: []*ir.Index{{Name: "kd_k_key", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}},
	}}}

	for _, tc := range []struct {
		name    string
		chunks  [][]ir.SchemaSnapshot
		keys    *ir.Schema
		refuse  bool
		entries int
	}{
		{"primary key ci then C, one chunk", [][]ir.SchemaSnapshot{{drainSnap(ci("k")), drainSnap(c("k"))}}, nil, true, 1},
		{"primary key ci then C, across chunks", [][]ir.SchemaSnapshot{{drainSnap(ci("k"))}, {drainSnap(c("k"))}}, nil, true, 1},
		{"unique key from the recorded schema", [][]ir.SchemaSnapshot{{drainSnap(ci("id")), drainSnap(c("id"))}}, uniqueK, true, 1},
		{"non-key column changes collation", [][]ir.SchemaSnapshot{{drainSnap(ci("id")), drainSnap(c("id"))}}, nil, false, 1},
		{"identical re-emit", [][]ir.SchemaSnapshot{{drainSnap(ci("k")), drainSnap(ci("k"))}}, nil, false, 1},
		{"distinct anchors", [][]ir.SchemaSnapshot{{drainSnap(ci("k")), {Position: ir.Position{Token: "0/16B3748"}, Schema: "public", Table: "kd", IR: c("k")}}}, nil, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &irbackup.Manifest{Schema: tc.keys}
			var d schemaHistoryDrain
			var err error
			for _, chunk := range tc.chunks {
				if err = d.into(m, chunk); err != nil {
					break
				}
			}
			if tc.refuse {
				if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationSameAnchorMarker) {
					t.Fatalf("into = %v; want %s", err, keyCollationSameAnchorMarker)
				}
			} else if err != nil {
				t.Fatalf("into = %v; want nil", err)
			}
			if len(m.SchemaHistory) != tc.entries {
				t.Errorf("SchemaHistory has %d entries; want %d", len(m.SchemaHistory), tc.entries)
			}
		})
	}
}

// TestSchemaHistoryDrain_NotEstablishedWeakensTheKeptVersion pins the review
// finding on the first cut: a later same-anchor version whose key collation
// is NOT ESTABLISHED (named, Unknown determinism — the Postgres reader read
// it from a catalog row newer than its rows) was "the same collation" as a
// kept {ci, NonDeterministic} and was dropped, so the restore armed nothing
// and the rows written under "C" merged. The kept entry must now record the
// key as not established, read back through the manifest codec (the restore
// decodes the entry, not the drain's in-memory table); and a kept
// not-established key, which already compares as bytes, absorbs a later
// named shape without refusing.
func TestSchemaHistoryDrain_NotEstablishedWeakensTheKeptVersion(t *testing.T) {
	nondet := drainTable("ci", ir.CollationNonDeterministic, "k")
	unest := drainTable("ci", ir.CollationDeterminismUnknown, "k")
	unestOther := drainTable("ci_ai", ir.CollationDeterminismUnknown, "k")
	strict := drainTable("C", ir.CollationDeterministic, "k")
	keyType := func(t *testing.T, m *irbackup.Manifest) ir.Text {
		t.Helper()
		if len(m.SchemaHistory) != 1 {
			t.Fatalf("SchemaHistory has %d entries; want 1", len(m.SchemaHistory))
		}
		tbl, err := ir.UnmarshalTable(m.SchemaHistory[0].TableJSON)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range tbl.Columns {
			if c.Name == "k" {
				return c.Type.(ir.Text)
			}
		}
		t.Fatal("no column k")
		return ir.Text{}
	}
	for _, tc := range []struct {
		name        string
		first, then *ir.Table
		wantColl    string
		wantDet     ir.CollationDeterminism
	}{
		{"kept ci, then ci not established: weakened", nondet, unest, "ci", ir.CollationDeterminismUnknown},
		{"kept ci, then another name not established: weakened", nondet, unestOther, "ci_ai", ir.CollationDeterminismUnknown},
		{"kept not established, then C: already bytes, no refusal", unest, strict, "ci", ir.CollationDeterminismUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &irbackup.Manifest{}
			var d schemaHistoryDrain
			firstBefore := tc.first.Columns[1].Type
			if err := d.into(m, []ir.SchemaSnapshot{drainSnap(tc.first), drainSnap(tc.then)}); err != nil {
				t.Fatalf("into = %v; want nil", err)
			}
			got := keyType(t, m)
			if !migcore.CollationUnestablished(got) || got.Collation != tc.wantColl || got.Determinism != tc.wantDet {
				t.Errorf("kept key k = %+v; want collation %q not established", got, tc.wantColl)
			}
			// The reader's own table is not modified in place.
			if tc.first.Columns[1].Type != firstBefore {
				t.Errorf("the kept table was modified in place: %+v, was %+v", tc.first.Columns[1].Type, firstBefore)
			}
		})
	}
}
