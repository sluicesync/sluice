// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// SLM-1b: on a warm resume the pipeline seeds each lane with the
// TARGET's zone witness — a table projected to its zone-family columns
// ONLY (pipeline.zoneWitnessProjection), never the full source shape. So
// a seed here is deliberately narrower than the table: no `id`, just the
// temporal column. Each of the three emitters must (1) refuse a swap
// against that narrow prior, (2) NOT refuse a same-family first boundary,
// and (3) emit that first boundary as a history version exactly as it
// would unseeded — the narrow prior is the refusal's prev and nothing
// else. Pinned per lane because the three do not share the code path
// (the sibling-sweep rule).

// witnessSeed is what the pipeline hands over for a warm-resumed table
// whose only zone-family column is created_at.
func witnessSeed(table string, zoned bool) []*ir.Table {
	var typ ir.Type = ir.DateTime{}
	if zoned {
		typ = ir.Timestamp{WithTimeZone: true}
	}
	return []*ir.Table{{Name: table, Columns: []*ir.Column{{Name: "created_at", Type: typ}}}}
}

func countSnapshots(out chan ir.Change) int {
	close(out)
	n := 0
	for c := range out {
		if _, ok := c.(ir.SchemaSnapshot); ok {
			n++
		}
	}
	return n
}

func TestB1_MaybeSnapshot_WitnessShapedSeed(t *testing.T) {
	anchor := ir.Position{Engine: engineNameMySQL, Token: "ddl-anchor"}
	newReader := func() *CDCReader {
		return &CDCReader{
			schema:                     "app",
			snapshotSig:                map[string]ir.SchemaSignature{},
			pendingDDLActive:           true,
			pendingDDLAnchor:           anchor,
			schemaDeltaAppliesToTarget: true,
		}
	}
	full := func(col *ir.Column) *tableSchema {
		return &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			col,
		}}
	}
	for _, tc := range []struct {
		name   string
		zoned  bool
		swap   *ir.Column
		same   *ir.Column
		wantRe string
	}{
		{"witness TIMESTAMP", true, dtCol("created_at", 0), tsCol("created_at", 6), "created_at"},
		{"witness DATETIME", false, tsCol("created_at", 0), dtCol("created_at", 6), "created_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReader()
			r.SetSchemaSeed(witnessSeed("events", tc.zoned))
			out := make(chan ir.Change, 8)
			requireSessionTZRefusal(t, r.maybeSnapshotSchemaB1(context.Background(), "app.events", full(tc.swap), out), tc.wantRe)

			r = newReader()
			r.SetSchemaSeed(witnessSeed("events", tc.zoned))
			out = make(chan ir.Change, 8)
			if err := r.maybeSnapshotSchemaB1(context.Background(), "app.events", full(tc.same), out); err != nil {
				t.Fatalf("same-family first boundary refused against the witness: %v", err)
			}
			if n := countSnapshots(out); n != 1 {
				t.Errorf("same-family first boundary emitted %d snapshots; want 1 — the narrow witness must not stand in for an emitted version", n)
			}
			// The emitted-version memo is the FULL table signature, not
			// the witness's: the seed never touches the ADR-0049 contract.
			want := ir.SchemaSignatureOf(projectTableIR(full(tc.same)))
			if got, ok := r.snapshotSig["app.events"]; !ok || !got.Equal(want) {
				t.Errorf("snapshotSig after the boundary = %v (present=%v); want the full projected signature", got, ok)
			}
		})
	}
}

func TestVStreamSchemaHistory_WitnessShapedSeed(t *testing.T) {
	run := func(t *testing.T, zoned bool, first string) (int, error) {
		t.Helper()
		r := newVStreamTestReader()
		r.schemaDeltaAppliesToTarget = true
		r.SetSchemaSeed(witnessSeed("users", zoned))
		out := make(chan ir.Change, 16)
		ctx := context.Background()
		if err := r.dispatch(ctx, vgtidEvent("gtid-1"), out); err != nil {
			t.Fatalf("vgtid: %v", err)
		}
		err := r.dispatch(ctx, tzFieldEvent(first), out)
		return countSnapshots(out), err
	}
	for _, tc := range []struct {
		name  string
		zoned bool
		swap  string
		same  string
	}{
		{"witness TIMESTAMP", true, "datetime", "timestamp(6)"},
		{"witness DATETIME", false, "timestamp", "datetime(6)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := run(t, tc.zoned, tc.swap)
			requireSessionTZRefusal(t, err, "created_at")
			n, err := run(t, tc.zoned, tc.same)
			if err != nil {
				t.Fatalf("same-family first FIELD refused against the witness: %v", err)
			}
			if n != 1 {
				t.Errorf("same-family first FIELD emitted %d snapshots; want 1", n)
			}
		})
	}
}

func TestVStreamSnapshotCDC_WitnessShapedSeed(t *testing.T) {
	run := func(t *testing.T, zoned bool, first string) (int, error) {
		t.Helper()
		s := newVStreamSnapshotTestStream()
		s.schemaDeltaAppliesToTarget = true
		(&vstreamSnapshotChanges{snap: s}).SetSchemaSeed(witnessSeed("users", zoned))
		out := make(chan ir.Change, 16)
		ctx := context.Background()
		if err := s.dispatchCDCEvent(ctx, vgtidEvent("gtid-pre"), out); err != nil {
			t.Fatalf("vgtid: %v", err)
		}
		err := s.dispatchCDCEvent(ctx, tzFieldEvent(first), out)
		return countSnapshots(out), err
	}
	for _, tc := range []struct {
		name  string
		zoned bool
		swap  string
		same  string
	}{
		{"witness TIMESTAMP", true, "datetime", "timestamp(6)"},
		{"witness DATETIME", false, "timestamp", "datetime(6)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := run(t, tc.zoned, tc.swap)
			requireSessionTZRefusal(t, err, "created_at")
			n, err := run(t, tc.zoned, tc.same)
			if err != nil {
				t.Fatalf("same-family first FIELD refused against the witness: %v", err)
			}
			if n != 1 {
				t.Errorf("same-family first FIELD emitted %d snapshots; want 1", n)
			}
		})
	}
}
