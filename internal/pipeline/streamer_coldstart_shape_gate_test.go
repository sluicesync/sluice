// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0166 pre-create shape gate on the SYNC
// cold-start leg (roadmap item 25 residual): the Streamer's default
// gate branch runs the SAME comparison migrate runs — a drifted
// pre-existing target table refuses coded (and Abandons the snapshot
// stream per the Bug-177 pre-anchor rule), a matching one is skipped
// out of the create subset with the "sync cold-start" INFO, and the
// skip branches (--reset-target-data, --schema-already-applied, the
// interrupted-COPY resume) never consult the target catalog.

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func syncGateStreamer(targetCatalog *ir.Schema) (*Streamer, *recordingEngine) {
	tgt := newRecordingEngine("mysql")
	tgt.schema = targetCatalog
	return &Streamer{
		Source:    newRecordingEngine("mysql"),
		Target:    tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}, tgt
}

// TestSyncColdStartShapeGate_DrifterRefusesCodedAndAbandons: an
// empty-but-drifted pre-existing target table passes the Bug-9
// populated check (it is empty) and must now hit the shape gate: the
// coded SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH refusal, phrased for the
// sync leg, with the snapshot stream ABANDONED (pre-anchor rule).
func TestSyncColdStartShapeGate_DrifterRefusesCodedAndAbandons(t *testing.T) {
	captureSlog(t)
	intended := &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}
	drifted := &ir.Schema{Tables: []*ir.Table{gateTable(
		"items",
		&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
	)}}
	s, _ := syncGateStreamer(drifted)

	var abandoned, closed bool
	stream := recordingSnapshotStream(&abandoned, &closed)
	rw := &stubEmptyChecker{empty: map[string]bool{"items": true}} // empty → passes Bug 9

	_, err := s.coldStartGatePreflight(
		context.Background(), intended, nil, rw, stream, nil, "stream-1",
		false /* resumingCopy */, false, /* forceFresh */
	)
	if err == nil {
		t.Fatal("empty-but-drifted target must refuse at the shape gate; got nil (the pre-fix silent-coercion hole)")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetTableShapeMismatch {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetTableShapeMismatch)
	}
	if !strings.Contains(err.Error(), "sync cold-start") {
		t.Errorf("refusal must be phrased for the sync leg; got %q", err.Error())
	}
	if !abandoned {
		t.Error("shape-gate refusal did not Abandon the snapshot stream (Bug 177 pre-anchor rule)")
	}
	if closed {
		t.Error("shape-gate refusal invoked CloseFn despite AbandonFn being set")
	}
}

// TestSyncColdStartShapeGate_MatchingSkipsCreate: a matching-shape
// pre-existing table leaves the create subset (returned createSchema)
// with the sync-phrased INFO — exactly like migrate — while a fresh
// sibling stays in it.
func TestSyncColdStartShapeGate_MatchingSkipsCreate(t *testing.T) {
	logs := captureLogs(t)
	intended := &ir.Schema{Tables: []*ir.Table{
		gateTable("existing", gateCols()...),
		gateTable("fresh", gateCols()...),
	}}
	s, _ := syncGateStreamer(&ir.Schema{Tables: []*ir.Table{gateTable("existing", gateCols()...)}})

	var abandoned, closed bool
	stream := recordingSnapshotStream(&abandoned, &closed)
	rw := &stubEmptyChecker{empty: map[string]bool{"existing": true, "fresh": true}}

	createSchema, err := s.coldStartGatePreflight(
		context.Background(), intended, nil, rw, stream, nil, "stream-1", false, false,
	)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if createSchema == nil || len(createSchema.Tables) != 1 || createSchema.Tables[0].Name != "fresh" {
		t.Fatalf("create subset = %+v; want just \"fresh\"", createSchema)
	}
	if !strings.Contains(logs.String(), "sync cold-start: target table exists with matching column shape") {
		t.Errorf("skip INFO missing the sync-phrased line:\n%s", logs.String())
	}
	if abandoned || closed {
		t.Errorf("passing gate tore the stream down (abandoned=%v closed=%v)", abandoned, closed)
	}
}

// TestSyncColdStartShapeGate_SkipBranchesNeverConsultTargetCatalog:
// --reset-target-data drops the in-scope tables first, and
// --schema-already-applied / the interrupted-COPY resume never create
// against an unvalidated pre-existing target — none of them may open
// the target SchemaReader for the compare (the gate is
// default-branch-only). The target catalog here is DRIFTED, so a
// mis-wired gate would refuse and fail this test.
func TestSyncColdStartShapeGate_SkipBranchesNeverConsultTargetCatalog(t *testing.T) {
	captureSlog(t)
	intended := &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}
	drifted := &ir.Schema{Tables: []*ir.Table{gateTable(
		"items",
		&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
	)}}

	t.Run("schema-already-applied", func(t *testing.T) {
		s, _ := syncGateStreamer(drifted)
		s.SchemaAlreadyApplied = true
		var abandoned, closed bool
		createSchema, err := s.coldStartGatePreflight(
			context.Background(), intended, nil, &stubEmptyChecker{empty: map[string]bool{"items": true}},
			recordingSnapshotStream(&abandoned, &closed), nil, "stream-1", false, false,
		)
		if err != nil {
			t.Fatalf("--schema-already-applied must skip the gate (operator promise): %v", err)
		}
		if createSchema != nil {
			t.Errorf("skip branch must return a nil create subset; got %+v", createSchema)
		}
	})

	t.Run("interrupted-COPY resume", func(t *testing.T) {
		s, _ := syncGateStreamer(drifted)
		var abandoned, closed bool
		createSchema, err := s.coldStartGatePreflight(
			context.Background(), intended, nil, &stubEmptyChecker{empty: map[string]bool{"items": true}},
			recordingSnapshotStream(&abandoned, &closed), nil, "stream-1", true /* resumingCopy */, false,
		)
		if err != nil {
			t.Fatalf("the COPY resume must skip the gate (resume contract): %v", err)
		}
		if createSchema != nil {
			t.Errorf("skip branch must return a nil create subset; got %+v", createSchema)
		}
	})
}
