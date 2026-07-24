// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the Bug 177 pre-anchor teardown rule: a cold start
// that exits before the CDC anchor position is persisted must ABANDON
// the snapshot stream (discarding the durable slot the open created),
// not merely Close it. The engine-side slot drop is pinned by the
// postgres package's integration test; these pins cover the
// orchestrator dispatch — the refusal paths actually invoke Abandon.

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// recordingSnapshotStream builds a SnapshotStream whose teardown calls
// are observable: abandoned/closed flip when the respective closure
// runs. AbandonFn is set, so a fallback-to-Close would be visible as
// closed=true, abandoned=false.
func recordingSnapshotStream(abandoned, closed *bool) *ir.SnapshotStream {
	return &ir.SnapshotStream{
		AbandonFn: func() error { *abandoned = true; return nil },
		CloseFn:   func() error { *closed = true; return nil },
	}
}

// TestColdStartGatePreflight_RefusalAbandonsStream is the Bug 177
// orchestrator pin: the populated-target cold-start refusal (Bug 9)
// tears the snapshot stream down via Abandon — on a PG source that is
// what drops the just-created replication slot instead of orphaning
// it to pin WAL and break the refusal hint's preferred recovery.
func TestColdStartGatePreflight_RefusalAbandonsStream(t *testing.T) {
	var abandoned, closed bool
	stream := recordingSnapshotStream(&abandoned, &closed)

	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubEmptyChecker{empty: map[string]bool{"users": false}} // populated → refuse
	s := &Streamer{Source: stubEngine{}, Target: stubEngine{}}

	_, err := s.coldStartGatePreflight(
		context.Background(), schema, nil, rw, stream, nil, "stream-1",
		false /* resumingCopy */, false, /* forceFresh */
	)
	if err == nil {
		t.Fatal("expected the populated-target refusal; got nil")
	}
	if !errors.Is(err, errColdStartRefused) {
		t.Fatalf("err = %v; want errColdStartRefused", err)
	}
	if !abandoned {
		t.Fatal("refusal did not Abandon the snapshot stream (Bug 177: the just-created slot would be orphaned)")
	}
	if closed {
		t.Fatal("refusal invoked CloseFn despite AbandonFn being set")
	}
}

// TestColdStartGatePreflight_PassLeavesStreamOpen guards the other
// side: when every preflight passes, the stream must stay open for
// the copy phase — no teardown of either kind.
func TestColdStartGatePreflight_PassLeavesStreamOpen(t *testing.T) {
	var abandoned, closed bool
	stream := recordingSnapshotStream(&abandoned, &closed)

	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubEmptyChecker{empty: map[string]bool{"users": true}}
	// Target is a recordingEngine (empty catalog), not a stubEngine: the
	// pass path now legitimately reaches Target.OpenSchemaReader — the
	// default branch runs the ADR-0166 shape gate (item 25 residual)
	// after the Bug-9 check, and an empty target catalog passes it.
	s := &Streamer{Source: stubEngine{}, Target: newRecordingEngine("stub")}

	if _, err := s.coldStartGatePreflight(
		context.Background(), schema, nil, rw, stream, nil, "stream-1", false, false,
	); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if abandoned || closed {
		t.Fatalf("passing preflight tore the stream down (abandoned=%v closed=%v)", abandoned, closed)
	}
}
