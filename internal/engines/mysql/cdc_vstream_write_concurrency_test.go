// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"vitess.io/vitess/go/vt/proto/vtgate"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0100: the VStream snapshot reader surfaces the disjoint concurrent-copy
// partition (the SAME groups ADR-0099 gave the producers) so the pipeline can
// run one read→write consumer pipeline per group concurrently (W = K). These
// pin the engine SIDE of that surface: the reader implements
// ir.ConcurrentCopyPartitioner, and returns exactly the producer partition on
// the concurrent path / nil on the sequential path (the zero-value-safe
// serial default).

// TestVStreamRows_ImplementsConcurrentCopyPartitioner pins the interface
// satisfaction (a compile-time + runtime assertion).
func TestVStreamRows_ImplementsConcurrentCopyPartitioner(t *testing.T) {
	var rows ir.RowReader = &vstreamSnapshotRows{snap: newTestSnapshotStream()}
	if _, ok := rows.(ir.ConcurrentCopyPartitioner); !ok {
		t.Fatal("vstreamSnapshotRows must implement ir.ConcurrentCopyPartitioner (ADR-0100)")
	}
}

// TestVStreamRows_ConcurrentCopyGroups_SurfacesProducerPartition pins that
// the reader surfaces EXACTLY the groups the producer driver was partitioned
// into — so the consumer partition the pipeline reads ≡ the producer
// partition (coverage + disjointness inherited from ADR-0099, never
// re-derived). The harness sets concurrentGroups the way the constructor does.
func TestVStreamRows_ConcurrentCopyGroups_SurfacesProducerPartition(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	byTable := map[string][]*vtgate.VStreamResponse{}
	for _, tbl := range tables {
		byTable[tbl] = perTableCopyScript(tbl, "MySQL56/"+uuidA+":1-10", 1)
	}
	byTable[""] = []*vtgate.VStreamResponse{}

	s, stream, _, cancel := newConcurrentHarness(t, tables, 2, byTable, 0)
	defer cancel()

	p, ok := stream.Rows.(ir.ConcurrentCopyPartitioner)
	if !ok {
		t.Fatal("snapshot Rows must implement ir.ConcurrentCopyPartitioner")
	}
	got := p.ConcurrentCopyGroups()

	// Must equal the engine's own partition for (tables, K=2) exactly — the
	// SAME pure function ADR-0099 unit-pins for coverage/disjointness.
	want := partitionTablesForStreams(tables, 2, nil)
	if len(got) != len(want) {
		t.Fatalf("ConcurrentCopyGroups returned %d groups; want %d (%v vs %v)", len(got), len(want), got, want)
	}
	// Coverage: every table appears exactly once across the surfaced groups.
	seen := map[string]int{}
	for _, g := range got {
		for _, tbl := range g {
			seen[tbl]++
		}
	}
	for _, tbl := range tables {
		if seen[tbl] != 1 {
			t.Fatalf("table %q surfaced %d times across groups; want exactly 1 (coverage/disjointness)", tbl, seen[tbl])
		}
	}
	_ = s
}

// TestVStreamRows_ConcurrentCopyGroups_NilOnSequentialPath pins the
// zero-value-safe default: a sequential (non-concurrent) snapshot stream
// surfaces NO groups, so the pipeline runs the serial table loop
// byte-identically (K = 1 / single-stream / one-table scope).
func TestVStreamRows_ConcurrentCopyGroups_NilOnSequentialPath(t *testing.T) {
	// A bare snapshot stream (no concurrentGroups set) is the sequential
	// path — exactly the shape every non-concurrent constructor produces.
	rows := &vstreamSnapshotRows{snap: newTestSnapshotStream()}
	if g := rows.ConcurrentCopyGroups(); g != nil {
		t.Fatalf("sequential-path ConcurrentCopyGroups = %v; want nil (serial default)", g)
	}
}
