// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"runtime"
	"testing"
)

// TestReadRows_GiantStatementAllocationBound is the Bug-191 pipeline
// gate in TEST form (audit 2026-07-16 M2.4): the end-to-end benchmark
// above proves linearity but is never executed by any workflow, so a
// quadratic reintroduced at another layer of the same stack would ship
// green — the exact shape Bug 191 itself had after the v0.99.259
// splitter-only fix. This test drives the WHOLE reader over one
// multi-MiB single-statement chunk and bounds TOTAL allocation:
// per-value buffers sized to the remaining statement TAIL (the Bug-191
// defect, O(rows × statement_size)) would allocate ~rows × chunk here
// (~2 GiB for this shape), while the linear decode measures a few ×
// chunk (~140 MB for 16 MiB in the v261 fix work). The 64× bound is
// generous headroom over linear and ~30× under the quadratic, so it
// discriminates the class without flaking on allocator noise.
//
// Guarded by testing.Short so `go test -short` skips the ~4 MiB dump
// build + walk; the default CI unit run executes it.
func TestReadRows_GiantStatementAllocationBound(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation-bound pipeline test; skipped under -short")
	}

	const stmtBytes = 4 << 20 // ~1k rows of ~4 KiB values; quadratic ≈ 2 GiB
	dir, chunkLen := buildGiantStatementDump(t, stmtBytes)
	table := benchCorpusTable(t, dir)

	// Warm-up drain: layout detect + schema caches allocate once and
	// must not count against the steady-state bound.
	benchDrainRows(t, dir, table)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	benchDrainRows(t, dir, table)
	runtime.ReadMemStats(&after)

	total := after.TotalAlloc - before.TotalAlloc
	if bound := uint64(64 * chunkLen); total > bound {
		t.Errorf("draining one %d-byte statement allocated %d bytes total (> %d = 64× the chunk) — per-value buffers are scaling with the statement tail again (Bug 191's O(rows × statement_size) class)",
			chunkLen, total, bound)
	}
}
