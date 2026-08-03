// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// What the legacy full-rewrite checkpoint actually costs
// (audit 2026-08-01 P4).
//
// The sidecar checkpoint is O(1) per event and already has a cost gate:
// TestManifestCommitter_SidecarCheckpointCost_10kTables pins the manifest to a
// SINGLE Put across 20,000 checkpoints. That gate measures the path only an
// [irbackup.Appender] store takes — and [blobcodec.LocalStore] is the sole
// implementor in the tree. Every CLOUD store (S3, GCS, Azure) falls back to
// legacy mode, where each checkpoint re-marshals and re-Puts the WHOLE
// manifest.
//
// So the fast path was gated and the slow path — the one every remote backup
// takes — was not measured at all. This measures it, and the existing
// legacy-mode test is deliberately about BEHAVIOUR (that the rewrite happens),
// not cost, so nothing here duplicates it.
//
// The cost is quadratic in the wrong variable. The manifest carries the schema
// plus one entry per table, and every checkpoint re-serialises all of it — so
// total bytes ≈ (checkpoints) × (manifest size), and manifest size grows with
// TABLE count while checkpoint count grows with CHUNK count. A wide corpus
// with many chunks per table pays the product.
//
// This test asserts the SHAPE (that legacy cost scales with the product, while
// sidecar does not) rather than a byte threshold, so it cannot fail for a
// harmless manifest-size change. The measured numbers are logged, so anyone
// costing the fix has them without re-deriving.

package backup

import (
	"context"
	"fmt"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// legacyCheckpointCost drives tables × chunksPerTable checkpoints through a
// committer and returns the manifest Put count and total manifest bytes.
func legacyCheckpointCost(t *testing.T, tables, chunksPerTable int, appendCapable bool) (puts int, bytes int64, appends int) {
	t.Helper()
	ctx := context.Background()

	var (
		acct   *accountingPutStore
		commit *manifestCommitter
		err    error
	)
	m, entries := sidecarTestManifest(tables)
	if appendCapable {
		s := newAccountingAppendStore()
		acct = s.accountingPutStore
		commit, err = newManifestCommitter(s, m)
		defer func() { appends = s.appendCalls }()
	} else {
		s := newAccountingPutStore()
		acct = s
		commit, err = newManifestCommitter(s, m)
	}
	if err != nil {
		t.Fatalf("newManifestCommitter: %v", err)
	}
	for _, e := range entries {
		commit.stageTable(e)
	}
	if err := commit.commitBase(ctx); err != nil {
		t.Fatalf("commitBase: %v", err)
	}

	for _, e := range entries {
		for c := range chunksPerTable {
			ci := &irbackup.ChunkInfo{
				File:     fmt.Sprintf("chunks/%s/%s-%d.jsonl.gz", e.Name, e.Name, c),
				RowCount: 100,
				SHA256:   strings.Repeat("a", 64),
			}
			if err := commit.appendChunk(ctx, e, ci); err != nil {
				t.Fatalf("appendChunk: %v", err)
			}
		}
		if err := commit.finishTable(ctx, e, int64(100*chunksPerTable)); err != nil {
			t.Fatalf("finishTable: %v", err)
		}
	}
	return acct.putCalls[lineage.ManifestFileName], acct.putBytes[lineage.ManifestFileName], appends
}

// TestLegacyCheckpointCost_ScalesWithChunksNotJustTables is the measurement.
func TestLegacyCheckpointCost_ScalesWithChunksNotJustTables(t *testing.T) {
	const tables = 50

	// Same table count, chunk count multiplied by 4. Sidecar cost must be flat
	// in manifest bytes; legacy cost must scale with the checkpoint count.
	few, fewBytes, _ := legacyCheckpointCost(t, tables, 2, false)
	many, manyBytes, _ := legacyCheckpointCost(t, tables, 8, false)

	sidecarPuts, sidecarBytes, sidecarAppends := legacyCheckpointCost(t, tables, 8, true)

	t.Logf("legacy  %d tables x 2 chunks: manifest Puts=%d  bytes=%d", tables, few, fewBytes)
	t.Logf("legacy  %d tables x 8 chunks: manifest Puts=%d  bytes=%d", tables, many, manyBytes)
	t.Logf("sidecar %d tables x 8 chunks: manifest Puts=%d  bytes=%d  appends=%d",
		tables, sidecarPuts, sidecarBytes, sidecarAppends)
	if fewBytes > 0 {
		t.Logf("legacy manifest-byte amplification, 2 -> 8 chunks/table: %.1fx (checkpoints went up 4x)",
			float64(manyBytes)/float64(fewBytes))
	}
	if sidecarBytes > 0 {
		t.Logf("legacy vs sidecar manifest bytes at 8 chunks/table: %.0fx",
			float64(manyBytes)/float64(sidecarBytes))
	}

	// The claim under test: legacy checkpoint cost is driven by CHUNK count,
	// not table count. Same tables, 4x the chunks, so the manifest is re-Put
	// roughly 4x as often.
	if many <= few {
		t.Errorf("legacy manifest Puts did not grow with chunk count (%d -> %d); the checkpoint is not "+
			"per-chunk and this measurement is testing the wrong thing", few, many)
	}
	if manyBytes <= fewBytes {
		t.Errorf("legacy manifest BYTES did not grow with chunk count (%d -> %d)", fewBytes, manyBytes)
	}

	// And the contrast that makes it a finding rather than a fact of life: the
	// sidecar store does the identical work for ONE manifest Put.
	if sidecarPuts != 1 {
		t.Errorf("sidecar manifest Puts = %d, want 1 — the comparison baseline is broken", sidecarPuts)
	}
	if manyBytes <= sidecarBytes {
		t.Errorf("legacy (%d bytes) did not exceed sidecar (%d bytes); the two modes are not diverging and "+
			"this test would not notice if legacy regressed further", manyBytes, sidecarBytes)
	}
}

// TestLegacyCheckpointCost_IsQuadraticInTheManifest pins the second half of
// the finding: cost scales with TABLE count too, because every checkpoint
// re-serialises every table entry. Together with the test above that is the
// product — which is why a wide corpus with many chunks per table is the
// expensive shape, and why a guard keyed on table count alone would miss it.
func TestLegacyCheckpointCost_IsQuadraticInTheManifest(t *testing.T) {
	const chunksPerTable = 2

	_, smallBytes, _ := legacyCheckpointCost(t, 25, chunksPerTable, false)
	_, bigBytes, _ := legacyCheckpointCost(t, 100, chunksPerTable, false)

	t.Logf("legacy  25 tables x %d chunks: bytes=%d", chunksPerTable, smallBytes)
	t.Logf("legacy 100 tables x %d chunks: bytes=%d", chunksPerTable, bigBytes)
	if smallBytes > 0 {
		t.Logf("4x the tables cost %.1fx the manifest bytes (linear would be 4x; the excess is the "+
			"per-checkpoint re-serialisation of every entry)", float64(bigBytes)/float64(smallBytes))
	}

	// 4x the tables means 4x the checkpoints AND a ~4x larger manifest at each
	// one, so the byte volume should grow super-linearly — meaningfully more
	// than 4x. A merely-linear result would mean the manifest is not carrying
	// per-table state and the quadratic reading is wrong.
	if float64(bigBytes) <= 4*float64(smallBytes) {
		t.Errorf("4x tables produced only %.1fx bytes (%d -> %d) — cost is linear, not quadratic, and the "+
			"finding's shape does not hold as described",
			float64(bigBytes)/float64(smallBytes), smallBytes, bigBytes)
	}
}
