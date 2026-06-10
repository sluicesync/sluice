// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migratestate

import (
	"encoding/json"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0082 checkpoint-cost benchmarks: what ONE per-batch checkpoint
// costs in-process at a 10k-table schema, before vs after the
// per-table-rows split.
//
//   - LegacyFullBlob reproduces the ≤v0.99.x write byte-for-byte on
//     the encode side: deep-clone the whole TableProgress map (the
//     pre-ADR-0082 cloneStateForWrite, inlined here verbatim since
//     the production helper became per-entry) + json.Marshal of the
//     whole map — the blob that was then upserted into the one hot
//     sluice_migrate_state row.
//   - PerTableRow is the new path: clone ONE entry + marshal ONE
//     entry — the payload of one sluice_migrate_table_progress
//     upsert.
//
// payload_bytes/op is the JSON written per checkpoint — the
// write-amplification factor that multiplies through the target's
// WAL/MVCC/TOAST on every one of the ≥2 writes/table (breadcrumb +
// terminal) plus every per-5000-row cursor write. The SQL round-trip
// side of the same comparison lives in the integration benchmark
// (engines/postgres/migration_state_bench_integration_test.go).
// Measured numbers are recorded in ADR-0082.

// benchProgress builds an n-table progress map shaped like a
// mid-flight resume run: every table carries a cursor-bearing
// in_progress entry (the worst realistic encode case; terminal
// bare-string entries would flatter the legacy number).
func benchProgress(n int) map[string]ir.TableProgress {
	m := make(map[string]ir.TableProgress, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("app_table_%05d", i)] = ir.TableProgress{
			State:      ir.TableProgressInProgress,
			LastPK:     []any{int64(i * 5000)},
			RowsCopied: int64(i * 5000),
		}
	}
	return m
}

// cloneFullState is the pre-ADR-0082 cloneStateForWrite, kept here as
// the benchmark baseline only.
func cloneFullState(state *ir.MigrationState) ir.MigrationState {
	cp := *state
	if state.TableProgress != nil {
		clone := make(map[string]ir.TableProgress, len(state.TableProgress))
		for k, v := range state.TableProgress {
			if len(v.Chunks) > 0 {
				chunks := make([]ir.TableChunkProgress, len(v.Chunks))
				copy(chunks, v.Chunks)
				v.Chunks = chunks
			}
			clone[k] = v
		}
		cp.TableProgress = clone
	}
	return cp
}

func BenchmarkCheckpoint10kTables_LegacyFullBlob(b *testing.B) {
	state := ir.MigrationState{
		MigrationID:   "bench",
		Phase:         ir.MigrationPhaseBulkCopy,
		TableProgress: benchProgress(10_000),
	}
	var payload int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp := cloneFullState(&state)
		blob, err := json.Marshal(cp.TableProgress)
		if err != nil {
			b.Fatal(err)
		}
		payload = len(blob)
	}
	b.ReportMetric(float64(payload), "payload_bytes/op")
}

func BenchmarkCheckpoint10kTables_PerTableRow(b *testing.B) {
	entry := ir.TableProgress{
		State:      ir.TableProgressInProgress,
		LastPK:     []any{int64(5000 * 5000)},
		RowsCopied: 5000 * 5000,
	}
	var payload int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mirror the production hot path: per-entry clone under the
		// lock + one-entry encode.
		cp := entry
		if len(cp.Chunks) > 0 {
			chunks := make([]ir.TableChunkProgress, len(cp.Chunks))
			copy(chunks, cp.Chunks)
			cp.Chunks = chunks
		}
		encoded, err := encodeProgressEntry(cp)
		if err != nil {
			b.Fatal(err)
		}
		payload = len(encoded)
	}
	b.ReportMetric(float64(payload), "payload_bytes/op")
}
