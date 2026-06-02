// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "testing"

const (
	mib = int64(1024 * 1024)
	gib = 1024 * mib
)

// TestComputeIndexBuildTuning pins the pure autotune across the tier
// matrix from docs/dev/notes/index-build-phase-tuning.md (PS-5 floor →
// PS-160 cap), plus the override-wins, never-below-provider-default,
// floor/cap, and max_worker_processes-ceiling shapes. The math is
// I/O-free so the whole matrix is table-driven without a database.
//
// shared_buffers is the RAM proxy: RAM ≈ shared_buffers × 7, and the
// auto value is 0.25 × RAM clamped to [64 MiB, 2 GiB], then floored at
// the provider's current maintenance_work_mem (sluice only ever raises).
func TestComputeIndexBuildTuning(t *testing.T) {
	tests := []struct {
		name        string
		probe       indexBuildTuningProbe
		override    int64
		wantMem     int64
		wantWorkers int
	}{
		{
			// PS-5 (512 MB RAM): shared_buffers 67 MB → ramEst 469 MB →
			// auto 0.25× ≈ 117 MB, above the 64 MiB floor and the 16 MB
			// provider default. Workers: max_worker_processes 4 − 1 = 3.
			name: "PS-5 smallest tier scales above provider default",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        67 * mib,
				effectiveCacheSizeBytes:   203 * mib,
				maintenanceWorkMemBytes:   16 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantMem:     int64(0.25 * float64(67*mib*ramFromSharedBuffersFactor)),
			wantWorkers: 3,
		},
		{
			// PS-20 (2 GB RAM): shared_buffers 335 MB → auto ≈ 586 MB.
			name: "PS-20 mid tier scales linearly",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        335 * mib,
				maintenanceWorkMemBytes:   83 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantMem:     int64(0.25 * float64(335*mib*ramFromSharedBuffersFactor)),
			wantWorkers: 3,
		},
		{
			// PS-80 (8 GB RAM): shared_buffers 1 GB → auto ≈ 1.75 GiB,
			// still under the 2 GiB cap.
			name: "PS-80 large tier under cap",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        1 * gib,
				maintenanceWorkMemBytes:   337 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantMem:     int64(0.25 * float64(1*gib*ramFromSharedBuffersFactor)),
			wantWorkers: 3,
		},
		{
			// PS-160 (16 GB RAM): shared_buffers 2 GB → auto ≈ 3.5 GiB,
			// clamped to the 2 GiB cap.
			name: "PS-160 top tier hits the cap",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        2 * gib,
				maintenanceWorkMemBytes:   690 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantMem:     indexBuildMemCap,
			wantWorkers: 3,
		},
		{
			// A node so small the auto value would fall below the 64 MiB
			// floor (and the provider default is also below it) → floor.
			name: "tiny node clamps up to the floor",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        8 * mib, // ramEst 56 MB → auto 14 MB
				maintenanceWorkMemBytes:   4 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantMem:     indexBuildMemFloor,
			wantWorkers: 3,
		},
		{
			// Operator override below the cap wins verbatim (still above
			// the provider default).
			name: "operator override wins",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        67 * mib,
				maintenanceWorkMemBytes:   16 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    512 * mib,
			wantMem:     512 * mib,
			wantWorkers: 3,
		},
		{
			// An explicit operator override wins verbatim, even below the
			// provider's current default — they may know the target isn't
			// idle, or want a gentler build. The "never below default" rule
			// is an auto-path guard only, not a floor on the operator.
			name: "override honored verbatim even below provider default",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        2 * gib,
				maintenanceWorkMemBytes:   690 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    100 * mib,
			wantMem:     100 * mib,
			wantWorkers: 3,
		},
		{
			// Auto value below the provider default (provider already
			// tuned high relative to shared_buffers) → never below default.
			name: "auto never below provider default",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        67 * mib, // auto ≈ 117 MB
				maintenanceWorkMemBytes:   256 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantMem:     256 * mib,
			wantWorkers: 3,
		},
		{
			// max_worker_processes ceiling respected: a big pool (PS-640:
			// max_worker_processes 6, default 4) lands at 6 − 1 = 5,
			// above the provider default of 4.
			name: "worker ceiling PS-640 honoured",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        4 * gib,
				maintenanceWorkMemBytes:   1 * gib,
				maxWorkerProcesses:        6,
				maxParallelMaintenanceWrk: 4,
			},
			override:    0,
			wantMem:     indexBuildMemCap,
			wantWorkers: 5,
		},
		{
			// Degenerate worker pool: max_worker_processes 2, default 2.
			// 2 − 1 = 1 is below the default 2 → never below default → 2,
			// still ≤ the ceiling.
			name: "tiny worker pool never below default",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        67 * mib,
				maintenanceWorkMemBytes:   16 * mib,
				maxWorkerProcesses:        2,
				maxParallelMaintenanceWrk: 2,
			},
			override:    0,
			wantWorkers: 2,
			wantMem:     int64(0.25 * float64(67*mib*ramFromSharedBuffersFactor)),
		},
		{
			// Pathological probe (max_worker_processes 1, default 0) must
			// not produce a negative SET.
			name: "single-worker pool floors at zero-safe",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        67 * mib,
				maintenanceWorkMemBytes:   16 * mib,
				maxWorkerProcesses:        1,
				maxParallelMaintenanceWrk: 0,
			},
			override:    0,
			wantWorkers: 0,
			wantMem:     int64(0.25 * float64(67*mib*ramFromSharedBuffersFactor)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem, workers := computeIndexBuildTuning(tt.probe, tt.override)
			if mem != tt.wantMem {
				t.Errorf("maintenance_work_mem = %d bytes, want %d", mem, tt.wantMem)
			}
			if workers != tt.wantWorkers {
				t.Errorf("max_parallel_maintenance_workers = %d, want %d", workers, tt.wantWorkers)
			}
			if workers < 0 {
				t.Errorf("max_parallel_maintenance_workers = %d must never be negative", workers)
			}
		})
	}
}

// TestComputeIndexBuildConcurrency pins the Phase B concurrency bound
// across the tier matrix and every clamping dimension: memory-budget-
// bounds-N, connection-budget-bounds-N, index-count-bounds-N, the
// operator cap winning verbatim (above the auto hard cap), and the N=1
// degenerate. The load-bearing invariant — aggregate (N × per-build mem)
// never exceeds the budget, and per-build mem never drops below the floor
// — is asserted on every case.
func TestComputeIndexBuildConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		memBudget   int64
		floor       int64
		override    int64
		connBudget  int
		numIndexes  int
		operatorCap int

		wantWorkers int
		wantPerMem  int64
	}{
		{
			// Tiny node (PS-5-ish): a 117 MB budget at a 64 MiB floor
			// affords only 1 floor-sized build → N stays 1 even with
			// plenty of connections and indexes. The memory budget is the
			// binding constraint — exactly the OOM guard.
			name:        "tiny node memory-bounds N to 1",
			memBudget:   117 * mib,
			floor:       64 * mib,
			override:    0,
			connBudget:  16,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 1,
			wantPerMem:  117 * mib, // budget / 1, above floor
		},
		{
			// Bigger budget (1 GiB) at a 64 MiB floor affords 16 builds,
			// but the auto hard cap (8) and a generous conn budget land it
			// at 8; mem is divided: 1 GiB / 8 = 128 MiB per build.
			name:        "large budget auto-caps at hard cap, mem divided",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    0,
			connBudget:  16,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 8,
			wantPerMem:  (1 * gib) / 8,
		},
		{
			// Connection budget is the binding constraint: only 3 spare
			// slots → N=3 even though memory affords 8 and there are 20
			// indexes. Mem divided across 3.
			name:        "connection budget bounds N",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    0,
			connBudget:  3,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 3,
			wantPerMem:  (1 * gib) / 3,
		},
		{
			// Index count is the binding constraint: only 2 indexes to
			// build → no point spawning more than 2 workers.
			name:        "index count bounds N",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    0,
			connBudget:  16,
			numIndexes:  2,
			operatorCap: 0,
			wantWorkers: 2,
			wantPerMem:  (1 * gib) / 2,
		},
		{
			// Operator cap wins verbatim and is NOT bounded by the auto
			// hard cap (8): a cap of 12, with budget/conn/indexes all
			// affording it, lands at 12. Mem divided across 12 (still
			// above floor: 1 GiB / 12 ≈ 89 MiB > 64 MiB).
			name:        "operator cap exceeds auto hard cap, honored verbatim",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    0,
			connBudget:  32,
			numIndexes:  50,
			operatorCap: 12,
			wantWorkers: 12,
			wantPerMem:  (1 * gib) / 12,
		},
		{
			// Operator cap of 1 forces the serial build even on a big node.
			name:        "operator cap of 1 forces serial",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    0,
			connBudget:  16,
			numIndexes:  20,
			operatorCap: 1,
			wantWorkers: 1,
			wantPerMem:  1 * gib, // budget / 1
		},
		{
			// Override path: the operator set --index-build-mem=256MiB.
			// Per-build mem is the override verbatim; N is bounded so that
			// N × 256 MiB <= the 1 GiB budget → N=4. Conn/indexes/hardcap
			// all afford more.
			name:        "override mem caps N so aggregate fits budget",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    256 * mib,
			connBudget:  16,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 4,
			wantPerMem:  256 * mib,
		},
		{
			// Override larger than the whole budget: N floors at 1, and
			// the override stands verbatim per-build (operator's explicit
			// choice; the serial build at that size is the Phase A
			// override behaviour).
			name:        "override exceeding budget floors N at 1",
			memBudget:   512 * mib,
			floor:       64 * mib,
			override:    1 * gib,
			connBudget:  16,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 1,
			wantPerMem:  1 * gib,
		},
		{
			// Zero connection budget (probe failed / no spare slots) →
			// serial, never below 1.
			name:        "zero connection budget floors N at 1",
			memBudget:   1 * gib,
			floor:       64 * mib,
			override:    0,
			connBudget:  0,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 1,
			wantPerMem:  1 * gib,
		},
		{
			// Degenerate floor (0, from a pathological probe) must not
			// divide-by-zero: memoryBound falls back to 1.
			name:        "zero floor degenerate does not divide by zero",
			memBudget:   1 * gib,
			floor:       0,
			override:    0,
			connBudget:  16,
			numIndexes:  20,
			operatorCap: 0,
			wantWorkers: 1,
			wantPerMem:  1 * gib, // budget / 1, floor 0 doesn't raise
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeIndexBuildConcurrency(
				tt.memBudget, tt.floor, tt.override,
				tt.connBudget, tt.numIndexes, tt.operatorCap,
			)
			if got.workers != tt.wantWorkers {
				t.Errorf("workers = %d, want %d", got.workers, tt.wantWorkers)
			}
			if got.perBuildMemBytes != tt.wantPerMem {
				t.Errorf("perBuildMemBytes = %d, want %d", got.perBuildMemBytes, tt.wantPerMem)
			}
			if got.workers < 1 {
				t.Errorf("workers = %d must never be below 1", got.workers)
			}
			// Aggregate guard: N × per-build mem must not exceed the
			// budget — EXCEPT the two correctness-forced corners where a
			// single serial build is the safe Phase A baseline:
			//   - override > budget (operator's explicit per-build choice), or
			//   - workers floored up to 1 and per-build = budget itself.
			aggregate := int64(got.workers) * got.perBuildMemBytes
			overrideExceedsBudget := tt.override > 0 && tt.override > tt.memBudget
			serialBaseline := got.workers == 1
			if aggregate > tt.memBudget && !overrideExceedsBudget && !serialBaseline {
				t.Errorf("aggregate %d (= %d workers × %d) exceeds budget %d",
					aggregate, got.workers, got.perBuildMemBytes, tt.memBudget)
			}
			// Per-build never below the floor on the auto path (the
			// override path is the operator's explicit choice).
			if tt.override <= 0 && tt.floor > 0 && got.perBuildMemBytes < tt.floor {
				t.Errorf("auto per-build mem %d below floor %d", got.perBuildMemBytes, tt.floor)
			}
		})
	}
}

// TestComputeIndexBuildConcurrencyTierMatrix walks the note's tier table
// end-to-end: from each tier's probe-derived budget + floor, with a
// generous connection budget and index count, confirm the auto worker
// count stays conservative on small tiers (parallelism barely helps below
// PS-640) and that the aggregate memory never exceeds the tier's budget.
func TestComputeIndexBuildConcurrencyTierMatrix(t *testing.T) {
	tiers := []struct {
		name         string
		sharedBuf    int64
		providerMWM  int64
		wantMaxWorks int // auto N must not exceed this on this tier
	}{
		{"PS-5", 67 * mib, 16 * mib, 2},
		{"PS-20", 335 * mib, 83 * mib, 8},
		{"PS-80", 1 * gib, 337 * mib, 8},
		{"PS-160", 2 * gib, 690 * mib, 8},
	}
	for _, tier := range tiers {
		t.Run(tier.name, func(t *testing.T) {
			p := indexBuildTuningProbe{
				sharedBuffersBytes:        tier.sharedBuf,
				maintenanceWorkMemBytes:   tier.providerMWM,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			}
			budget := indexBuildMemBudget(p)
			floor := indexBuildMemFloorFor(p)
			got := computeIndexBuildConcurrency(budget, floor, 0, 64, 50, 0)
			if got.workers > tier.wantMaxWorks {
				t.Errorf("%s auto workers = %d, want <= %d", tier.name, got.workers, tier.wantMaxWorks)
			}
			if agg := int64(got.workers) * got.perBuildMemBytes; agg > budget && got.workers > 1 {
				t.Errorf("%s aggregate %d exceeds budget %d", tier.name, agg, budget)
			}
		})
	}
}

// TestComputeIndexBuildTuningMemMonotonic confirms the auto-derived
// maintenance_work_mem is monotonic in shared_buffers up to the cap —
// a larger node never gets less memory than a smaller one. Guards
// against a future formula regression that inverts the proxy.
func TestComputeIndexBuildTuningMemMonotonic(t *testing.T) {
	sharedBuffers := []int64{67 * mib, 159 * mib, 335 * mib, 644 * mib, 1 * gib, 2 * gib}
	var prev int64
	for _, sb := range sharedBuffers {
		mem, _ := computeIndexBuildTuning(indexBuildTuningProbe{
			sharedBuffersBytes:        sb,
			maintenanceWorkMemBytes:   16 * mib,
			maxWorkerProcesses:        4,
			maxParallelMaintenanceWrk: 2,
		}, 0)
		if mem < prev {
			t.Errorf("shared_buffers=%d gave mem=%d, less than previous tier's %d", sb, mem, prev)
		}
		prev = mem
	}
}
