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
			// Override below the provider's current default is raised to
			// the provider default — sluice only ever raises.
			name: "override below provider default floored at default",
			probe: indexBuildTuningProbe{
				sharedBuffersBytes:        2 * gib,
				maintenanceWorkMemBytes:   690 * mib,
				maxWorkerProcesses:        4,
				maxParallelMaintenanceWrk: 2,
			},
			override:    100 * mib,
			wantMem:     690 * mib,
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
