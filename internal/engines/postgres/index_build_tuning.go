// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// Index-build phase tuning (deferred-index speedup, Phase A).
//
// sluice already defers every secondary index to the dedicated
// CreateIndexes phase that runs *after* the bulk COPY (pgcopydb-derived).
// During that phase the target is idle — not serving traffic — so the
// session can safely push maintenance_work_mem far above the provider's
// steady-state ~4%-of-RAM default and raise the parallel-maintenance
// worker count. That is the dominant index-build lever (in-memory sort
// vs small external-merge passes); see
// docs/dev/notes/index-build-phase-tuning.md for the empirical tier data.
//
// This file holds the probe (raw pg_settings reading) and the pure
// autotune function. The application — SET on a dedicated connection,
// best-effort — lives in CreateIndexes (schema_writer.go).

// indexBuildTuningConsts names the heuristic constants the autotune uses
// so they are a single greppable source of truth rather than magic
// numbers scattered through the formula. Tuning rationale (the note's
// tier table is the ground truth):
//
//   - ramFromSharedBuffersFactor: shared_buffers is the cleanest memory
//     proxy in the tier data — a monotonic ~13-16% of RAM with no
//     anomaly (effective_cache_size has a PS-40 anomaly, so it is only a
//     cross-check). RAM ≈ shared_buffers × ~7 falls out of that ratio.
//   - indexBuildMemFraction: the fraction of estimated RAM to hand a
//     single serial index build. 0.25 is well above the provider's ~4%
//     steady-state default (the whole point — the build phase is idle)
//     yet conservative enough that, combined with the cap, it can't OOM
//     a small node. Phase A's build loop is serial, so there is no
//     concurrency multiplier to guard against here (that is Phase B).
//   - indexBuildMemFloor / Cap: clamp the auto value so a tiny node
//     stays sane and a huge node doesn't hand a single build an
//     absurd allocation. 64 MiB floor / 2 GiB cap mirror the note's
//     suggested bounds.
const (
	ramFromSharedBuffersFactor = 7
	indexBuildMemFraction      = 0.25
	indexBuildMemFloor         = 64 * 1024 * 1024       // 64 MiB
	indexBuildMemCap           = 2 * 1024 * 1024 * 1024 // 2 GiB

	// parallelMaintenanceReserve leaves one worker slot free of the
	// max_worker_processes ceiling so the auto value never claims the
	// whole (small, shared) pool. On a node where max_worker_processes
	// is 4 this lands the effective parallel-maintenance workers at ~3,
	// which the note flags as expected and fine — parallelism is the
	// secondary lever below PS-640.
	parallelMaintenanceReserve = 1
)

// indexBuildTuningProbe is the raw pg_settings reading
// [probeIndexBuildTuning] collects before [computeIndexBuildTuning]
// turns it into the SET values. Split out so the math is a pure
// function unit-testable without a database.
//
// All memory fields are in *bytes* — the probe reads them via
// pg_size_bytes(current_setting(...)) so it never has to do 8 kB-page
// unit math (shared_buffers / effective_cache_size come back in 8 kB
// pages from a bare SHOW). The worker fields are plain counts.
type indexBuildTuningProbe struct {
	sharedBuffersBytes        int64 // pg_size_bytes(current_setting('shared_buffers'))
	effectiveCacheSizeBytes   int64 // pg_size_bytes(current_setting('effective_cache_size')) — cross-check only
	maintenanceWorkMemBytes   int64 // pg_size_bytes(current_setting('maintenance_work_mem')) — provider current/default
	maxWorkerProcesses        int   // SHOW max_worker_processes (hard ceiling, restart-set)
	maxParallelMaintenanceWrk int   // SHOW max_parallel_maintenance_workers (current/default)
}

// computeIndexBuildTuning turns a raw probe (+ an optional operator
// override in bytes) into the two values CreateIndexes SETs on the
// dedicated build connection. Pure function (no I/O) so the whole tier
// matrix is table-unit-testable.
//
// maintenance_work_mem (the dominant lever):
//   - override > 0 → used verbatim (operator knows their box), still
//     floored at the provider's current default so an override can only
//     ever raise, never lower below what the provider already tuned.
//   - else auto: clamp(fraction × shared_buffers × factor, floor, cap),
//     then raised to at least the provider's current default. The
//     provider already auto-scales the default to ~4% of RAM; sluice
//     only ever pushes it *up* for the idle build phase.
//
// max_parallel_maintenance_workers (secondary): raise toward
// (max_worker_processes − reserve), bounded by the max_worker_processes
// ceiling, but never below the current default. On a node where
// max_worker_processes is 4 this lands ~3.
func computeIndexBuildTuning(p indexBuildTuningProbe, override int64) (maintenanceWorkMemBytes int64, parallelMaintenanceWorkers int) {
	maintenanceWorkMemBytes = computeMaintenanceWorkMem(p, override)
	parallelMaintenanceWorkers = computeParallelMaintenanceWorkers(p)
	return maintenanceWorkMemBytes, parallelMaintenanceWorkers
}

// computeMaintenanceWorkMem implements the maintenance_work_mem half of
// [computeIndexBuildTuning]. Split out to keep each lever's policy
// readable in isolation.
func computeMaintenanceWorkMem(p indexBuildTuningProbe, override int64) int64 {
	var target int64
	if override > 0 {
		target = override
	} else {
		ramEst := p.sharedBuffersBytes * ramFromSharedBuffersFactor
		auto := int64(indexBuildMemFraction * float64(ramEst))
		target = clampInt64(auto, indexBuildMemFloor, indexBuildMemCap)
	}
	// Never set below the provider's existing default — the provider
	// already tuned it; sluice only ever raises for the idle build
	// phase. Applies to the override path too: an operator who passes a
	// value smaller than the provider default still gets at least the
	// provider default (lowering it would be a pessimisation with no
	// upside on an idle node).
	if p.maintenanceWorkMemBytes > target {
		target = p.maintenanceWorkMemBytes
	}
	return target
}

// computeParallelMaintenanceWorkers implements the
// max_parallel_maintenance_workers half of [computeIndexBuildTuning].
func computeParallelMaintenanceWorkers(p indexBuildTuningProbe) int {
	// The hard ceiling is max_worker_processes (restart-set, shared
	// pool). Setting max_parallel_maintenance_workers above it is
	// pointless — the effective worker count can't exceed it.
	target := p.maxWorkerProcesses - parallelMaintenanceReserve
	if target > p.maxWorkerProcesses {
		target = p.maxWorkerProcesses
	}
	// Never below the provider's current default.
	if target < p.maxParallelMaintenanceWrk {
		target = p.maxParallelMaintenanceWrk
	}
	// Floor at 0 — a degenerate probe (max_worker_processes reported as
	// 0/1) must not produce a negative SET, which PG would reject.
	if target < 0 {
		target = 0
	}
	return target
}

// clampInt64 bounds v to [lo, hi]. lo is assumed <= hi.
func clampInt64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// probeIndexBuildTuning reads the five pg_settings values the autotune
// needs, on the supplied connection. Best-effort by contract, mirroring
// [probeConnectionBudget]: any individual probe failure (a managed-PG
// quirk, a permission gap, an engine variant that doesn't expose a GUC)
// returns a wrapped error so the caller can degrade to the untuned
// serial build with a WARN rather than hard-failing a working index
// phase. The tuning must never be the thing that breaks a migration.
//
// The memory GUCs are read as bytes via pg_size_bytes(current_setting(…))
// so the caller never does 8 kB-page unit math (shared_buffers /
// effective_cache_size return in 8 kB pages from a bare SHOW). The
// worker GUCs are plain integer counts read via SHOW.
//
// Takes the narrow rowQueryer surface so it can run on either a pooled
// *sql.DB or a dedicated *sql.Conn (CreateIndexes probes on the same
// dedicated connection it then SETs on).
func probeIndexBuildTuning(ctx context.Context, q rowQueryer) (indexBuildTuningProbe, error) {
	var p indexBuildTuningProbe

	if err := q.QueryRowContext(
		ctx, `SELECT pg_size_bytes(current_setting('shared_buffers'))`,
	).Scan(&p.sharedBuffersBytes); err != nil {
		return p, fmt.Errorf("probe shared_buffers: %w", err)
	}
	if err := q.QueryRowContext(
		ctx, `SELECT pg_size_bytes(current_setting('effective_cache_size'))`,
	).Scan(&p.effectiveCacheSizeBytes); err != nil {
		return p, fmt.Errorf("probe effective_cache_size: %w", err)
	}
	if err := q.QueryRowContext(
		ctx, `SELECT pg_size_bytes(current_setting('maintenance_work_mem'))`,
	).Scan(&p.maintenanceWorkMemBytes); err != nil {
		return p, fmt.Errorf("probe maintenance_work_mem: %w", err)
	}
	if err := q.QueryRowContext(
		ctx, `SHOW max_worker_processes`,
	).Scan(&p.maxWorkerProcesses); err != nil {
		return p, fmt.Errorf("probe max_worker_processes: %w", err)
	}
	if err := q.QueryRowContext(
		ctx, `SHOW max_parallel_maintenance_workers`,
	).Scan(&p.maxParallelMaintenanceWrk); err != nil {
		return p, fmt.Errorf("probe max_parallel_maintenance_workers: %w", err)
	}
	return p, nil
}

// rowQueryer is the narrow surface probeIndexBuildTuning needs from
// *sql.DB / *sql.Conn — kept private to this package so the helper
// works on either the pooled handle or a dedicated connection.
type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
