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

	// indexBuildConcurrencyHardCap bounds the auto concurrency regardless
	// of how generous the connection + memory budgets look. The note's
	// tier data shows max_worker_processes is flat at 4 until PS-640, so
	// concurrent builds contend for a tiny shared pool on the vast
	// majority of plans — spawning 16 build workers on such a node buys
	// nothing and just multiplies connection + memory pressure. The auto
	// path therefore never exceeds this without an explicit operator
	// --index-build-parallelism. (The operator cap is honored verbatim
	// and is NOT bounded by this — they may know their box.) 8 is well
	// above the point where index-build parallelism stops helping on
	// every tier below PS-640 yet leaves headroom on the largest
	// instances; the connection + memory budgets clamp it further.
	indexBuildConcurrencyHardCap = 8
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
//   - override > 0 → used verbatim (operator knows their box; they may
//     know the target isn't idle, or want a gentler build).
//   - else auto: clamp(fraction × shared_buffers × factor, floor, cap),
//     then raised to at least the provider's current default. The
//     provider already auto-scales the default to ~4% of RAM; on the auto
//     path sluice only ever pushes it *up* for the idle build phase.
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
	// An explicit operator override wins verbatim — they know their box
	// (the target may not actually be idle; they may want a gentler build).
	// The "never below the provider default" floor below is an *auto*-path
	// guard, not a cap on what the operator is allowed to ask for.
	if override > 0 {
		return override
	}
	return indexBuildMemBudget(p)
}

// indexBuildMemBudget is the auto-derived per-build maintenance_work_mem
// for a *single* serial build — the Phase A value. It doubles as the
// aggregate memory envelope Phase B divides across N concurrent builds:
// the total build memory across the worker pool stays within this number
// (see [computeIndexBuildConcurrency] and the note's "memory × concurrency
// trap"). Pulled out of [computeMaintenanceWorkMem] so the concurrency
// math and the serial-mem math share one definition of "the budget".
//
// Sized from shared_buffers (the cleanest RAM proxy), clamped to
// [floor, cap], then raised to at least the provider's current default.
// The provider already auto-scales the default to ~4% of RAM, so sluice
// only ever pushes it *up* for the idle build phase — it never guesses
// lower than what the provider already tuned.
func indexBuildMemBudget(p indexBuildTuningProbe) int64 {
	ramEst := p.sharedBuffersBytes * ramFromSharedBuffersFactor
	auto := clampInt64(int64(indexBuildMemFraction*float64(ramEst)), indexBuildMemFloor, indexBuildMemCap)
	if p.maintenanceWorkMemBytes > auto {
		return p.maintenanceWorkMemBytes
	}
	return auto
}

// indexBuildMemFloorFor returns the lowest per-build maintenance_work_mem
// a single concurrent build may be handed: the larger of sluice's own
// floor and the provider's current default. Dividing the aggregate budget
// across N builds must never drop a build below this — both because the
// provider default is the "known-safe" baseline and because a build below
// the floor spills to disk and erases the whole point of the tuning.
func indexBuildMemFloorFor(p indexBuildTuningProbe) int64 {
	floor := int64(indexBuildMemFloor)
	if p.maintenanceWorkMemBytes > floor {
		return p.maintenanceWorkMemBytes
	}
	return floor
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

// indexBuildConcurrency is the Phase B verdict [computeIndexBuildConcurrency]
// returns: how many concurrent build workers to spawn, and the
// per-build maintenance_work_mem each worker SETs on its own dedicated
// connection. The invariant the caller relies on:
//
//	workers >= 1
//	workers × perBuildMemBytes <= the memory budget   (aggregate guard)
//	perBuildMemBytes >= the per-build floor            (never spill)
//
// N=1 degenerates to exactly Phase A: one worker, perBuildMemBytes equal
// to the single-build value [computeMaintenanceWorkMem] returns.
type indexBuildConcurrency struct {
	workers          int
	perBuildMemBytes int64
}

// computeIndexBuildConcurrency is the pure heart of Phase B: it turns the
// memory + connection budgets, the index count, and the operator cap into
// a worker count N and the per-build maintenance_work_mem each of the N
// concurrent builds SETs. No I/O — the whole tier matrix is
// table-unit-testable.
//
// The memory × concurrency trap (the note's critical constraint): N
// concurrent builds each consume their own maintenance_work_mem, so total
// build memory ≈ N × perBuildMem. Sizing each build at Phase A's
// single-build value and then running N of them would OOM a small node.
// So the budget is DIVIDED: the aggregate (N × perBuildMem) is held within
// memBudget, and N itself is bounded by how many full-floor builds fit in
// the budget.
//
// N = clamp(min(memoryBound, connBound, numIndexes, operatorCapOrAuto), 1, …)
// where:
//   - memoryBound  = memBudget / perBuildFloor   (how many floor-sized
//     builds the budget affords — the OOM guard)
//   - connBound    = connBudget                  (extra build connections
//     the target can spare; <=0 means "no extra room" → fall to 1)
//   - numIndexes   = no point spawning more workers than indexes
//   - operatorCap  = --index-build-parallelism when >0 (honored verbatim,
//     NOT bounded by the auto hard cap — the operator knows their box),
//     else the conservative auto hard cap (the note: parallelism barely
//     helps below PS-640, so auto stays modest).
//
// perBuildMem:
//   - override > 0 (operator set --index-build-mem): per-build mem is the
//     override verbatim; the memoryBound above already capped N so that
//     N × override stays within the budget (at least 1 worker always).
//   - else auto: perBuildMem = max(memBudget / N, perBuildFloor) — the
//     budget divided across the N workers, never below the floor.
//
// memBudget and perBuildFloor are passed in (not recomputed) so the caller
// derives them once from the probe via [indexBuildMemBudget] /
// [indexBuildMemFloorFor] and the override, keeping one definition of
// "the budget".
func computeIndexBuildConcurrency(
	memBudget, perBuildFloor, override int64,
	connBudget, numIndexes, operatorCap int,
) indexBuildConcurrency {
	// Per-build memory: the operator override wins verbatim, else the
	// floor-or-better auto baseline. This is what each worker would SET if
	// it were the only worker; the division below may lower the auto value
	// (never below the floor) once N is known.
	perBuild := override
	if perBuild <= 0 {
		perBuild = perBuildFloor
	}

	// Memory bound: how many full perBuild-sized builds the aggregate
	// budget affords. This is the OOM guard — it never lets N × perBuild
	// exceed memBudget. Guard against a zero/negative floor (degenerate
	// probe) producing a divide-by-zero.
	memoryBound := 1
	if perBuild > 0 && memBudget >= perBuild {
		memoryBound = int(memBudget / perBuild)
	}

	// The operator cap (when set) is honored verbatim and is NOT bounded by
	// the conservative auto hard cap. When unset (0), auto stays modest per
	// the note (parallelism barely helps below PS-640).
	capBound := operatorCap
	if capBound <= 0 {
		capBound = indexBuildConcurrencyHardCap
	}

	workers := minInt(memoryBound, capBound)
	workers = minInt(workers, connBudget)
	workers = minInt(workers, numIndexes)
	if workers < 1 {
		workers = 1
	}

	// Per-build memory on the auto path: divide the aggregate budget across
	// the chosen worker count, never below the floor. On the override path
	// the operator's value stands verbatim (N was already bounded so the
	// aggregate fits). The max() with perBuildFloor on the auto path can,
	// in a tight-budget corner, push the aggregate slightly over memBudget
	// when workers was floored up to 1 — but a single build at the floor is
	// exactly the Phase A serial baseline, which is by definition safe.
	perBuildMem := perBuild
	if override <= 0 {
		divided := memBudget / int64(workers)
		if divided < perBuildFloor {
			divided = perBuildFloor
		}
		perBuildMem = divided
	}

	return indexBuildConcurrency{workers: workers, perBuildMemBytes: perBuildMem}
}

// minInt returns the smaller of a and b.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// probeIndexBuildConnBudget reads the target's spare connection budget for
// the concurrent index-build pool. It reuses the Phase 1 catalog probe +
// pure budget math ([probeConnectionBudget] / [computeConnectionBudget])
// so the two features share one definition of "how many slots are free".
//
// The returned int is the number of *additional* connections the build
// pool may open, i.e. the [connectionBudget.CopyBudget]
// (available − reserve). It is best-effort: a probe failure returns
// (0, err) so the caller degrades to serial (N=1) with a WARN rather than
// hard-failing a working index phase — same disposition as the tuning
// probe. A non-error return is always >= 0; the concurrency math floors
// the worker count at 1 regardless, so even a zero budget still builds
// serially.
//
// The same connBudgetReserve the COPY pool uses applies: the index phase
// runs after the COPY pool has closed, but a sync's long-lived control /
// CDC connection and operator headroom still want the slack, and the
// reserve is deliberately conservative.
func probeIndexBuildConnBudget(ctx context.Context, db *sql.DB) (int, error) {
	probe, err := probeConnectionBudget(ctx, db)
	if err != nil {
		return 0, err
	}
	budget := computeConnectionBudget(probe, connBudgetReserve)
	if budget.CopyBudget < 0 {
		return 0, nil
	}
	return budget.CopyBudget, nil
}
