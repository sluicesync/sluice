// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Range-splitting for the parallel within-table bulk-copy path.
//
// pgcopydb's signature performance comes from splitting each large
// table into N PK ranges and copying them in parallel. v0.5.0 adopts
// the same shape: for tables above a row-count threshold and with an
// integer primary key, the orchestrator divides the PK numeric range
// into N disjoint chunks and runs N reader/writer goroutine pairs
// concurrently, each scoped to its own chunk.
//
// The splitting strategy in v1 is the simplest one that works:
// MIN/MAX/divide on the leading PK column. One small query per table
// produces N near-equal numeric ranges. Skew matters only when the PK
// distribution is heavily clustered; the threshold (default 100k
// rows) is set high enough that small or skewed tables stay on the
// single-reader path where skew is irrelevant.
//
// Three classes of table fall back to single-reader:
//
//  1. Below the row-count threshold (--bulk-parallel-min-rows). The
//     per-chunk overhead dominates on tiny tables.
//  2. No primary key. Required for cursor ordering in the per-batch
//     resume path; without it there is no notion of "chunk 0..N-1".
//  3. Composite PK or non-integer leading column. The MIN/MAX/divide
//     trick is well-defined only on a single integer column. ADR-0019
//     records (b) OFFSET-based and (c) NTILE strategies as future
//     enhancements that handle these cases.
//
// All three fall back transparently to the v0.4.x single-reader path
// (cursor-bearing resume on the resume branch, plain INSERT/COPY on
// the cold-start branch).

package pipeline

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// defaultBulkParallelMinRows is the row-count threshold below which a
// table is copied with a single reader/writer pair regardless of the
// configured --bulk-parallelism. The constant exists as a named
// default rather than a literal so the docstring on [Migrator] and
// the CLI help text can reference one source of truth.
//
// 80,000 absorbs the typical information_schema row-count estimate
// undershoot on InnoDB (0.1-5% below actual) while still favouring
// the single-reader path for tables small enough that per-chunk
// overhead (extra connections, MIN/MAX query, per-chunk state
// writes) dwarfs the parallelism gain. Empirical: tables with 100k
// actual rows commonly report as ~95-99k in information_schema; at
// the prior 100,000 threshold they fell to single-reader despite
// being "100k tables", costing ~64% throughput on a typical
// medium-fixture (see sluice-testing local-rig/baselines.md
// v0.62.0 commit). 80,000 sets the threshold low enough that a
// genuine 100k table always crosses it.
//
// Operators with workloads of many small-but-hot-tables can dial
// it down via --bulk-parallel-min-rows; operators wanting the
// pre-v0.62.0 behaviour pass --bulk-parallel-min-rows=100000.
// Nothing in the design depends on the specific value.
const defaultBulkParallelMinRows int64 = 80_000

// adaptiveBulkParallelMinRowsFloor is the lowest the auto threshold is
// dialled to on a many-table schema (roadmap item 3, phase (b)). Below
// this, per-chunk overhead (extra connections, MIN/MAX query, per-chunk
// state writes) outweighs the parallelism gain even when many tables are
// waiting, so genuinely small tables still take the single-reader path.
const adaptiveBulkParallelMinRowsFloor int64 = 10_000

// defaultBulkParallelism is the per-table reader/writer pair count
// when --bulk-parallelism is left at zero. The orchestrator caps it at
// min(8, NumCPU) to avoid saturating per-target connection pools on
// small hosts; operators with large hosts and large connection pools
// can override.
//
// 8 is pgcopydb's documented sweet spot for typical Postgres-target
// hardware (single-NUMA, sata or nvme, 16-32 vCPU). Higher values
// rarely help: bulk-copy is dominated by target-side fsync and disk
// I/O, both of which max out well before 8 parallel writers.
const defaultBulkParallelism = 8

// defaultTableParallelism is the cross-table copy-pool width when
// --table-parallelism is left at the 0 = auto sentinel (ADR-0076). It
// matches pgcopydb's --table-jobs default of 4: enough to keep cores
// busy across a many-medium-table schema without aggressively
// oversubscribing the target's connection budget (the auto value is
// further bounded by the budget split in [resolveCopyParallelismBudget],
// so this constant is the upper end of the auto range, not a hard floor).
const defaultTableParallelism = 4

// resolveTableParallelism returns the effective cross-table parallelism,
// applying the "0 = use defaultTableParallelism" rule. 1 disables
// cross-table concurrency (the pre-ADR-0076 serial-table behaviour);
// negative values clamp to 1 (defensive against bad CLI input). The
// budget split downstream further bounds this so the table × within
// product fits the target's connection budget.
func resolveTableParallelism(configured int) int {
	if configured < 0 {
		return 1
	}
	if configured == 0 {
		return defaultTableParallelism
	}
	return configured
}

// chunkBoundary describes one chunk's PK range. LowerPK is the
// exclusive lower bound (rows with PK > LowerPK), UpperPK is the
// inclusive upper bound (rows with PK <= UpperPK). nil bounds mean
// "no bound" — chunk 0's LowerPK is nil, chunk N-1's UpperPK is nil.
//
// The shape mirrors [ir.TableChunkProgress] so the orchestrator can
// transcribe boundaries into per-chunk progress entries on the first
// attempt and reuse them across resume runs.
type chunkBoundary struct {
	chunkIndex int
	lowerPK    []any
	upperPK    []any
}

// resolveBulkParallelism returns the effective parallelism, applying
// the "0 = use min(defaultBulkParallelism, NumCPU)" rule. Negative
// values clamp to 1 (defensive against bad CLI input).
func resolveBulkParallelism(configured, numCPU int) int {
	if configured < 0 {
		return 1
	}
	if configured == 0 {
		if numCPU < defaultBulkParallelism {
			return numCPU
		}
		return defaultBulkParallelism
	}
	return configured
}

// resolveBulkParallelMinRows resolves the within-table-split threshold.
//
// An explicit operator value (configured > 0) is honoured verbatim — we
// never override a knob the operator set.
//
// configured <= 0 is the "auto" sentinel (the CLI default). Here we ADAPT
// the threshold to the table count (roadmap item 3, phase (b)): a
// single/few-table schema keeps the full default (preserving the
// single-large-table auto-split win), but as the table count rises the
// threshold is dialled DOWN — toward adaptiveBulkParallelMinRowsFloor — so a
// many-medium-table schema engages within-table parallelism instead of
// copying each medium table serially AND single-streamed (the pgcopydb
// many-table gap: 30 medium tables sat below the fixed 80k threshold, so
// every one was single-streamed and the table loop ran them serially,
// leaving cores idle). The curve is default/tableCount clamped to
// [floor, default]: monotonic, tableCount==1 is unchanged, and small tables
// still skip chunking via the floor.
func resolveBulkParallelMinRows(configured int64, tableCount int) int64 {
	if configured > 0 {
		return configured
	}
	if tableCount <= 1 {
		return defaultBulkParallelMinRows
	}
	adapted := defaultBulkParallelMinRows / int64(tableCount)
	if adapted < adaptiveBulkParallelMinRowsFloor {
		return adaptiveBulkParallelMinRowsFloor
	}
	return adapted
}

// canParallelChunkTable reports whether table is eligible for the
// parallel-copy path. A table is eligible when:
//
//   - It has exactly one PK column. Composite PKs fall back to
//     single-reader for v1; ADR-0019 documents the limitation.
//   - The PK column type is an integer. Non-integer leading PKs
//     (text, UUID, decimal) fall back; future strategies (b)/(c) in
//     ADR-0019 will lift this.
//   - The configured parallelism is > 1. Single-pair parallelism is
//     the same as the v0.4.x path.
//
// Returns (true, nil) when eligible, (false, reason) otherwise.
// The reason string is suitable for a single-line operator-facing
// log message.
func canParallelChunkTable(table *ir.Table, parallelism int) (eligible bool, reason string) {
	if parallelism <= 1 {
		return false, "parallelism is 1; single-reader path"
	}
	if table == nil {
		return false, "table is nil"
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return false, "table has no primary key"
	}
	if len(table.PrimaryKey.Columns) > 1 {
		return false, "composite primary key (v1 supports single-column PKs)"
	}
	pkColName := table.PrimaryKey.Columns[0].Column
	col := lookupColumn(table, pkColName)
	if col == nil {
		return false, fmt.Sprintf("primary key column %q not found in column list", pkColName)
	}
	if _, ok := col.Type.(ir.Integer); !ok {
		return false, fmt.Sprintf("primary key column %q is %s; v1 supports integer PKs", pkColName, col.Type.String())
	}
	return true, ""
}

// lookupColumn returns the column with the given name, or nil if not
// found. Linear scan; tables typically have <100 columns. Spelled
// "lookup" to avoid colliding with the test-only [findColumn] helper
// in [migrate_cross_integration_test.go]; both could share an
// implementation but the test helper has a different signature
// (`*testing.T`-returning shape) and renaming the test would touch
// every cross-engine integration test.
func lookupColumn(table *ir.Table, name string) *ir.Column {
	for _, c := range table.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// computeChunkBoundaries divides the PK range of an integer-PK table
// into n disjoint chunks using MIN/MAX/divide.
//
// The implementation runs a single SELECT MIN(pk), MAX(pk) on the
// source via the supplied query function. With min/max in hand, the
// numeric range is split into n equal slices. Boundaries are recorded
// as []any so the wire shape matches [ir.TableChunkProgress.LowerPK]
// /UpperPK directly.
//
// Edge cases handled:
//
//   - Empty table (min/max both NULL): returns a single chunk with
//     nil bounds; the parallel path collapses to single-reader.
//   - Single-row table (min == max): same — one chunk covering the
//     whole row.
//   - Range smaller than n (e.g. 5 rows, n=8): collapses chunks until
//     each has at least one row's worth of range. Returns fewer
//     chunks than requested.
//
// Returns the boundary list in chunk-index order. Chunk 0 has
// LowerPK==nil; chunk N-1 has UpperPK==nil. This matches the
// `WHERE (lower IS NULL OR pk > lower) AND (upper IS NULL OR pk <= upper)`
// shape the parallel reader emits.
func computeChunkBoundaries(ctx context.Context, q rangeQuerier, table *ir.Table, n int) ([]chunkBoundary, error) {
	if n <= 0 {
		return nil, errors.New("pipeline: computeChunkBoundaries: n must be > 0")
	}
	if eligible, reason := canParallelChunkTable(table, n); !eligible {
		return nil, fmt.Errorf("pipeline: computeChunkBoundaries: table %q not eligible: %s", table.Name, reason)
	}

	pkCol := table.PrimaryKey.Columns[0].Column
	minVal, maxVal, err := q.RangeBounds(ctx, table, pkCol)
	if err != nil {
		return nil, fmt.Errorf("pipeline: range bounds for %q: %w", table.Name, err)
	}

	// Empty table: produce a single chunk covering the whole (empty)
	// range. The parallel path collapses to single-reader on a
	// one-chunk return.
	if minVal == nil || maxVal == nil {
		return []chunkBoundary{{chunkIndex: 0}}, nil
	}

	minInt, ok := coerceInt64(minVal)
	if !ok {
		return nil, fmt.Errorf("pipeline: MIN(%s) returned non-integer %T (%v); expected integer PK",
			pkCol, minVal, minVal)
	}
	maxInt, ok := coerceInt64(maxVal)
	if !ok {
		return nil, fmt.Errorf("pipeline: MAX(%s) returned non-integer %T (%v); expected integer PK",
			pkCol, maxVal, maxVal)
	}

	if minInt > maxInt {
		// Defensive: shouldn't happen for a well-formed table, but
		// surface clearly rather than producing inverted ranges.
		return nil, fmt.Errorf("pipeline: %q: MIN(%s)=%d > MAX(%s)=%d",
			table.Name, pkCol, minInt, pkCol, maxInt)
	}

	// Range size + 1 because both endpoints are inclusive when we
	// think of the PK domain.
	span := maxInt - minInt + 1
	if span <= 0 {
		// int64 overflow on huge tables. Defensive — production PKs
		// don't span 2^63 — but bail cleanly if we hit it.
		return nil, fmt.Errorf("pipeline: %q: PK range overflow (min=%d, max=%d)",
			table.Name, minInt, maxInt)
	}

	// Collapse n when there are fewer rows than chunks requested:
	// every chunk needs at least one row's worth of range, otherwise
	// we produce empty chunks that just cost overhead.
	if int64(n) > span {
		n = int(span)
	}

	// Equal-slice division. Chunk k covers (minInt + k*step,
	// minInt + (k+1)*step]. The last chunk extends to maxInt to absorb
	// the modulo remainder; chunk 0's lower bound is nil to capture
	// "anything <= minInt+step" without a lower-bound predicate.
	step := span / int64(n)
	if step < 1 {
		step = 1
	}

	out := make([]chunkBoundary, 0, n)
	for k := 0; k < n; k++ {
		var lower, upper []any
		// Lower bound: chunk 0 has nil; subsequent chunks pick up
		// where the previous chunk's upper ended.
		if k > 0 {
			lower = []any{minInt + int64(k)*step}
		}
		// Upper bound: chunk N-1 has nil to capture rows beyond the
		// computed step boundary (handles modulo remainder + any rows
		// inserted with PK > maxInt during the bulk-copy phase, though
		// the snapshot path means new inserts shouldn't be visible).
		if k < n-1 {
			upper = []any{minInt + int64(k+1)*step}
		}
		out = append(out, chunkBoundary{
			chunkIndex: k,
			lowerPK:    lower,
			upperPK:    upper,
		})
	}
	return out, nil
}

// coerceInt64 normalises common integer-typed driver-return values
// into int64. *sql.DB scan behaviour returns int64 for most SQL
// integer types, but range-bounds queries that go through QueryRow
// can produce either int64 or — for certain drivers — *big.Int /
// uint64. The helper handles the realistic shapes; anything else
// fails the parallel-eligibility check so the table falls back to
// single-reader.
func coerceInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case uint64:
		// Truncating to int64 would lose values >= 2^63, but in
		// practice no production PK uses uint64 above that range; the
		// MIN/MAX query would return a value driver-encoded as int64
		// anyway. If we hit it, the canParallelChunkTable check will
		// have already routed to fallback.
		if n > (1<<63 - 1) {
			return 0, false
		}
		return int64(n), true
	case uint32:
		return int64(n), true
	case []byte:
		// Some drivers return DECIMAL/NUMERIC as []byte even when the
		// declared SQL type is integer; we reject the shape rather
		// than parse, since computeChunkBoundaries is only called for
		// columns the IR has classified as ir.Integer.
		return 0, false
	}
	return 0, false
}

// rangeQuerier is a local alias for [ir.RangeBoundsQuerier]. The
// type-asserted shape on the row reader is the canonical surface; the
// alias here lets the chunk-boundary code be a small focused unit
// without the test fixtures having to refer to the IR import.
type rangeQuerier = ir.RangeBoundsQuerier
