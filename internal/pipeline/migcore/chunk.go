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

package migcore

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// AdaptiveBulkParallelMinRowsFloor is the lowest the auto threshold is
// dialled to on a many-table schema (roadmap item 3, phase (b)). Below
// this, per-chunk overhead (extra connections, MIN/MAX query, per-chunk
// state writes) outweighs the parallelism gain even when many tables are
// waiting, so genuinely small tables still take the single-reader path.
const AdaptiveBulkParallelMinRowsFloor int64 = 10_000

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

// DefaultTableParallelism is the cross-table copy-pool width when
// --table-parallelism is left at the 0 = auto sentinel (ADR-0076). It
// matches pgcopydb's --table-jobs default of 4: enough to keep cores
// busy across a many-medium-table schema without aggressively
// oversubscribing the target's connection budget (the auto value is
// further bounded by the budget split in [ResolveCopyParallelismBudget],
// so this constant is the upper end of the auto range, not a hard floor).
const DefaultTableParallelism = 4

// ResolveTableParallelism returns the effective cross-table parallelism,
// applying the "0 = use DefaultTableParallelism" rule. 1 disables
// cross-table concurrency (the pre-ADR-0076 serial-table behaviour);
// negative values clamp to 1 (defensive against bad CLI input). The
// budget split downstream further bounds this so the table × within
// product fits the target's connection budget.
func ResolveTableParallelism(configured int) int {
	if configured < 0 {
		return 1
	}
	if configured == 0 {
		return DefaultTableParallelism
	}
	return configured
}

// ChunkBoundary describes one chunk's PK range. LowerPK is the
// exclusive lower bound (rows with PK > LowerPK), UpperPK is the
// inclusive upper bound (rows with PK <= UpperPK). nil bounds mean
// "no bound" — chunk 0's LowerPK is nil, chunk N-1's UpperPK is nil.
//
// The shape mirrors [ir.TableChunkProgress] so the orchestrator can
// transcribe boundaries into per-chunk progress entries on the first
// attempt and reuse them across resume runs.
type ChunkBoundary struct {
	ChunkIndex int
	LowerPK    []any
	UpperPK    []any
}

// ResolveBulkParallelism returns the effective parallelism, applying
// the "0 = use min(defaultBulkParallelism, NumCPU)" rule. Negative
// values clamp to 1 (defensive against bad CLI input).
func ResolveBulkParallelism(configured, numCPU int) int {
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

// ResolveBulkParallelMinRows resolves the within-table-split threshold.
//
// An explicit operator value (configured > 0) is honoured verbatim — we
// never override a knob the operator set.
//
// configured <= 0 is the "auto" sentinel (the CLI default). Here we ADAPT
// the threshold to the table count (roadmap item 3, phase (b)): a
// single/few-table schema keeps the full default (preserving the
// single-large-table auto-split win), but as the table count rises the
// threshold is dialled DOWN — toward AdaptiveBulkParallelMinRowsFloor — so a
// many-medium-table schema engages within-table parallelism instead of
// copying each medium table serially AND single-streamed (the pgcopydb
// many-table gap: 30 medium tables sat below the fixed 80k threshold, so
// every one was single-streamed and the table loop ran them serially,
// leaving cores idle). The curve is default/tableCount clamped to
// [floor, default]: monotonic, tableCount==1 is unchanged, and small tables
// still skip chunking via the floor.
func ResolveBulkParallelMinRows(configured int64, tableCount int) int64 {
	if configured > 0 {
		return configured
	}
	if tableCount <= 1 {
		return defaultBulkParallelMinRows
	}
	adapted := defaultBulkParallelMinRows / int64(tableCount)
	if adapted < AdaptiveBulkParallelMinRowsFloor {
		return AdaptiveBulkParallelMinRowsFloor
	}
	return adapted
}

// ChunkStrategy names how a table's PK range is divided into chunks.
//
//   - StrategyMinMaxDivide: ADR-0019's MIN/MAX/divide on a single
//     integer PK column. One cheap two-aggregate query; no skew on
//     dense integer keys. Requires [ir.RangeBoundsQuerier].
//   - StrategyKeysetSample: ADR-0096's sampled-keyset (ROW_NUMBER()
//     over the PK index, split by row count → skew-free) for a single
//     non-integer orderable PK or a composite PK. Requires
//     [ir.KeysetSampler].
//   - StrategyNone: the table is not eligible for parallel chunking and
//     takes the single-reader path.
type ChunkStrategy int

// The chunk strategies: StrategyNone (single-reader fall-back),
// StrategyMinMaxDivide (ADR-0019 integer-PK MIN/MAX/divide), and
// StrategyKeysetSample (ADR-0096 sampled-keyset for non-integer /
// composite PKs). See [CanParallelChunkTable] for the dispatch.
const (
	StrategyNone ChunkStrategy = iota
	StrategyMinMaxDivide
	StrategyKeysetSample
)

// CanParallelChunkTable reports whether table is eligible for the
// parallel-copy path and, if so, which boundary strategy applies.
//
// A table is chunkable when parallelism is > 1, it has a primary key,
// and every PK column has an orderable type (one the engines' row-
// comparison cursor — ReadRowsBatch — already ORDER BYs and compares
// correctly). The strategy is then:
//
//   - StrategyMinMaxDivide for a single integer PK (ADR-0019), and
//   - StrategyKeysetSample for everything else orderable — a single
//     non-integer PK (UUID/string/binary/decimal/temporal) or a
//     composite PK (ADR-0096).
//
// A no-PK table, or a PK with any non-orderable column type
// (JSON/Array/Geometry — which no sane schema uses as a key), falls back
// to single-reader: we never invent a chunking that could miss or
// double-copy rows.
//
// Returns (true, strategy, "") when eligible, (false, StrategyNone,
// reason) otherwise. The reason string is suitable for a single-line
// operator-facing log message.
func CanParallelChunkTable(table *ir.Table, parallelism int) (eligible bool, strategy ChunkStrategy, reason string) {
	if parallelism <= 1 {
		return false, StrategyNone, "parallelism is 1; single-reader path"
	}
	if table == nil {
		return false, StrategyNone, "table is nil"
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return false, StrategyNone, "table has no primary key"
	}

	// Every PK column must be orderable for the keyset cursor + boundary
	// comparison to be well-defined.
	for _, pkc := range table.PrimaryKey.Columns {
		col := LookupColumn(table, pkc.Column)
		if col == nil {
			return false, StrategyNone, fmt.Sprintf("primary key column %q not found in column list", pkc.Column)
		}
		// A sluice-injected PK column (ADR-0048 Shape A: the leading
		// discriminator of the rewritten composite PK) exists ONLY on the
		// target-planning schema, NOT on the source — every chunk read
		// (keyset boundary sample, ReadRowsBatchBounded predicate, ORDER BY)
		// would reference it against the source and fail SQLSTATE 42703
		// "column does not exist" (the Bug-80 class). It is also constant for
		// the whole per-shard run, so it can't partition the keyspace anyway.
		// Route the table to single-reader, which reads only sourceReadable
		// columns and stamps the discriminator between read and write. (This
		// restores the pre-ADR-0096 behaviour: a shard-injected table has a
		// composite PK, which was never chunk-eligible before keyset landed.)
		if col.SluiceInjected {
			return false, StrategyNone, fmt.Sprintf("primary key column %q is sluice-injected (not present on source); single-reader path", pkc.Column)
		}
		if !IsOrderablePKType(col.Type) {
			return false, StrategyNone, fmt.Sprintf("primary key column %q is %s; not an orderable chunk key", pkc.Column, col.Type.String())
		}
	}

	// Single integer PK → the cheap MIN/MAX/divide path (ADR-0019).
	if len(table.PrimaryKey.Columns) == 1 {
		col := LookupColumn(table, table.PrimaryKey.Columns[0].Column)
		if _, ok := col.Type.(ir.Integer); ok {
			return true, StrategyMinMaxDivide, ""
		}
	}

	// Single non-integer orderable PK, or composite orderable PK → the
	// sampled-keyset path (ADR-0096).
	return true, StrategyKeysetSample, ""
}

// IsOrderablePKType reports whether an IR type can serve as (part of) a
// chunk key: it sorts deterministically under SQL ORDER BY and its
// values round-trip through a parameter placeholder for the row-
// comparison predicate. This is exactly the set the engines'
// ReadRowsBatch already orders and compares (ADR-0018), and the set the
// boundary comparator [ComparePKTuple] handles per family.
//
// ir.Bit is included: the cursor reader orders it and the comparator
// treats it as its string bit-form. ir.Domain unwraps to its base type.
// JSON / Array / Geometry / Set / Enum and unknown types are NOT
// orderable as keys and route the table to single-reader.
func IsOrderablePKType(t ir.Type) bool {
	if dom, ok := t.(ir.Domain); ok {
		if dom.BaseType == nil {
			return false
		}
		return IsOrderablePKType(dom.BaseType)
	}
	switch t.(type) {
	case ir.Integer, ir.Decimal,
		ir.Char, ir.Varchar, ir.Text, ir.UUID,
		ir.Binary, ir.Varbinary, ir.Blob, ir.Bit,
		ir.Date, ir.Time, ir.Timestamp, ir.DateTime:
		return true
	default:
		return false
	}
}

// LookupColumn returns the column with the given name, or nil if not
// found. Linear scan; tables typically have <100 columns. Spelled
// "lookup" to avoid colliding with the test-only [findColumn] helper
// in [migrate_cross_integration_test.go]; both could share an
// implementation but the test helper has a different signature
// (`*testing.T`-returning shape) and renaming the test would touch
// every cross-engine integration test.
func LookupColumn(table *ir.Table, name string) *ir.Column {
	for _, c := range table.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// ComputeChunkBoundaries divides the PK range of an integer-PK table
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
func ComputeChunkBoundaries(ctx context.Context, q RangeQuerier, table *ir.Table, n int) ([]ChunkBoundary, error) {
	if n <= 0 {
		return nil, errors.New("pipeline: ComputeChunkBoundaries: n must be > 0")
	}
	if eligible, _, reason := CanParallelChunkTable(table, n); !eligible {
		return nil, fmt.Errorf("pipeline: ComputeChunkBoundaries: table %q not eligible: %s", table.Name, reason)
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
		return []ChunkBoundary{{ChunkIndex: 0}}, nil
	}

	minInt, ok := CoerceInt64(minVal)
	if !ok {
		return nil, fmt.Errorf("pipeline: MIN(%s) returned non-integer %T (%v); expected integer PK",
			pkCol, minVal, minVal)
	}
	maxInt, ok := CoerceInt64(maxVal)
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

	out := make([]ChunkBoundary, 0, n)
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
		out = append(out, ChunkBoundary{
			ChunkIndex: k,
			LowerPK:    lower,
			UpperPK:    upper,
		})
	}
	return out, nil
}

// CoerceInt64 normalises common integer-typed driver-return values
// into int64. *sql.DB scan behaviour returns int64 for most SQL
// integer types, but range-bounds queries that go through QueryRow
// can produce either int64 or — for certain drivers — *big.Int /
// uint64. The helper handles the realistic shapes; anything else
// fails the parallel-eligibility check so the table falls back to
// single-reader.
func CoerceInt64(v any) (int64, bool) {
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
		// anyway. If we hit it, the CanParallelChunkTable check will
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
		// than parse, since ComputeChunkBoundaries is only called for
		// columns the IR has classified as ir.Integer.
		return 0, false
	}
	return 0, false
}

// RangeQuerier is a local alias for [ir.RangeBoundsQuerier]. The
// type-asserted shape on the row reader is the canonical surface; the
// alias here lets the chunk-boundary code be a small focused unit
// without the test fixtures having to refer to the IR import.
type RangeQuerier = ir.RangeBoundsQuerier

// KeysetSampler is a local alias for [ir.KeysetSampler] (ADR-0096).
type KeysetSampler = ir.KeysetSampler

// ComputeKeysetChunkBoundaries divides a non-integer / composite-PK
// table into chunks using the sampled-keyset strategy (ADR-0096). It
// asks the sampler for n-1 interior boundary tuples (each split point an
// actual row's PK, so the split is by row count and skew-free), then
// assembles them into half-open (LowerPK, UpperPK] chunk ranges with the
// same nil-bound convention ComputeChunkBoundaries uses for integers:
// chunk 0 has LowerPK==nil, chunk N-1 has UpperPK==nil, and boundary[k]
// is the INCLUSIVE upper of chunk k and the EXCLUSIVE lower of chunk k+1.
//
// Edge cases (all → fewer chunks, never a mis-split):
//
//   - Empty table / sampler returns 0 boundaries: a single nil-bounded
//     chunk; the parallel path collapses to single-reader.
//   - Fewer than n-1 DISTINCT boundaries (tiny or heavily-duplicate-
//     keyed table): consecutive equal boundaries would produce a
//     zero-width interior chunk (LowerPK == UpperPK under the half-open
//     convention captures NO rows — those rows fall in the earlier
//     chunk under pk <= boundary). We drop the duplicates, yielding
//     fewer, non-empty chunks. No row is placed in two chunks or zero
//     chunks.
//
// Boundary tuples must be width == len(pkCols); the sampler contract
// guarantees it, and a mismatch is a loud programming error rather than
// a silent partial bound.
func ComputeKeysetChunkBoundaries(ctx context.Context, s KeysetSampler, table *ir.Table, n int) ([]ChunkBoundary, error) {
	if n <= 0 {
		return nil, errors.New("pipeline: ComputeKeysetChunkBoundaries: n must be > 0")
	}
	if eligible, strategy, reason := CanParallelChunkTable(table, n); !eligible || strategy != StrategyKeysetSample {
		return nil, fmt.Errorf("pipeline: ComputeKeysetChunkBoundaries: table %q not keyset-eligible: %s", table.Name, reason)
	}

	pkCols := PrimaryKeyColumnNames(table)
	boundaries, err := s.SampleKeysetBoundaries(ctx, table, pkCols, n)
	if err != nil {
		return nil, fmt.Errorf("pipeline: sample keyset boundaries for %q: %w", table.Name, err)
	}

	// Validate width and drop consecutive duplicates so no zero-width
	// interior chunk is produced. Boundaries arrive in PK order.
	deduped := make([][]any, 0, len(boundaries))
	for _, b := range boundaries {
		if len(b) != len(pkCols) {
			return nil, fmt.Errorf("pipeline: keyset boundary for %q has %d values; want %d PK columns",
				table.Name, len(b), len(pkCols))
		}
		if len(deduped) > 0 && ComparePKTuple(deduped[len(deduped)-1], b) == 0 {
			// Equal to the previous boundary: the interior chunk between
			// them would be empty (pk > prev AND pk <= b with prev == b).
			// Drop it so we don't burn a connection on a no-row chunk.
			continue
		}
		deduped = append(deduped, b)
	}

	// 0 boundaries → single nil-bounded chunk (collapses to single-reader).
	if len(deduped) == 0 {
		return []ChunkBoundary{{ChunkIndex: 0}}, nil
	}

	// k boundaries → k+1 chunks. Chunk i:
	//   lower = boundary[i-1] (nil for i==0),
	//   upper = boundary[i]   (nil for the last chunk).
	out := make([]ChunkBoundary, 0, len(deduped)+1)
	for i := 0; i <= len(deduped); i++ {
		var lower, upper []any
		if i > 0 {
			lower = deduped[i-1]
		}
		if i < len(deduped) {
			upper = deduped[i]
		}
		out = append(out, ChunkBoundary{ChunkIndex: i, LowerPK: lower, UpperPK: upper})
	}
	return out, nil
}

// ComparePKTuple compares two PK tuples under the SAME total order the
// engines' row-comparison cursor uses (ORDER BY pk1, pk2, ... and
// WHERE (pk1,...) > (...)): lexicographic, column by column, with each
// column compared per its value family. Returns -1, 0, or +1.
//
// This is the load-bearing exactly-once surface (ADR-0096): the boundary
// clip in [filterByUpperBound] uses it to decide whether a row is at or
// past a chunk's inclusive UpperPK, and the dedup above uses it to drop
// zero-width chunks. A wrong comparison for any family would silently
// mis-place a boundary row (the Bug-74 class), so the comparator is
// pinned across every orderable family × {single, composite}.
//
// Tuples must be equal width (the orchestrator only ever compares a row's
// PK projection against a same-width boundary). A nil/empty tuple sorts
// before any non-empty one; equal-prefix-but-shorter sorts first.
func ComparePKTuple(a, b []any) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if c := comparePKValue(a[i], b[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// comparePKValue compares two single PK column values within their
// family. The families mirror [IsOrderablePKType]:
//
//   - integer-like (int64/int32/int/uint64/uint32) → numeric compare.
//   - everything else is normalised to a comparable scalar:
//     []byte (binary / driver-returned strings / decimal-as-bytes) and
//     string compare BYTEWISE — which matches SQL's binary collation for
//     BINARY/VARBINARY and the C-locale ORDER BY the cursor relies on;
//     time.Time compares chronologically.
//
// Mixed integer/non-integer (shouldn't happen — a column is one family)
// falls through to a stable bytewise compare of the fmt forms so the
// result is deterministic rather than panicking. NULL PK values cannot
// occur (PK columns are NOT NULL); a nil here is a programming error and
// sorts before any value rather than silently equal.
func comparePKValue(a, b any) int {
	// Integer family first (the common case + the MIN/MAX/divide path).
	ai, aok := CoerceInt64(a)
	bi, bok := CoerceInt64(b)
	if aok && bok {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}

	// nil sorts first (defensive — PK columns are NOT NULL).
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}

	// time.Time chronological compare.
	if at, ok := a.(time.Time); ok {
		if bt, ok := b.(time.Time); ok {
			switch {
			case at.Before(bt):
				return -1
			case at.After(bt):
				return 1
			default:
				return 0
			}
		}
	}

	// Everything else: bytewise compare of the canonical byte form
	// (matches SQL binary collation + the C-locale cursor ORDER BY).
	return bytesCompare(pkBytes(a), pkBytes(b))
}

// pkBytes renders a non-integer, non-time PK value to its canonical byte
// form for bytewise comparison. string and []byte are the two shapes the
// drivers return for orderable string/binary/decimal/uuid/bit columns;
// anything else falls back to its fmt form so the compare stays
// deterministic.
func pkBytes(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return []byte(t)
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// bytesCompare is bytes.Compare without importing bytes for one call.
func bytesCompare(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}
