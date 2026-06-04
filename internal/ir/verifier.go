// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// Verifier is the optional engine-side surface for data-integrity
// verification. The `sluice verify` command (proto-ADR
// docs/dev/design/sluice-verify.md) uses this interface to compare
// source and target row-level state without committing sluice's row-
// reader (which is bulk-copy-shaped) to a "verify many small queries"
// shape.
//
// v0.12.0 shipped count-mode (`ExactRowCount`). v0.14.0 adds sample
// mode (`SampleRowHashes`). Full mode (`FullScanHash`) follows per
// the proto-ADR sequencing.
//
// "Exact" here distinguishes from [RowCounter], which returns
// approximate counts (pg_class.reltuples, MySQL information_schema
// estimates) suitable for ETA hints in bulk-copy progress lines but
// NOT for verification — `verify` needs authoritative counts that
// would not silently disagree with what's actually stored.
//
// Engines that don't implement [Verifier] cause `sluice verify` to
// surface a clear "verify not supported on this engine" operational
// error — same shape as the optional-interface fallbacks elsewhere in
// the codebase ([RowCounter], [SnapshotImporter], [SchemaTypeDropper]).
type Verifier interface {
	// ExactRowCount returns the precise row count for the given table
	// via SELECT COUNT(*) (or engine-equivalent that's authoritative
	// rather than approximate). The cost is one full-table scan per
	// call on most engines; verify callers may parallelise across
	// tables but should expect this to be the bottleneck on large
	// tables.
	//
	// **PlanetScale + large-table behavior** (v0.14.0): MySQL engines
	// implement chunked counting via the [RangeBoundsQuerier] surface
	// when the table has a single integer PK, splitting the COUNT
	// across PK ranges to stay under PlanetScale's per-query
	// row-read budget (~100K rows by default). Tables without a
	// single-int PK fall back to a single SELECT COUNT(*) which may
	// hit the budget on multi-100K-row tables; in that case the
	// engine surfaces a clear error.
	//
	// Returns the table's row count and a nil error on success.
	// Returns (0, error) on any operational failure (table doesn't
	// exist on this engine, query timeout, connection broke). Callers
	// distinguish these from "table is empty" (0, nil).
	ExactRowCount(ctx context.Context, table *Table) (int64, error)
}

// SampleVerifier extends [Verifier] with the sample-mode surface
// added in v0.14.0. Engines that implement Verifier may opt into
// sample-mode by also implementing this interface; engines without
// SampleVerifier cause `sluice verify --depth sample` to surface a
// clear "sample mode not supported on this engine" operational error.
//
// **Same-engine constraint** (v0.14.0 MVP): sample mode compares
// server-side row hashes computed via MD5(CONCAT_WS('|', col::text,
// ...)). The text rendering of values is engine-specific (MySQL
// TINYINT(1) → 0/1 vs PG BOOLEAN → t/f, etc.), so cross-engine
// sample comparison would produce silent false-positive mismatches.
// The orchestrator enforces source.Name() == target.Name() before
// calling SampleRowHashes; cross-engine sample mode is deferred to a
// future phase that adds client-side canonicalization.
type SampleVerifier interface {
	// SampleRowHashes returns up to n sampled rows from the table,
	// each represented as its primary-key string + the hash of its
	// concatenated column values. The seed parameter makes sampling
	// deterministic across calls — passing the same seed to source
	// and target picks the same row subset on both sides, so the
	// returned hashes can be compared 1:1.
	//
	// algo selects the hash function used for the row-content
	// digest. Both source and target sides MUST be called with the
	// same algo or the comparison is meaningless.
	//
	// Result ordering: sorted by primary key (lexicographically on
	// the PrimaryKey string). Both sides return the same order so
	// the orchestrator can do an O(n) comparison.
	//
	// Returns an empty slice for an empty table (not an error).
	// Returns an error when the table has no usable PK (sample mode
	// requires deterministic sampling), when n <= 0, or on any
	// operational failure.
	SampleRowHashes(ctx context.Context, table *Table, n int, seed int64, algo HashAlgorithm) ([]SampledRowHash, error)
}

// HashAlgorithm selects the hash function used by sample-mode verify
// to digest a row's column-values for cross-side comparison. v0.14.2
// introduced this enum so operators can opt into SHA-256 (`--strict-
// hash`) for an extra confidence margin on top of MD5's already-
// negligible-for-honest-data collision probability.
//
// See `docs/verify-vs-vitess-vdiff.md` for the collision-math
// rationale and the operator-facing comparison.
type HashAlgorithm uint8

// Recognised HashAlgorithm values.
const (
	// HashMD5 produces a 128-bit hash. Default; sufficient for
	// honest-data scenarios at any practical row count.
	HashMD5 HashAlgorithm = iota
	// HashSHA256 produces a 256-bit hash. Operator opt-in via the
	// `--strict-hash` flag. ~2× slower server-side than MD5 but
	// the difference is sub-second at sample-mode's typical sizes.
	HashSHA256
)

// SQLFunction returns the SQL function name the engine writers use
// to invoke this hash algorithm. Both supported engines (MySQL,
// Postgres) accept these function names — MySQL via the built-in
// MD5/SHA2 functions; Postgres via MD5 (built-in) and SHA-256 via
// pgcrypto's digest+encode (which sluice converts to a literal
// `MD5(...)` or `ENCODE(DIGEST(..., 'sha256'), 'hex')` shape per the
// engine's verifier implementation).
//
// Callers shouldn't assume the function-name shape works as a drop-
// in literal; the engine-side SampleRowHashes builds the right
// SQL fragment based on this enum value.
func (a HashAlgorithm) String() string {
	switch a {
	case HashMD5:
		return "MD5"
	case HashSHA256:
		return "SHA-256"
	default:
		return "unknown"
	}
}

// SampledRowHash is one row's contribution to a sample-mode verify
// comparison. PrimaryKey is the canonical string rendering of the
// row's PK (composite PKs joined with '|'); Hash is the hex MD5 of
// the row's concatenated column values.
type SampledRowHash struct {
	PrimaryKey string
	Hash       string
}
