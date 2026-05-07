// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// Verifier is the optional engine-side surface for data-integrity
// verification. The `sluice verify` command (proto-ADR
// docs/dev/design-sluice-verify.md) uses this interface to compare
// source and target row-level state without committing sluice's row-
// reader (which is bulk-copy-shaped) to a "verify many small queries"
// shape.
//
// v0.12.0 ships only the count-mode MVP — `ExactRowCount`. Sample
// mode (`SampleRows`) and full mode (`FullScanHash`) follow in later
// patch releases per the proto-ADR's sequencing.
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
	// Returns the table's row count and a nil error on success.
	// Returns (0, error) on any operational failure (table doesn't
	// exist on this engine, query timeout, connection broke). Callers
	// distinguish these from "table is empty" (0, nil).
	ExactRowCount(ctx context.Context, table *Table) (int64, error)
}
