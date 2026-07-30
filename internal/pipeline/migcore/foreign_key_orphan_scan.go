// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// # Post-constraints foreign-key orphan verification (roadmap item 109)
//
// The item-109 statement-wall recovery adds a wall-blocked `ADD FOREIGN KEY`
// metadata-only under foreign_key_checks=0, which skips InnoDB's O(rows)
// child-row validation. That is a convenience the loud-failure tenet does not
// grant for free: sluice deliberately withholds degraded-FK tolerance from
// MySQL (`--allow-degraded-fks` is PG-target-only), so a silently-accepted
// source orphan would invert the documented baseline.
//
// This file recovers the loud signal WITHOUT re-incurring the wall: after the
// constraints phase, every metadata-only-added FK is PROVEN clean with a
// bounded chunked orphan scan. The child table's PK range is split with the
// SAME machinery the parallel copy pins ([ComputeChunkBoundaries] integer
// MIN/MAX-divide, [ComputeKeysetChunkBoundaries] ADR-0096 keyset), so every
// chunk's scan query is PK-range-bounded and cannot itself hit the ~900 s wall.
// A clean scan turns "assume FK-consistent by construction" into "proven clean,
// cheaply"; a violation is refused LOUDLY (the FK is dropped first, so the
// target never keeps a trusted-wrong constraint).

// orphanScanChunkTargetRows is the target child-row count per bounded scan
// chunk. Conservative: an indexed LEFT-JOIN LIMIT-1 probe over this many rows
// is far under the statement wall, and the FK's referencing columns are indexed
// (the metadata-only add required it), so parent lookups ride the parent PK.
const orphanScanChunkTargetRows = 2_000_000

// orphanScanMaxChunks caps the chunk count so an enormous table doesn't emit an
// unreasonable number of (cheap) probe queries.
const orphanScanMaxChunks = 4096

// ValidateUnvalidatedForeignKeys proves every foreign key the writer added
// metadata-only (item-109 wall recovery) is actually satisfied by the copied
// child rows, via a bounded chunked orphan scan. It is a NO-OP when the writer
// added nothing metadata-only (every non-armed / vanilla / filtered path, and
// the common by-construction-clean armed path where no FK walled) — the writer
// simply reports an empty set.
//
//   - sw is the target schema writer that just ran the constraints phase; it
//     must expose the item-109 scan/drop surfaces when it reports unvalidated
//     FKs (the MySQL writer does).
//   - schema locates each child table (for its PK, to bound the scan).
//   - sampler is a TARGET-side reader used only for chunk-boundary computation
//     ([ir.RangeBoundsQuerier] / [ir.KeysetSampler] / optional [ir.RowCounter]);
//     the scan itself runs on the writer's own connection.
//
// On the first orphan found it DROPS the offending FK (so the run refuses in
// the same "FK absent" state a validating ADD would have left) and returns a
// SLUICE-E-FK-SOURCE-ORPHAN refusal naming the child table, the FK, and an
// example violating key.
func ValidateUnvalidatedForeignKeys(ctx context.Context, sw ir.SchemaWriter, schema *ir.Schema, sampler any) error {
	reporter, ok := sw.(ir.UnvalidatedForeignKeyReporter)
	if !ok {
		return nil
	}
	unvalidated := reporter.UnvalidatedForeignKeys()
	if len(unvalidated) == 0 {
		return nil
	}
	scanner, sok := sw.(ir.ForeignKeyOrphanScanner)
	dropper, dok := sw.(ir.ForeignKeyDropper)
	if !sok || !dok {
		// A writer that reports unvalidated FKs but cannot scan or drop them is
		// a programming error — refuse loudly rather than let the unvalidated
		// FKs stand unproven.
		return fmt.Errorf("pipeline: %d foreign key(s) were added without validation but the target writer cannot scan/drop them to prove them clean — refusing rather than trusting them", len(unvalidated))
	}

	for _, uf := range unvalidated {
		child := findTableByName(schema, uf.Table)
		if child == nil {
			return fmt.Errorf("pipeline: FK orphan scan: child table %q not found in schema", uf.Table)
		}
		if err := scanOneForeignKey(ctx, scanner, dropper, sampler, child, uf.FK); err != nil {
			return err
		}
	}
	return nil
}

// scanOneForeignKey chunks child's PK range and scans each chunk for an orphan
// of fk. On a violation it drops fk and returns the coded refusal.
func scanOneForeignKey(ctx context.Context, scanner ir.ForeignKeyOrphanScanner, dropper ir.ForeignKeyDropper, sampler any, child *ir.Table, fk *ir.ForeignKey) error {
	bounds := orphanScanBoundaries(ctx, sampler, child)
	slog.InfoContext(ctx,
		"pipeline: proving a metadata-only-added foreign key clean with a bounded chunked orphan scan (roadmap item 109)",
		slog.String("table", child.Name),
		slog.String("foreign_key", fk.Name),
		slog.Int("chunks", len(bounds)))

	for _, b := range bounds {
		violating, found, err := scanner.ScanForeignKeyOrphan(ctx, child, fk, b.LowerPK, b.UpperPK)
		if err != nil {
			return fmt.Errorf("pipeline: FK orphan scan on %q (fk %q): %w", child.Name, fk.Name, err)
		}
		if !found {
			continue
		}
		// Drop the trusted-wrong FK first, then refuse — the target ends in the
		// same "FK absent, run failed loud" state a validating ADD would have
		// left, never "FK present but unproven".
		if derr := dropper.DropForeignKey(ctx, child.Name, fk.Name); derr != nil {
			return fmt.Errorf("pipeline: FK orphan scan found a violation on %q (fk %q) but could not drop the unvalidated constraint: %w", child.Name, fk.Name, derr)
		}
		return sluicecode.Wrap(
			sluicecode.CodeFKSourceOrphan,
			"clean the orphaned child rows on the source, or re-run with --skip-foreign-keys to complete without the constraint (its referencing columns stay indexed)",
			fmt.Errorf("pipeline: foreign key %q on table %q is violated by the copied data: child row with primary key %v references a %q row that does not exist — the source was not FK-consistent, so the metadata-only wall recovery could not be trusted; the constraint has been dropped and the run refused",
				fk.Name, child.Name, formatKey(violating), fk.ReferencedTable),
		)
	}
	return nil
}

// orphanScanBoundaries computes bounded PK chunks for child, reusing the same
// strategy the parallel copy uses. A degenerate result (unchunkable PK, tiny
// table, sampler error) collapses to a single unbounded chunk — a scan that, if
// the table is genuinely huge, will itself surface the wall as a LOUD error
// from ScanForeignKeyOrphan rather than silently skip verification.
func orphanScanBoundaries(ctx context.Context, sampler any, child *ir.Table) []ChunkBoundary {
	single := []ChunkBoundary{{ChunkIndex: 0}}

	n := orphanScanChunkCount(ctx, sampler, child)
	if n < 2 {
		return single
	}
	eligible, strategy, _ := CanParallelChunkTable(child, 2)
	if !eligible {
		return single
	}

	var (
		bounds []ChunkBoundary
		err    error
	)
	switch strategy {
	case StrategyMinMaxDivide:
		if q, ok := sampler.(RangeQuerier); ok {
			bounds, err = ComputeChunkBoundaries(ctx, q, child, n)
		}
	case StrategyKeysetSample:
		if s, ok := sampler.(KeysetSampler); ok {
			bounds, err = ComputeKeysetChunkBoundaries(ctx, s, child, n)
		}
	}
	if err != nil || len(bounds) == 0 {
		return single
	}
	return bounds
}

// orphanScanChunkCount sizes the chunk count from the child's estimated row
// count (when the sampler can supply one), clamped to [1, orphanScanMaxChunks].
func orphanScanChunkCount(ctx context.Context, sampler any, child *ir.Table) int {
	counter, ok := sampler.(ir.RowCounter)
	if !ok {
		return orphanScanMaxChunks
	}
	est, err := counter.CountRows(ctx, child)
	if err != nil || est <= 0 {
		return orphanScanMaxChunks
	}
	n := int((est + orphanScanChunkTargetRows - 1) / orphanScanChunkTargetRows)
	if n < 1 {
		n = 1
	}
	if n > orphanScanMaxChunks {
		n = orphanScanMaxChunks
	}
	return n
}

// findTableByName returns the schema table with the given name, or nil.
func findTableByName(schema *ir.Schema, name string) *ir.Table {
	if schema == nil {
		return nil
	}
	for _, t := range schema.Tables {
		if t != nil && t.Name == name {
			return t
		}
	}
	return nil
}

// formatKey renders a violating child PK tuple for the operator-facing message.
// []byte values (MySQL string/binary columns) are shown as their string form.
func formatKey(key []any) []any {
	out := make([]any, len(key))
	for i, v := range key {
		if b, ok := v.([]byte); ok {
			out[i] = string(b)
			continue
		}
		out[i] = v
	}
	return out
}
