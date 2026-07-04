// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declarations of the OPTIONAL ir interfaces this package's
// concrete types intentionally implement — for BOTH engines it registers: the
// local-file `sqlite` engine and the live Cloudflare `d1` engine (ADR-0132).
//
// Why this file exists: the orchestrator discovers optional surfaces by
// runtime type-assertion, so a method-set break — a signature change, a
// renamed method, a receiver flipped between value and pointer — doesn't fail
// the build; it makes the assertion quietly stop matching and the pipeline
// SILENTLY downgrades to the fallback path. Each group below names what that
// downgrade would cost. When removing an interface from a type ON PURPOSE,
// delete its line here in the same commit and call out the downgrade in the
// commit message.
//
// The surfaces are intentionally NARROW where they are narrow. [D1RowReader]
// implements NONE of the batched/counter surfaces: its keyset pagination is
// internal to ReadRows (HTTP pages, ADR-0132), so the orchestrator must route
// D1 tables through the single-reader whole-table copy — do NOT "fix" a
// missing-interface report by widening it (a decoded-value cursor re-bound
// over the HTTP transport would re-open the storage-class round-trip hazard
// that [ir.BatchedReadDisqualifier] exists to veto). Neither engine implements
// any CDC/snapshot/slot surface here: the trigger-CDC variants live in the
// sqlite-trigger / d1-trigger packages, which compose these engines by
// delegation precisely to keep the openers narrow.
var (
	// Engine-level: the registry holds Engine values; a break here would
	// unregister the engine's whole surface at compile time (loud), but the
	// pin keeps the intent auditable next to the optional ones.
	_ ir.Engine = Engine{}
	_ ir.Engine = d1Engine{}

	// Schema readers: validated rich-type inference (ADR-0144). Both readers
	// run the SAME validation SQL; the pipeline REFUSES --infer-types when the
	// assertion stops matching, so a silent method-set break would turn a
	// supported SQLite/D1 flag into a spurious "only supported for SQLite/D1
	// sources" refusal.
	_ ir.InferredTypeValidator = (*SchemaReader)(nil)
	_ ir.InferredTypeValidator = (*D1SchemaReader)(nil)

	// File RowReader: the cursor/batched read family. Losing BatchedRowReader/
	// BoundedBatchedRowReader silently demotes every large-table copy to the
	// single-reader path (no within-table parallelism, no per-batch resume);
	// losing BatchedReadDisqualifier is WORSE than a slow path — it is the
	// veto that keeps a non-round-trippable SQLite PK (temporal/NUMERIC
	// storage-class ambiguity) OFF the cursor, so its silent loss re-opens a
	// silent truncation/duplication class, not just a perf regression.
	_ ir.BatchedRowReader        = (*RowReader)(nil)
	_ ir.BoundedBatchedRowReader = (*RowReader)(nil)
	_ ir.BatchedReadDisqualifier = (*RowReader)(nil)

	// File RowReader: the chunk-planning family. Silent loss would quietly
	// disable the parallel-copy split (RangeBounds/KeysetSampler pick the
	// chunk boundaries; RowCounter/RowCountEstimator decide whether a table is
	// worth splitting) — every table would fall back to one reader.
	_ ir.KeysetSampler      = (*RowReader)(nil)
	_ ir.RangeBoundsQuerier = (*RowReader)(nil)
	_ ir.RowCountEstimator  = (*RowReader)(nil)
	_ ir.RowCounter         = (*RowReader)(nil)

	// SchemaWriter: post-load ANALYZE (--analyze-after). Silent loss means the
	// flag quietly does nothing on a SQLite target and the first post-migrate
	// queries run on stale planner stats.
	_ ir.TableAnalyzer = (*SchemaWriter)(nil)

	// RowWriter: target-side bulk-copy hygiene. Losing TableTruncator/
	// TableDropper turns the no-PK resume redo and --reset-target-data on a
	// SQLite target into spurious refusals (loud, but of a supported
	// operation); losing MaxBufferBytesSetter quietly discards the operator's
	// memory bound (unbounded batch buffering instead of a capped one).
	_ ir.MaxBufferBytesSetter = (*RowWriter)(nil)
	_ ir.TableDropper         = (*RowWriter)(nil)
	_ ir.TableTruncator       = (*RowWriter)(nil)
)
