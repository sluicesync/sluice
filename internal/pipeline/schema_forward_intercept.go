// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0091 — Default-on online schema-change forwarding for single-
// stream (non-Shape-A) CDC apply. (Extends ADR-0058, which shipped the
// opt-in ADD-COLUMN-only form this file originally implemented.)
//
// The Shape A multi-shard path (ADR-0054) already forwards every
// recognized shape via the lease + boundary router
// (shard_consolidation_router.go). This file fills the single-stream
// gap: when --schema-changes=forward (the default), observed source
// DDL forwards to the target via [ir.ShapeDeltaApplier] using the same
// per-shape dispatch (applyShapeDelta) the boundary router uses.
//
// Forwarded shapes (every unambiguous one):
//   - ADD COLUMN — via [ir.SchemaDeltaApplier.AlterAddColumn], followed
//     by the default-on source-side backfill of the rows the target
//     already held (schema_forward_backfill.go; opt out with
//     --no-backfill-added-column), and a computed-DEFAULT volatility
//     refusal (ADR-0058 §2a).
//   - DROP COLUMN, ALTER COLUMN TYPE / NULLABILITY, CREATE / DROP
//     INDEX, ADD / DROP / MODIFY CHECK — via applyShapeForward, after
//     retargeting the post IR to the target dialect + scrubbing its
//     Schema (ADR-0091 §5).
//   - column reorder (ShapeKindNone) — no DDL; sluice decodes by name.
//
// Refuse-loudly catalog:
//   - RENAME COLUMN — indistinguishable from drop+add of a same-type
//     column from the stream alone; forwarding the wrong guess risks
//     silent data loss (ADR-0091 §3; F7b adds PG attnum-proven rename).
//   - Multi-shape combo (ShapeKindUnrecognized) — can't be ordered
//     unambiguously.
//   - ADD COLUMN with a volatile/computed DEFAULT (ADR-0058 §2a).
//
// The intercept activates when:
//   - Streamer schema-change forwarding is enabled (the --schema-changes
//     mode is not "refuse"; see Streamer.forwardSchemaEnabled), AND
//   - Streamer.boundaryRouter is nil (i.e. not Shape A — Shape A's
//     intercept already forwards every shape via the lease).
//
// When both hold, this file's intercept replaces the pass-through
// branch in [interceptSchemaSnapshotsForCoordination]'s nil-router
// case.

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"
)

// schemaForwardDeps is the dependency surface
// [interceptAddColumnForward] needs. Plumbed by the Streamer's apply
// loop; the test harness constructs a minimal one with fakes.
type schemaForwardDeps struct {
	// applier issues the per-shape ALTER/CREATE/DROP DDL on the target
	// (ADR-0091). Both PG and MySQL SchemaWriters implement the full
	// [ir.ShapeDeltaApplier] surface; the Streamer opens one alongside
	// the existing apply-side resources. The ADD COLUMN branch uses
	// the embedded [ir.SchemaDeltaApplier.AlterAddColumn]; the other
	// forwarded shapes use the extended methods via [applyShapeDelta].
	applier ir.ShapeDeltaApplier

	// sourceEngineName is the source engine's [ir.Engine.Name] for the
	// translate.RetargetForEngine call (cross-engine type rewrite on
	// the ADD COLUMN's column definition).
	sourceEngineName string

	// targetEngineName is the target engine's name for the same
	// translate.RetargetForEngine call.
	targetEngineName string

	// backfill, when non-nil, fills the added column on the rows the
	// target already held from the source's own values after the ALTER
	// lands (ADR-0058 §1c). Built by [Streamer.addedColumnBackfill] —
	// non-nil unless the operator passed --no-backfill-added-column.
	backfill *schemaForwardBackfill

	// defaultProber, when non-nil, returns the source's canonical
	// [ir.DefaultValue] for the named column. Used by the ADR-0058 §2a
	// volatility refusal to surface the DEFAULT text that the CDC
	// reader's projection drops (pgoutput RelationMessage doesn't
	// carry attdefault; MySQL's TableMapEvent doesn't carry it
	// either). Built by [engageAddColumnForward] using a source-side
	// SchemaReader; nil in unit tests that exercise the intercept
	// against synthesized SchemaSnapshots whose IR already carries the
	// DEFAULT field (e.g. [ir.DefaultExpression]). Errors from the
	// prober propagate as refuse-loudly via the standard intercept
	// path — refuse-on-uncertainty.
	defaultProber defaultProberFunc

	// defaultCarrier, when non-nil, returns the source column's DEFAULT as the
	// source SchemaReader translates it into the IR — the value a cold-start
	// migrate would emit. It is CARRIED into the forwarded ADD COLUMN
	// ([carrySourceDefaults]) because the target fills every pre-existing row
	// with the column's DEFAULT at ALTER time and no row event ever corrects
	// them: a forward without it left those rows NULL where the source holds
	// the default (the pre-existing-row gate, Bug 287 class). Distinct from
	// [schemaForwardDeps.defaultProber], which may return RAW catalog text
	// for the volatility classification (Bug 91) and is not emittable across
	// engines.
	defaultCarrier defaultProberFunc

	// recoveryHint renders the recovery text the ADD COLUMN DEFAULT
	// refusals ([refuseComputedDefaults], [carryAddedColumnDefaults]) end
	// with. nil is the single-stream forwarder's [forwardRecoveryHint]; the
	// Shape A boundary router, which runs the same two DEFAULT steps, sets
	// its own fleet-wide [RecoveryHint]. Read through [schemaForwardDeps.hint].
	recoveryHint func(tableName string) string

	// normalizer is the SOURCE engine's optional Bug 84/86 comparison
	// lens ([ir.CDCSchemaSnapshotNormalizer]; nil → identity). Applied
	// to every incoming CDC snapshot before it is classified or cached
	// so the classifier ALWAYS compares two normalized tables — the
	// seed side is normalized at synthesis
	// ([synthesizeColdStartSeedSnapshots]). One-sided normalization is
	// the TRIAGE-#3 phantom-alter regression shape (see
	// [normalizeSnapshotForComparison]). The RAW snapshot keeps flowing
	// into the apply paths (retarget re-resolves type-bearing payloads
	// by name against snap.IR) and downstream to the applier's
	// schema-history write.
	normalizer ir.CDCSchemaSnapshotNormalizer
}

// hint is the recovery text for a DEFAULT refusal on tableName — the
// caller's [schemaForwardDeps.recoveryHint], or the single-stream
// [forwardRecoveryHint] when none is set.
func (d schemaForwardDeps) hint(tableName string) string {
	if d.recoveryHint != nil {
		return d.recoveryHint(tableName)
	}
	return forwardRecoveryHint(tableName)
}

// defaultProberFunc returns the source's canonical [ir.DefaultValue]
// for the column identified by (schema, table, column). Non-nil
// (schema, table) must match the source's catalog naming; the
// schema may be empty (MySQL's bare-name convention). Returns
// ([ir.DefaultNone]{}, nil) if the column has no DEFAULT clause.
//
// The closure may issue a database query on every call — callers
// should batch when possible (the intercept calls once per ADD
// COLUMN forward, not once per row).
type defaultProberFunc func(ctx context.Context, schema, table, column string) (ir.DefaultValue, error)

// schemaForwardBackfill bundles the source-read side of the added-column
// backfill (ADR-0058 §1c). Held by [schemaForwardDeps] — and by the Shape A
// boundary intercept — unless the operator opted out with
// --no-backfill-added-column; built by [Streamer.addedColumnBackfill].
type schemaForwardBackfill struct {
	// reader returns the source-side row reader the bounded PK-cursor
	// iteration pages through, opening it on first use
	// ([lazyBackfillReader]). An error — including a source reader
	// that does not implement [ir.BatchedRowReader] — refuses the
	// boundary loudly.
	reader func(ctx context.Context) (ir.BatchedRowReader, error)

	// primaryKey, when non-nil, resolves the table's primary key from the
	// source catalog for a boundary whose projection carries none (the
	// VStream FIELD event; [sourcePrimaryKeyResolver]). nil in unit tests,
	// which put the key on the snapshot IR.
	primaryKey func(ctx context.Context, schema, table string) (*ir.Index, error)

	// ledger records every backfill a forwarded boundary owes until the
	// attempt that ran it can prove it reached the target
	// ([addedColumnBackfillLedger]). nil (unit tests) records nothing.
	ledger *addedColumnBackfillLedger

	// streamID is the per-stream identifier used for log
	// correlation.
	streamID string

	// batchSize bounds each SELECT's row count. Reuses the streamer's
	// BulkBatchSize when > 0; otherwise [migcore.DefaultBulkBatchSize].
	batchSize int
}

// interceptAddColumnForward wraps the change channel for the
// non-Shape-A single-stream forwarding path (ADR-0058). On each
// [ir.SchemaSnapshot] event, the intercept compares (cached → snap.IR)
// via [ClassifyShape] and:
//
//   - [ShapeKindAddColumn] → call deps.applier.AlterAddColumn on the
//     target; optionally backfill via deps.backfill.
//   - [ShapeKindNone] → no-op pass-through (forward the snapshot to the
//     applier so the ADR-0049 schema-history row records the version).
//   - Any other recognized shape → refuse loudly with the drained-model
//     recovery hint (DROP / ALTER TYPE / RENAME / CHECK / generated /
//     CREATE INDEX / DROP INDEX / multi-shape combo).
//
// seed is the cold-start cache seed — one synthetic SchemaSnapshot per
// filtered table reflecting the source IR captured at cold-start
// (built by [Streamer.coldStart] via
// [synthesizeColdStartSeedSnapshots]). The seed pre-populates the
// per-table cache so the FIRST CDC SchemaSnapshot is classified as a
// real boundary against the cold-start IR rather than treated as the
// anchor. Without the seed, MySQL sources fail Bug 89: pgoutput emits
// a Relation on first-touch (giving PG an effective pre-DDL seed), but
// MySQL's binlog only surfaces SchemaSnapshot on DDL detection
// (already POST-DDL), so the intercept's cache stays empty until the
// first DDL fires and the ALTER silently passes through as the anchor.
// Seed snapshots are NOT forwarded downstream — the applier already
// wrote the schema-history row at cold-start via the schema-apply
// phase.
//
// On any refuse-loudly error, the intercept closes the out-channel
// and stores the error in errStore for the streamer's
// surfaceSourceError path.
func interceptAddColumnForward(
	ctx context.Context,
	in <-chan ir.Change,
	seed []ir.SchemaSnapshot,
	deps schemaForwardDeps,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	if deps.applier == nil {
		// Defensive — the engagement guard upstream should refuse
		// before reaching this point.
		return in
	}
	out := make(chan ir.Change)
	cache := map[string]*ir.Table{}
	// seedSourced marks cache entries whose pre-state came from the
	// cold-start seed (a full SchemaReader read) rather than a prior CDC
	// snapshot. ADR-0091 §3: a seed's IR fidelity differs from the CDC
	// projection's (pgoutput omits generated columns, secondary indexes,
	// check constraints, nullability, …), so a seed-vs-firstCDC diff can
	// surface a PHANTOM destructive/mutating shape. The normalizer
	// (CDCSchemaSnapshotNormalizer) closes the known asymmetries, but it
	// cannot be proven complete (Bug 84/86 found gaps incrementally). So
	// destructive/mutating shapes are NEVER forwarded against a
	// seed-sourced pre — only against a genuine CDC→CDC boundary where
	// both sides share projection fidelity. The cost is a missed
	// first-post-coldstart DROP/ALTER (rare, and a SAFE non-destructive
	// divergence — the target keeps the column); the benefit is that no
	// residual fidelity gap can ever forward a phantom drop/alter.
	seedSourced := map[string]bool{}
	// Pre-populate the cache from the cold-start seed BEFORE consuming
	// from `in`. The seed is keyed under whatever QualifiedName() the
	// SchemaReader produced (MySQL: bare table name because the reader
	// leaves Schema empty; PG: schema.table). lookupSeedCache below
	// handles the bare-name fallback on the CDC side so MySQL's
	// schema-qualified first CDC SchemaSnapshot still finds the seed.
	for i := range seed {
		snap := seed[i]
		if snap.IR == nil {
			continue
		}
		cache[snap.QualifiedName()] = snap.IR
		seedSourced[snap.QualifiedName()] = true
		slog.DebugContext(
			ctx, "forward-add-column intercept: seeded from cold-start handoff",
			"table", snap.QualifiedName(),
		)
	}
	go func() {
		defer close(out)
		// The first positioned change after a backfill is its durability
		// watermark (schema_forward_backfill_ledger.go).
		var watermark backfillWatermark
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				snap, isSnap := c.(ir.SchemaSnapshot)
				if !isSnap {
					watermark.observe(c)
					if !forwardChange(ctx, out, c) {
						return
					}
					continue
				}
				key := snap.QualifiedName()
				pre, hadPre, preKey := lookupSeedCache(cache, key, snap.Table)
				preIsSeed := hadPre && seedSourced[preKey]
				// Promote a bare-name seed hit to the qualified key so
				// subsequent snapshots resolve directly (mirrors the
				// Shape A intercept's behaviour).
				if hadPre && preKey != key {
					delete(cache, preKey)
					delete(seedSourced, preKey)
				}
				// Comparison form of the post-DDL IR — the same Bug 84/86
				// lens the seed went through, applied to BOTH classifier
				// sides (see schemaForwardDeps.normalizer). Cached so the
				// NEXT boundary's pre is already in comparison form; the
				// RAW snapshot still drives the apply paths + downstream
				// forward.
				post := normalizeSnapshotForComparison(deps.normalizer, snap.IR)
				cache[key] = post
				// This snapshot's IR is now the pre-state for the NEXT
				// boundary, and it came from CDC — so the next comparison
				// is CDC→CDC (full guard lifts).
				delete(seedSourced, key)
				if !hadPre {
					slog.DebugContext(
						ctx, "forward-add-column intercept: seeded table cache",
						"table", key,
					)
					if !forwardChange(ctx, out, c) {
						return
					}
					continue
				}
				if err := routeForwardBoundary(ctx, deps, key, pre, post, snap, preIsSeed); err != nil {
					slog.ErrorContext(
						ctx, "forward-add-column intercept: refuse",
						"table", key,
						"error", err,
					)
					// Rewind the cache so a retry replays the same
					// boundary from the same pre-state.
					cache[key] = pre
					if preIsSeed {
						seedSourced[key] = true
					}
					wrapped := fmt.Errorf("pipeline: forward schema add-column: %w", err)
					errStore.Store(&wrapped)
					return
				}
				// An ADD COLUMN owes the rows the target already held a
				// backfill from the source; it is on the ledger from here.
				owed, err := planBoundaryBackfill(ctx, deps.backfill, key, pre, post, snap, deps.hint)
				if err != nil {
					wrapped := fmt.Errorf("pipeline: forward schema add-column: %w", err)
					errStore.Store(&wrapped)
					return
				}
				// Forward the snapshot to the applier so the
				// ADR-0049 schema-history row still records the
				// version on the same tx as the position write.
				if !forwardChange(ctx, out, c) {
					return
				}
				// Then run the backfill — after the snapshot, so the
				// applier has dropped its pre-ALTER view of the table,
				// and ahead of every change that follows.
				if err := owed.run(ctx, out); err != nil {
					slog.ErrorContext(
						ctx, "forward-add-column intercept: added-column backfill failed",
						"table", key,
						"error", err,
					)
					wrapped := fmt.Errorf("pipeline: forward schema add-column: %w", err)
					errStore.Store(&wrapped)
					return
				}
				watermark.await(owed)
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// forwardChange writes c to out, returning false if the context
// terminates first. Centralized so the per-branch select pattern
// doesn't repeat.
func forwardChange(ctx context.Context, out chan<- ir.Change, c ir.Change) bool {
	select {
	case out <- c:
		return true
	case <-ctx.Done():
		return false
	}
}

// routeForwardBoundary classifies the (pre, post) delta and dispatches
// the recognized shapes. Returns nil on success (ALTER landed and
// optional backfill emitted), a wrapped error on any refuse-loudly
// case.
//
// pre and post are both in COMPARISON form (normalized by the source
// engine's Bug 84/86 lens — see schemaForwardDeps.normalizer); snap
// carries the RAW snapshot for the apply paths, whose retarget step
// re-resolves every type-bearing payload by name against snap.IR so
// the DDL materialized on the target keeps the wire's full fidelity.
func routeForwardBoundary(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	pre, post *ir.Table,
	snap ir.SchemaSnapshot,
	preIsSeed bool,
) error {
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		// ADR-0060 (F11) — surface the structured per-table drift
		// alongside the classify error so the operator sees WHAT
		// changed even on multi-shape / unrecognized combos. The
		// classify error itself names the class counts; the drift
		// rendering names the specific columns / indexes / checks
		// (rendered from the same comparison-form pair the classifier
		// actually saw).
		return fmt.Errorf("classify shape on %q: %w.%s %s",
			tableName, err, renderDriftForRefusal(pre, post),
			forwardRecoveryHint(tableName))
	}
	// ADR-0091 §3 seed-guard: never forward a destructive/mutating shape
	// classified against a cold-start seed pre-state — the seed's
	// fidelity differs from the CDC projection, so such a delta may be a
	// phantom (e.g. a residual type asymmetry surfaces as a phantom ALTER
	// that is an idempotent no-op same-engine but a real, harmful MODIFY
	// cross-engine). Skip it as a no-op (the column/index stays on the
	// target, a safe non-destructive outcome); the next CDC→CDC boundary
	// is unguarded. ADD / CREATE INDEX / ADD CHECK are non-destructive
	// and a phantom of them cannot arise against the seed (the CDC
	// projection is a subset of the seed's fidelity), so they pass.
	if preIsSeed && shapeIsDestructiveOrMutating(shape.Kind) {
		slog.InfoContext(
			ctx,
			"schema-forward: skipping a destructive/mutating shape at the "+
				"first post-cold-start boundary (classified against the "+
				"cold-start seed, whose fidelity differs from the CDC "+
				"projection — not forwarding to avoid a phantom; ADR-0091 §3)",
			"table", tableName,
			"shape", shape.Kind.String(),
		)
		return nil
	}
	// SLM-1: the session-zone cast door on the single-stream path, the
	// sibling of the one in [BoundaryRouter.RouteBoundary]. The reader
	// refuses this class first whenever it has a prior shape; this is the
	// pipeline's own door for the boundary where it did not. Placed AFTER
	// the seed guard on purpose: against a seed the shape is already
	// skipped, and a seed-vs-CDC phantom (an operator's `--type-override`
	// onto the zone sibling) must not be reported as a swap they never made.
	if err := refuseSessionZoneSwap(tableName, shape, forwardRecoveryHint(tableName)); err != nil {
		return err
	}
	switch shape.Kind {
	case ShapeKindNone:
		// No structural delta (incl. a pure column reorder — sluice
		// decodes rows by name, so the target's physical column order
		// is irrelevant; ADR-0091 §4). Forward the snapshot so the
		// ADR-0049 schema-history row records; no DDL.
		return nil
	case ShapeKindAddColumn:
		// ADD COLUMN keeps its dedicated path: computed-DEFAULT
		// volatility refusal (ADR-0058 §2a) + optional source-side
		// backfill of already-shipped rows.
		return applyAddColumnForward(ctx, deps, tableName, snap, shape)
	case ShapeKindRenameColumn:
		// ADR-0091 §3 + F7b — a rename and a drop+add-of-same-type are
		// indistinguishable from the IR delta ALONE, and guessing wrong
		// silently drops the column's data on the target. The ONLY safe
		// disambiguation is a stable column identity that survives a
		// rename: PG's pg_attribute.attnum (carried as ir.Column.StableID
		// by the PG CDC reader). Same non-zero StableID on before & after
		// = PROVEN rename → forward (data preserved). Anything else
		// (different attnum = real drop+add; or StableID==0 = MySQL / no
		// stable id = unprovable) → refuse loudly. The proof is
		// definitive, so this can only ever REFUSE safely, never
		// mis-forward (ADR-0091 §3, F7b).
		return routeRenameColumn(ctx, deps, tableName, snap, shape, pre)
	case ShapeKindDropColumn,
		ShapeKindCreateIndex,
		ShapeKindDropIndex,
		ShapeKindAlterColumnType,
		ShapeKindAlterColumnNullability,
		ShapeKindAddCheck,
		ShapeKindDropCheck,
		ShapeKindModifyCheck:
		// Every unambiguous shape forwards via the same proven
		// per-shape dispatch Shape A uses (ADR-0091 §1).
		return applyShapeForward(ctx, deps, tableName, snap, shape)
	case ShapeKindUnrecognized:
		// Multi-shape combo — already returned as a classify error
		// above; this arm is defensive.
		return refuseShapeMultiCombo(tableName, shape, pre, post)
	}
	return fmt.Errorf("unrecognized shape kind %v on %q.%s %s",
		shape.Kind, tableName, renderDriftForRefusal(pre, post),
		forwardRecoveryHint(tableName))
}

// renderDriftForRefusal computes the [irdiff.SchemaDriftReport] for the
// (pre, post) pair and renders it for inclusion in a refuse-loudly
// error message. Returns the empty string when there are no drift
// entries to surface (caller's outer message reads naturally without
// a blank section).
//
// ADR-0060 — this is the F11 "tell the operator what changed" half
// of the refuse-loudly contract. The classify error names the SHAPE
// (multi-shape combo, drop-column, etc.); this function names the
// SPECIFIC columns / indexes / constraints. Both go into the same
// error so the operator gets a single grep-friendly message.
func renderDriftForRefusal(pre, post *ir.Table) string {
	report := irdiff.TableDrift(pre, post)
	rendered := RenderSchemaDriftReport(report)
	if rendered == "" {
		return ""
	}
	// Lead with " observed drift:" so the rendered block reads as a
	// natural continuation of the outer error sentence.
	return " observed drift:" + rendered + "\n"
}

// routeRenameColumn decides a RENAME COLUMN boundary using the
// attnum-proof (ADR-0091 F7b). The classifier already established the
// drop+add-of-same-type pattern and carried the real per-column
// StableIDs out on shape.RenamedColumnBefore/After. A rename PROVES
// itself when both sides carry the SAME non-zero StableID
// (pg_attribute.attnum is stable across RENAME COLUMN); a different
// attnum is a genuine drop+add, and a zero attnum (MySQL source, or an
// unresolved PG lookup) is unprovable. Only a proven rename forwards —
// via the same per-shape applyShapeForward dispatch (retarget + Schema
// scrub) every other forwarded shape uses, reaching
// ir.ShapeDeltaApplier.AlterRenameColumn on the target. Everything else
// refuses loudly. Because the proof is definitive, a bug here can only
// ever REFUSE (safe), never mis-forward a real drop+add as a rename.
func routeRenameColumn(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	shape Shape,
	pre *ir.Table,
) error {
	var beforeID, afterID int
	if shape.RenamedColumnBefore != nil {
		beforeID = shape.RenamedColumnBefore.StableID
	}
	if shape.RenamedColumnAfter != nil {
		afterID = shape.RenamedColumnAfter.StableID
	}
	proven := beforeID != 0 && beforeID == afterID
	// DEBUG diagnostic (gated behind --log-level=debug): logs the
	// captured attnums + verdict at the decision point so an operator can
	// confirm same-attnum (rename→forward) vs different-attnum
	// (drop+add→refuse) without re-deriving it. Not INFO — this fires on
	// every rename boundary.
	slog.DebugContext(
		ctx, "schema-forward: rename-column intercept decision (stable-id proof)",
		"table", tableName,
		"before_name", renamedName(shape.RenamedColumnBefore),
		"after_name", renamedName(shape.RenamedColumnAfter),
		"before_stable_id", beforeID,
		"after_stable_id", afterID,
		"proven", proven,
	)
	if !proven {
		return refuseRenameAmbiguous(tableName, shape, pre, snap.IR)
	}
	slog.InfoContext(
		ctx, "schema-forward: RENAME COLUMN proven via stable id (PG attnum); forwarding",
		"table", tableName,
		"before", renamedName(shape.RenamedColumnBefore),
		"after", renamedName(shape.RenamedColumnAfter),
		"stable_id", beforeID,
	)
	return applyShapeForward(ctx, deps, tableName, snap, shape)
}

// renamedName returns a column's Name for log lines, or "?" when the
// column pointer is nil.
func renamedName(c *ir.Column) string {
	if c == nil {
		return "?"
	}
	return c.Name
}

// refuseRenameAmbiguous is the operator-actionable refusal for a
// RENAME COLUMN boundary (ADR-0091 §3). A rename and a drop+add of a
// same-type column are indistinguishable from the IR delta; forwarding
// the wrong guess silently drops the renamed column's data on the
// target (or leaves stale data under the new name). Refuse loudly on
// both engines until F7b adds PG attnum-proven rename detection. Names
// the table, the inferred old→new pair, the per-change drift
// (ADR-0060 / F11), and the drained-recovery hint.
func refuseRenameAmbiguous(tableName string, shape Shape, pre, post *ir.Table) error {
	oldName, newName := "?", "?"
	if shape.RenamedColumnBefore != nil {
		oldName = shape.RenamedColumnBefore.Name
	}
	if shape.RenamedColumnAfter != nil {
		newName = shape.RenamedColumnAfter.Name
	}
	return fmt.Errorf(
		"RENAME COLUMN %q→%q on %q cannot be auto-forwarded: a rename is "+
			"indistinguishable from a drop+add of a same-type column from the "+
			"replication stream alone, and forwarding the wrong guess risks "+
			"silently dropping the column's data on the target (ADR-0091 §3). "+
			"Rename on the target manually via the drained model.%s %s",
		oldName, newName, tableName, renderDriftForRefusal(pre, post),
		forwardRecoveryHint(tableName),
	)
}

// refuseShapeMultiCombo is the refusal for a multi-shape combo boundary
// (more than one structural change in one boundary; ShapeKindUnrecognized).
// ClassifyShape returns this as an error that routeForwardBoundary
// surfaces before the dispatch switch, so this is a defensive arm.
func refuseShapeMultiCombo(tableName string, shape Shape, pre, post *ir.Table) error {
	return fmt.Errorf(
		"shape %s on %q is a multi-shape combo that cannot be unambiguously "+
			"ordered from the replication stream (ADR-0091 §2).%s %s",
		shape.Kind, tableName, renderDriftForRefusal(pre, post),
		forwardRecoveryHint(tableName),
	)
}

// forwardRecoveryHint is the single-stream variant of
// [RecoveryHint] for ADR-0058 refusals. The Shape A version mentions
// "every shard" and `--no-coordinate-live-ddl`, neither of which
// applies to a single-stream (non-shard) deployment. This variant
// drops the multi-shard language and names the drop-flag recovery as
// the safe path.
func forwardRecoveryHint(tableName string) string {
	return fmt.Sprintf(
		"recovery: drained model — run 'sluice sync stop --wait', "+
			"then apply the schema change against %q yourself (sluice has no "+
			"schema-migrate command; on PlanetScale, 'sluice deploy-ddl' ships "+
			"one statement safely), then resume by re-running 'sluice sync start' with "+
			"the SAME --stream-id (there is no resume flag; a restart picks up "+
			"the persisted position and skips the snapshot + bulk-copy phase). "+
			"Set --schema-changes=refuse to keep the drained model as the "+
			"default for any subsequent source DDL.",
		tableName,
	)
}

// shapeIsDestructiveOrMutating reports whether a shape removes or
// rewrites existing target state (vs. purely adding new state). These
// are the shapes the ADR-0091 §3 seed-guard refuses to forward against
// a cold-start seed pre, because a phantom of one (from seed-vs-CDC
// fidelity asymmetry) would be destructive. ADD COLUMN / CREATE INDEX /
// ADD CHECK are additive and excluded.
//
// RENAME COLUMN (F7b) is included: against the cold-start SEED there is
// no attnum on the seed side (the SchemaReader leaves StableID=0), so the
// rename can never be PROVEN at the first boundary and forwarding the
// drop+add guess would risk silent data loss. Skipping it at the seed
// boundary (no-op — the column keeps its old name on the target, a safe
// non-destructive divergence) means a real PG rename only forwards on a
// genuine CDC→CDC boundary where both sides carry attnum. For PG the
// first-touch RelationMessage primes the cache before any DDL, so a real
// rename is always a CDC→CDC boundary.
func shapeIsDestructiveOrMutating(kind ShapeKind) bool {
	switch kind {
	case ShapeKindDropColumn,
		ShapeKindDropIndex,
		ShapeKindDropCheck,
		ShapeKindAlterColumnType,
		ShapeKindAlterColumnNullability,
		ShapeKindModifyCheck,
		ShapeKindRenameColumn:
		return true
	default:
		return false
	}
}

// applyShapeForward forwards a single unambiguous schema-change shape
// (DROP / ALTER TYPE / NULLABILITY / CREATE/DROP INDEX / CHECK, and a
// PG-attnum-PROVEN RENAME COLUMN — F7b) to the target via the shared
// [applyShapeDelta] dispatch — the same per-shape apply path Shape A's
// boundary router uses (ADR-0091 §1). Before dispatch the post IR is
// retargeted to the target dialect and its Schema field scrubbed
// (ADR-0091 §5): the CDC-emitted IR carries the SOURCE engine's column
// types and database name, neither of which the target SchemaWriter can
// consume directly.
//
// ADD COLUMN does NOT route here — it has its own volatility/backfill
// path. RENAME routes here only AFTER routeRenameColumn proves it via
// stable id (an unproven rename refuses; ADR-0091 §3, F7b); the rename
// arm of [applyShapeDelta] consumes only the before/after column NAMES,
// so the retargeted post table suffices for qualifyTable.
func applyShapeForward(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	shape Shape,
) error {
	rtTable, rtShape := retargetShapeForTarget(
		snap.IR, shape, deps.sourceEngineName, deps.targetEngineName,
	)
	if err := applyShapeDelta(ctx, deps.applier, rtTable, rtShape); err != nil {
		return fmt.Errorf("apply %s on %q: %w. %s",
			shape.Kind, tableName, err, forwardRecoveryHint(tableName))
	}
	slog.InfoContext(
		ctx, "schema-forward: target DDL applied",
		"table", tableName,
		"shape", shape.Kind.String(),
	)
	return nil
}

// retargetShapeForTarget retargets the post table to the target
// engine's dialect (via [translate.RetargetForEngine]) and re-resolves
// the shape's type-bearing payloads — AddedColumns, AlteredColumn,
// AddedChecks, ModifiedCheckAfter — against the retargeted table by
// name, so the applier emits target-dialect column/constraint
// definitions. Name-only payloads (DroppedColumns, dropped/created
// indexes, dropped/modified-before checks) keep their original pointers
// — the applier consumes only their names. The returned table has its
// Schema scrubbed (Bug 89 fix generalized; ADR-0091 §5).
//
// Same-engine pairs are a [translate.RetargetForEngine] pass-through,
// so the re-resolution is a cheap by-name lookup that returns the
// identical column definitions.
func retargetShapeForTarget(post *ir.Table, shape Shape, sourceEngine, targetEngine string) (*ir.Table, Shape) {
	rt := retargetTableScrub(post, sourceEngine, targetEngine)

	colByName := make(map[string]*ir.Column, len(rt.Columns))
	for _, c := range rt.Columns {
		if c != nil {
			colByName[c.Name] = c
		}
	}
	checkByName := make(map[string]*ir.CheckConstraint, len(rt.CheckConstraints))
	for _, ck := range rt.CheckConstraints {
		if ck != nil {
			checkByName[ck.Name] = ck
		}
	}

	out := shape // value copy; pointer/slice fields reassigned below
	if len(shape.AddedColumns) > 0 {
		out.AddedColumns = resolveColumnsByName(shape.AddedColumns, colByName)
	}
	if shape.AlteredColumn != nil {
		if c, ok := colByName[shape.AlteredColumn.Name]; ok {
			out.AlteredColumn = c
		}
	}
	if len(shape.AddedChecks) > 0 {
		out.AddedChecks = resolveChecksByName(shape.AddedChecks, checkByName)
	}
	if shape.ModifiedCheckAfter != nil {
		if ck, ok := checkByName[shape.ModifiedCheckAfter.Name]; ok {
			out.ModifiedCheckAfter = ck
		}
	}
	return rt, out
}

// retargetTableScrub runs [translate.RetargetForEngine] on a single-
// table schema and returns the retargeted table with its Schema field
// scrubbed so the target SchemaWriter's qualifyTable falls back to its
// own DSN-bound database. Mirrors the retarget+scrub in
// [retargetAddedColumns]; factored so every shape's forward path shares
// it (ADR-0091 §5) — the single-stream forwarder and, since Bug 262,
// the Shape A boundary router.
//
// The scrub is applied to a COPY: a same-engine pair is a retarget
// pass-through that hands back the caller's own table, and post is the
// intercept's cached pre-state for the next boundary and (on the Shape
// A path) the very IR the downstream ADR-0049 schema-history row
// serialises. Scrubbing that in place would persist a namespace-less
// snapshot for the same-engine pairs only.
func retargetTableScrub(post *ir.Table, sourceEngine, targetEngine string) *ir.Table {
	retargeted := translate.RetargetForEngine(
		&ir.Schema{Tables: []*ir.Table{post}},
		sourceEngine, targetEngine,
	)
	rt := post
	if len(retargeted.Tables) > 0 {
		rt = retargeted.Tables[0]
	}
	scrubbed := *rt
	scrubbed.Schema = ""
	return &scrubbed
}

// resolveColumnsByName maps each column in src to the same-named column
// in byName, falling back to the original pointer when absent
// (defensive — added/altered columns exist in the post table by
// construction).
func resolveColumnsByName(src []*ir.Column, byName map[string]*ir.Column) []*ir.Column {
	out := make([]*ir.Column, len(src))
	for i, c := range src {
		if c == nil {
			continue
		}
		if rt, ok := byName[c.Name]; ok {
			out[i] = rt
		} else {
			out[i] = c
		}
	}
	return out
}

// resolveChecksByName maps each CHECK constraint in src to the
// same-named constraint in byName, falling back to the original pointer
// when absent.
func resolveChecksByName(src []*ir.CheckConstraint, byName map[string]*ir.CheckConstraint) []*ir.CheckConstraint {
	out := make([]*ir.CheckConstraint, len(src))
	for i, ck := range src {
		if ck == nil {
			continue
		}
		if rt, ok := byName[ck.Name]; ok {
			out[i] = rt
		} else {
			out[i] = ck
		}
	}
	return out
}

// applyAddColumnForward executes the ADD COLUMN forward — the load-
// bearing branch of [routeForwardBoundary]. Steps:
//
//  1. Refuse loudly on any column with [ir.DefaultExpression]
//     (computed default; ADR-0058 §2a).
//  2. Retarget the post IR's added columns to the target engine's
//     dialect via [translate.RetargetForEngine] (cross-engine type
//     rewrite — same path the broker + chain-restore use).
//  3. Call deps.applier.AlterAddColumn(ctx, retargetedTable,
//     retargetedAdded). Idempotent via the engine's IF NOT EXISTS.
//
// The added-column backfill is NOT run here: it follows the boundary's
// snapshot downstream ([boundaryBackfill]), so the applier has
// refreshed its view of the table before the first backfilled value
// reaches it.
func applyAddColumnForward(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	shape Shape,
) error {
	if err := refuseComputedDefaults(ctx, deps, tableName, snap, shape.AddedColumns); err != nil {
		return err
	}
	post, err := carrySourceDefaults(ctx, deps, tableName, snap, shape.AddedColumns)
	if err != nil {
		return err
	}
	retargetedTable, retargetedAdded := retargetAddedColumns(
		post, shape.AddedColumns,
		deps.sourceEngineName, deps.targetEngineName,
	)
	// The forwarded column gets the same up-front translation notices a
	// cold-start copy of it would have (an unconstrained PG numeric landing
	// as MySQL DECIMAL(65,30), a wide varchar down-mapped to TEXT, …): the
	// type policy is shared with migrate, so the warning about it is too.
	emitCrossEngineTranslationNotices(ctx, addedColumnsSchema(post, shape.AddedColumns),
		deps.sourceEngineName, deps.targetEngineName, "sync schema-forward")
	// The backfill this ALTER makes owed is recorded on the target first, so
	// a process killed before the backfill settles restarts refusing
	// (schema_forward_backfill_ledger.go, "The write-ahead record").
	if err := deps.backfill.writeAhead(ctx, tableName, columnNames(shape.AddedColumns)); err != nil {
		return fmt.Errorf("%w. %s", err, forwardRecoveryHint(tableName))
	}
	if err := deps.applier.AlterAddColumn(ctx, retargetedTable, retargetedAdded); err != nil {
		return fmt.Errorf("alter add column on %q: %w. %s",
			tableName, err, forwardRecoveryHint(tableName))
	}
	slog.InfoContext(
		ctx, "forward-add-column: target ALTER applied",
		"table", tableName,
		"added_columns", columnNames(retargetedAdded),
	)
	return nil
}

// backfillAddedColumns runs the added-column backfill for one forwarded
// ADD COLUMN boundary — or, when the operator opted out (bf nil), says what
// that leaves behind. Reached through [boundaryBackfill.run].
//
// The backfill reads with the SOURCE IR (snap.IR), not the retargeted IR —
// the BatchedRowReader is engine-specific (the PG reader expects PG types
// in its query). The applier consumes the Updates like any CDC event, so
// cross-engine value translation happens in its existing per-event path.
func backfillAddedColumns(
	ctx context.Context,
	bf *schemaForwardBackfill,
	tableName string,
	snap ir.SchemaSnapshot,
	added []*ir.Column,
	out chan<- ir.Change,
) error {
	if bf == nil {
		slog.WarnContext(
			ctx,
			"forward-add-column: backfill suppressed (--no-backfill-added-column) — the rows the target already "+
				"held keep the target's fill for the added column (the DEFAULT the forward carried, NULL if none), "+
				"not the values the source holds for them; a DEFAULT dropped or changed on the source after the "+
				"ADD COLUMN, or a non-constant one, leaves them different. Rows changed after this point are unaffected",
			"table", tableName,
			"added_columns", columnNames(added),
		)
		return nil
	}
	if err := runBackfillForAddedColumn(ctx, bf, snap, added, out); err != nil {
		return fmt.Errorf("backfill on %q: %w", tableName, err)
	}
	return nil
}

// addedColumnsSchema is the one-table schema of just the forwarded added
// columns, resolved by name against post (the raw source IR the ALTER is
// built from — shape.AddedColumns come off the comparison lens), for the
// cross-engine translation-notice scanners.
func addedColumnsSchema(post *ir.Table, added []*ir.Column) *ir.Schema {
	if post == nil {
		return nil
	}
	names := make(map[string]bool, len(added))
	for _, c := range added {
		if c != nil {
			names[c.Name] = true
		}
	}
	tbl := &ir.Table{Schema: post.Schema, Name: post.Name}
	for _, c := range post.Columns {
		if c != nil && names[c.Name] {
			tbl.Columns = append(tbl.Columns, c)
		}
	}
	return &ir.Schema{Tables: []*ir.Table{tbl}}
}

// refuseComputedDefaults walks the added columns and returns an
// operator-actionable error if any column's source DEFAULT expression
// is non-deterministic / stateful / session-dependent. ADR-0058 §2a —
// such DEFAULTs evaluate in the target's session at ALTER time, not
// the source's per-row insert context; silent forwarding would
// diverge. Bug 90 (v0.79.1) is exactly this gap on the live CDC path.
//
// Probe order:
//
//   - If the column's [ir.Column.Default] is already populated as a
//     non-nil [ir.DefaultValue] (i.e. the test harness wired the IR
//     directly, or a future CDC reader carries the field), classify
//     the in-band value via [migcore.ClassifyDefaultValueVolatility].
//   - Otherwise (the production CDC case — pgoutput's RelationMessage
//     and MySQL's TableMapEvent both drop the DEFAULT), the
//     deps.defaultProber is called to surface the source's canonical
//     Default text. The probe is a single targeted query against the
//     source SchemaReader; cost is one round-trip per ADD COLUMN
//     forward (rare event).
//   - If the prober is also nil (a unit test that didn't wire one),
//     pass through — the test's IR is the ground truth.
//   - If the prober returns an error, refuse loudly with the probe
//     error wrapped (refuse-on-uncertainty).
//
// Detection is text-based — see [migcore.ClassifyDefaultVolatility]. The
// allowlist + denylist is the documented examples from ADR-0058 §2a
// (NOW, nextval, random) plus the obvious cousins across PG and
// MySQL. Unknown function names trigger refusal (better safe than
// silent corruption).
func refuseComputedDefaults(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	cols []*ir.Column,
) error {
	for _, c := range cols {
		if c == nil {
			continue
		}
		def := c.Default
		// CDC projection drops the DEFAULT — probe the source for
		// the canonical text when the in-band value is nil.
		if def == nil && deps.defaultProber != nil {
			probed, err := deps.defaultProber(ctx, snap.Schema, snap.Table, c.Name)
			if err != nil {
				// Refuse-on-uncertainty: a probe error can't be
				// distinguished from "the column does have a
				// volatile default but we couldn't read it." Safe
				// path is to refuse.
				return fmt.Errorf(
					"probe DEFAULT for ADD COLUMN %q on %q: %w "+
						"(refusing on uncertainty; ADR-0058 §2a). %s",
					c.Name, tableName, err, deps.hint(tableName),
				)
			}
			def = probed
		}
		safe, reason := migcore.ClassifyDefaultValueVolatility(def)
		if !safe {
			exprText := defaultExpressionText(def)
			return fmt.Errorf(
				"ADD COLUMN %q on %q has a computed DEFAULT expression "+
					"%q which %s — target-session evaluation diverges from "+
					"source per-row insert (ADR-0058 §2a). %s",
				c.Name, tableName, exprText, reason,
				deps.hint(tableName),
			)
		}
		// LOUD-NOT-SILENT (roadmap item 78b). The in-band CDC IR carried no
		// DEFAULT but the source does have one: the emitted ADD COLUMN will
		// therefore omit it, so the target's PRE-EXISTING rows land NULL while
		// the source's hold the default — and no row event will ever reconcile
		// them, because adding a column with a default writes no per-row
		// changes on either engine. Rows changed AFTER the ALTER are fine (CDC
		// carries explicit values), so the divergence is confined to rows that
		// existed at the moment of the ALTER, is invisible to a row-count
		// check, and surfaces only under a value-comparing verify. That
		// combination — real, bounded, and silent — is what this WARN exists
		// to break.
		//
		// Gated on `c.Default == nil && def != DefaultNone`: the in-band value
		// being absent is what says the projection dropped it, and the probe
		// finding a real default is what says something is actually lost. A
		// column with genuinely no default reaches neither condition and stays
		// quiet.
		// Reached only when no carrier is wired (a test harness); with one,
		// [carrySourceDefaults] puts the default on the forwarded column.
		if c.Default == nil && !isDefaultNone(def) && deps.defaultCarrier == nil {
			slog.WarnContext(
				ctx,
				"forward-add-column: the source's column DEFAULT cannot be carried through this source's change stream — "+
					"the target column is being added WITHOUT it, so rows that already exist on the target will hold NULL "+
					"where the source holds the default value. Rows changed after this point are unaffected. "+
					"Remedy: apply the default on the target and backfill the existing rows "+
					"(ALTER TABLE ... ALTER COLUMN ... SET DEFAULT, then UPDATE ... WHERE <col> IS NULL), "+
					"or leave the added-column backfill on (it is unless --no-backfill-added-column) so sluice populates them from source values",
				"table", tableName,
				"column", c.Name,
				"source_default", defaultExpressionText(def),
			)
		}
	}
	return nil
}

// isDefaultNone reports whether d is the explicit "this column has no
// DEFAULT" value. A nil DefaultValue is NOT the same thing — nil means
// "unknown / the projection did not carry one", which is precisely the
// case the item-78b WARN distinguishes.
func isDefaultNone(d ir.DefaultValue) bool {
	if d == nil {
		return false
	}
	_, none := d.(ir.DefaultNone)
	return none
}

// defaultExpressionText returns the human-readable expression text for
// a [ir.DefaultValue] — for inclusion in the refuse-loudly message.
// Empty string for [ir.DefaultNone] / nil.
func defaultExpressionText(d ir.DefaultValue) string {
	switch v := d.(type) {
	case ir.DefaultExpression:
		return v.Expr
	case ir.DefaultLiteral:
		return v.Value
	default:
		return ""
	}
}

// retargetAddedColumns wraps the post IR + the slice of added columns
// in a single-table schema, runs [translate.RetargetForEngine] to
// rewrite types for the target dialect, and returns the
// post-retarget table + the slice of added columns located in that
// table. Same call pattern as broker.go:997 and chain_restore.go:574.
//
// Same-engine pairs are a no-op pass-through inside RetargetForEngine.
//
// The returned table's Schema field is cleared so the target
// SchemaWriter's `qualifyTable` falls back to its own bound database
// (DSN-derived). The CDC-emitted source IR carries the SOURCE's DB
// name (e.g. "source_db") which doesn't exist on the target, and a
// raw forwarded ALTER would fail with "schema does not exist" (PG)
// or "Unknown database" (MySQL). The chain-restore caller passes
// manifest-derived tables with Schema unset (see chain_restore_cross_test.go)
// so it never hit this problem; the live CDC caller has to scrub the
// field here.
func retargetAddedColumns(post *ir.Table, added []*ir.Column, sourceEngine, targetEngine string) (*ir.Table, []*ir.Column) {
	retargetedSchema := translate.RetargetForEngine(
		&ir.Schema{Tables: []*ir.Table{post}},
		sourceEngine, targetEngine,
	)
	if len(retargetedSchema.Tables) == 0 {
		scrubbed := *post
		scrubbed.Schema = ""
		return &scrubbed, added
	}
	retargetedTable := retargetedSchema.Tables[0]
	retargetedTable.Schema = ""
	// Find the retargeted columns matching the added names.
	addedNames := make(map[string]struct{}, len(added))
	for _, c := range added {
		if c != nil {
			addedNames[c.Name] = struct{}{}
		}
	}
	retargetedAdded := make([]*ir.Column, 0, len(added))
	for _, c := range retargetedTable.Columns {
		if _, ok := addedNames[c.Name]; ok {
			retargetedAdded = append(retargetedAdded, c)
		}
	}
	return retargetedTable, retargetedAdded
}

// columnNames returns the Name slice for cols. For log lines.
func columnNames(cols []*ir.Column) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if c != nil {
			out = append(out, c.Name)
		}
	}
	return out
}

// carrySourceDefaults returns the table the forwarded ADD COLUMN is built
// from, with each added column whose in-band DEFAULT the change stream did
// not carry given the source's own DEFAULT (via
// [schemaForwardDeps.defaultCarrier]). The single-stream face of
// [carryAddedColumnDefaults]; see there for why it is load-bearing.
func carrySourceDefaults(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	added []*ir.Column,
) (*ir.Table, error) {
	post, _, err := carryAddedColumnDefaults(ctx, deps, tableName, snap, added)
	return post, err
}

// carryAddedColumnDefaults returns a copy of snap.IR in which each added
// column whose in-band DEFAULT the change stream did not carry holds the
// source's own DEFAULT (read through [schemaForwardDeps.defaultCarrier]
// for the source table snap.Schema / snap.Table). It also returns the
// columns that received a real default (not [ir.DefaultNone]); the Shape A
// boundary router folds those into its lease checksum
// ([leaseDDLTextWithCarriedDefaults]).
//
// Why it is load-bearing: when the target runs `ADD COLUMN … DEFAULT d`, it
// fills every row it ALREADY holds with d, and the source's own fill of its
// existing rows writes no row event, so nothing downstream ever corrects
// the target's copy. A forward that omitted the default therefore left
// every pre-existing target row NULL where the source holds d — for
// NOT NULL columns too — at exit 0 with only a WARN. Measured by
// TestStreamer_AddColumnForward_PreexistingRowDefaults_* on PG→PG,
// PG→MySQL and VStream→PG, whose change streams carry no DEFAULT at all,
// and on the Shape A router by its ShapeAPostgresToPostgres lane.
//
// Two callers, one per ADD COLUMN emission that reads a live source:
// the single-stream forwarder ([applyAddColumnForward], via
// [carrySourceDefaults]) and the Shape A boundary router
// ([BoundaryRouter.RouteBoundary]). The third ADD COLUMN emission, backup-
// chain replay, has no live source to read (GC-36 (2)).
//
// The value carried is the SchemaReader's translated IR default, so the
// target's emitter treats it exactly as it treats the same column on a
// cold-start migrate (a cross-engine default is translated, or dropped
// or refused by the same rules). A column that already carries an in-band
// DEFAULT (the MySQL binlog projection) is left alone, and so is snap.IR:
// both callers hand in a table they also cache as the next boundary's
// pre-state, so the result is a copy. A nil carrier (a test harness with
// no source reader) passes snap.IR through. A read failure refuses —
// forwarding the column without its default is exactly the silent
// outcome this prevents.
func carryAddedColumnDefaults(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	added []*ir.Column,
) (*ir.Table, []*ir.Column, error) {
	if deps.defaultCarrier == nil || snap.IR == nil {
		return snap.IR, nil, nil
	}
	missing := map[string]bool{}
	for _, c := range added {
		if c != nil && c.Default == nil {
			missing[c.Name] = true
		}
	}
	if len(missing) == 0 {
		return snap.IR, nil, nil
	}
	out := *snap.IR
	out.Columns = append([]*ir.Column(nil), snap.IR.Columns...)
	var carriedCols []*ir.Column
	for i, c := range out.Columns {
		if c == nil || !missing[c.Name] || c.Default != nil {
			continue
		}
		d, err := deps.defaultCarrier(ctx, snap.Schema, snap.Table, c.Name)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"read the source DEFAULT for ADD COLUMN %q on %q: %w (refusing: forwarding the column without its "+
					"default would leave every row already on the target without the value the source filled in). %s",
				c.Name, tableName, err, deps.hint(tableName),
			)
		}
		if d == nil {
			continue
		}
		carried := *c
		carried.Default = d
		out.Columns[i] = &carried
		if !isDefaultNone(d) {
			carriedCols = append(carriedCols, &carried)
		}
	}
	return &out, carriedCols, nil
}
