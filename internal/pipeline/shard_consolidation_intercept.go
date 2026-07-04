// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0054 Shape A Phase 2d — SchemaSnapshot → BoundaryRouter intercept.
//
// The Streamer's CDC change pipeline forwards a mixed stream of
// ir.Insert/Update/Delete/Truncate/TxBegin/TxCommit/SchemaSnapshot to
// the applier. When live cross-shard DDL coordination is engaged
// (Phase 2b's [Streamer.engageShardCoordination]), this file's
// intercept wraps the change channel to route each ir.SchemaSnapshot
// through a [BoundaryRouter] before forwarding it to the applier:
//
//   1. The intercept maintains a per-table latest-IR cache (the "pre"
//      schema for the next boundary's classifier input). The cache is
//      pre-seeded at cold-start completion with the pre-Shape-A-rewrite
//      source IR per filtered table (Bug 83 fix — see below); from then
//      on each SchemaSnapshot computes shape via (cached → snap.IR).
//   2. For each SchemaSnapshot whose table already has a cache entry
//      (either from the cold-start seed or a prior in-stream snapshot),
//      the intercept calls [BoundaryRouter.RouteBoundary] with the
//      (pre, post) pair.
//   3. On success the snapshot's IR replaces the cache entry, and the
//      SchemaSnapshot continues to the applier so the ADR-0049
//      schema-history write still records the version downstream.
//   4. On error (refused shape / probe Inconsistent / peer checksum
//      mismatch) the intercept closes the out-channel and stores the
//      error for the streamer to surface via runOnce's standard
//      dispatchErr classification.
//   5. If a CDC SchemaSnapshot arrives for a table that wasn't in the
//      cold-start seed (a NEW table appearing live), the intercept
//      treats the first such snapshot as the cache anchor (no
//      RouteBoundary call) and forwards it. This preserves the
//      pre-Bug-83 first-seen behaviour for the live-add-table path
//      while the Bug 83 fix governs the cold-start-tracked tables.
//
// Bug 83 (v0.73.0 hotfix): the previous "first SchemaSnapshot == cold-
// start anchor" rule was incorrect because the cold-start phase does
// not emit ir.SchemaSnapshot through this channel — only CDC readers
// do. If source DDL occurred between cold-start completion and the
// first CDC row event, the first snapshot reflected the POST-DDL
// source schema; caching it as the anchor meant the boundary never
// routed and the next row crashed the applier with column-does-not-
// exist. The cold-start seed pre-populates the cache with the pre-
// Shape-A-rewrite source IR (matching what CDC will later emit
// shape-wise — CDC emits SOURCE schema, which does not contain the
// discriminator column).
//
// The intercept is opt-in: nil router → pass-through (seed is
// dropped).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
)

// interceptSchemaSnapshotsForCoordination wraps changes with a
// SchemaSnapshot intercept that drives the BoundaryRouter. nil router
// is a no-op (returns in verbatim; seed is dropped — no router means
// no coordination means the seed is irrelevant). The errStore is the
// streamer's sourceErrFn-bound error sink; the intercept writes any
// RouteBoundary error there so the streamer's standard
// surfaceSourceError() path picks it up.
//
// seed is the ADR-0054 Bug 83 cold-start cache seed — one synthetic
// SchemaSnapshot per filtered table reflecting the pre-Shape-A-rewrite
// source IR (built by the streamer's coldStart before
// translate.InjectShardColumn runs). Seeds are NOT forwarded
// downstream — the applier already knows the target schema from
// cold-start's schema-apply phase, and forwarding a seed would write
// a redundant ADR-0049 schema-history row.
//
// normalizer is the source engine's optional
// [ir.CDCSchemaSnapshotNormalizer] (nil → identity). The Bug 84/86
// normalization is a comparison LENS and MUST be applied to BOTH sides
// of every classifier comparison: the seed is normalized at synthesis
// ([synthesizeColdStartSeedSnapshots]) and each incoming CDC snapshot
// is normalized here before it is classified or cached. Normalizing
// only the seed regressed when TRIAGE #3 changed the CDC projection's
// temporal representation — the collapsed seed no longer matched the
// raw post and every bare temporal column phantom-altered at the first
// boundary (the TypeFamilyMatrix extra_timestamp_nullable failure).
// The ORIGINAL snapshot is still what is forwarded downstream, so the
// ADR-0049 schema-history row records the faithful projection, and the
// lease's ddl_text/checksum stays derived from the raw IR (peer
// checksums compare raw-to-raw exactly as before).
func interceptSchemaSnapshotsForCoordination(
	ctx context.Context,
	in <-chan ir.Change,
	seed []ir.SchemaSnapshot,
	router *BoundaryRouter,
	normalizer ir.CDCSchemaSnapshotNormalizer,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	if router == nil {
		return in
	}
	out := make(chan ir.Change)
	cache := map[string]*ir.Table{}
	version := map[string]int64{}
	// Pre-populate the cache from the cold-start seed. This MUST happen
	// before the for-select loop drains `in` so the first CDC
	// SchemaSnapshot per seeded table is correctly classified as a
	// post-DDL boundary (rather than treated as the cold-start anchor
	// — the Bug 83 root cause). Key alignment between the seed and
	// the CDC-emitted SchemaSnapshot is handled by lookupSeedCache
	// below — see its docstring for the MySQL bare-name fallback case.
	for i := range seed {
		snap := seed[i]
		key := snap.QualifiedName()
		cache[key] = snap.IR
		version[key] = 1
		slog.DebugContext(
			ctx, "shard consolidation intercept: seeded from cold-start handoff",
			"table", key,
		)
	}
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				snap, isSnap := c.(ir.SchemaSnapshot)
				if !isSnap {
					select {
					case out <- c:
					case <-ctx.Done():
						return
					}
					continue
				}
				key := snap.QualifiedName()
				pre, hadPre, preKey := lookupSeedCache(cache, key, snap.Table)
				// Promote the seed entry to the qualified key when the
				// fallback hit (so subsequent CDC snapshots also resolve
				// against the qualified name; preKey != key means the
				// seed was stored under a different key — the bare-name
				// MySQL case).
				if hadPre && preKey != key {
					delete(cache, preKey)
					delete(version, preKey)
				}
				// Comparison form of the post-DDL IR: normalized with the
				// same lens the seed was (both sides of every classifier
				// comparison — see the function docstring). Cached so the
				// NEXT boundary's pre is already in comparison form.
				post := normalizeSnapshotForComparison(normalizer, snap.IR)
				cache[key] = post
				version[key]++
				if !hadPre {
					// First snapshot for this table — cold-start seed,
					// no DDL boundary to route. Forward as-is so the
					// applier records the initial schema-history row.
					slog.DebugContext(
						ctx, "shard consolidation intercept: seeded table cache",
						"table", key,
					)
					select {
					case out <- c:
					case <-ctx.Done():
						return
					}
					continue
				}
				// Subsequent snapshot — drive RouteBoundary against
				// (pre, post), both in comparison form. DDL text is the
				// deterministic IR-marshalled rendering of the RAW
				// post-IR schema (no raw source DDL is available
				// through the SchemaSnapshot path — DP-E's "shapes are
				// sluice's own structural categories" applies; raw
				// keeps peer checksums byte-compatible with the
				// pre-normalization lease rows).
				ddlText := deriveDDLText(snap.IR)
				// Pass the SchemaSnapshot's source-side Position as the
				// lease row's anchor — the v0.76.0 lease GC sweep (task
				// #21) compares it against every stream's persisted
				// position via the engine's PositionOrderer.
				if err := router.RouteBoundary(ctx, key, pre, post, ddlText, version[key], snap.Position); err != nil {
					slog.ErrorContext(
						ctx, "shard consolidation intercept: route boundary failed",
						"table", key,
						"error", err,
					)
					// Rewind the cache: the post-state didn't land,
					// so the next boundary still classifies from the
					// pre-state.
					cache[key] = pre
					version[key]--
					wrapped := fmt.Errorf("pipeline: shard consolidation: %w", err)
					errStore.Store(&wrapped)
					return
				}
				// Forward the snapshot to the applier so the
				// ADR-0049 schema-history write still records the
				// version (the existing per-engine SchemaSnapshot
				// dispatch path handles that under the same tx as the
				// position write).
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// synthesizeColdStartSeedSnapshots builds the cold-start cache seed
// for [interceptSchemaSnapshotsForCoordination]: one synthetic
// SchemaSnapshot per table in `schema`, with the table's current IR
// as the cached pre-state. Called by [Streamer.coldStart] BEFORE
// [translate.InjectShardColumn] runs so the IR matches what CDC
// readers will later emit (CDC emits the SOURCE's current schema,
// which doesn't carry the discriminator column).
//
// The Position is left zero — the seed isn't anchored to a CDC
// position and isn't forwarded downstream (the applier already knows
// the target schema from cold-start's schema-apply phase). The
// intercept consumes the seed purely to populate its per-table cache
// so the first real CDC SchemaSnapshot is classified as a true
// boundary (pre = seeded source IR, post = CDC snapshot IR).
//
// ADR-0054 Bug 83 fix (v0.73.1).
//
// ADR-0054 Bug 84 fix (v0.73.2): when sourceEngine implements
// [ir.CDCSchemaSnapshotNormalizer], the seed tables are normalised
// before being wrapped as SchemaSnapshots. Without this, PG sources
// that populate richer IR Type fields than pgoutput's RelationMessage
// projection (Integer.AutoIncrement for IDENTITY columns; Varchar /
// Char / Text Collation; Decimal.Unconstrained) would trigger a false
// `altered-col=true` in [ClassifyShape] on every existing column,
// combining with a legitimate ADD COLUMN into a multi-shape combo
// refusal. Engines without the normalizer interface (MySQL today) are
// passed through unchanged — their CDC projection already matches the
// SchemaReader's shape.
func synthesizeColdStartSeedSnapshots(schema *ir.Schema, sourceEngine ir.Engine) []ir.SchemaSnapshot {
	if schema == nil {
		return nil
	}
	normalizer, _ := sourceEngine.(ir.CDCSchemaSnapshotNormalizer)
	out := make([]ir.SchemaSnapshot, 0, len(schema.Tables))
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		seedTable := tbl
		if normalizer != nil {
			seedTable = normalizer.NormalizeForCDCComparison(tbl)
		}
		out = append(out, ir.SchemaSnapshot{
			Schema: seedTable.Schema,
			Table:  seedTable.Name,
			IR:     seedTable,
		})
	}
	return out
}

// normalizeSnapshotForComparison applies the source engine's Bug 84/86
// comparison lens to a CDC-projected snapshot table. nil normalizer
// (an engine without the [ir.CDCSchemaSnapshotNormalizer] surface) is
// the identity. Both live-coordination intercepts call this on every
// incoming snapshot so the classifier ALWAYS compares two normalized
// tables — the seed side is normalized at synthesis
// ([synthesizeColdStartSeedSnapshots]), and each cached CDC post-state
// is normalized here. One-sided normalization is the TRIAGE-#3
// phantom-alter regression shape; keep the lens two-sided.
func normalizeSnapshotForComparison(normalizer ir.CDCSchemaSnapshotNormalizer, t *ir.Table) *ir.Table {
	if normalizer == nil {
		return t
	}
	return normalizer.NormalizeForCDCComparison(t)
}

// lookupSeedCache resolves a SchemaSnapshot's cache entry, falling
// back to the bare table-name key when the qualified-name lookup
// misses. This handles the MySQL Bug 83 v0.73.1 fix: the MySQL
// source schema reader doesn't populate ir.Table.Schema (it reads
// information_schema for a single bound DB; the IR convention pre-
// v0.73.1 left Schema empty), so the cold-start seed's
// QualifiedName() is the bare table name. The MySQL CDC reader, in
// contrast, sets Schema to the DSN's DB name on its emitted
// SchemaSnapshot, so the first CDC snapshot's QualifiedName is
// "<db>.<table>" — a MISS against the bare-name seed without this
// fallback.
//
// On a fallback hit, the returned preKey is the bare key (caller
// promotes to qualified key so subsequent snapshots resolve directly).
// On no hit, preKey is empty.
func lookupSeedCache(cache map[string]*ir.Table, qualifiedKey, bareKey string) (pre *ir.Table, hadPre bool, preKey string) {
	if t, ok := cache[qualifiedKey]; ok {
		return t, true, qualifiedKey
	}
	if qualifiedKey != bareKey {
		if t, ok := cache[bareKey]; ok {
			return t, true, bareKey
		}
	}
	return nil, false, ""
}

// deriveDDLText renders a deterministic textual identity for a
// post-DDL IR table. Used as the lease's ddl_text field + as the
// checksum input. DP-E (recognized-shape catalog) — the "DDL text"
// here is sluice's own IR-marshal rendering, not raw source SQL.
//
// SHA-256 hex of the marshalled IR Table is deterministic across
// engines (the codec is engine-neutral). For human-readable
// diagnostics, the prefix "ir-schema:" identifies the source.
func deriveDDLText(t *ir.Table) string {
	if t == nil {
		return ""
	}
	payload, err := ir.MarshalTable(t)
	if err != nil {
		// Defensive — if marshalling fails the caller still gets
		// SOMETHING distinct (the table name + column count); the
		// router's downstream checksum is still useful for divergence
		// detection.
		return fmt.Sprintf("ir-schema:%s:%d-cols:marshal-err:%v", t.Name, len(t.Columns), err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("ir-schema:%s:%s", t.Name, hex.EncodeToString(sum[:]))
}
