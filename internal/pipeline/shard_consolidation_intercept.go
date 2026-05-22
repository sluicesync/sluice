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
//      schema for the next boundary's classifier input). Cold-start
//      seeds the cache with the table's first SchemaSnapshot (no
//      DDL — that's the bulk-copy schema); from then on each
//      subsequent SchemaSnapshot computes shape via (cached → snap.IR).
//   2. For each non-first SchemaSnapshot, the intercept calls
//      [BoundaryRouter.RouteBoundary] with the (pre, post) pair.
//   3. On success the snapshot's IR replaces the cache entry, and the
//      SchemaSnapshot continues to the applier so the ADR-0049
//      schema-history write still records the version downstream.
//   4. On error (refused shape / probe Inconsistent / peer checksum
//      mismatch) the intercept closes the out-channel and stores the
//      error for the streamer to surface via runOnce's standard
//      dispatchErr classification.
//
// The intercept is opt-in: nil router → pass-through.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/orware/sluice/internal/ir"
)

// interceptSchemaSnapshotsForCoordination wraps changes with a
// SchemaSnapshot intercept that drives the BoundaryRouter. nil router
// is a no-op (returns in verbatim). The errStore is the streamer's
// sourceErrFn-bound error sink; the intercept writes any
// RouteBoundary error there so the streamer's standard
// surfaceSourceError() path picks it up.
func interceptSchemaSnapshotsForCoordination(
	ctx context.Context,
	in <-chan ir.Change,
	router *BoundaryRouter,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	if router == nil {
		return in
	}
	out := make(chan ir.Change)
	cache := map[string]*ir.Table{}
	version := map[string]int64{}
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
				pre, hadPre := cache[key]
				cache[key] = snap.IR
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
				// (pre, post). DDL text is the deterministic
				// IR-marshalled rendering of the post-IR schema (no
				// raw source DDL is available through the
				// SchemaSnapshot path — DP-E's "shapes are sluice's
				// own structural categories" applies).
				ddlText := deriveDDLText(snap.IR)
				if err := router.RouteBoundary(ctx, key, pre, snap.IR, ddlText, version[key]); err != nil {
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
