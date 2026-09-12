// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Raw-copy MID-COPY reconnect-retry — the fourth sibling
//
// sluice has three cold-copy retries, and until this file the raw lane was
// covered by none of them:
//
//   - ADR-0108 — target-WRITE reparent retry (MySQL flush path).
//   - ADR-0109 — source-READ reconnect-and-resume (typed reader, table level).
//   - ADR-0146 — chunk connection-OPEN retry (before any COPY runs).
//   - **this file** — the raw byte-pipe's MID-COPY transient, on either side.
//
// The typed lane gets its mid-copy retry inside the engine
// (postgres.RowWriter.copyChunkWithRetry, reached via WriteRows). The raw lane
// cannot: its copy is a PIPE between an exporter and an importer that only the
// pipeline layer holds both ends of, so no single engine can retry it. The
// result was that `copyChunkRaw` called [runRawCopyChunk] exactly once and
// returned on the first error, while its sibling `copyChunkFast` rode the same
// transients out.
//
// # What this cost, measured
//
// 2026-09-12, copying a 29 GB table into a FRESHLY CREATED PlanetScale Neki
// database (10 GiB starting volume) from an in-region client, twice:
//
//	fresh PS-160 : FAILED at 116s — 53100 "could not extend file … No space
//	               left on device" (the platform's storage auto-grow is
//	               reactive; the volume grew ~2m20s AFTER the refusal)
//	fresh PS-10  : FAILED at 191s — 08006 "write: broken pipe", primary at
//	               100% CPU
//
// Both SQLSTATEs were already classified retriable, and ADR-0110's grow-gate
// tripped correctly on both runs and logged its quiesce. The copies died
// anyway, because classification decides WHETHER to retry and this lane had
// nothing to retry WITH. The same copy into a target whose volume had already
// grown succeeded at ~29.9k rows/s. So the fastest lane was the one least able
// to survive the interruption most likely to hit it.
//
// # Why retrying is SAFE here, which is the load-bearing part
//
// Re-running a chunk cannot double-copy, for two independent reasons:
//
//  1. [runRawCopyChunk] builds a FRESH io.Pipe and re-invokes ExportRawCopy
//     with the chunk's own PK bounds on every call. Nothing is carried over
//     from a failed attempt — the "no resume point" in the raw lane's error
//     text is about the importer's reader WITHIN one attempt, not about the
//     chunk being unrepeatable.
//  2. The import side is TRANSACTIONAL: ImportRawCopy runs its COPY inside
//     postgres.withCopySessionPins, which wraps it in BEGIN/COMMIT and rolls
//     back on failure. A failed attempt therefore leaves zero rows. That holds
//     even when the connection dies mid-COPY (the 08006 case above) — the
//     server aborts the transaction when the client goes away.
//
// The whole-table path (chunk == nil) is retried on the same argument: it too
// re-exports from scratch and its import too is one transaction, so a retry
// redoes more WORK but cannot produce different DATA. That is a deliberate
// inclusion rather than an oversight — see the caller note in migrate_bulk.go.
//
// # The grow gate needs no plumbing here
//
// ImportRawCopy awaits the run's grow gate itself before opening its COPY
// (the 2026-08-04 ADR-0110 audit fix) and trips it on a transient. Each
// attempt therefore inherits the coordinated pause and the sibling-signalling
// for free; adding a second await in this loop would double-count the wait.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Raw-copy mid-copy retry bounds. Package vars, not config fields, so every
// construction path gets the same envelope and there is no
// EnableX-defaulting-true trap (the v0.99.51 lesson).
//
// The envelope deliberately MATCHES the chunk-open retry's (ADR-0146) rather
// than inventing a second one: a storage auto-grow that refuses a COPY
// mid-stream and one that refuses a connection open are the same platform
// event striking at different moments, and a run should ride it identically
// whichever moment it lands on. The measured grow window that motivated this
// file was ~2m20s, comfortably inside the 30-minute wall.
//
// Vars ONLY so tests can shrink the envelope; production never mutates them.
var (
	rawCopyRetryMaxWall  = 30 * time.Minute
	rawCopyRetryAttempts = 100000
)

// isRetriableRawCopyError reports whether a failed raw-copy attempt should be
// retried.
//
// It delegates to [isRetriableChunkOpenError], and the delegation is the point
// rather than a shortcut: that predicate is already the pipeline's
// engine-neutral transient classifier — it honours an engine-classified
// [ir.RetriableError] first (which is how 53100 and 08006 arrive here), then a
// conservative allow-list of transport shapes, and leaves everything unknown
// FATAL. Sharing it means a new transient recognised for one lane is
// recognised for both.
//
// It exists as its own name because the shared predicate is called
// "...ChunkOpenError", and a reader of this loop should not have to work out
// that a mid-copy failure is being graded by something named for opens.
func isRetriableRawCopyError(err error) bool {
	return isRetriableChunkOpenError(err)
}

// runRawCopyChunkWithRetry runs one raw-copy chunk (or, with chunk == nil, a
// whole table) through [runRawCopyChunk], retrying a classified transient with
// bounded exponential backoff.
//
// A terminal error returns unchanged and immediately — no retry, no backoff —
// so a genuine fault still fails as fast as it did before. Budget exhaustion
// returns a LOUD error wrapping the last transient: never silent, never
// infinite.
//
// See the file header for why re-running a chunk cannot double-copy.
func runRawCopyChunkWithRetry(
	ctx context.Context,
	exp ir.RawCopyExporter,
	imp ir.RawCopyImporter,
	table *ir.Table,
	chunk *ir.RawCopyChunk,
	format ir.RawCopyFormat,
	stallChunk int,
) (int64, error) {
	deadline := time.Now().Add(rawCopyRetryMaxWall)

	for attempt := 1; ; attempt++ {
		rows, err := runRawCopyChunk(ctx, exp, imp, table, chunk, format, stallChunk)
		if err == nil {
			if attempt > 1 {
				slog.InfoContext(ctx, "migration: raw-copy chunk succeeded after retry",
					slog.String("table", table.Name),
					slog.Int("chunk", stallChunk),
					slog.Int("attempts", attempt))
			}
			return rows, nil
		}

		// A cancelled run is not a transient — surface it unchanged so a
		// Ctrl-C or a parent failure does not spend the retry budget.
		if ctx.Err() != nil {
			return 0, err
		}
		if !isRetriableRawCopyError(err) {
			return 0, err
		}

		if attempt >= rawCopyRetryAttempts || !time.Now().Before(deadline) {
			return 0, fmt.Errorf(
				"pipeline: raw-copy chunk retry exhausted after %d attempt(s) within %s "+
					"(the target kept refusing the copy; on a managed platform this is usually a storage "+
					"auto-grow or a saturated primary that did not clear): %w",
				attempt, rawCopyRetryMaxWall, err,
			)
		}

		backoff := chunkOpenRetryBackoff(attempt)
		slog.WarnContext(ctx, "migration: raw-copy chunk hit a transient; retrying",
			slog.String("table", table.Name),
			slog.Int("chunk", stallChunk),
			slog.Int("attempt", attempt),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()))

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}
