// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # PG-target cold-copy storage-grow resilience (roadmap item 38, ADR-0110)
//
// The Postgres analog of the MySQL cold-copy reparent-retry
// (internal/engines/mysql/row_writer_reparent_retry.go, ADR-0108/0110).
// A PG-target bulk cold-copy uses the COPY-protocol writer, which — before
// this file — had NO equivalent of the MySQL flush retry/grow-gate: a
// mid-COPY transient (a PlanetScale non-Metal PG volume that does not grow
// ahead of the streaming COPY → `could not extend file … No space left on
// device`, SQLSTATE 53100) aborted the whole table's COPY fatally instead
// of riding the storage-grow window. Live finding #94 (MySQL→PS-160-PG).
//
// This file carries the engine-neutral wiring (SetGrowGate + the gate
// helpers) and the per-chunk bounded retry loop. The chunked COPY path
// itself lives in row_writer.go's writeViaCopy (it engages ONLY when a
// grow-gate is attached, i.e. a PlanetScale-class target; vanilla PG keeps
// the monolithic single-CopyFrom path byte-for-byte).
//
// Shape mirrors the MySQL helper deliberately: same backoff envelope, same
// ~30-min wall-clock bound, same "re-acquire a FRESH conn per retry, Await
// the gate, replay the buffered chunk, Trip on a classified transient"
// discipline. The one structural difference: a PG chunk's CopyFrom is its
// OWN atomic COPY into the append-only fresh cold-copy table, so a
// rolled-back chunk wrote NOTHING — a replay is clean (no dup, no partial),
// and there is no MySQL-style 1062-on-retry tolerance wart to carry.

package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// pgCopyChunkRowsVar caps how many rows accumulate into one buffered chunk on
// the grow-gate-engaged chunked-COPY path before that chunk is flushed via a
// single CopyFrom. Bounds the buffered []ir.Row replay slice so a failed
// chunk can be replayed without holding the whole table in memory, and gives
// a mid-COPY grow a per-chunk resume point (the gap item 38 closes: a single
// monolithic COPY of one big table has no resume point). 50_000 rows is a
// balance — large enough to keep COPY throughput near the monolithic path,
// small enough to bound replay memory for wide rows.
//
// A package var (not const) ONLY so the integration test can shrink it to fire
// many chunks over a small fixture; production NEVER mutates it (no config
// field, no zero-value path — the chunked path engages on a non-nil gate, not
// on this value).
var pgCopyChunkRowsVar = 50_000

// pgCopyChunkBytes is the soft byte cap on a buffered chunk — whichever of
// the row-count or byte cap trips first flushes the chunk. Bounds heap for
// wide-row workloads (mirrors writeViaBatch's byte-cap intent). 64 MiB
// matches defaultMaxBufferBytes.
const pgCopyChunkBytes int64 = 64 << 20 // 64 MiB

// Cold-copy reparent-retry bounds for the PG chunked-COPY path. These mirror
// the MySQL helper's envelope exactly (see row_writer_reparent_retry.go for
// the full rationale): the terminal bound is ELAPSED WALL-CLOCK time
// (~30 min, sized to ride a prolonged multi-step PlanetScale storage
// auto-grow), NOT an attempt count; the attempt count survives only as a
// high runaway backstop in case backoff were ever zero.
//
// Package vars (not consts) ONLY so the unit tests can shrink the envelope to
// keep the suite fast — production NEVER mutates them, so there is no config
// field and no zero-value path (the zero-value-safe-default reasoning is
// unaffected). They are read only inside the SYNCHRONOUS per-chunk retry loop
// (never a long-lived background goroutine that could outlive a test's
// restore), so the gate's per-instance-snapshot -race lesson does not apply.
var (
	pgCopyReparentMaxWallVar       = 30 * time.Minute
	pgCopyReparentRetryAttemptsVar = 100000
	pgCopyReparentBackoffBaseVar   = 100 * time.Millisecond
	pgCopyReparentBackoffCapVar    = 30 * time.Second
)

// pgCopyReparentBackoff returns the per-attempt backoff for the chunked-COPY
// reparent-retry loop: exponential doubling from pgCopyReparentBackoffBaseVar,
// capped at pgCopyReparentBackoffCapVar. attempt is 1-based (attempt 1 is the
// first RETRY, i.e. the wait BEFORE the second CopyFrom try). Mirrors the
// MySQL coldCopyReparentBackoff shape.
func pgCopyReparentBackoff(attempt int) time.Duration {
	b := pgCopyReparentBackoffBaseVar
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > pgCopyReparentBackoffCapVar {
			return pgCopyReparentBackoffCapVar
		}
	}
	return b
}

// SetGrowGate implements [ir.GrowGateSetter] (ADR-0110, roadmap item 38).
// The pipeline wires the cold-copy run's shared [ir.GrowGate] here, right
// after OpenRowWriter, so every per-table / per-fan-out-worker writer in the
// run shares ONE pause coordinator — the same construction-time wiring as the
// MySQL RowWriter. On a cold-copy run the gate is constructed UNCONDITIONALLY
// (signal-driven universal floor — any auto-grow target benefits, not just a
// PlanetScale-class one), so a PG target — vanilla PG included — receives a
// non-nil gate here and writeViaCopy takes the chunked-COPY path. A nil gate
// — only the no-gate CONSTRUCTIONS: direct unit tests and the non-cold-copy
// apply path — disables the coordinated pause AND keeps writeViaCopy on the
// monolithic single-CopyFrom path (per-value encoding is byte-identical
// either way). The per-chunk reparent-retry budget is the authoritative
// loud-on-exhaustion floor whenever a gate IS attached.
func (w *RowWriter) SetGrowGate(gate ir.GrowGate) {
	w.growGate = gate
}

// awaitGrowGate blocks while the run's shared coordinated-pause gate
// (ADR-0110) is closed and returns ctx.Err() promptly on cancel. A nil gate
// ⇒ instant nil return. Mirrors the MySQL helper.
func (w *RowWriter) awaitGrowGate(ctx context.Context) error {
	if w.growGate == nil {
		return nil
	}
	return w.growGate.Await(ctx)
}

// tripGrowGate trips the run's shared coordinated-pause gate so sibling
// cold-copy lanes quiesce together for a grow window. A nil gate ⇒ no-op.
// Idempotent + coalescing (see [ir.GrowGate.Trip]). Mirrors the MySQL helper.
func (w *RowWriter) tripGrowGate(reason string) {
	if w.growGate == nil {
		return
	}
	w.growGate.Trip(reason)
}

// copyChunkWithRetry runs ONE buffered chunk's COPY with the bounded
// reparent-retry around it (roadmap item 38). It is the single place the PG
// chunked-COPY retry policy lives.
//
//   - tableName names the table for the WARN/terminal messages.
//   - rows is the chunk's row count (for the logs).
//   - attempt runs ONE CopyFrom of the buffered chunk against a FRESH conn
//     acquired by attempt itself. The chunk slice is owned by the caller and
//     is byte-identical across replays — each attempt re-encodes it through
//     the SAME prepareValue path (via newSliceCopySource), so a replay
//     produces EXACTLY the same target rows as the first try.
//
// A chunk's CopyFrom is its own atomic COPY into the append-only fresh table:
// a rolled-back attempt wrote nothing, so replaying the buffered chunk is
// clean (no dup, no partial). The first error is routed through
// classifyApplierError; the loop retries ONLY a transient that satisfies
// ir.RetriableError (53100 disk-full / 57P0x reparent / 08* connection / bad
// conn) — exactly the storage-grow / serving-transition set. Any non-
// retriable (terminal) error returns unchanged. On budget exhaustion it
// returns a LOUD terminal error wrapping the most recent transient.
func (w *RowWriter) copyChunkWithRetry(
	ctx context.Context,
	tableName string,
	rows int,
	attempt func(ctx context.Context) error,
) error {
	// ADR-0110: quiesce with the run's other cold-copy lanes if a coordinated
	// grow-window pause is in effect before the first try.
	if err := w.awaitGrowGate(ctx); err != nil {
		return err
	}
	err := attempt(ctx)
	if err == nil {
		return nil
	}

	// WALL-CLOCK BOUND: the chunk retries until it succeeds or this deadline
	// passes — NOT a fixed attempt count (the gate's fast probe cycles consume
	// attempts faster than wall-clock).
	deadline := time.Now().Add(pgCopyReparentMaxWallVar)

	for try := 1; ; try++ {
		// Classify the MOST RECENT error. Only a transient (disk-full /
		// reparent / connection-reset class) is retriable; everything else —
		// including a real terminal value-fidelity / constraint failure —
		// returns unchanged.
		var re ir.RetriableError
		if !errors.As(classifyApplierError(err), &re) || !re.Retriable() {
			return err
		}
		// ADR-0110: this lane hit a classified grow-transient — TRIP the shared
		// gate so every sibling cold-copy lane quiesces together for the grow
		// window instead of independently hammering the struggling target.
		w.tripGrowGate("postgres cold-copy chunk transient: " + err.Error())
		// Terminal on the WALL-CLOCK deadline (the real bound) or the runaway
		// attempt backstop. A genuinely-wedged target surfaces loudly after
		// ~30 min; a transient grow is ridden regardless of probe cadence.
		if time.Now().After(deadline) || try >= pgCopyReparentRetryAttemptsVar {
			return fmt.Errorf(
				"postgres: cold-copy into %q: chunk COPY (%d rows) still failing after riding the storage-grow window "+
					"(%s wall-clock, %d attempts; the target may be undergoing a prolonged storage-grow/reparent or be genuinely out of disk): %w",
				tableName, rows, pgCopyReparentMaxWallVar, try, err,
			)
		}

		backoff := pgCopyReparentBackoff(try)
		slog.WarnContext(
			ctx, "postgres: cold-copy chunk COPY hit a transient target error (likely a storage auto-grow / 'could not extend file' / reparent); "+
				"re-acquiring a fresh connection and retrying the chunk",
			slog.String("table", tableName),
			slog.Int("rows", rows),
			slog.Int("attempt", try),
			slog.Duration("elapsed", time.Since(deadline.Add(-pgCopyReparentMaxWallVar))),
			slog.Duration("max_wall", pgCopyReparentMaxWallVar),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}

		// ADR-0110: before the retry attempt, Await the coordinated pause again
		// — if the gate is (still) closed for the grow window this lane parks
		// calmly here instead of re-acquiring a conn and hammering the target.
		if aerr := w.awaitGrowGate(ctx); aerr != nil {
			return aerr
		}
		// attempt() re-acquires a FRESH conn from the pool (the pinned conn is
		// dead after a reparent / a 53100 may have poisoned the COPY) and
		// replays the buffered chunk. NEVER reuse a dead conn.
		err = attempt(ctx)
		if err == nil {
			return nil
		}
	}
}
