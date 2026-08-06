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
// ROLLED-BACK chunk wrote NOTHING — a replay is clean (no dup, no partial),
// and there is no MySQL-style 1062-on-retry tolerance wart to carry.
//
// That argument covers ONE of the two branches, and the correction matters
// (audit B-9's PG sibling). A transient can also arrive AFTER the server
// committed the chunk's COPY and BEFORE pgx read the CommandComplete — the
// committed-but-unacked branch. Replaying then re-COPIES rows that are
// already durable. On a KEYED table the target refuses the replay loudly
// (23505 unique violation — this path has no ON CONFLICT), which is
// annoying but not silent. On a KEYLESS table there is no constraint to
// notice and the chunk is silently doubled. So the replay is gated on the
// same predicate the MySQL core uses: see the keyless refusal in
// [RowWriter.copyChunkWithRetry].

package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
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
// cold-copy lanes quiesce together. A nil gate ⇒ no-op. Idempotent +
// coalescing (see [ir.GrowGate.Trip]). Mirrors the MySQL helper.
//
// It takes the ERROR rather than a pre-computed verdict so every PG trip site
// classifies through the one predicate ([growEvidenceOf]) instead of each
// deciding for itself what it saw — item 143's whole subject is a claim that
// was made per-site and checked nowhere.
func (w *RowWriter) tripGrowGate(reason string, cause error) {
	if w.growGate == nil {
		return
	}
	w.growGate.Trip(reason, growEvidenceOf(cause))
}

// quiesceAndReportTransient gives a write core the two HALVES of the grow
// gate that are safe without replay — Await before the write, Trip after a
// classified transient — for a path whose flush cannot be re-driven
// (audit 2026-08-01 Q1).
//
// The plain multi-row INSERT core is that path. A batch that fails with a
// dropped connection is AMBIGUOUS: the server may have applied it before the
// ack was lost, so replaying would duplicate rows into a table with no
// conflict target to absorb them. Retrying is therefore wrong there, and the
// upsert core next door gets the full [RowWriter.copyChunkWithRetry] for
// exactly the reason this one cannot.
//
// Await and Trip are unconditionally safe regardless: Await only parks this
// lane while a sibling is riding a grow window, and Trip only tells the
// siblings that this lane hit one. Withholding them bought nothing and cost
// the run its coordination — a lane that can neither wait nor signal hammers
// a struggling target while every other lane politely backs off.
//
// Returns err unchanged; the classification is used only to decide whether to
// Trip.
func (w *RowWriter) quiesceAndReportTransient(err error, what string) error {
	if err == nil {
		return nil
	}
	var re ir.RetriableError
	if errors.As(classifyApplierError(err), &re) && re.Retriable() {
		w.tripGrowGate("postgres cold-copy "+what+" transient: "+err.Error(), err)
	}
	return err
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
// clean (no dup, no partial). The committed-but-unacked branch is the one the
// file header now spells out, and it is why this helper takes the *ir.Table
// rather than its name: a table with no PRIMARY KEY and no all-NOT-NULL
// UNIQUE index cannot notice a doubled chunk, so the replay is refused for it
// (audit B-9). The first error is routed through
// classifyApplierError; the loop retries ONLY a transient that satisfies
// ir.RetriableError (53100 disk-full / 57P0x reparent / 08* connection / bad
// conn) — exactly the storage-grow / serving-transition set. Any non-
// retriable (terminal) error returns unchanged. On budget exhaustion it
// returns a LOUD terminal error wrapping the most recent transient.
func (w *RowWriter) copyChunkWithRetry(
	ctx context.Context,
	table *ir.Table,
	rows int,
	attempt func(ctx context.Context) error,
) error {
	tableName := pgTableNameOf(table)
	replaySafe := irbackup.TableReplayIdempotent(table)
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
		w.tripGrowGate("postgres cold-copy chunk transient: "+err.Error(), err)
		// Audit B-9 (PG sibling): the replay below re-COPIES a byte-identical
		// chunk. Safe when the prior attempt rolled back, and safe-because-
		// loud on a keyed table when it did not (23505). On a keyless table
		// neither the target nor this code can tell the two apart, so the
		// chunk would silently double. Refuse before replaying. The Trip
		// above still runs — the transient was real and the sibling lanes
		// should still quiesce.
		if !replaySafe {
			return errKeylessAmbiguousReplay(tableName, rows, err)
		}
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
		// The message states the VERDICT, not a guess at the cause. It used to
		// read "(likely a storage auto-grow / 'could not extend file' /
		// reparent)" for every transient, including the connection drops that
		// are the overwhelming majority — see [ir.GrowEvidence] for the two
		// datasets and roadmap item 143. `evidence` is derived from this exact
		// error by [growEvidenceOf].
		slog.WarnContext(
			ctx, "postgres: cold-copy chunk COPY hit a transient target error; "+
				"re-acquiring a fresh connection and retrying the chunk",
			slog.String("table", tableName),
			slog.String("evidence", growEvidenceOf(err).String()),
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

// pgTableNameOf renders a table's name for a message, total over nil so a
// refusal path can never itself panic.
func pgTableNameOf(table *ir.Table) string {
	if table == nil {
		return "<nil>"
	}
	return table.Name
}

// errKeylessAmbiguousReplay is the PG twin of the MySQL core's refusal
// (audit B-9). Same predicate ([irbackup.TableReplayIdempotent]), same
// code, same reasoning: a transient that arrived after the server
// committed the chunk but before the client saw the acknowledgement is
// indistinguishable from one that rolled it back, and a table with no
// PRIMARY KEY and no NOT NULL UNIQUE index has nothing that would make
// the second COPY of those rows fail. The MySQL helper's doc carries
// the full refuse-vs-reconcile argument; it applies here unchanged.
//
// The PG-specific note: on a KEYED table this path would have surfaced a
// 23505 rather than duplicating, so the refusal changes behaviour only
// for the keyless case — which is exactly the case that was silent.
func errKeylessAmbiguousReplay(tableName string, rows int, cause error) error {
	return sluicecode.Wrap(
		sluicecode.CodeCopyRetryAmbiguousKeyless,
		"add a PRIMARY KEY or a NOT NULL UNIQUE index to the table, then re-run",
		fmt.Errorf(
			"postgres: cold-copy into %q: the target hit a transient error mid-chunk (%d rows) and this table has "+
				"no PRIMARY KEY and no NOT NULL UNIQUE index, so replaying the chunk could silently duplicate every "+
				"row in it: an attempt that committed but lost its acknowledgement is indistinguishable from one "+
				"that rolled back, and with no unique key there is no constraint violation to tell them apart. "+
				"Refusing rather than risking duplicate rows. Add a PRIMARY KEY or a NOT NULL UNIQUE index to the "+
				"source table and re-run, or re-run this table's copy from an empty target: %w",
			tableName, rows, cause,
		),
	)
}
