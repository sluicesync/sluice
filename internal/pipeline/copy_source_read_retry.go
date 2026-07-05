// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Cold-copy SOURCE-READ reconnect-and-resume retry (ADR-0109)
//
// The READ-side sibling of ADR-0108's cold-copy target-WRITE
// reparent-retry. A bulk cold-copy that runs for tens of minutes can have
// its SOURCE read connection dropped mid-table by a transient that is not
// itself a source fault — most concretely the backpressure-EOF a STALLED
// target induces: a non-Metal PlanetScale target hits `errno 28 — No
// space left on device` during a storage auto-grow, its primary's writes
// BLOCK (semi-sync) rather than error, sluice's reader/writer pipeline
// backpressures, the source read connection goes idle past the source
// server's net_write_timeout, and the source closes it →
// `unexpected EOF` / `invalid connection` → the whole cold-copy aborted.
//
// ADR-0108 cannot help (the target write BLOCKED, so the first error was
// on the READ side). This file wraps the per-table cold-copy read in a
// bounded reconnect-and-resume retry: on a CLASSIFIED-retriable source-read
// error it opens a FRESH source reader and resumes the table copy from a
// position that is provably dup-free and loss-free, bounded, and loud on
// exhaustion. It is per-TABLE — a transient on one table reconnects and
// resumes WITHOUT aborting its sibling table copies (the cross-table pool's
// errgroup only unwinds peers on a TERMINAL error, which a recovered
// transient never returns).
//
// Classification is engine-neutral: the MySQL reader routes its sticky
// rows-iteration error through the SAME classifyApplierError the CDC apply
// path uses (row_reader.go), so its Err() carries an ir.RetriableError for
// the connection-drop class; this helper checks that surface via
// errors.As — it never imports an engine package or invents a retry class.
//
// Resume-position safety by path (the value-fidelity core, ADR-0109 §2):
//
//   - KEYSET-CHUNKED tables resume each unfinished chunk from its persisted
//     chunk.LastPK (WHERE (pk) > LastPK), the existing crash-resume
//     machinery; a transient is routed into the same path by re-invoking
//     the chunk copy in RESUME mode. Dup-free (pk > LastPK) + loss-free
//     (no gap). This is the path the live-failing fan-out `documents`
//     table takes.
//   - NON-CHUNKABLE tables (plain single-stream, no-PK, non-orderable PK —
//     no safe mid-table cursor) TRUNCATE the target table + restart that
//     table's copy from a fresh reader. Always dup-free + loss-free; the
//     named efficiency wart (re-copies the table) bounded by the same
//     retry budget. Keyless cold-copy is already at-least-once (Bug 143),
//     so a restart is consistent with the existing contract.
//
// Scope: the MySQL SOURCE path (the demonstrated gap), and specifically
// the `migrate` cold-copy — where a fresh per-table source reader is
// structurally available (each table-pool worker / chunk opens its own
// reader via the engine factory). The `sync` cold-start path pins ONE
// snapshot reader across all tables (re-minting it per-table mid-snapshot
// would break the consistent point / VStream position stitching) and the
// PG COPY-protocol source path have the analogous gap; both are noted as
// follow-ups in ADR-0109 and are NOT addressed here.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// Cold-copy source-read retry bounds. Zero-value-safe by construction:
// these are package vars (not config fields), so there is no
// EnableX-defaulting-true trap (the v0.99.51 lesson) — every construction
// path (CLI, tests, broker, future callers) gets the same bounds. They
// MIRROR ADR-0108's target-write reparent-retry envelope:
//
//	100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s →
//	25.6s → 30s (cap) → 30s → 30s → ...
//
// 12 attempts × max(30s) ≈ up to ~4 min of failure tolerated before a LOUD
// terminal error — long enough to ride a storage-grow / failover, short
// enough that a genuinely-down source surfaces rather than hiding for
// hours.
//
// These are vars (not consts) ONLY so the unit tests can shrink the
// envelope to keep the suite fast — production NEVER mutates them, so the
// zero-value-safe reasoning is unaffected (there is no config field and no
// zero-value path; the values are baked at package init).
var (
	// WALL-CLOCK BOUND (v0.99.103): the real terminal bound is elapsed time
	// (~30 min), mirroring the write-side coldCopyReparentMaxWallVar — a
	// single table's source-read reconnect rides a prolonged multi-step
	// storage-grow stall regardless of how many fast retry cycles elapsed.
	coldCopySourceReadMaxWall = 30 * time.Minute
	// coldCopySourceReadRetryAttempts is now a high RUNAWAY BACKSTOP only
	// (not the operational bound — the wall-clock deadline is).
	coldCopySourceReadRetryAttempts = 100000
	coldCopySourceReadBackoffBase   = 100 * time.Millisecond
	coldCopySourceReadBackoffCap    = 30 * time.Second
)

// coldCopySourceReadBackoff returns the per-attempt backoff for the
// source-read retry loop: exponential doubling from
// coldCopySourceReadBackoffBase, capped at coldCopySourceReadBackoffCap.
// attempt is 1-based (attempt 1 is the wait BEFORE the second try).
// Mirrors coldCopyReparentBackoff's shape (ADR-0108).
func coldCopySourceReadBackoff(attempt int) time.Duration {
	b := coldCopySourceReadBackoffBase
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > coldCopySourceReadBackoffCap {
			return coldCopySourceReadBackoffCap
		}
	}
	return b
}

// isRetriableSourceReadError reports whether err (or anything it wraps)
// is the connection-drop class the source-read retry should ride out. It
// walks the chain via errors.As for [ir.RetriableError] — the SAME
// engine-neutral surface the CDC apply / target-write retries check — and
// honours its Retriable() verdict. A non-implementing error (a real
// decode/query fault, a schema mismatch) is NOT retriable, exactly as
// today: the copy stays terminal. The MySQL reader populates this surface
// by classifying its rows-iteration error through classifyApplierError
// (row_reader.go); readerStreamErr's `%w` wrap keeps it reachable here.
func isRetriableSourceReadError(err error) bool {
	if err == nil {
		return false
	}
	var re ir.RetriableError
	return errors.As(err, &re) && re.Retriable()
}

// sourceReadResumeStrategy names how a table's copy resumes after a
// classified source-read drop (ADR-0109 §2).
type sourceReadResumeStrategy int

const (
	// resumeFromChunkCursor: a keyset-chunked table re-runs in RESUME
	// mode so each unfinished chunk continues from its persisted
	// chunk.LastPK (WHERE (pk) > LastPK) — dup-free + loss-free.
	resumeFromChunkCursor sourceReadResumeStrategy = iota
	// resumeTruncateRestart: a non-chunkable table truncates the target
	// and re-copies from row 0 on a fresh reader — always correct, the
	// named efficiency wart.
	resumeTruncateRestart
)

// copyTableWithSourceReadRetry runs one table's cold-copy attempt with the
// ADR-0109 bounded reconnect-and-resume retry around it. It is the ONE
// place the read-side retry policy lives so every cold-copy entry point
// shares one shape, one log, one bound — the read-side analog of
// flushWithReparentRetry.
//
//   - attempt copies the table on the supplied reader. On the FIRST try the
//     caller passes its already-open reader and resuming==the run's resume
//     flag; on every RETRY this helper opens a FRESH reader (freshReader)
//     and passes it in. For the chunk-cursor strategy the retry forces
//     resuming=true so the chunk copy continues from chunk.LastPK; for the
//     truncate-restart strategy it stays at the run's resume flag (the
//     truncate already cleared the target, so the copy re-runs from the
//     start regardless).
//   - freshReader opens a brand-new source reader (its own pooled
//     connection, reconnected to a healthy source) and a closer for it. The
//     dead post-drop reader is NEVER reused: the first attempt runs on the
//     caller's reader; each retry opens + closes its own.
//   - truncate empties the target table for the resumeTruncateRestart
//     strategy (nil for the chunk-cursor strategy, which never truncates).
//
// The first error is routed through isRetriableSourceReadError; the loop
// retries ONLY a classified transient (connection-reset / EOF / invalid-
// connection / vttablet-unavailable). Any non-retriable (terminal) error
// — including a real decode fault, a non-retriable query error, or a
// ctx-cancel that survived readerStreamErr's benign-cancel filter —
// returns unchanged, exactly as today (no retry, no truncate). On retry the
// helper backs off (honoring ctx.Done()), opens a fresh reader, runs the
// attempt, closes the reader, and tries again. On budget exhaustion it
// returns a LOUD terminal error wrapping the most recent transient (never
// silent, never infinite).
func copyTableWithSourceReadRetry(
	ctx context.Context,
	tableName string,
	strategy sourceReadResumeStrategy,
	firstReader ir.RowReader,
	firstResuming bool,
	attempt func(ctx context.Context, rows ir.RowReader, resuming bool) error,
	freshReader func(ctx context.Context) (ir.RowReader, func(), error),
	truncate func(ctx context.Context) error,
	gate ir.GrowGate,
) error {
	// ADR-0110: quiesce with the run's other cold-copy lanes if a
	// coordinated grow-window pause is in effect before reading. Await is a
	// cheap open read when no pause is active and returns ctx.Err() promptly
	// on cancel. nil gate ⇒ instant return (pre-ADR-0110 behaviour).
	if aerr := migcore.AwaitGrowGate(ctx, gate); aerr != nil {
		return aerr
	}
	err := attempt(ctx, firstReader, firstResuming)
	if err == nil {
		return nil
	}

	// WALL-CLOCK BOUND (v0.99.103): retry until success or this deadline —
	// not a fixed attempt count — so a table's source-read reconnect rides a
	// prolonged grow regardless of retry cadence (mirrors the write-side
	// coldCopyReparentMaxWallVar).
	deadline := time.Now().Add(coldCopySourceReadMaxWall)

	for try := 1; ; try++ {
		// Classify the MOST RECENT error. Only the connection-drop class is
		// retriable; everything else — a real value-fidelity decode failure,
		// a terminal query error — returns unchanged (terminal).
		if !isRetriableSourceReadError(err) {
			return err
		}
		// ADR-0110: a classified source-read drop is the READ-side face of a
		// target storage-grow stall (the backpressure-EOF a stalled target
		// induces). TRIP the shared gate so sibling lanes quiesce together
		// for the grow window. Coalescing + idempotent; this lane keeps its
		// own bounded retry below as the authoritative floor.
		migcore.TripGrowGate(gate, "pipeline cold-copy source-read transient: "+err.Error())
		if time.Now().After(deadline) || try >= coldCopySourceReadRetryAttempts {
			return fmt.Errorf(
				"pipeline: cold-copy of %q: source read still failing after riding the reconnect-and-resume window "+
					"(%s wall-clock, %d attempts; the source connection keeps dropping — the source may be wedged, or a prolonged target stall keeps "+
					"backpressuring the read past the source's net_write_timeout): %w",
				tableName, coldCopySourceReadMaxWall, try, err,
			)
		}

		backoff := coldCopySourceReadBackoff(try)
		slog.WarnContext(
			ctx, "pipeline: cold-copy hit a transient source-read drop; re-opening a fresh source reader and resuming the table copy",
			slog.String("table", tableName),
			slog.String("resume", resumeStrategyLabel(strategy)),
			slog.Int("attempt", try),
			slog.Duration("elapsed", time.Since(deadline.Add(-coldCopySourceReadMaxWall))),
			slog.Duration("max_wall", coldCopySourceReadMaxWall),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}

		// Truncate-restart strategy: clear the target table FIRST so the
		// re-copy from row 0 cannot dup-key on the partial prior attempt.
		// A truncate failure rides the SAME budget (it surfaces as the
		// loop's err on the next iteration), but a truncate that is itself
		// non-retriable falls through the classifier and returns terminal.
		if strategy == resumeTruncateRestart && truncate != nil {
			if terr := truncate(ctx); terr != nil {
				err = terr
				continue
			}
		}

		// ADR-0110: before re-opening + retrying, Await the coordinated
		// pause again — if the gate is still closed for the grow window this
		// lane parks calmly instead of opening a fresh reader against a
		// stalled target. Returns promptly on ctx-cancel; nil gate ⇒ instant.
		if aerr := migcore.AwaitGrowGate(ctx, gate); aerr != nil {
			return aerr
		}
		// Open a FRESH source reader — the post-drop reader is dead; the
		// engine factory reconnects to a healthy source here. NEVER reuse
		// firstReader. A re-open failure is itself classified on the next
		// loop iteration (a still-unreachable source surfaces the same
		// transient shape), so it rides the budget too.
		rows, closeReader, oerr := freshReader(ctx)
		if oerr != nil {
			err = oerr
			continue
		}
		// resuming is forced true ONLY for the chunk-cursor strategy (so the
		// chunk copy continues from chunk.LastPK). The truncate-restart
		// strategy keeps the run's resume flag — the truncate already cleared
		// the target, so it re-copies from the start regardless.
		retryResuming := firstResuming || strategy == resumeFromChunkCursor
		err = attempt(ctx, rows, retryResuming)
		closeReader()
		if err == nil {
			return nil
		}
	}
}

// resumeStrategyLabel renders a strategy for the WARN/terminal logs.
func resumeStrategyLabel(s sourceReadResumeStrategy) string {
	switch s {
	case resumeFromChunkCursor:
		return "chunk-cursor"
	case resumeTruncateRestart:
		return "truncate-restart"
	default:
		return "unknown"
	}
}
