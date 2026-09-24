// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The added-column backfill ledger: an interrupted backfill ends the run
// LOUDLY (ADD-COLUMN-BACKFILL-INCOMPLETE) instead of being forgotten.
//
// # Why an interrupted backfill cannot simply resume
//
// A forwarded ADD COLUMN is recognised by the intercept comparing the
// boundary's table against the shape it cached for that table. A resumed
// stream — a new process, or an apply retry that reopens the change stream
// inside the same Run — starts with no cached shape (the cold-start seed is
// consumed once) and a reader whose own prior shape already includes the
// column, so the boundary is never seen again as an ADD COLUMN and the rest
// of the backfill never runs. MEASURED on Postgres 16 before this ledger
// existed: a stream stopped 12 rows into a 60,000-row backfill restarted
// cleanly and left 59,989 pre-existing rows NULL where the source holds the
// value, at exit 0. ADR-0058 §1c/§2c described a cursor-persisted resume;
// it was never built.
//
// # What the ledger does instead
//
// Every backfill a forwarded boundary owes is entered here the moment its
// ALTER has landed, and leaves only when the attempt that ran it can prove
// it reached the target: every row's Update was handed to the applier, AND
// the applier's persisted position has reached the first change that
// followed the backfill in the channel (its watermark). The applier commits
// in channel order and checkpoints only at transaction boundaries, so a
// persisted position at or past the watermark means every backfilled row
// before it is durable. Anything else — a failed page, a stop or Ctrl-C
// mid-backfill, an apply retry that reopened the stream before the
// backfilled rows committed — settles as ADD-COLUMN-BACKFILL-INCOMPLETE at
// the end of the attempt: a terminal error that wraps
// [ir.ErrUnforwardedSchemaChange], so the run's existing refusal recorder
// persists it on the stream's control-table row and every restart refuses
// until the operator has repaired the rows and acknowledged it with
// --accept-unforwarded-schema-change. The independent evidence is the
// applier's own persisted position, read back from the target — not the
// intercept's belief that it finished.
//
// Deliberately conservative: a stop that lands after the backfilled rows
// committed but before the applier checkpointed past the watermark refuses
// too. A false refusal costs one acknowledged restart; a false "complete"
// is the silent loss this exists to prevent.
//
// # The residual, stated
//
// A process that dies without running its exit path (SIGKILL, OOM, power)
// settles nothing, and the restart resumes past the boundary silently.
// Closing that needs the owed backfill recorded durably on the target
// BEFORE the ALTER — a new control-table column, a design decision this
// change does not make.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// addColumnBackfillIncompleteMarker is the grep-stable marker an interrupted
// added-column backfill ends the run with.
const addColumnBackfillIncompleteMarker = "ADD-COLUMN-BACKFILL-INCOMPLETE"

// addedColumnBackfillLedger is the Streamer's record of owed backfills
// (see the file comment). One per Streamer, shared by every attempt, so an
// entry an interrupted attempt left owing is still owed when the attempt
// ends. The mutex orders the intercept goroutine's writes against the
// attempt-exit settle.
type addedColumnBackfillLedger struct {
	mu      sync.Mutex
	entries []*addedColumnBackfillEntry
}

// addedColumnBackfillEntry is one owed backfill.
type addedColumnBackfillEntry struct {
	table   string
	columns []string

	// emitted is true once every row's Update was handed to the applier.
	emitted bool
	// cause is why the backfill stopped short, when it did.
	cause error
	// watermark is the position of the first change forwarded after the
	// backfill; the zero Position until one is.
	watermark ir.Position
}

// open enters an owed backfill. nil-safe (a unit harness has no ledger).
func (l *addedColumnBackfillLedger) open(table string, columns []string) *addedColumnBackfillEntry {
	if l == nil {
		return nil
	}
	e := &addedColumnBackfillEntry{table: table, columns: columns}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
	return e
}

// finished records how the backfill run ended: err nil means every row's
// Update reached the channel.
func (l *addedColumnBackfillLedger) finished(e *addedColumnBackfillEntry, err error) {
	if l == nil || e == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e.emitted = err == nil
	e.cause = err
}

// markWatermark records the position of the first change forwarded after
// e's backfill. A change without a position token cannot anchor it and is
// skipped by the caller.
func (l *addedColumnBackfillLedger) markWatermark(e *addedColumnBackfillEntry, pos ir.Position) {
	if l == nil || e == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e.watermark = pos
}

// settle removes every entry durable reports as having reached the target
// and returns copies of the rest.
func (l *addedColumnBackfillLedger) settle(durable func(e *addedColumnBackfillEntry) bool) []addedColumnBackfillEntry {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var owed []addedColumnBackfillEntry
	kept := l.entries[:0]
	for _, e := range l.entries {
		if durable(e) {
			continue
		}
		owed = append(owed, *e)
		kept = append(kept, e)
	}
	l.entries = kept
	return owed
}

// backfillWatermark hands the watermark to the backfill a boundary just
// ran: the first change the intercept forwards after it that carries a
// position. Held by each intercept goroutine; zero cost when nothing is
// awaiting one.
type backfillWatermark struct {
	ledger  *addedColumnBackfillLedger
	pending *addedColumnBackfillEntry
}

// await arms the watermark for b's entry (nil-safe for a boundary that owed
// nothing, or a backfill that did not finish — which cannot become durable).
func (w *backfillWatermark) await(b *boundaryBackfill) {
	if b == nil || b.bf == nil || b.owed == nil {
		return
	}
	w.ledger, w.pending = b.bf.ledger, b.owed
}

// observe is called with every change the intercept forwards.
func (w *backfillWatermark) observe(c ir.Change) {
	if w.pending == nil {
		return
	}
	if _, isSnap := c.(ir.SchemaSnapshot); isSnap {
		return // a schema anchor's position is not a data checkpoint
	}
	pos := c.Pos()
	if pos.Token == "" {
		return
	}
	w.ledger.markWatermark(w.pending, pos)
	w.pending = nil
}

// addedColumnBackfillIncompleteError ends an attempt that still owes a
// backfill. It wraps [ir.ErrUnforwardedSchemaChange] so the run's refusal
// recorder persists it and every restart refuses until acknowledged, and is
// terminal: a retry would reopen the stream past the boundary and continue
// silently.
type addedColumnBackfillIncompleteError struct {
	owed   []addedColumnBackfillEntry
	runErr error // what else ended the attempt, if anything
}

func (e *addedColumnBackfillIncompleteError) Error() string {
	var what strings.Builder
	for i, o := range e.owed {
		if i > 0 {
			what.WriteString("; ")
		}
		fmt.Fprintf(&what, "%s (%s): ", o.table, strings.Join(o.columns, ", "))
		switch {
		case o.cause != nil:
			fmt.Fprintf(&what, "the backfill stopped early: %v", o.cause)
		case !o.emitted:
			what.WriteString("the backfill did not finish")
		default:
			what.WriteString("the backfilled rows were not confirmed on the target (its persisted position never reached the change after them)")
		}
	}
	also := ""
	if e.runErr != nil {
		also = fmt.Sprintf(" The attempt also ended with: %v.", e.runErr)
	}
	return fmt.Sprintf("%s: %s: a forwarded ADD COLUMN's backfill did not provably reach the target — %s. "+
		"Rows that existed on the target before the ADD COLUMN may hold the target's own fill for the added column "+
		"(the DEFAULT the forward carried, NULL if none) instead of the source's values, and a restart does NOT resume "+
		"the backfill: the boundary is not seen again.%s Remedy: (1) copy the added column's values from the source to "+
		"the target for the rows that predate the ADD COLUMN (or re-copy the table, e.g. --restart-from-scratch); "+
		"(2) restart with %s=<the fingerprint the next start prints>",
		ir.ErrUnforwardedSchemaChange, addColumnBackfillIncompleteMarker, what.String(), also, unforwardedRefusalAckFlag)
}

func (e *addedColumnBackfillIncompleteError) Unwrap() error            { return ir.ErrUnforwardedSchemaChange }
func (e *addedColumnBackfillIncompleteError) Terminal() bool           { return true }
func (e *addedColumnBackfillIncompleteError) Retriable() bool          { return false }
func (e *addedColumnBackfillIncompleteError) RetryHint() time.Duration { return 0 }

var (
	_ ir.TerminalError  = (*addedColumnBackfillIncompleteError)(nil)
	_ ir.RetriableError = (*addedColumnBackfillIncompleteError)(nil)
)

// addedColumnBackfillSettleTimeout bounds the persisted-position read at
// attempt exit, which runs on a cancel-immune context for the same reason
// the refusal record does: the attempt usually ends by cancellation.
const addedColumnBackfillSettleTimeout = 30 * time.Second

// settleAddedColumnBackfills runs at the end of every attempt (see the file
// comment). It returns runErr unchanged when nothing is owed, and otherwise
// an [addedColumnBackfillIncompleteError] — unless runErr is itself a fresh
// unforwarded-schema-change refusal, which the control-table row (one
// record per stream) keeps instead; the owed backfill is then logged at
// ERROR as not recorded.
func (s *Streamer) settleAddedColumnBackfills(ctx context.Context, applier ir.ChangeApplier, streamID string, runErr error) error {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), addedColumnBackfillSettleTimeout)
	defer cancel()
	var (
		persisted ir.Position
		havePos   bool
		read      bool
	)
	orderer, _ := s.Source.(ir.PositionOrderer)
	owed := s.addedColumnBackfills.settle(func(e *addedColumnBackfillEntry) bool {
		if !e.emitted || e.watermark.Token == "" || orderer == nil || applier == nil {
			return false
		}
		if !read {
			read = true
			pos, ok, err := applier.ReadPosition(readCtx, streamID)
			if err != nil {
				slog.WarnContext(ctx, "added-column backfill: could not read the persisted position to confirm the backfill reached the target; treating it as unconfirmed",
					slog.String("stream_id", streamID), slog.String("error", err.Error()))
			}
			persisted, havePos = pos, ok && err == nil
		}
		if !havePos {
			return false
		}
		// The applier hands its persisted position back tagged with the
		// TARGET engine; the token is the source's. Re-tag it with the
		// watermark's own engine (the same source) before ordering, as
		// warm resume does (retagPositionForSource) — measured: an untagged
		// PG → MySQL compare refused every settle.
		reached, err := orderer.PositionAtOrAfter(retagPositionForSource(persisted, e.watermark.Engine), e.watermark)
		return err == nil && reached
	})
	if len(owed) == 0 {
		return runErr
	}
	gap := &addedColumnBackfillIncompleteError{owed: owed, runErr: runErr}
	if freshUnforwardedRefusal(runErr) {
		slog.ErrorContext(ctx, addColumnBackfillIncompleteMarker+": NOT recorded — this stream is already stopping on another "+
			"unforwarded-schema-change refusal, which its control-table row records instead; repair these rows before "+
			"acknowledging that refusal, because nothing will report them again",
			slog.String("stream_id", streamID), slog.String("owed", gap.Error()))
		return runErr
	}
	slog.ErrorContext(ctx, gap.Error(), slog.String("stream_id", streamID))
	return gap
}
