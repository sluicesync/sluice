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
// # The write-ahead record (a process that never reaches its exit path)
//
// The settle above runs on the attempt's exit path, so a process that dies
// without running it — SIGKILL, an OOM kill, power loss — would settle
// nothing, and its restart would resume past the boundary silently. So the
// owed backfill is ALSO written to the target before it can be lost: an
// ADD-COLUMN-BACKFILL-INCOMPLETE refusal, in the same control-table record
// the startup door reads (sluice_cdc_state.unforwarded_refusal — no new
// column), written BEFORE the target ALTER (the single-stream forward's
// AlterAddColumn; on Shape A the lease holder's apply) and, for a Shape A
// stream that only observes a peer's ALTER, before its boundary's snapshot
// is forwarded. It is removed only by a compare-and-clear once every
// backfill it covers is proven durable by the same evidence the settle
// uses: the record is cleared only while it still holds EXACTLY the text
// this ledger wrote, so a genuine UNFORWARDED-SCHEMA-CHANGE or the
// attempt-end record that replaced it is never cleared. A record that
// cannot be written refuses the boundary: forwarding without it is the
// silent restart this exists to prevent.
//
// The proof is checked at the attempt's end and, mid-run, every
// [addedColumnBackfillDurabilityInterval] by
// [Streamer.watchAddedColumnBackfillDurability], so a long-lived stream
// killed hours after a completed backfill does not come back refusing. The
// false-refusal window that remains after a hard kill is bounded by that
// interval plus the applier's checkpoint lag past the first change after
// the backfill — and a crash between the write and the ALTER, or an ALTER
// that fails, leaves the record too (whether the ALTER landed is not known,
// so the restart refuses). Both are conservative: a false refusal costs one
// acknowledged restart.
//
// # The residual, stated
//
//   - An applier without [ir.UnforwardedRefusalStore] (a unit-test stub;
//     every real applier implements it) gets no write-ahead, exactly as the
//     startup door reads nothing from it.
//   - On the Shape A fan-in, a peer shard's ALTER fills THIS shard's rows
//     before this stream has seen its own boundary; a hard kill in that
//     window predates any record this stream could write.
//   - The compare-and-clear is a read then a clear, not one conditional
//     statement. Every other in-process writer of the record is ordered
//     against it ([addedColumnBackfillLedger.recordMu], and the attempt-end
//     recorder runs after the mid-run watch has stopped); a second PROCESS
//     writing the same stream's row at that instant is not.

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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

	// The write-ahead record (see the file comment), guarded by mu. store
	// and streamID are bound per attempt ([addedColumnBackfillLedger.bind]);
	// waStored is the exact text this ledger last recorded, "" when none of
	// its records is outstanding; waPending are write-aheads whose boundary
	// has not been entered yet (its ALTER is being applied).
	store     ir.UnforwardedRefusalStore
	streamID  string
	waStored  string
	waPending []addedColumnBackfillEntry

	// recordMu orders every write of the record against the
	// compare-and-clear, so a clear never removes a record written after
	// its read.
	recordMu sync.Mutex
}

// addedColumnBackfillEntry is one owed backfill.
type addedColumnBackfillEntry struct {
	table   string
	columns []string

	// writeAhead is true when the durable write-ahead record covers it.
	writeAhead bool

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
	if i := slices.IndexFunc(l.waPending, func(p addedColumnBackfillEntry) bool { return p.table == table }); i >= 0 {
		e.writeAhead = true
		l.waPending = slices.Delete(l.waPending, i, i+1)
	}
	l.entries = append(l.entries, e)
	return e
}

// bind points the write-ahead record at this attempt's applier and stream.
// An applier without [ir.UnforwardedRefusalStore] binds none.
func (l *addedColumnBackfillLedger) bind(applier ir.ChangeApplier, streamID string) {
	store, _ := applier.(ir.UnforwardedRefusalStore)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.store, l.streamID = store, streamID
}

// writeAhead durably records, on the target, that table's added columns owe
// a backfill — before the ALTER that makes them owed (see the file
// comment). Idempotent per table until the boundary is entered, so the
// pre-ALTER call and the plan-time call of one boundary write once. The
// record names every backfill still outstanding, since it replaces the
// previous one. An error means the record is NOT on the target, and the
// caller refuses the boundary. nil-safe.
func (l *addedColumnBackfillLedger) writeAhead(ctx context.Context, table string, columns []string) error {
	if l == nil {
		return nil
	}
	l.recordMu.Lock()
	defer l.recordMu.Unlock()
	l.mu.Lock()
	store, streamID := l.store, l.streamID
	if slices.ContainsFunc(l.waPending, func(p addedColumnBackfillEntry) bool { return p.table == table }) {
		l.mu.Unlock()
		return nil
	}
	covered := slices.Clone(l.waPending)
	for _, e := range l.entries {
		if e.writeAhead {
			covered = append(covered, addedColumnBackfillEntry{table: e.table, columns: e.columns})
		}
	}
	l.mu.Unlock()
	if store == nil {
		return nil
	}
	covered = append(covered, addedColumnBackfillEntry{table: table, columns: columns})
	stored := recordedUnforwardedRefusalText(&addedColumnBackfillIncompleteError{owed: covered, writeAhead: true})
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unforwardedRefusalWriteTimeout)
	defer cancel()
	if err := store.RecordUnforwardedRefusal(writeCtx, streamID, stored); err != nil {
		return fmt.Errorf("%s: could not record the owed added-column backfill on the target before its ALTER "+
			"(stream %q, sluice_cdc_state.unforwarded_refusal); forwarding without that record would let a process "+
			"killed during the backfill restart past it silently, so the boundary is refused: %w",
			addColumnBackfillIncompleteMarker, streamID, err)
	}
	l.mu.Lock()
	l.waStored = stored
	l.waPending = append(l.waPending, addedColumnBackfillEntry{table: table, columns: columns})
	l.mu.Unlock()
	slog.InfoContext(ctx, "forward-add-column: recorded the owed backfill on the target before the ALTER; "+
		"it is cleared once the backfill is confirmed there",
		slog.String("stream_id", streamID), slog.String("table", table), slog.Any("added_columns", columns))
	return nil
}

// outstanding reports whether anything is left to settle or clear.
func (l *addedColumnBackfillLedger) outstanding() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries) > 0 || l.waStored != ""
}

// clearWriteAheadIfSettled removes the write-ahead record once nothing it
// covers is owed any more — compare-and-clear: only while the target still
// holds exactly the text this ledger wrote. A record another refusal (or
// the attempt-end ADD-COLUMN-BACKFILL-INCOMPLETE) replaced is left alone. A
// failed read or clear is logged and retried at the next settle.
func (l *addedColumnBackfillLedger) clearWriteAheadIfSettled(ctx context.Context) {
	if l == nil {
		return
	}
	l.recordMu.Lock()
	defer l.recordMu.Unlock()
	l.mu.Lock()
	stored, store, streamID := l.waStored, l.store, l.streamID
	settled := stored != "" && len(l.waPending) == 0 &&
		!slices.ContainsFunc(l.entries, func(e *addedColumnBackfillEntry) bool { return e.writeAhead })
	l.mu.Unlock()
	if !settled || store == nil {
		return
	}
	c, cancel := context.WithTimeout(context.WithoutCancel(ctx), addedColumnBackfillSettleTimeout)
	defer cancel()
	cur, found, err := store.ReadUnforwardedRefusal(c, streamID)
	if err == nil && found && cur == stored {
		err = store.ClearUnforwardedRefusal(c, streamID)
	}
	if err != nil {
		slog.WarnContext(ctx, "added-column backfill: could not clear the settled write-ahead record; retrying at the next settle "+
			"(if this process ends first, its restart refuses with "+addColumnBackfillIncompleteMarker+" although the backfill completed)",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return
	}
	l.mu.Lock()
	if l.waStored == stored {
		l.waStored = ""
	}
	l.mu.Unlock()
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

	// writeAhead marks the durable write-ahead record's text: written
	// before the ALTER, found by a restart only when the process that wrote
	// it never settled it.
	writeAhead bool
}

func (e *addedColumnBackfillIncompleteError) Error() string {
	var what strings.Builder
	for i, o := range e.owed {
		if i > 0 {
			what.WriteString("; ")
		}
		fmt.Fprintf(&what, "%s (%s): ", o.table, strings.Join(o.columns, ", "))
		switch {
		case e.writeAhead:
			what.WriteString("the process ended while this backfill was owed, without settling it (this record is written " +
				"before the target ALTER and cleared once the backfill is confirmed on the target, so a restart that finds it " +
				"follows a hard kill, an OOM kill or power loss during the backfill — or an ALTER that failed or was interrupted)")
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
// comment). It returns runErr unchanged when nothing is owed — clearing the
// write-ahead record, which then covers nothing — and otherwise
// an [addedColumnBackfillIncompleteError] — unless runErr is itself a fresh
// unforwarded-schema-change refusal, which the control-table row (one
// record per stream) keeps instead; the owed backfill is then logged at
// ERROR as not recorded.
func (s *Streamer) settleAddedColumnBackfills(ctx context.Context, applier ir.ChangeApplier, streamID string, runErr error) error {
	if s.simulateHardKillForTest {
		// A hard kill runs no exit path: nothing is settled, recorded or
		// cleared, so the restart sees only what was already on the target.
		return runErr
	}
	owed := s.settleDurableAddedColumnBackfills(ctx, applier, streamID)
	if len(owed) == 0 {
		s.addedColumnBackfills.clearWriteAheadIfSettled(ctx)
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

// addedColumnBackfillDurabilityInterval is how often a running attempt
// checks whether its owed backfills have become durable, so the
// write-ahead record is cleared while the stream runs rather than only when
// it stops (see the file comment).
const addedColumnBackfillDurabilityInterval = 10 * time.Second

// watchAddedColumnBackfillDurability runs the mid-run half of the settle
// for one attempt: every [addedColumnBackfillDurabilityInterval], while
// anything is outstanding, it drops the backfills proven durable and clears
// the write-ahead record once none it covers remains. Nothing is reported
// as owed here — that is the attempt-end settle's job. The returned stop
// waits for the watch to exit, so it must run before that settle.
func (s *Streamer) watchAddedColumnBackfillDurability(ctx context.Context, applier ir.ChangeApplier, streamID string) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := addedColumnBackfillDurabilityInterval
		if s.backfillDurabilityIntervalForTest > 0 {
			interval = s.backfillDurabilityIntervalForTest
		}
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if !s.addedColumnBackfills.outstanding() {
				continue
			}
			s.settleDurableAddedColumnBackfills(ctx, applier, streamID)
			s.addedColumnBackfills.clearWriteAheadIfSettled(ctx)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// settleDurableAddedColumnBackfills drops from the ledger every backfill
// proven to have reached the target — finished, and the applier's persisted
// position (read back from the target) at or past its watermark — and
// returns copies of the rest.
func (s *Streamer) settleDurableAddedColumnBackfills(ctx context.Context, applier ir.ChangeApplier, streamID string) []addedColumnBackfillEntry {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), addedColumnBackfillSettleTimeout)
	defer cancel()
	var (
		persisted ir.Position
		havePos   bool
		read      bool
	)
	orderer, _ := s.Source.(ir.PositionOrderer)
	return s.addedColumnBackfills.settle(func(e *addedColumnBackfillEntry) bool {
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
}
