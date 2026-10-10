// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// EmptyTxRuns collapses each run of consecutive EMPTY source transactions in
// a change stream to the last transaction of the run (Bug 300).
//
// An empty source transaction is an [ir.TxBegin] opened outside any
// transaction and followed immediately by its [ir.TxCommit], nothing between.
// A CDC reader emits one for every source transaction whose changes all fell
// outside the stream's scope: the MySQL binlog is server-wide, so a stream
// over one database sees a BEGIN/XID pair for every transaction of every
// other database on the server; Postgres before 15 decodes a pair for every
// transaction of the database that touched no published table; a table or
// `--where` filter empties a transaction downstream of either; and the
// readers' own rotation and keepalive boundaries are empty pairs by design.
// Each such pair was recorded by `backup incremental` and `backup stream`,
// and committed as its own position write by every serial apply path —
// about 12 ms apiece on the cycle's MySQL target, so a single-database stream
// on a shared server stopped keeping up at roughly 80 foreign transactions a
// second, with no error.
//
// WHAT IS DROPPED, and why it loses nothing. Only an empty transaction that
// is immediately followed by ANOTHER empty transaction. Its sole content is
// its position, and the transaction after it carries a later one at the same
// standing — a boundary with nothing applied in between — so persisting the
// later one says everything the earlier one did. Everything else passes
// through in order, unchanged: every transaction that carries a change, every
// change outside a transaction, and the LAST empty transaction of each run,
// which is the boundary the next real transaction (or the end of the stream)
// follows. That last rule is what keeps each consumer's tail exact:
//
//   - a capture window's EndPosition is still the last position its chunks
//     record (the severed-transaction door's shape C and chain restore's tail
//     backstop compare exactly that);
//   - the boundary a replay persists just before any real transaction is the
//     same boundary as before, so a resume lands where it always did;
//   - nothing is ever dropped from INSIDE a transaction, so the door's shapes
//     A and B, which judge open transactions and row positions, see what they
//     always saw.
//
// It never reorders and never invents an event; it only withholds a begin
// until it knows whether its transaction is empty, and withholds an empty
// pair until it knows whether another follows. [EmptyTxRuns.Flush] releases
// both. A caller that stops without flushing (an error, a cancel) loses
// only boundaries a crash at that point would have lost too.
//
// Not safe for concurrent use; each stream owns one.
type EmptyTxRuns struct {
	// MaxRun bounds how many empty transactions one emitted pair may stand
	// for: once MaxRun of a run have been dropped, the held pair is emitted
	// and the run starts again. Zero is unbounded. A live stream sets it so a
	// source that is busy elsewhere forever still persists a position now and
	// then (the GC-43 (r) purge hazard); a capture or replay, whose stream
	// ends, leaves it unbounded.
	MaxRun int

	// held is the newest empty transaction of the current run (begin,
	// commit), not yet emitted.
	held     [2]ir.Change
	hasHeld  bool
	run      int // empty transactions dropped since the run's last emission
	begin    ir.Change
	hasBegin bool // begin is withheld: a transaction opened, nothing in it yet
	open     bool // a transaction that carries a change is open (and emitted)
	elided   int64
	released int64 // held pairs emitted so far: a change means a new run
}

// Push feeds c and hands emit every change that must go out now, in order. An
// error from emit is returned as is, and the runner is spent: the caller is
// expected to stop.
func (r *EmptyTxRuns) Push(c ir.Change, emit func(ir.Change) error) error {
	switch c.(type) {
	case ir.TxBegin:
		if r.open {
			// A begin inside an open transaction (a MySQL ROLLBACK group, a
			// merged VStream group): verbatim, it is part of that transaction.
			return emit(c)
		}
		if r.hasBegin {
			// The withheld begin's transaction is not empty: its first event
			// is this one.
			if err := r.openWithheld(emit); err != nil {
				return err
			}
			return emit(c)
		}
		r.begin, r.hasBegin = c, true
		return nil
	case ir.TxCommit:
		if r.hasBegin {
			return r.closeEmpty(c, emit)
		}
		// The commit of a transaction that carried a change, or a stray
		// commit: the run (if any) ends before it.
		//
		// PREMISE: a commit closes the WHOLE transaction. A nested begin is
		// passed through verbatim above, but its inner commit would clear
		// open here while an outer transaction was still open, and an empty
		// pair after it would then be collapsible from inside that outer
		// transaction. No reader emits that shape: the MySQL/MariaDB binlog
		// reader emits one TxCommit per XID/COMMIT group, and the one shape
		// that nests a begin (a ROLLBACK group leaves its TxBegin with no
		// commit) is closed by the NEXT group's commit, which ends both — as
		// the appliers read it too; the VStream reader suppresses an
		// interleaved BEGIN and emits only the COMMIT that leaves nothing open
		// (vstreamTxState); pgoutput frames one transaction (or one streamed
		// chunk) at a time; the keepalive and rotation boundaries are emitted
		// only while no transaction is open; the trigger sources emit no
		// markers. Code-read, not pinned against every reader.
		if err := r.releaseHeld(emit); err != nil {
			return err
		}
		r.open = false
		return emit(c)
	}
	if r.hasBegin {
		if err := r.openWithheld(emit); err != nil {
			return err
		}
		return emit(c)
	}
	if !r.open {
		// A change outside any transaction (a MySQL TRUNCATE, a trigger-CDC
		// change, a schema snapshot): the run ends before it.
		if err := r.releaseHeld(emit); err != nil {
			return err
		}
	}
	return emit(c)
}

// Flush emits whatever is withheld — the current run's last empty pair, then
// a begin whose transaction has shown nothing yet — so the stream that
// reaches the consumer ends exactly where the input did.
func (r *EmptyTxRuns) Flush(emit func(ir.Change) error) error {
	if err := r.releaseHeld(emit); err != nil {
		return err
	}
	if r.hasBegin {
		begin := r.begin
		r.begin, r.hasBegin = nil, false
		r.open = true
		return emit(begin)
	}
	return nil
}

// Pending reports whether [EmptyTxRuns.Flush] would emit anything.
func (r *EmptyTxRuns) Pending() bool { return r.hasHeld || r.hasBegin }

// HoldsRun reports whether an empty transaction is held — a boundary that
// would persist a position if released.
func (r *EmptyTxRuns) HoldsRun() bool { return r.hasHeld }

// Releases counts the held pairs emitted so far. A caller timing a run reads
// it around a Push: if it moved, the pair now held (if any) starts a new run.
func (r *EmptyTxRuns) Releases() int64 { return r.released }

// ReleaseRun emits the held empty transaction, ending the current run, and
// keeps a withheld begin withheld: a begin persists nothing, so only the held
// pair is worth releasing to a consumer that is waiting.
func (r *EmptyTxRuns) ReleaseRun(emit func(ir.Change) error) error { return r.releaseHeld(emit) }

// Elided is how many empty transactions have been dropped.
func (r *EmptyTxRuns) Elided() int64 { return r.elided }

// closeEmpty completes the withheld begin as an empty transaction closed by
// commit: it becomes the held pair, and the pair it replaces is dropped —
// unless MaxRun says this run has stood in for enough, in which case that one
// is emitted first.
func (r *EmptyTxRuns) closeEmpty(commit ir.Change, emit func(ir.Change) error) error {
	if r.hasHeld {
		if r.MaxRun > 0 && r.run >= r.MaxRun {
			if err := r.releaseHeld(emit); err != nil {
				return err
			}
		} else {
			r.run++
			r.elided++
		}
	}
	r.held = [2]ir.Change{r.begin, commit}
	r.hasHeld = true
	r.begin, r.hasBegin = nil, false
	return nil
}

// openWithheld emits the held pair and then the withheld begin, whose
// transaction has just shown that it carries something.
func (r *EmptyTxRuns) openWithheld(emit func(ir.Change) error) error {
	if err := r.releaseHeld(emit); err != nil {
		return err
	}
	begin := r.begin
	r.begin, r.hasBegin = nil, false
	r.open = true
	return emit(begin)
}

// ChannelEmit is the emit function for a stream whose consumer reads a
// channel: it sends c to out, and gives up with ctx's error when ctx ends
// first.
func ChannelEmit(ctx context.Context, out chan<- ir.Change) func(ir.Change) error {
	return func(c ir.Change) error {
		select {
		case out <- c:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// releaseHeld emits the held empty pair, if any, ending the run.
func (r *EmptyTxRuns) releaseHeld(emit func(ir.Change) error) error {
	if !r.hasHeld {
		return nil
	}
	pair := r.held
	r.hasHeld, r.held, r.run = false, [2]ir.Change{}, 0
	r.released++
	if err := emit(pair[0]); err != nil {
		return err
	}
	return emit(pair[1])
}
