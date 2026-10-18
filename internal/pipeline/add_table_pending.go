// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The add-table pending record's lifecycle and its abandon path
// (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321).
//
// A pending entry is written, under a token unique to the run, before an
// add-table changes anything, and is completed — removed, with the table
// recorded as established, in one transaction — once the table is fully
// brought in. Between the two the add refreshes the entry's heartbeat, so a
// reader can tell an add that is still copying (fresh: `sync health` reports
// it in progress, and a warm resume defers that table with the restartable
// INCLUDE-TABLE-ADD-IN-PROGRESS) from one that died (stale after
// [ir.PendingAddStaleAfter]: ADD-TABLE INCOMPLETE, and the terminal
// refusal). Every stamp and comparison is the control-table server's clock.
//
// Every write after the first compares-and-sets on the token. Without it an
// add that ran on after `--abandon` retired its entry (it looked dead — a
// stalled host, a paused VM) would heartbeat a fresh entry back, or record a
// table as established that the operator had just been told was abandoned
// and may since have emptied. With it, the run stops loudly
// ([ir.ErrPendingAddSuperseded]): a heartbeat that finds its entry gone or
// taken over cancels the copy, and the completion records nothing.
//
// `--abandon` is how an operator retires an entry no add-table can clear: its
// table was dropped on the source, or taken out of the stream's scope. It marks
// the entry ABANDONED; a stream start clears it once it proves nothing is
// applied onto the table, and refuses otherwise.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// newPendingToken returns a token no other add-table run will draw: the start
// time (for an operator reading the row) and 64 random bits.
func newPendingToken() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails (Go 1.24+)
	return time.Now().UTC().Format("20060102T150405.000Z") + "-" + hex.EncodeToString(b[:])
}

// bringInTableUnderHeartbeat runs [AddTable.bringInTable] while the pending
// entry's heartbeat runs. A heartbeat that finds the entry abandoned or taken
// over cancels the copy, and the returned error then wraps
// [ir.ErrPendingAddSuperseded].
func (a *AddTable) bringInTableUnderHeartbeat(ctx context.Context, applier ir.ChangeApplier, preflight addTablePreflight, scoped *ir.Schema, sw ir.SchemaWriter, rw ir.RowWriter) error {
	copyCtx, cancelCopy := context.WithCancelCause(ctx)
	defer cancelCopy(nil)
	stopHeartbeat := a.startPendingHeartbeat(copyCtx, applier, cancelCopy)
	defer stopHeartbeat()
	err := a.bringInTable(copyCtx, applier, preflight, scoped, sw, rw)
	if cause := context.Cause(copyCtx); err != nil && errors.Is(cause, ir.ErrPendingAddSuperseded) {
		return cause
	}
	return err
}

// startPendingHeartbeat refreshes the pending entry's heartbeat until the
// returned stop is called (which waits for the goroutine to exit). A refresh
// that finds the entry gone or under another token cancels the copy through
// cancelCopy. Any other failed refresh WARNs: the entry going stale while the
// add still runs makes a warm resume refuse terminally instead of deferring,
// which is loud, not lossy — and the completion's token check still holds.
func (a *AddTable) startPendingHeartbeat(ctx context.Context, applier ir.ChangeApplier, cancelCopy context.CancelCauseFunc) (stop func()) {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return func() {}
	}
	every := a.pendingHeartbeatEvery
	if every <= 0 {
		every = ir.PendingAddHeartbeatInterval
	}
	hbCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-tick.C:
				err := store.TouchPendingAddedTable(hbCtx, a.StreamID, a.TableName, a.pendingToken, true)
				switch {
				case err == nil || hbCtx.Err() != nil:
				case errors.Is(err, ir.ErrPendingAddSuperseded):
					cancelCopy(err)
					return
				default:
					slog.WarnContext(hbCtx, "add-table: could not refresh the pending record's heartbeat",
						slog.String("table", a.TableName), slog.String("error", err.Error()))
				}
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// recordCompletedAdd completes the pending entry once the table is fully
// brought in: removes it and records the table as established, in one
// transaction, if the entry is still this run's. A failure is reported, not
// swallowed: the table IS copied, but a pending entry left behind makes the
// next warm resume refuse with INCLUDE-TABLE-SCOPE-DRIFT, and the operator
// should know why.
func (a *AddTable) recordCompletedAdd(ctx context.Context, applier ir.ChangeApplier) error {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return nil
	}
	err := store.CompletePendingAddedTable(ctx, a.StreamID, a.TableName, a.pendingToken)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ir.ErrPendingAddSuperseded):
		return a.supersededError(err)
	}
	return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
		"pipeline: add-table: %q was copied and handed to the stream, but completing its pending record failed, "+
			"so the next `sync start` will refuse INCLUDE-TABLE-SCOPE-DRIFT naming it until the record is "+
			"cleared (re-running add-table needs the target table emptied first; --restart-from-scratch "+
			"re-copies it): %w", a.TableName, err,
	))
}

// supersededError is the add's loud stop when its pending entry was removed
// or taken over while it ran. Nothing is undone and nothing is recorded: the
// entry, and the publication membership, now belong to whoever retired or
// replaced it.
func (a *AddTable) supersededError(cause error) error {
	return fmt.Errorf("pipeline: add-table: the pending add-table record of %q was abandoned (`sluice schema "+
		"add-table --abandon`) or taken over by another add-table run while this one ran, so this run stopped "+
		"without recording the table as established; the target table may hold a partial copy — drop or empty it "+
		"before bringing the table in again: %w", a.TableName, cause)
}

// undoFailedAdd is the add's failure path after the pending record (Bug 321
// path 2). The record stays, so the next warm resume refuses rather than
// streaming into a table whose copy did not land. On a publication source it
// also takes back the publication add this run made, so a LIVE stream stops
// streaming the table's changes into that partial copy now rather than at
// its next restart. Both steps are skipped when the entry is no longer this
// run's (it was abandoned or taken over: the publication is someone else's
// now). Runs on a context detached from cancellation: a Ctrl-C is the
// commonest way to get here. The returned error names what happened.
func (a *AddTable) undoFailedAdd(ctx context.Context, applier ir.ChangeApplier, cause error) error {
	note := fmt.Sprintf("the add of %q did not complete", a.TableName)
	undoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if store, ok := applier.(ir.PendingAddedTablesStore); ok {
		// This add is not running any more: age its heartbeat out now, so a
		// restart reads it as dead (terminal) at once rather than as an add
		// in progress (restartable) for the next PendingAddStaleAfter. A kill
		// cannot do this; the heartbeat simply stops and ages out.
		err := store.TouchPendingAddedTable(undoCtx, a.StreamID, a.TableName, a.pendingToken, false)
		if errors.Is(err, ir.ErrPendingAddSuperseded) {
			return a.supersededError(fmt.Errorf("%w (%s): %w", cause, note, err))
		}
		if err != nil {
			slog.WarnContext(ctx, "add-table: could not mark the failed add's pending record as no longer running; "+
				"a restart in the next few minutes defers as INCLUDE-TABLE-ADD-IN-PROGRESS", slog.String("error", err.Error()))
		}
		note += "; it stays recorded as pending, so `sync start` refuses INCLUDE-TABLE-SCOPE-DRIFT for it until " +
			"a re-run of add-table succeeds (empty the target table first if rows landed), `sluice schema add-table " +
			"--abandon` retires it, or a --restart-from-scratch re-copies it"
	}
	if dropper, ok := a.Source.(publicationTableDropper); ok && a.publicationAddedByThisRun {
		if err := dropper.DropPublicationTables(undoCtx, a.SourceDSN, []string{a.TableName}); err != nil {
			slog.WarnContext(ctx, "add-table: could not take the table back out of the source publication after the "+
				"add failed; a running stream keeps streaming its changes into the partial target copy until the "+
				"table is removed from the publication by hand", slog.String("table", a.TableName), slog.String("error", err.Error()))
			note += "; taking it back out of the source publication FAILED (" + err.Error() + ")"
		} else {
			note += "; it was taken back out of the source publication"
		}
	}
	return fmt.Errorf("%w (%s)", cause, note)
}

// runAbandon is `sluice schema add-table --abandon <table>`: it marks the
// table's pending entry ABANDONED rather than removing it (R4-1: a running
// stream may still be applying onto the partial copy, and the entry is what
// keeps `sync health` red until a restart judges the table — see
// [ir.PendingAddedTable.Abandoned]). On a publication source it also takes the table back
// out of the publication when nothing else needs it there — not when the
// stream established it, and not when another sluice slot on the same
// database exists (that stream may read it through the same publication) —
// and drops the dead add's snapshot slot (`sluice_addtable_<table>`, or the
// --slot-name given), which otherwise pins source WAL and makes the next
// add-table of the table refuse "slot already exists". It refuses an entry
// whose heartbeat is fresh by the control-table server's clock: that
// add-table is still running. The target table is left as it is, and may
// hold a partial copy.
func (a *AddTable) runAbandon(ctx context.Context) error {
	applier, err := a.Target.OpenChangeApplier(ctx, a.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table --abandon: open target applier: %w", err))
	}
	defer migcore.CloseIf(applier)
	status, found, err := findStream(ctx, applier, a.StreamID)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table --abandon: %w", err))
	}
	if !found {
		return fmt.Errorf("pipeline: add-table --abandon: no stream %q on the target", a.StreamID)
	}
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return errors.New("pipeline: add-table --abandon: the target keeps no pending add-table record")
	}
	pending, now, err := store.ReadPendingAddedTables(ctx, a.StreamID)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table --abandon: %w", err))
	}
	var entry *ir.PendingAddedTable
	for i := range pending {
		if pending[i].Table == a.TableName {
			entry = &pending[i]
		}
	}
	if entry == nil {
		return a.abandonCompletedAdd(ctx, applier)
	}
	if entry.Fresh(now) {
		return fmt.Errorf("pipeline: add-table --abandon: the add-table of %q is still running (heartbeat %s ago); "+
			"let it finish, or stop it and wait %s for its entry to go stale", a.TableName,
			now.Sub(entry.HeartbeatAt).Round(time.Second), ir.PendingAddStaleAfter)
	}
	warnFutureHeartbeat(ctx, *entry, now)

	if scoper, ok := a.Source.(ir.PublicationScoper); ok && status.PublicationName != "" {
		a.Source = scoper.WithPublicationScope(status.PublicationName, activeSlotName(status))
	}
	if err := a.abandonPublicationMembership(ctx, applier); err != nil {
		return err
	}
	a.dropAbandonedSnapshotSlot(ctx)
	if err := a.removeFromLiveAdded(ctx, applier); err != nil {
		return err
	}

	if err := store.AbandonPendingAddedTable(ctx, a.StreamID, a.TableName); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: add-table --abandon: %w", err))
	}
	slog.WarnContext(ctx, "add-table --abandon: marked the pending add-table record ABANDONED. RESTART THE STREAM: a "+
		"running stream may still be applying changes onto this table's partial copy, and nothing re-checks a running "+
		"stream. On restart the record is cleared if the table is out of the stream's scope or its changes are not "+
		"applied; otherwise the start refuses INCLUDE-TABLE-SCOPE-DRIFT. Until then `sync health` exits 1 with ADD-TABLE "+
		"ABANDONED. The target table was left as it is and may hold a partial copy: drop or empty it, or bring the table "+
		"in with `sluice schema add-table` after emptying it", slog.String("stream_id", a.StreamID), slog.String("table", a.TableName))
	return nil
}

// warnFutureHeartbeat WARNs about a pending entry whose heartbeat is ahead of
// the control-table server's clock by more than [ir.PendingAddFutureTolerance]:
// it counts as stale, but only a clock that went backwards produces one.
func warnFutureHeartbeat(ctx context.Context, p ir.PendingAddedTable, now time.Time) {
	if !p.FromTheFuture(now) {
		return
	}
	slog.WarnContext(ctx, "pending add-table entry carries a heartbeat ahead of the control-table server's clock; "+
		"treating it as stale (no running add-table stamps the future — the server's clock went backwards)",
		slog.String("table", p.Table), slog.Time("heartbeat_at", p.HeartbeatAt), slog.Time("server_now", now))
}

// abandonCompletedAdd is `--abandon` of a table whose add-table completed:
// it has no pending entry, but it is in the stream's live-added set, which
// keeps it in scope on every start whatever --include-table says. Taking it
// out of that set is the only way an include-mode stream (which cannot also
// pass --exclude-table) lets go of an added table (Bug 321 R6 A). The next
// start narrows it: its changes stop being applied, and the record moves it
// to the dropped set, so a later re-admit refuses until it is copied again.
func (a *AddTable) abandonCompletedAdd(ctx context.Context, applier ir.ChangeApplier) error {
	reader, ok := applier.(liveAddedTablesReader)
	if ok {
		added, err := reader.ReadLiveAddedTables(ctx, a.StreamID)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table --abandon: %w", err))
		}
		ok = toSet(added)[a.TableName]
	}
	if !ok {
		return fmt.Errorf("pipeline: add-table --abandon: stream %q has no pending add-table for %q, and the table is not in "+
			"its live-added set", a.StreamID, a.TableName)
	}
	if err := a.removeFromLiveAdded(ctx, applier); err != nil {
		return err
	}
	slog.WarnContext(ctx, "add-table --abandon: took the table out of the stream's live-added set. RESTART THE STREAM: on "+
		"its next start the table leaves the stream's scope unless --include-table names it — its changes are no longer "+
		"applied, and bringing it back later refuses until it is copied again. The target table is left as it is",
		slog.String("stream_id", a.StreamID), slog.String("table", a.TableName))
	return nil
}

// removeFromLiveAdded takes the table out of the stream's live-added set.
// A target without the set has nothing to remove.
func (a *AddTable) removeFromLiveAdded(ctx context.Context, applier ir.ChangeApplier) error {
	remover, ok := applier.(liveAddedTablesRemover)
	if !ok {
		return nil
	}
	if err := remover.RemoveLiveAddedTable(ctx, a.StreamID, a.TableName); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: add-table --abandon: take %q out of the "+
			"stream's live-added set: %w", a.TableName, err))
	}
	return nil
}

// abandonPublicationMembership takes an abandoned table out of the
// publication when nothing else needs it there. Best-effort with a WARN: a
// member left behind is streamed onto the target table again, and the next
// warm resume judges it like any other unestablished table. It REFUSES —
// before anything is changed — when the stream's record cannot be read
// (R6 S2): an undecodable record is not "the stream did not establish it",
// and taking an established table out of the publication would stop its
// changes at exit 0.
func (a *AddTable) abandonPublicationMembership(ctx context.Context, applier ir.ChangeApplier) error {
	dropper, ok := a.Source.(publicationTableDropper)
	checker, ok2 := a.Source.(publicationMembershipChecker)
	if !ok || !ok2 {
		return nil
	}
	rec, err := readEstablishedRecord(ctx, applier, a.StreamID)
	if err != nil {
		return fmt.Errorf("pipeline: add-table --abandon: cannot read the stream's established-table record, so it cannot "+
			"tell whether the stream established %q and must keep it in the publication; nothing was changed: %w", a.TableName, err)
	}
	if toSet(rec.Established)[a.TableName] {
		return nil // the stream established it; it belongs in the publication
	}
	m, err := checker.UnpublishedTables(ctx, a.SourceDSN, []string{a.TableName})
	if err != nil {
		// Best-effort, and this direction is safe: a member left behind is
		// judged at the next start like any other unestablished table.
		slog.WarnContext(ctx, "add-table --abandon: could not read the publication's membership; the table is left in it",
			slog.String("table", a.TableName), slog.String("error", err.Error()))
		return nil //nolint:nilerr // best-effort, see above
	}
	if m.Publication == "" || len(m.Missing) == 1 || m.AllTables {
		return nil
	}
	if m.Shared {
		slog.WarnContext(ctx, "add-table --abandon: left the table in the source publication, because another sluice "+
			"slot exists on the source database and may read it through the same publication", slog.String("table", a.TableName))
		return nil
	}
	if err := dropper.DropPublicationTables(ctx, a.SourceDSN, []string{a.TableName}); err != nil {
		slog.WarnContext(ctx, "add-table --abandon: could not take the table out of the source publication",
			slog.String("table", a.TableName), slog.String("error", err.Error()))
	}
	return nil
}

// dropAbandonedSnapshotSlot drops the dead add's snapshot slot. The slot is
// persistent, so an add killed mid-copy leaves it behind. (A
// protocol-TEMPORARY slot would die with the add's session, as the backup
// anchor's does; the add-table snapshot shares the cold start's persistent
// slot-creating open instead — a residual filed in docs/dev/audit-backlog.md
// as ADD-TABLE-SNAPSHOT-SLOT-PERSISTENT.) Only an inactive slot is dropped; a
// missing one is the goal state. Best-effort with a WARN naming `sluice slot
// drop`.
func (a *AddTable) dropAbandonedSnapshotSlot(ctx context.Context) {
	if _, ok := a.Source.(snapshotSlotOpener); !ok {
		return
	}
	opener, ok := a.Source.(slotDropper)
	if !ok {
		return
	}
	slot := a.tempSlotName()
	mgr, err := opener.OpenSlotManager(ctx, a.SourceDSN)
	if err == nil {
		defer migcore.CloseIf(mgr)
		err = mgr.Drop(ctx, slot, false)
	}
	switch {
	case err == nil:
		slog.InfoContext(ctx, "add-table --abandon: dropped the dead add's snapshot slot", slog.String("slot", slot))
	case errors.Is(err, ir.ErrSlotNotFound):
	default:
		slog.WarnContext(ctx, "add-table --abandon: could not drop the dead add's snapshot slot; it pins source WAL, "+
			"and the next add-table of this table refuses while it exists — drop it with `sluice slot drop`",
			slog.String("slot", slot), slog.String("error", err.Error()))
	}
}

// findStream reads streamID's control row.
func findStream(ctx context.Context, applier ir.ChangeApplier, streamID string) (ir.StreamStatus, bool, error) {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return ir.StreamStatus{}, false, err
	}
	for _, st := range streams {
		if st.StreamID == streamID {
			return st, true, nil
		}
	}
	return ir.StreamStatus{}, false, nil
}
