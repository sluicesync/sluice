// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The add-table pending record's liveness and its abandon path
// (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321 F1/F2).
//
// A pending entry is written before an add-table changes anything and is
// cleared when the table is fully brought in. Between the two the add
// refreshes the entry's heartbeat, so a reader can tell an add that is still
// copying (fresh: `sync health` reports it in progress, a warm resume defers
// with the restartable INCLUDE-TABLE-ADD-IN-PROGRESS) from one that died
// (stale after [ir.PendingAddStaleAfter]: ADD-TABLE INCOMPLETE, and the
// terminal refusal). `--abandon` is how an operator retires an entry no
// add-table can clear: its table was dropped on the source, or taken out of
// the stream's scope.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// startPendingHeartbeat refreshes the pending entry's heartbeat until the
// returned stop is called (which waits for the goroutine to exit). A failed
// refresh WARNs: the entry going stale while the add still runs makes a warm
// resume refuse terminally instead of deferring, which is loud, not lossy.
func (a *AddTable) startPendingHeartbeat(ctx context.Context, applier ir.ChangeApplier) (stop func()) {
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
				if err := store.HeartbeatPendingAddedTable(hbCtx, a.StreamID, a.TableName, time.Now()); err != nil && hbCtx.Err() == nil {
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

// runAbandon is `sluice schema add-table --abandon <table>`: it removes the
// table's pending entry. On a publication source it also takes the table back
// out of the publication when nothing else needs it there — not when the
// stream established it, and not when another sluice slot exists (that
// stream may read it through the same publication). It refuses an entry
// whose heartbeat is fresh: that add-table is still running. The target table
// is left as it is, and may hold a partial copy.
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
	pending, err := store.ReadPendingAddedTables(ctx, a.StreamID)
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
		return fmt.Errorf("pipeline: add-table --abandon: stream %q has no pending add-table for %q", a.StreamID, a.TableName)
	}
	if now := time.Now(); entry.Fresh(now) {
		return fmt.Errorf("pipeline: add-table --abandon: the add-table of %q is still running (heartbeat %s ago); "+
			"let it finish, or stop it and wait %s for its entry to go stale", a.TableName,
			now.Sub(entry.HeartbeatAt).Round(time.Second), ir.PendingAddStaleAfter)
	}

	if scoper, ok := a.Source.(ir.PublicationScoper); ok && status.PublicationName != "" {
		a.Source = scoper.WithPublicationScope(status.PublicationName, activeSlotName(status))
	}
	a.abandonPublicationMembership(ctx, applier)

	if err := store.ClearPendingAddedTables(ctx, a.StreamID, []string{a.TableName}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: add-table --abandon: %w", err))
	}
	slog.WarnContext(ctx, "add-table --abandon: removed the pending add-table record. The target table was left as it "+
		"is and may hold a partial copy: drop or empty it, or bring the table in with `sluice schema add-table` after "+
		"emptying it", slog.String("stream_id", a.StreamID), slog.String("table", a.TableName))
	return nil
}

// abandonPublicationMembership takes an abandoned table out of the
// publication when nothing else needs it there. Best-effort with a WARN: a
// member left behind is streamed onto the target table again, and the next
// warm resume judges it like any other unestablished table.
func (a *AddTable) abandonPublicationMembership(ctx context.Context, applier ir.ChangeApplier) {
	dropper, ok := a.Source.(publicationTableDropper)
	checker, ok2 := a.Source.(publicationMembershipChecker)
	if !ok || !ok2 {
		return
	}
	if rec, err := readEstablishedRecord(ctx, applier, a.StreamID); err == nil && toSet(rec.Established)[a.TableName] {
		return // the stream established it; it belongs in the publication
	}
	m, err := checker.UnpublishedTables(ctx, a.SourceDSN, []string{a.TableName})
	if err != nil || m.Publication == "" || len(m.Missing) == 1 || m.AllTables {
		return
	}
	if m.Shared {
		slog.WarnContext(ctx, "add-table --abandon: left the table in the source publication, because another sluice "+
			"slot exists and may read it through the same publication", slog.String("table", a.TableName))
		return
	}
	if err := dropper.DropPublicationTables(ctx, a.SourceDSN, []string{a.TableName}); err != nil {
		slog.WarnContext(ctx, "add-table --abandon: could not take the table out of the source publication",
			slog.String("table", a.TableName), slog.String("error", err.Error()))
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
