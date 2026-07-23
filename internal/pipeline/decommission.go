// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// `sluice sync decommission` — retiring a FINISHED stream's durable
// footprint (audit 2026-07-23 DEVEX-3 / open question Q3).
//
// A stream that has served its purpose (a cut-over wave, an abandoned
// experiment) leaves three durable objects behind: its replication
// slot and its per-stream publication on the SOURCE, and its control
// row on the TARGET. The slot is the expensive one — it pins WAL for
// the rest of a multi-week migration (slot invalidation has been
// field-observed at a few hundred MB on small managed instances) and,
// since the v0.99.289 existence-semantics guard, blocks every later
// differently-scoped cold start. Before this command the remedy was
// raw SQL in the staged-wave guide.
//
// The orchestration is engine-neutral: everything routes through
// [ir.ChangeApplier]/[ir.StreamCleaner] on the target and
// [ir.SlotManager]/[ir.StreamPublicationDropper] on the source.
// Engines without source-side objects (the MySQL family: the binlog
// is the stream; trigger-CDC flavors: the change-log table and
// triggers are SHARED across streams and deliberately never dropped
// here — `sluice trigger prune` bounds their growth and
// `sluice trigger teardown` removes them once NO streams remain) get
// the control-row-only path, which is still the useful half: it
// retires the stream id and its warm-resume state.
//
// Partial-failure posture: source-side removals are best-effort in
// order (slot, then publication) so each run makes maximal progress,
// but the control row is cleared ONLY when both source-side steps
// finished — the row is the record of the slot/publication names, and
// keeping it is what lets an idempotent re-run complete the rest.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// DecommissionReport says exactly what a decommission run removed,
// found already gone, or deliberately left alone — the loud-failure
// tenet's reporting half: an operator must never have to guess what
// state the source was left in.
type DecommissionReport struct {
	StreamID        string
	SlotName        string // as recorded on the control row; "" = none recorded
	PublicationName string // as recorded on the control row; "" = none recorded
	DryRun          bool   // when true, "Dropped/Cleared" mean "would drop/clear"

	SlotDropped       bool
	SlotAlreadyAbsent bool
	SlotSkipped       string // non-empty reason when no slot removal applies

	PublicationDropped       bool
	PublicationAlreadyAbsent bool
	PublicationSkipped       string // non-empty reason when no publication removal applies

	ControlRowCleared bool
}

// Slot-drop retry budget for the lingering-walsender window: a stream
// stopped moments ago can hold its slot "active" for a beat after the
// client connection died (the same reap race
// startReplicationWithSlotActiveRetry documents on the START_
// REPLICATION side), and pg_drop_replication_slot refuses with
// SQLSTATE 55006 through that window. Same attempts/backoff shape as
// that idiom; a slot still active past the budget is a genuinely live
// consumer and surfaces as the coded refusal. Vars, not consts, so
// unit tests can compress the backoff.
var (
	decommissionDropAttempts    = 8
	decommissionDropBaseBackoff = 500 * time.Millisecond
	decommissionDropMaxBackoff  = 8 * time.Second
)

// DecommissionStream retires the named stream's durable footprint:
// verify the stream exists (control row) and is not running (slot not
// active), drop its replication slot and RECORDED per-stream
// publication on the source, then clear its control row on the
// target. slots is nil for source engines without replication slots
// (the MySQL family, trigger-CDC flavors) — only the control row is
// cleared then.
//
// dryRun performs every check and existence probe but mutates
// nothing; the report's Dropped/Cleared flags then mean "would".
// Refusals (unknown stream, active slot, an applier that cannot clear
// control rows) return a nil report — nothing was, or would have
// been, done. A partial failure returns BOTH the report of what did
// happen and the error naming what didn't; re-running completes the
// rest.
func DecommissionStream(ctx context.Context, applier ir.ChangeApplier, slots ir.SlotManager, streamID string, dryRun bool) (*DecommissionReport, error) {
	// Preconditions first, mutations after — a refusal must leave every
	// object untouched.
	//
	// (a) The stream must be recorded on the target. The control row is
	// the only record of the slot/publication names, so without it
	// there is nothing safe to remove ("already decommissioned" and
	// "never existed" are indistinguishable here, deliberately).
	st, rowExists, err := readRecordedPublicationState(ctx, applier, streamID)
	if err != nil {
		return nil, fmt.Errorf("pipeline: decommission: read the target's control table: %w", err)
	}
	if !rowExists {
		return nil, fmt.Errorf(
			"pipeline: decommission: no stream %q is recorded on the target — nothing to decommission "+
				"(run `sluice sync status` against this target to list the streams it knows; "+
				"an already-decommissioned stream no longer appears there)",
			streamID,
		)
	}
	cleaner, canClear := applier.(ir.StreamCleaner)
	if !canClear {
		// Checked BEFORE any source-side drop: if the run can't finish
		// (clear the row), it must not start (drop the slot) — a
		// half-decommissioned stream with no error would be worse than
		// a clean refusal.
		return nil, errors.New("pipeline: decommission: the target engine's change applier does not support clearing the cdc-state row; remove the stream's control row manually")
	}

	rep := &DecommissionReport{
		StreamID:        streamID,
		SlotName:        st.SlotName,
		PublicationName: st.PublicationName,
		DryRun:          dryRun,
	}

	// ---- (b)+(c): the replication slot on the source ----
	var firstErr error
	switch {
	case slots == nil:
		rep.SlotSkipped = "the source engine has no replication slots (the binlog/change-log is the stream); nothing durable to remove on the source"
	case st.SlotName == "":
		// A legacy control row (pre-slot_name column) or a slotless
		// source flavor recorded through a slot-capable engine name.
		// Refuse to guess a name — dropping the engine DEFAULT slot on
		// a hunch could take out a different stream. Same posture as
		// the publication below.
		rep.SlotSkipped = "the control row records no slot name (a legacy row from an older sluice); no slot was dropped — check `sluice slot list` and drop any leftover with `sluice slot drop`"
	default:
		slot, err := findSlot(ctx, slots, st.SlotName)
		switch {
		case err != nil:
			firstErr = fmt.Errorf("pipeline: decommission: list source replication slots: %w", err)
		case slot == nil:
			rep.SlotAlreadyAbsent = true
		case slot.Active:
			// The stream is (or looks) live. Decommissioning a running
			// stream is an operator error; refuse before touching
			// anything.
			return nil, decommissionActiveRefusal(streamID, st.SlotName)
		case dryRun:
			rep.SlotDropped = true
		default:
			if err := dropSlotWithActiveRetry(ctx, slots, st.SlotName); err != nil {
				switch {
				case isSlotActiveShapeErr(err):
					// Inactive at the pre-check, still held past the
					// whole retry budget: a consumer re-attached under
					// us. Same refusal as the pre-check, nothing else
					// touched.
					return nil, decommissionActiveRefusal(streamID, st.SlotName)
				case isSlotGoneShapeErr(err):
					// Raced with a manual drop between List and Drop —
					// the goal state either way.
					rep.SlotAlreadyAbsent = true
				default:
					firstErr = fmt.Errorf("pipeline: decommission: drop replication slot %q: %w", st.SlotName, err)
				}
			} else {
				rep.SlotDropped = true
			}
		}
	}

	// ---- (d): the RECORDED per-stream publication on the source ----
	// Never derived, never guessed: only the name the control row
	// carries, and the engine's own guard keeps the shared default off
	// limits (dropOwnPublicationIfPerStream semantics).
	dropper, canDropPub := slots.(ir.StreamPublicationDropper)
	switch {
	case slots == nil:
		rep.PublicationSkipped = "the source engine has no publications"
	case st.PublicationName == "":
		rep.PublicationSkipped = "the control row records no publication name (a legacy stream on the shared engine default, which is never dropped); nothing to remove"
	case !canDropPub:
		rep.PublicationSkipped = "the source engine does not expose publication management; drop the publication manually if the stream had its own"
	default:
		outcome, err := dropper.DropStreamPublication(ctx, st.PublicationName, dryRun)
		switch {
		case err != nil:
			err = fmt.Errorf("pipeline: decommission: drop per-stream publication %q: %w", st.PublicationName, err)
			if firstErr == nil {
				firstErr = err
			} else {
				firstErr = errors.Join(firstErr, err)
			}
		case outcome == ir.PublicationDropDropped:
			rep.PublicationDropped = true
		case outcome == ir.PublicationDropAlreadyAbsent:
			rep.PublicationAlreadyAbsent = true
		default: // ir.PublicationDropSkippedShared
			rep.PublicationSkipped = "the recorded publication is the shared engine default, which other streams may read through — never dropped"
		}
	}

	// ---- (e): the control row on the target — last, and only clean ----
	if firstErr != nil {
		return rep, fmt.Errorf(
			"pipeline: decommission of stream %q is INCOMPLETE: %w — the control row was kept so a re-run can finish the remaining source-side removals",
			streamID, firstErr,
		)
	}
	if !dryRun {
		if err := cleaner.ClearStream(ctx, streamID); err != nil {
			return rep, fmt.Errorf("pipeline: decommission: clear control row for stream %q: %w (the source-side objects reported above are already removed; re-run to retry the row)", streamID, err)
		}
	}
	rep.ControlRowCleared = true
	return rep, nil
}

// decommissionActiveRefusal is the coded (b)-step refusal: the
// stream's slot has a connected consumer, so the stream is live and
// must be drained before its footprint is removed.
func decommissionActiveRefusal(streamID, slotName string) error {
	return &sluicecode.CodedError{
		Code: sluicecode.CodeDecommissionStreamActive,
		Hint: "drain the stream first: `sluice sync stop --stream-id " + streamID + " --wait`, then re-run decommission",
		Err: fmt.Errorf(
			"pipeline: decommission refused: stream %q looks LIVE — its replication slot %q is active on the source (a CDC consumer is attached). "+
				"Decommissioning a running stream would yank the slot out from under it mid-stream; "+
				"drain it first with `sluice sync stop --stream-id %s --wait`, then re-run",
			streamID, slotName, streamID,
		),
	}
}

// findSlot returns the named slot's info from the manager's listing,
// or nil when no slot of that name exists.
func findSlot(ctx context.Context, slots ir.SlotManager, name string) (*ir.SlotInfo, error) {
	list, err := slots.List(ctx)
	if err != nil {
		return nil, err
	}
	for i := range list {
		if list[i].Name == name {
			return &list[i], nil
		}
	}
	return nil, nil
}

// dropSlotWithActiveRetry drops the slot, retrying with bounded
// backoff while the failure has the active-slot shape (the
// lingering-walsender reap window; see the budget vars above). The
// final attempt's error propagates unchanged.
func dropSlotWithActiveRetry(ctx context.Context, slots ir.SlotManager, name string) error {
	var err error
	for attempt := 1; ; attempt++ {
		err = slots.Drop(ctx, name, false)
		if err == nil || !isSlotActiveShapeErr(err) || attempt >= decommissionDropAttempts {
			return err
		}
		backoff := decommissionDropBaseBackoff << (attempt - 1)
		if backoff > decommissionDropMaxBackoff {
			backoff = decommissionDropMaxBackoff
		}
		slog.InfoContext(
			ctx, "decommission: slot is still held (prior owner likely not yet reaped); waiting to retry the drop",
			slog.String("slot", name),
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", decommissionDropAttempts),
			slog.Duration("backoff", backoff),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// isSlotActiveShapeErr reports whether err is the active-slot refusal
// from any engine: the manager's own pre-check wording ("slot ... is
// active") or Postgres's SQLSTATE 55006 from pg_drop_replication_slot
// racing a not-yet-reaped walsender. String-matched rather than typed
// so this package stays engine-neutral — the same precedent as the
// CLI's isSlotNotFoundErr.
func isSlotActiveShapeErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "55006") || strings.Contains(msg, "is active")
}

// isSlotGoneShapeErr reports whether err is the slot-not-found shape
// (the manager's sentinel wording, or PG's "does not exist") — the
// goal state for a drop, so idempotent re-runs treat it as success.
func isSlotGoneShapeErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "slot not found") || strings.Contains(msg, "does not exist")
}
