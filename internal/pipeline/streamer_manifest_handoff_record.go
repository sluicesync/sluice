// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// manifestHandoffUnrecordedMarker names the refusal of a --position-from-manifest
// handoff whose established-table record could not be written.
const manifestHandoffUnrecordedMarker = "MANIFEST-HANDOFF-UNRECORDED"

// recordManifestHandoff records a PlanetScale/Vitess --position-from-manifest
// handoff the way a cold start records its copy: the chain's terminal
// position as the stream's anchor and, on a row with no record yet, the
// tables the chain restored as established, in one write
// ([ir.ColdStartAnchorWriter]).
//
// Why: the restored tables are complete by construction, but nothing
// recorded that. The stream's control row did not exist until its first
// applied batch and carried no record, so every later plain restart was
// judged as a stream an older release cold-started, which the
// VSTREAM-COPY-COMPLETION-UNPROVEN door refuses (Bug 327).
//
// What is recorded is the stream's current scope INTERSECTED with the
// chain's table set ([LoadChainTerminalTables]) and with the tables that
// exist on the target (a restore filtered by `restore --include-table` left
// the rest out, and nothing else records that), never the scope alone: a
// table in scope that the backup never held was never copied, and recording
// it would disable the INCLUDE-TABLE-SCOPE-DRIFT door for exactly the table
// it exists to catch. Such a table stays not established and is judged by
// that door at the next start. A chain whose table set cannot be read
// refuses rather than falling back to the scope.
//
// A restored table that is EMPTY on the target is recorded, unlike under
// --assert-copy-finished ([Streamer.recordAfterCopyDoor], F4), which records
// only tables that hold rows. The asymmetry is deliberate: here the backup
// chain is independent evidence that the table was restored, and an empty
// table in a backup is legitimate (it was empty on the source when the
// backup was taken); the flag has no such evidence, only the operator's word,
// so an empty table there is as likely a copy that never reached it.
//
// VStream sources only (the door's predicate): every other source keeps the
// behaviour it had, the smallest change the door needs. It runs once the
// handoff's CDC reader has opened, so a handoff that fails to start leaves
// no row a plain re-run would warm-resume from. A stream that already has a
// record is left alone. A write or read that fails refuses: the stream would
// otherwise run now and refuse at its next plain restart, for a reason this
// run knew and did not say. An applier without the anchor surface (no real
// target today: the Postgres and MySQL-family appliers both implement it)
// cannot record, and WARNs with the remedy instead of refusing a handoff
// that would otherwise run correctly.
func (s *Streamer) recordManifestHandoff(ctx context.Context, applier ir.ChangeApplier, streamID string, pos ir.Position) error {
	if s.PositionFromManifestStore == nil || s.Source.Capabilities().CDC != ir.CDCVStream {
		return nil
	}
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		rec, err := store.ReadEstablishedRecord(ctx, streamID)
		if err != nil {
			return recordReadFailure(err)
		}
		if rec.Recorded {
			return nil
		}
	}
	aw, ok := applier.(ir.ColdStartAnchorWriter)
	if !ok {
		slog.WarnContext(ctx, manifestHandoffUnrecordedMarker+": this target cannot record the tables a --position-from-manifest "+
			"handoff brings in, so a later warm resume cannot tell them from a copy that never finished; the next plain "+
			"restart refuses VSTREAM-COPY-COMPLETION-UNPROVEN and needs --assert-copy-finished",
			slog.String("stream_id", streamID))
		return nil
	}
	restored, err := LoadChainTerminalTables(ctx, s.PositionFromManifestStore)
	if err != nil {
		return handoffUnrecordedError("could not read the tables the backup chain holds", err)
	}
	scope, _, err := s.currentScopeKeys(ctx, applier, streamID)
	if err != nil {
		return handoffUnrecordedError("could not read the stream's scope", err)
	}
	onTarget, _, err := s.targetTableFacts(ctx, scope)
	if err != nil {
		return handoffUnrecordedError("could not read which tables exist on the target", err)
	}
	var keys, names, left []string
	for _, k := range scope {
		if restored[bareTableOfKey(k)] && onTarget[k] {
			keys = append(keys, k)
			names = append(names, displayKey(k))
		} else {
			left = append(left, displayKey(k))
		}
	}
	if err := aw.WriteColdStartAnchor(ctx, streamID, pos, keys, s.establishedIDs(ctx, keys)); err != nil {
		if errors.Is(err, ir.ErrControlTableDDLRequired) {
			return handoffDDLRefusal(err)
		}
		return handoffUnrecordedError("could not record the tables it brings in", err)
	}
	// The anchor write records the set only where the record's columns exist:
	// on a control table this role cannot ALTER it writes the position, WARNs
	// COLD-START-ANCHOR-UNRECORDED and returns nil. Read the record back
	// rather than report what was asked for.
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		rec, err := store.ReadEstablishedRecord(ctx, streamID)
		if err != nil {
			return handoffUnrecordedError("could not read back the record it wrote", err)
		}
		if !rec.Recorded {
			return handoffDDLRefusal(errors.New(ir.ColdStartAnchorUnrecordedMarker +
				": the anchor was written without the established-table record"))
		}
	}
	slog.InfoContext(ctx, "position-from-manifest: recorded the handoff's anchor and the restored tables as established",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	if len(left) > 0 {
		slog.WarnContext(ctx, "position-from-manifest: tables in the stream's scope that the backup chain does not hold, or "+
			"that do not exist on the target, were not recorded as copied; the next start judges them as never copied "+
			"(INCLUDE-TABLE-SCOPE-DRIFT) — bring each in with `sluice schema add-table`, or take it out of scope",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(left, ", ")))
	}
	return nil
}

// handoffDDLRefusal refuses a handoff whose record cannot be written because
// this role cannot bring sluice_cdc_state up to date. The position may already
// be written; the next plain start then refuses VSTREAM-COPY-COMPLETION-UNPROVEN
// rather than resume silently.
func handoffDDLRefusal(err error) error {
	return fmt.Errorf("pipeline: %w: %s: the --position-from-manifest handoff could not record the tables it brings in, "+
		"because this role cannot bring the sluice_cdc_state control table up to date (the stream's position may already "+
		"be written; a plain start refuses VSTREAM-COPY-COMPLETION-UNPROVEN until the record exists). %s, then re-run the "+
		"same command. No change has been applied by this run: %w",
		ir.ErrIncludeTableScopeDrift, manifestHandoffUnrecordedMarker, controlTableDDLRemedy(err), err)
}

func handoffUnrecordedError(what string, err error) error {
	return controlReadFailure(fmt.Errorf("pipeline: %s: the --position-from-manifest handoff %s (no change has been "+
		"applied by this run); without that record a later plain restart cannot tell this stream's restored copy from "+
		"one that never finished. Fix what the error names and re-run the same command: %w",
		manifestHandoffUnrecordedMarker, what, err))
}
