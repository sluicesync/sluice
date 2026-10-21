// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
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
// chain's table set ([LoadChainTerminalTables]), never the scope alone: a
// table in scope that the backup never held was never copied, and recording
// it would disable the INCLUDE-TABLE-SCOPE-DRIFT door for exactly the table
// it exists to catch. Such a table stays not established and is judged by
// that door at the next start. A chain whose table set cannot be read
// refuses rather than falling back to the scope.
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
	var keys, names, notInChain []string
	for _, k := range scope {
		if restored[bareTableOfKey(k)] {
			keys = append(keys, k)
			names = append(names, displayKey(k))
		} else {
			notInChain = append(notInChain, displayKey(k))
		}
	}
	if err := aw.WriteColdStartAnchor(ctx, streamID, pos, keys, s.establishedIDs(ctx, keys)); err != nil {
		return handoffUnrecordedError("could not record the tables it brings in", err)
	}
	slog.InfoContext(ctx, "position-from-manifest: recorded the handoff's anchor and the restored tables as established",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	if len(notInChain) > 0 {
		slog.WarnContext(ctx, "position-from-manifest: tables in the stream's scope that the backup chain does not hold were "+
			"not recorded as copied; the next start judges them as never copied (INCLUDE-TABLE-SCOPE-DRIFT) — bring each "+
			"in with `sluice schema add-table`, or take it out of scope",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(notInChain, ", ")))
	}
	return nil
}

func handoffUnrecordedError(what string, err error) error {
	return controlReadFailure(fmt.Errorf("pipeline: %s: the --position-from-manifest handoff %s (no change has been "+
		"applied by this run); without that record a later plain restart cannot tell this stream's restored copy from "+
		"one that never finished. Fix what the error names and re-run the same command: %w",
		manifestHandoffUnrecordedMarker, what, err))
}
