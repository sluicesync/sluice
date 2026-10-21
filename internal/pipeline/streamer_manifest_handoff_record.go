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

// recordManifestHandoff records a --position-from-manifest handoff the way a
// cold start records its copy: the chain's terminal position as the stream's
// anchor and, on a row with no record yet, the stream's current scope as its
// established tables, in one write ([ir.ColdStartAnchorWriter]).
//
// Why: the target was restored from the chain, so its copy is complete by
// construction, but nothing recorded that. The stream's control row did not
// exist until its first applied batch and carried no established-table
// record, so every later plain restart was judged as a stream an older
// release cold-started — and on a PlanetScale/Vitess source the
// VSTREAM-COPY-COMPLETION-UNPROVEN door refuses that shape (Bug 327).
//
// It runs once the handoff's CDC reader has opened, so a handoff that fails
// to start leaves the target exactly as before (no row a plain re-run would
// warm-resume from). A stream that already has a record is left alone. A
// write that fails refuses: the stream would otherwise run now and refuse at
// its next plain restart, for a reason this run knew and did not say. An
// applier without the anchor surface (no real target today: the Postgres and
// MySQL-family appliers both implement it) cannot record, and WARNs with the
// remedy instead of refusing a handoff that would otherwise run correctly.
func (s *Streamer) recordManifestHandoff(ctx context.Context, applier ir.ChangeApplier, streamID string, pos ir.Position) error {
	if s.PositionFromManifestStore == nil {
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
			"handoff brings in, so a later warm resume cannot tell them from a copy that never finished; on a PlanetScale/Vitess "+
			"source the next plain restart refuses VSTREAM-COPY-COMPLETION-UNPROVEN and needs --assert-copy-finished",
			slog.String("stream_id", streamID))
		return nil
	}
	keys, names, err := s.currentScopeKeys(ctx, applier, streamID)
	if err == nil {
		err = aw.WriteColdStartAnchor(ctx, streamID, pos, keys, s.establishedIDs(ctx, keys))
	}
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: the --position-from-manifest handoff could not record the "+
			"tables it brings in (no change has been applied by this run); without that record a later plain restart "+
			"cannot tell this stream's restored copy from one that never finished. Fix what the error names and re-run "+
			"the same command: %w", manifestHandoffUnrecordedMarker, err))
	}
	slog.InfoContext(ctx, "position-from-manifest: recorded the handoff's anchor and the tables it brings in as established",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	return nil
}
