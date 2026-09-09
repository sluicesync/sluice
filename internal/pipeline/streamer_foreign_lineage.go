// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The foreign-lineage door (audit 2026-09-09, A0909-MYSQL-HIGH-1).
//
// A persisted position can be unusable for two reasons that arrive at the
// streamer as the same thing, an error: the SAME source has moved past it
// (a purge, a reset), or a DIFFERENT source now answers the DSN (a
// replaced instance, a restore from the wrong backup, a load-balanced or
// DNS-failed-over endpoint, a keyspace that is simply another database).
// The automatic recovery for the first — drop the target's in-scope
// tables, re-copy — is exactly the wrong thing for the second: it
// destroys a correct target and repopulates it from the wrong database,
// at exit 0, looking like success. The v0.146.0 SLM-6 fix refused this on
// the file/pos arm; its own sibling enumeration then declared the GTID arm
// covered "by construction", and the GTID arm was measured doing it.
//
// Engines now carry [ir.ErrPositionForeignLineage] on every "different
// lineage" verdict, and that sentinel deliberately does not satisfy
// [ir.ErrPositionInvalid], so the pre-flight fall-through never engages.
// The REACTIVE path has no pre-flight: it sets RestartFromScratch and
// cold-starts. So before it does, it asks the reader through
// [ir.LineageVerifier]. Both doors render one refusal, below.

// foreignLineageMarker is the grep-stable marker the refusal carries.
const foreignLineageMarker = "FOREIGN-LINEAGE-REFUSED"

// foreignLineageRefusalError frames an engine's foreign-lineage verdict
// as the pipeline's refusal, naming what was NOT done and the deliberate
// ways to do it.
func foreignLineageRefusalError(err error) error {
	return fmt.Errorf(
		"pipeline: "+foreignLineageMarker+": the source answering this DSN is not the lineage the persisted "+
			"position was captured from, so sluice REFUSED the automatic re-snapshot — that recovery drops the "+
			"target's in-scope tables and re-copies from whatever now answers the DSN, which is correct for a "+
			"purge on the same source and destroys a correct target here. Nothing on the target was touched. "+
			"First confirm the DSN points at the database you mean (a stale connection string, a DNS or "+
			"load-balancer failover, and a node restored from the wrong backup all look identical from here). "+
			"If the replacement IS intended, re-copy from it deliberately: `sluice sync start` with "+ // remedy-partial: the operator's own invocation
			"--restart-from-scratch (keeps the cdc-state row) or --reset-target-data (clears it too): %w", err,
	)
}

// refuseIfForeignLineage is the reactive path's door. It reads the
// persisted position for this stream, opens a reader, and asks it. It
// returns the framed refusal only on a foreign verdict; every other
// outcome — no capability, no persisted position, a probe that could not
// run — is nil with a log line, because this door narrows what the
// automatic recovery may destroy and must never block a recovery it
// cannot judge.
func (s *Streamer) refuseIfForeignLineage(ctx context.Context) error {
	if s.Source == nil || s.Target == nil {
		// A streamer driven through runOnceFn alone (the reactive-path
		// unit pins) has no engines to ask; the door has nothing to
		// judge and stays out of the way.
		return nil
	}
	streamID := s.resolveStreamID()
	applier, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		slog.WarnContext(ctx, "pipeline: reactive re-snapshot: could not open the target to read the persisted "+
			"position, so the source's lineage was NOT checked before the re-copy",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return nil
	}
	defer migcore.CloseIf(applier)
	persisted, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil || !found {
		if err != nil {
			slog.WarnContext(ctx, "pipeline: reactive re-snapshot: could not read the persisted position, so the "+
				"source's lineage was NOT checked before the re-copy",
				slog.String("stream_id", streamID), slog.String("error", err.Error()))
		}
		return nil
	}
	reader, err := s.Source.OpenCDCReader(ctx, s.SourceDSN)
	if err != nil {
		slog.WarnContext(ctx, "pipeline: reactive re-snapshot: could not open the source to check its lineage "+
			"before the re-copy", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return nil
	}
	defer migcore.CloseIf(reader)
	verifier, ok := reader.(ir.LineageVerifier)
	if !ok {
		return nil
	}
	verr := verifier.VerifyLineage(ctx, persisted)
	if errors.Is(verr, ir.ErrPositionForeignLineage) {
		return foreignLineageRefusalError(verr)
	}
	if verr != nil {
		slog.WarnContext(ctx, "pipeline: reactive re-snapshot: the lineage check did not complete; proceeding "+
			"with the re-copy as before", slog.String("stream_id", streamID), slog.String("error", verr.Error()))
	}
	return nil
}
