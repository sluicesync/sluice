// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The slot-ack CEILING (GC-41): the one mechanism by which a continuous
// sync lets a Postgres source's logical slot release WAL.
//
// A slot-keeping CDC reader ([slotAckReleaser]) never acks past what its
// consumer has released. The streamer's release comes from ONE source of
// evidence: the position the TARGET has durably persisted for this stream,
// read back from the target through the mandatory
// [ir.ChangeApplier.ReadPosition]. That is the position warm resume will
// restart from, so releasing it can never let the slot skip a change the
// target does not hold — whatever target engine it is, however it batches,
// whichever apply path (per-change, serial batch, W concurrent lanes, the
// lane barrier) wrote the row, and however far an --apply-delay hold runs
// the reader ahead.
//
// Why a read-back rather than feedback from the apply path. The defect this
// closes was feedback that did not arrive: the Postgres applier reported its
// commits to an applied-LSN tracker, no other engine's applier did, and the
// reader acked the STREAMED LSN for every non-Postgres target — past rows
// still sitting in a MySQL batch, so a stop mid-batch resumed past them
// (silent loss). And the Postgres lane checkpoint, the one write path in
// that engine that never reported, left the tracker at 0 and pinned
// confirmed_flush_lsn at the start position for the life of a
// concurrent-lane stream (unbounded source WAL retention, GC-41 (b)); the
// tracker is gone. A feedback design is correct only while every
// position-writing site in every engine remembers to call it; the read-back
// needs no site to remember anything, because the evidence it reads IS the
// durable write. It is the same shape the trigger-CDC change-log registry
// already uses ([runChangeLogConsumerTick]).
//
// The PREMISE it rests on is that ReadPosition returns the durably committed
// position, not an in-memory frontier. Every registered engine is classified
// against that premise in TestSlotAckCeilingRoster_EveryRegisteredEngine
// (internal/engineroster), which fails for an engine nobody has checked.
//
// Cost: one primary-key read of the control row per interval. Lag: the slot
// trails the target's durable position by at most one interval plus the
// reader's keepalive cadence.

// slotAckCeilingInterval is how often the sidecar reads the target's durable
// position and releases it to the source slot. Faster than the Postgres
// reader's 10s keepalive so each keepalive advertises a fresh ceiling.
const slotAckCeilingInterval = 5 * time.Second

// captureSlotAckReleaser records the per-attempt CDC reader when it keeps
// server-side consumer state an ack releases. Called from every site that
// starts a stream (cold start, warm resume, and their multi-database twins —
// held to it by TestSlotAckReleaseRoster_EveryStreamChangesSiteReleases). A
// reader without the surface leaves it nil and the sidecar never starts.
func (s *Streamer) captureSlotAckReleaser(reader any) {
	if r, ok := reader.(slotAckReleaser); ok {
		s.slotAck = r
	}
}

// startSlotAckCeiling starts the apply-phase sidecar that releases the
// target's durable position to the source slot on a cadence. No-op when the
// source reader keeps no slot. Runs for the duration of ctx (applyCtx).
func (s *Streamer) startSlotAckCeiling(ctx context.Context, streamID string, applier ir.ChangeApplier) {
	releaser := s.slotAck
	if releaser == nil {
		return
	}
	sourceEngine := s.Source.Name()
	go func() {
		ticker := time.NewTicker(slotAckCeilingInterval)
		defer ticker.Stop()
		failing := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				failing = releaseDurableSlotAck(ctx, applier, releaser, streamID, sourceEngine, failing)
			}
		}
	}()
}

// releaseDurableSlotAck is one tick of the slot-ack ceiling: read the
// target's durable position for streamID and release it to the source slot.
// Every failure holds the ack where it is — WAL retention, never loss — so it
// is logged and swallowed; the WARN fires once per failing streak (failing is
// the latch) rather than every tick. Returns the new latch value.
//
// The persisted position is re-stamped with the source engine's name first:
// the applier stamps positions it reads with its OWN engine name, and the
// source reader's decoder rejects a foreign engine tag (the same re-stamp
// warm resume does, [retagPositionForSource]).
func releaseDurableSlotAck(
	ctx context.Context,
	applier ir.ChangeApplier,
	releaser slotAckReleaser,
	streamID, sourceEngine string,
	failing bool,
) bool {
	pos, found, err := applier.ReadPosition(ctx, streamID)
	if err == nil && found {
		err = releaser.ReleaseSlotAckTo(retagPositionForSource(pos, sourceEngine))
	}
	if err != nil {
		if ctx.Err() != nil {
			return failing
		}
		if !failing {
			slog.WarnContext(
				ctx, "slot-ack ceiling: could not release the target's durable position to the source slot; "+
					"the slot keeps retaining WAL from its last released position until this succeeds (nothing is lost)",
				slog.String("stream_id", streamID),
				slog.String("error", err.Error()),
			)
		}
		return true
	}
	return false
}

// slotAckedPastAcceptor is the structural seam to a slot-keeping reader's
// warm-resume door (Postgres: SLOT-ACKED-PAST-TARGET-POSITION, which refuses
// a resume whose slot was acknowledged past the persisted position). The
// door itself runs inside the reader's StreamChanges on every resume, so it
// needs no wiring; only the operator's one-shot acknowledgement does.
type slotAckedPastAcceptor interface {
	AcceptSlotAckedPastPosition(confirmedFlush string)
}

// wireSlotAckedPastAcceptance hands the operator's acknowledgement to a
// slot-keeping reader before a warm resume's StreamChanges. Called from
// every warm-resume site — warmResume (also the stopped-cold-start resume)
// and warmResumeMultiDatabase — and held to it by
// TestSlotAckReleaseRoster_EveryStreamChangesSiteReleases. An empty value is
// a no-op, so the door refuses.
func (s *Streamer) wireSlotAckedPastAcceptance(reader any) {
	if s.AcceptSlotAckedPastPosition == "" {
		return
	}
	if a, ok := reader.(slotAckedPastAcceptor); ok {
		a.AcceptSlotAckedPastPosition(s.AcceptSlotAckedPastPosition)
	}
}
