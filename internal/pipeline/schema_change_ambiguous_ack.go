// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Binding an AMBIGUOUS-SCHEMA-BOUNDARY acknowledgement to its occurrence
// (GC-44 F5 fifth review).
//
// The acknowledgement says "this boundary is a replay". Its fingerprint used
// to hash only the table and the refused columns, and the value was read
// from [Streamer.AcceptUnforwardedSchemaChange] on every wiring and never
// cleared. So an acknowledged replay of `(10,2)` → `(12,4)` pre-accepted a
// LATER, genuine source narrowing `(12,4)` → `(10,2)` of the same column —
// the identical table and diff — after an ADR-0038 retry in the same process,
// or on any restart with the flag left in a unit file. The target kept the
// wider values the source's own ALTER had converted, at exit 0.
//
// What tells the two apart is the stream's persisted resume position. A
// replay is re-delivered from the position the target last persisted, which
// does not move until the replayed transaction is applied; a later genuine
// change arrives after the stream has applied — and persisted — past the
// replay. So:
//
//   - the fingerprint also hashes the position this attempt resumed from
//     ([ambiguousAckBinding.resumedFrom]): a refusal printed at one position
//     is not accepted at another, in process or across a restart; and
//   - once an attempt has accepted a boundary on the acknowledgement, the
//     first later attempt that resumes from a different persisted position
//     clears the value in process — the one-shot consumption the recorded
//     refusal's door makes ([Streamer.phaseRefuseRecordedUnforwardedChange]).
//
// An attempt that resumes from the SAME position keeps it: an ADR-0038
// retry re-delivers the same replay, which must stay accepted (clearing on
// acceptance would turn every transient retry into a terminal refusal of
// the boundary the operator just confirmed). Nothing new can arrive between
// the two: the retry re-reads from that position, and the replayed
// boundary is the first thing it shows for the table.
//
// The cost, stated: an attempt that persists progress BEFORE reaching the
// refused boundary (other tables' transactions ahead of it in the replay)
// prints a different fingerprint on the next start, so the operator
// acknowledges again; it converges once the stream resumes directly before
// the boundary.
//
// Neither `sync run` (the fleet) nor any environment variable carries the
// acknowledgement: the fleet config's SyncSpec (cmd/sluice/sync_run.go) has no key for it (the supervisor
// says so in its refusal log) and the CLI binds no env var to the flag.

import (
	"context"
	"log/slog"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
)

// ambiguousAckBinding is the Streamer's per-attempt state for the binding.
type ambiguousAckBinding struct {
	// resumedFrom is the persisted position the current attempt resumed
	// from — the zero Position on a cold start. Written on the run
	// goroutine before the intercept chain is wired.
	resumedFrom ir.Position

	// acceptedAt is the resumedFrom of an attempt whose intercept accepted
	// a boundary on the acknowledgement; nil until one has. Written by the
	// intercept goroutine.
	acceptedAt atomic.Pointer[ir.Position]
}

// bindAmbiguousAcknowledgement records the position this attempt resumes
// from (persisted, when found) and expires an acknowledgement an earlier
// attempt of this process already used, once the stream has persisted a
// different position since.
func (s *Streamer) bindAmbiguousAcknowledgement(ctx context.Context, persisted ir.Position, found bool) {
	if !found {
		persisted = ir.Position{}
	}
	s.ambiguousAck.resumedFrom = persisted
	at := s.ambiguousAck.acceptedAt.Load()
	if at == nil || *at == persisted {
		return
	}
	s.ambiguousAck.acceptedAt.Store(nil)
	if s.AcceptUnforwardedSchemaChange == "" {
		return
	}
	slog.InfoContext(ctx, "the "+unforwardedRefusalAckFlag+" acknowledgement of an "+ambiguousBoundaryMarker+
		" is consumed: the stream has persisted past the replay it accepted, so it is cleared for the rest of "+
		"this process and cannot accept a later boundary",
		slog.String("accepted_at", at.Token), slog.String("resumed_from", persisted.Token))
	s.AcceptUnforwardedSchemaChange = ""
}

// ambiguousAckDeps fills the acknowledgement half of the refuse
// intercept's dependencies for the current attempt.
func (s *Streamer) ambiguousAckDeps(deps unforwardedBoundaryDeps) unforwardedBoundaryDeps {
	resumedFrom := s.ambiguousAck.resumedFrom
	deps.acknowledged = s.AcceptUnforwardedSchemaChange
	deps.resumedFrom = resumedFrom
	deps.ambiguousAccepted = func() { s.ambiguousAck.acceptedAt.Store(&resumedFrom) }
	return deps
}
