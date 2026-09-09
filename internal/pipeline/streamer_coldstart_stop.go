// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// stoppedSlotKeptMarker is the grep-stable marker on the WARN a
// preserved-on-stop cold start emits. An operator who stopped a sync and
// later wonders why their source's WAL is growing needs one token to
// search their logs for.
const stoppedSlotKeptMarker = "STOPPED-SLOT-KEPT"

// abandonUnlessStopped is the single door every POST-COPY cold-start
// error path goes through. It abandons a genuinely failed cold start —
// dropping the just-created replication slot, per Bug 177, so a refused
// start does not orphan a WAL-pinning slot — but CLOSES a stopped one,
// keeping the slot so the completed copy stays resumable.
//
// # Why the distinction exists
//
// An operator stop and a real failure arrive at these call sites as the
// same thing: a non-nil error. They are not the same event.
//
// A genuine failure means the cold start is not going to produce a usable
// target, so the slot is debris and must go. A `context.Canceled` means
// the operator pressed Ctrl-C — and by the time these sites run, the bulk
// copy may already have COMMITTED every row. Abandoning there drops the
// slot for a copy that succeeded: warm resume then has no position and
// cold start refuses on the populated target, so the only way out is
// `--reset-target-data` and a full re-copy. Ctrl-C during a long index
// build should not cost an operator their whole migration.
//
// # The cost this accepts, and why it WARNs
//
// A kept slot PINS WAL on the source until the stream is resumed or the
// slot is dropped. On a busy source that is not free: WAL accumulates
// from the slot's position and can fill the disk. So preserving is only
// half the fix — the operator is TOLD, loudly, at the moment it happens,
// with the slot's name and both ways out. A slot preserved silently would
// trade a recoverable re-copy for an unrecoverable outage, which is a
// worse bargain than the one it replaces.
//
// # Precedent
//
// This is the same call the v0.116-era anchor fix (c26708f4) made one
// phase later: it put the CDC anchor write on an uncancellable context
// precisely because failing it ran Abandon and dropped the slot. That fix
// closed the door at the anchor and left it open across the copy and
// index phases, which is where a stop actually tends to land on a long
// migration — a door that reached one path and not its sibling.
//
// Returns the original error unchanged in both cases; cleanup failures
// never replace the operator's cause.
func (s *Streamer) abandonUnlessStopped(ctx context.Context, stream *ir.SnapshotStream, cause error) {
	if !stopRequested(ctx, cause) {
		_ = stream.Abandon()
		return
	}
	_ = stream.Close()
	slot := s.SlotName
	if slot == "" {
		slot = "the stream's replication slot (default sluice_slot)"
	}
	slog.WarnContext(ctx, "pipeline: "+stoppedSlotKeptMarker+": the cold start was STOPPED after rows had "+
		"already been written, so the source's replication slot was KEPT rather than dropped — without it the "+
		"copy on the target would be unresumable and would have to be redone from scratch. The slot now PINS "+
		"WAL on the source and will keep doing so until you act: re-run `sluice sync start` with the same "+
		"--stream-id to resume and release it, or, if you are abandoning this migration, drop the slot on the "+
		"source (SELECT pg_drop_replication_slot(...)) so it stops retaining WAL. On a busy source an "+
		"unattended slot can fill the disk",
		slog.String("slot", slot),
		slog.String("cause", cause.Error()))
}

// stopRequested reports whether an error is an operator stop rather than
// a genuine failure.
//
// It consults BOTH the error and the context, deliberately. A cancelled
// context whose error got flattened on the way up (a driver that reports
// "connection closed" when its ctx dies, a phase that wraps without %w)
// is still a stop, and grading it as a failure is the expensive direction:
// it destroys a completed copy. Grading a real failure as a stop only
// leaves a slot behind, which the WARN tells the operator how to clear.
func stopRequested(ctx context.Context, cause error) bool {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return true
	}
	return ctx.Err() != nil
}
