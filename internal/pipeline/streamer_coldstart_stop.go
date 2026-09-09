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

// defaultSlotNameForAdvice is the slot the PG engine creates when no
// --slot-name is given. Duplicated from the engine (this package must not
// import it) and held to it by TestStoppedSlotAdviceNamesTheRealDefault,
// because a recovery instruction that names a nonexistent slot is worse
// than one that names none.
const defaultSlotNameForAdvice = "sluice_slot"

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

	// The RESOLVED name, not s.SlotName. `--slot-name` is a SUFFIX —
	// sluice prepends "sluice_" ([ResolveSlotName]) — so an operator who
	// passed `--slot-name prod` has a slot called `sluice_prod` on the
	// server. Printing the raw field here would name an object that does
	// not exist and send them looking for the wrong thing in
	// pg_replication_slots, in a message whose entire job is telling them
	// what to go and act on.
	slot := ResolveSlotName(s.SlotName)
	if slot == "" {
		slot = defaultSlotNameForAdvice
	}
	// What the kept slot buys, stated in the message rather than assumed
	// (audit 2026-09-09 A0909-STOP-1). This door fires BEFORE
	// coldStartBeginCDC's anchor write, so no CDC position exists yet —
	// and until A0909-STOP-1 shipped, that meant the kept slot bought
	// nothing at all and the message had to say so.
	//
	// It now buys the handoff resume ([Streamer.resumeStoppedColdStart]):
	// the slot's consistent point IS the anchor, and a re-run picks the
	// copy up where it stopped. But only for the case that resume can
	// PROVE — a PostgreSQL source, whose recording cold start finished
	// copying every in-scope table — so this message must not promise it
	// unconditionally. It names both outcomes and the one signal that
	// separates them at re-run time (COLD-START-RESUMED in the log), and
	// keeps the drop-and-re-copy exit for the cases that still need it.
	//
	// The WAL warning stays first either way: a slot kept for a resume
	// the operator never runs pins WAL exactly as hard as one kept for
	// nothing.
	slog.WarnContext(ctx, "pipeline: "+stoppedSlotKeptMarker+": the cold start was STOPPED after rows had "+
		"already been written but BEFORE the CDC anchor was recorded, so the source's replication slot was KEPT "+
		"rather than dropped. The slot now PINS WAL on the source and will keep doing so until you resume or "+
		"drop it. "+
		// remedy-partial: every `sync start` mention is the operator's OWN original invocation, which sluice cannot
		// render — the source and target flags are DSNs carrying credentials, and echoing them into a log line to
		// satisfy a paste-ability gate would leak them into every log sink the operator ships to.
		"TO RESUME: re-run `sluice sync start` with the same --stream-id. "+ // remedy-partial: the operator's own invocation
		"If the copy had finished every in-scope table and this slot is still exactly where the snapshot was "+
		"taken, the re-run SKIPS the copy, finishes the remaining phases and starts CDC from this slot — look "+
		"for COLD-START-RESUMED in its output. That resume needs a PostgreSQL source; on any other source, and "+
		"on a copy that had not finished, the re-run instead refuses on the existing slot. "+
		"IF IT REFUSES: drop the slot with `sluice slot drop --source-driver postgres --source <DSN> "+slot+
		" --yes` (add --force only if a consumer is still attached), then re-run `sluice sync start` with "+ // remedy-partial: the operator's own invocation
		"--reset-target-data to copy again. On a busy source an unattended slot can fill the disk",
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
