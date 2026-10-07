// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The broker's answer to audit F-E1, as ADR-0191 narrows it.
//
// A run interrupted partway through an incremental re-applies, on the next
// run, the one source transaction that was in flight (ADR-0191 §3.2 — the
// position stands inside the incremental). On a table the applier upserts
// into, re-applying it converges for inserts and same-key updates/deletes; on
// a table it plain-INSERTs into — keyless, or keyed only on a column the rows
// do not carry — it would silently duplicate every committed row of that
// transaction, and a key-CHANGING transaction can misapply (audit backlog
// F-E1-KEY-REUSE-REPLAY). ADR-0190's apply marks close both, but only where
// three things hold, which is why the keyless door is judged PER INCREMENTAL
// ([SyncFromBackup.refuseKeylessIncremental], §3.5):
//
//   - the incremental records the reader's change identities
//     ([irbackup.Manifest.ApplyIdentity]), and every change to the table in it
//     carries one;
//   - the target's mark table is usable and a mark commits with its rows
//     ([ir.ApplyMarksCoverageProber]);
//   - the table's mark key on the target is one the replayed rows supply.
//
// A table that fails any of them, and that the incremental touches, is refused
// (SLUICE-E-BROKER-KEYLESS-TABLE) before anything of the incremental is
// applied. A cancel that interrupts an incremental exits non-zero with
// [BrokerIncrementalPartialMarker] instead of exit 0.

// BrokerIncrementalPartialMarker is the grep-stable token on the error a
// broker run returns when its context is cancelled (SIGINT/SIGTERM, a
// supervisor's deadline) while an incremental is being applied.
const BrokerIncrementalPartialMarker = "BROKER-INCREMENTAL-PARTIAL"

// brokerIncrementalPartialError reports a cancel that interrupted an
// incremental. It deliberately has NO Unwrap: the cause is a context
// cancellation, and every layer that sees errors.Is(err, context.Canceled)
// treats it as the clean stop the operator asked for (the broker's own Run,
// the live panel's "stopped." exit 0). This error must reach the process exit
// code, so it must not be recognisable as one.
type brokerIncrementalPartialError struct {
	backupID   string
	resumeFrom string
	cause      error

	// identity reports that the interrupted incremental records the
	// reader's change identities (irbackup.Manifest.ApplyIdentity), so the
	// re-applied transaction is covered by ADR-0190's apply marks.
	identity bool
}

func (e *brokerIncrementalPartialError) Error() string {
	head := fmt.Sprintf(
		"broker: %s: the run was interrupted partway through incremental %s. The broker's position stands INSIDE it, at "+
			"the last source transaction whose effects are durable on the target (the last incremental fully applied is "+
			"%s), so the next run re-reads it, skips what is already applied and re-applies only the source transaction "+
			"that was in flight (ADR-0191). ",
		BrokerIncrementalPartialMarker, e.backupID, e.resumeFrom,
	)
	var body string
	if e.identity {
		body = "This incremental records the source's own change identities, so wherever the target holds apply marks " +
			"(a run without them logs APPLY-MARKS-UNAVAILABLE) the changes of that transaction already applied are " +
			"skipped and the rest applied once, key changes included: re-run the same command to finish it. "
	} else {
		body = "This incremental records no change identities (it was written before ADR-0191, or rewritten by smart " +
			"compaction), so that one transaction is re-applied without apply marks. Every table the broker replays was " +
			"judged to have a key the replayed rows carry and collide on (a table without one is refused before anything " +
			"is applied), so re-applied inserts, and updates and deletes that keep each row's key, converge: re-run the " +
			"same command to finish it. A transaction that CHANGED a row's key value is different: its re-run can fail on " +
			"a duplicate key (MySQL 1062 / Postgres 23505) on every attempt, and when a key value was moved off one row " +
			"and onto another inside that transaction, the re-run can apply a change to the wrong row without any error. " +
			"If the source changes key values, recover with `--reset-target-data` instead of re-running. "
	}
	return head + body + "To stop a broker without interrupting an incremental, use `sluice sync from-backup stop`, which " +
		"takes effect between ticks. Cause: " + fmt.Sprint(e.cause)
}

// BrokerColdStartPartialMarker is the grep-stable token on the error a
// `--reset-target-data` cold start returns when its context is cancelled
// after it began dropping the target's tables and before it recorded a
// position: the target holds a partial restore and no broker position.
const BrokerColdStartPartialMarker = "BROKER-COLD-START-PARTIAL"

// brokerColdStartPartialError reports a cancel that interrupted the
// --reset-target-data drop-and-restore. Like
// [brokerIncrementalPartialError] it has NO Unwrap: a context.Canceled cause
// would read as a clean stop to every layer above (the live panel prints
// "stopped." and exits 0 on it), and a half-dropped, half-restored target
// with no position is not one.
type brokerColdStartPartialError struct {
	streamID string
	cause    error
}

func (e *brokerColdStartPartialError) Error() string {
	return fmt.Sprintf(
		"broker: %s: the --reset-target-data cold start for stream %q was interrupted after it began dropping the "+
			"target's tables and before it recorded a position, so the target holds a PARTIAL restore of the chain and "+
			"no broker position. Re-run the same command with --reset-target-data: it drops the chain's tables again and "+
			"restores the chain from its full. Do not start the broker with --at-chain-id against this target. Cause: %v",
		BrokerColdStartPartialMarker, e.streamID, e.cause,
	)
}

// coldStartPartialOr returns err unchanged unless ctx is done, in which case
// the failure is the interrupted cold start [brokerColdStartPartialError]
// describes. Called only for failures AFTER the drop began.
func (b *SyncFromBackup) coldStartPartialOr(ctx context.Context, err error) error {
	if ctx.Err() == nil {
		return err
	}
	slog.ErrorContext(
		ctx, "broker: --reset-target-data cold start interrupted; exiting non-zero",
		slog.String("marker", BrokerColdStartPartialMarker),
		slog.String("stream_id", b.StreamID),
	)
	return &brokerColdStartPartialError{streamID: b.StreamID, cause: err}
}

// tickErrorExit turns a failed tick into Run's return value.
//
// Audit F-E1 (a): a cancel that lands while an incremental is being applied
// is NOT a clean stop. Part of the incremental may be committed and the
// position was not advanced past it, so exit 0 would tell a supervisor the
// run ended cleanly when the next run will re-apply the whole incremental.
// The partial arm is checked BEFORE the clean-cancel arm, and the partial
// error does not unwrap to context.Canceled, so no caller up the stack can
// launder it back into a clean exit. Any other cancel — the tick had applied
// nothing of an unadvanced incremental — keeps the historical nil.
func (b *SyncFromBackup) tickErrorExit(ctx context.Context, applyErr error, lastAppliedID string) error {
	var partial *brokerIncrementalPartialError
	if errors.As(applyErr, &partial) {
		slog.ErrorContext(
			ctx, "broker: interrupted partway through an incremental; exiting non-zero",
			slog.String("marker", BrokerIncrementalPartialMarker),
			slog.String("stream_id", b.StreamID),
			slog.String("backup_id", partial.backupID),
			slog.String("last_applied_backup_id", lastAppliedID),
		)
		return partial
	}
	if errors.Is(applyErr, context.Canceled) || errors.Is(applyErr, context.DeadlineExceeded) {
		slog.InfoContext(
			ctx, "broker: context cancelled; exiting",
			slog.String("stream_id", b.StreamID),
			slog.String("last_applied_backup_id", lastAppliedID),
		)
		return nil
	}
	return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("broker: tick: %w", applyErr))
}
