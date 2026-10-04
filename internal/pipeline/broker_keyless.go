// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The broker's interim answer to audit F-E1.
//
// The broker stamps every change of an incremental with the PARENT position
// (BRK-1) and gives those changes no apply identity, so ADR-0190's apply marks
// cannot skip them. A run interrupted partway through an incremental therefore
// re-applies the WHOLE incremental on the next run. That is harmless on a table
// the applier upserts into and silently duplicating on a table it plain-INSERTs
// into. Until broker changes carry an apply identity (the exactly-once
// follow-up, which lifts the refusal), the broker:
//
//   - refuses keyless tables before it applies anything
//     ([SyncFromBackup.refuseKeylessTables], SLUICE-E-BROKER-KEYLESS-TABLE);
//   - exits non-zero, with [BrokerIncrementalPartialMarker], when a cancel
//     interrupts an incremental, instead of exit 0.

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
}

func (e *brokerIncrementalPartialError) Error() string {
	return fmt.Sprintf(
		"broker: %s: the run was interrupted partway through incremental %s. Part of it may already be committed on "+
			"the target, and the broker's position was NOT advanced past it (it stays at %s), so the next run re-applies "+
			"the WHOLE incremental. Every table the broker replays has a key (keyless tables are refused before anything "+
			"is applied), so the re-applied changes upsert and converge: re-run the same command to finish it. "+
			"To stop a broker without interrupting an incremental, use `sluice sync from-backup stop`, which takes effect "+
			"between ticks. Cause: %v",
		BrokerIncrementalPartialMarker, e.backupID, e.resumeFrom, e.cause,
	)
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

// brokerKeylessHint is the remedy riding SLUICE-E-BROKER-KEYLESS-TABLE.
const brokerKeylessHint = "give each named table a PRIMARY KEY or a NOT NULL UNIQUE index on the SOURCE and take a new full " +
	"backup (or, for a table keyless only on the target, give the target table the source's key), or replicate the " +
	"table with `sluice sync start` instead of through a backup chain"

// refuseKeylessTables is the F-E1 door. It judges every table the chain
// records that this run has not already cleared, and refuses — naming every
// offender at once — if a re-applied change could duplicate rows in one.
//
// probeTarget adds the target-catalog judgment ([ir.ReplayKeyProber]) to the
// recorded-schema one. Tables are marked cleared only after a judgment that
// included the target, so a --reset-target-data cold start (recorded schema
// only, because the target is about to be rebuilt) is re-judged against the
// rebuilt target on the first tick that has work.
//
// Scope: every table any link of the chain records, in its Schema or in an
// AddTable/AlterTable delta. The broker has no table filter (`sync
// from-backup run` takes no --include-table / --exclude-table), so there is
// nothing narrower to honour; the backup's own table selection already shaped
// what the chain records.
//
// Residual, stated rather than implied: the door judges the tables the chain
// RECORDS. A change chunk carrying rows for a table no link records (a table
// created mid-chain that a scoped window-end schema read did not pick up) is
// not judged here. The applier skips a table the target lacks, so those rows
// can only land — and duplicate — if someone created that table on the target
// by hand. The exactly-once follow-up closes it with the rest of the class.
func (b *SyncFromBackup) refuseKeylessTables(ctx context.Context, probeTarget bool) error {
	chain, err := b.brokerChain(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("broker: build chain: %w", err))
	}
	var pending []*ir.Table
	for _, t := range backup.ChainRecordedTables(chain, migcore.TableFilter{}) {
		if !b.keylessCleared[t.Name] {
			pending = append(pending, t)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	var rw ir.RowWriter
	if probeTarget {
		rw, err = b.Target.OpenRowWriter(ctx, b.TargetDSN)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect,
				fmt.Errorf("broker: keyless-table check: open target row writer: %w", err))
		}
		defer migcore.CloseIf(rw)
	}
	keyless, err := migcore.FindReplayKeylessTables(ctx, rw, pending, migcore.ReplayJudgeOptions{ProbeTarget: probeTarget})
	if err != nil {
		return fmt.Errorf("broker: keyless-table check: %w", err)
	}
	if len(keyless) > 0 {
		return errBrokerKeylessTables(keyless)
	}
	if probeTarget {
		if b.keylessCleared == nil {
			b.keylessCleared = make(map[string]bool, len(pending))
		}
		for _, t := range pending {
			b.keylessCleared[t.Name] = true
		}
	}
	return nil
}

// errBrokerKeylessTables renders the coded refusal.
func errBrokerKeylessTables(tables []migcore.ReplayKeylessTable) error {
	return sluicecode.Wrap(sluicecode.CodeBrokerKeylessTable, brokerKeylessHint, fmt.Errorf(
		"broker: refusing to replay this chain: %d table(s) have no key a re-applied change can collide on: %s. "+
			"The broker re-applies a WHOLE incremental after any interruption (an error, a crash, SIGINT/SIGTERM), "+
			"because its changes carry no apply identity; a keyed table converges, but a keyless one falls back to a "+
			"plain INSERT and silently gains a duplicate of every row the interrupted run had committed (audit F-E1). "+
			"Nothing has been applied",
		len(tables), migcore.RenderReplayKeylessTables(tables),
	))
}
