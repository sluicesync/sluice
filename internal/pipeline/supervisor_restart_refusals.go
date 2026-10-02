// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// refusalsARestartRepeats lists the refusals the fleet supervisor does not
// restart, besides [ir.ErrUnforwardedSchemaChange] (handled before it with a
// remedy of its own). Each is raised from state a fresh run reads back
// unchanged, so a restart can only refuse again. Under the restart-forever
// default (max-consecutive-failures 0) the leg would otherwise refuse behind
// backoff indefinitely while the fleet looked healthy; the fleet config has no
// key for the flags their remedies need, so the operator runs the leg once by
// hand with `sync start`.
//
// # Why a list and not [ir.IsTerminal]
//
// TerminalError means "the in-process retry cannot succeed", which is a
// different property: a dead snapshot-pinned copy connection and an in-doubt
// raw-copy commit are both terminal, and both are recovered by exactly the
// fresh run this supervisor provides (TestSupervisor_OtherTerminalFailuresAreStillRestarted).
// Coded refusals ([sluicecode.ClassRefusal]) are not a fit either: some clear
// without any change to this leg (SLUICE-E-CDC-REPLICATION-HEADROOM once
// another slot is freed, SLUICE-E-TARGET-TABLE-BLOCKED-BY-WORKFLOW once the
// MoveTables move completes or is reversed), so honouring the class would
// take that recovery away. They stay under the failure cap unless listed here
// individually, with the reason a restart repeats them. The registry carries
// no per-code "a restart repeats it" attribute yet; until it does, a coded
// refusal is listed only after its raise site has been read for that
// property (audit backlog, gate proposals: the restart-repeats attribute).
//
// # The enumeration (every codeless exit-1 refusal AGENTS.md tells an agent not to retry, plus the coded ones a restart repeats)
//
//   - UNFORWARDED-SCHEMA-CHANGE (incl. ADD-COLUMN-BACKFILL-INCOMPLETE): not
//     restarted, by its own branch in superviseOne.
//   - SLOT-ACKED-PAST-TARGET-POSITION: listed. The slot's confirmed_flush_lsn
//     and the target's persisted position are both durable; a restart
//     compares the same two values.
//   - SHARDED-TARGET-VINDEX-UPDATE: listed. The refused change is after the
//     persisted position, so a restart re-delivers it to the same vtgate.
//   - APPLY-MARK-MISMATCH: listed. The marks are durable on the target and a
//     restart re-delivers the same transaction onto them.
//   - CHARSET-NOT-DECODABLE: listed. The value is in the source row or
//     binlog event a restart reads again.
//   - DSN-TIME-ZONE-NOT-UTC: listed. The DSN is the leg's configuration; a
//     restart parses the same one.
//   - KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS (coded,
//     SLUICE-E-CDC-KEY-MATCHED-MULTIPLE-ROWS): listed. The refused write's
//     transaction is rolled back, and a restart replays the same source
//     changes, in source order, onto the same committed rows, so the key is
//     shared by the same rows when the write comes round again. Batch and
//     lane boundaries may fall differently on the replay, but they only move
//     where the earlier changes commit, not what the write finds. (A replay
//     that commits a deferrable key's shared state before the write refuses
//     with DEFERRED-KEY-CHECK-FAILED-AT-COMMIT instead, which is below.)
//   - CHANGE-LOG-WATERMARK-STALLED (postgres-trigger source, codeless):
//     listed. A gap-free poll window the watermark did not reach is a sluice
//     bug; a restart resumes at or below the same watermark and reads the
//     same immutable change-log rows through the same code. A cause held
//     only in the stopped process's memory would clear on a restart, and the
//     cause is unknown by definition, so that half is UNVERIFIED — but
//     restarting forever behind backoff is the silent stall the refusal
//     exists to end, while not restarting costs one `sync start` by hand
//     (see [ir.ErrChangeLogWatermarkStalled]).
//   - DEFERRED-KEY-CHECK-FAILED-AT-COMMIT (Postgres target, codeless): NOT
//     listed. It is the COMMIT of a target transaction that a deferrable
//     constraint's re-check refused, and one of its causes is a source
//     transaction split across target transactions by the batch loop's idle
//     flush, byte cap or AIMD-sized row cap (appliershared.RunBatchLoop). Those
//     boundaries depend on arrival timing, and a restart replays a backlog
//     that is all available at once, so the replay can hold the transaction
//     in one target transaction and succeed. Its other causes (a key change
//     applied alone as a lane barrier, --apply-batch-size 1, a target
//     stricter than its source) do repeat, and they stay under the failure
//     cap with the coded refusals above. It carries no sentinel either: the
//     SQLSTATE in its chain is the only handle.
//
// Each engine refusal wraps its sentinel with %w; the engine packages pin
// that (TestCheckSlotNotAckedPast, TestClassifyApplierError_VindexUpdateRefusalIsMarkedTerminal,
// TestFinishParseDSN_TimeZoneSpellings, TestDecodeBinlogRow_RefusesWhatItCannotDecode,
// TestRestartRefusalSentinelsAreTheMarkers, TestRefuseKeyScopedMultiMatch,
// TestRefuseStalledWatermark_CarriesTheSupervisorSentinel). A wrapper between the engine and
// the runner that dropped the chain would degrade a leg to the old restart
// loop, never to a silent skip.
var refusalsARestartRepeats = []error{
	ir.ErrSlotAckedPastTargetPosition,
	ir.ErrShardedTargetVindexUpdate,
	applymarks.ErrMismatch,
	ir.ErrCharsetNotDecodable,
	ir.ErrDSNTimeZoneNotUTC,
	ir.ErrKeyScopedWriteMatchedMultipleRows,
	ir.ErrChangeLogWatermarkStalled,
}

// refusalARestartRepeats returns the listed sentinel err carries, or nil.
func refusalARestartRepeats(err error) error {
	for _, sentinel := range refusalsARestartRepeats {
		if errors.Is(err, sentinel) {
			return sentinel
		}
	}
	return nil
}
