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
// take that recovery away. They stay under the failure cap.
//
// # The enumeration (every codeless exit-1 refusal AGENTS.md tells an agent not to retry)
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
//
// Each engine refusal wraps its sentinel with %w; the engine packages pin
// that (TestCheckSlotNotAckedPast, TestClassifyApplierError_VindexUpdateRefusalIsMarkedTerminal,
// TestFinishParseDSN_TimeZoneSpellings, TestDecodeBinlogRow_RefusesWhatItCannotDecode,
// TestRestartRefusalSentinelsAreTheMarkers). A wrapper between the engine and
// the runner that dropped the chain would degrade a leg to the old restart
// loop, never to a silent skip.
var refusalsARestartRepeats = []error{
	ir.ErrSlotAckedPastTargetPosition,
	ir.ErrShardedTargetVindexUpdate,
	applymarks.ErrMismatch,
	ir.ErrCharsetNotDecodable,
	ir.ErrDSNTimeZoneNotUTC,
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
