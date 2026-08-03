// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "sluicesync.dev/sluice/internal/ir"

// freshCopyReason names WHY a cold-start is re-copying onto a target that
// already holds a prior copy (audit 2026-08-01 S5).
//
// This used to be a single `forceFresh bool`, and collapsing the two reasons
// into one bit is what hid the defect. Both reasons need the Bug-9
// populated-target refusal suppressed — the target holding the prior copy is
// the EXPECTED state, not an operator mistake — but they do NOT want the same
// treatment of the rows already there:
//
//   - On an idempotent reader (VStream, Postgres) the re-copy UPSERTs, which
//     absorbs rows that still exist at the source and CANNOT remove rows the
//     source DELETED since the prior copy. Those rows survive on the target
//     permanently, silently.
//   - The non-idempotent path (native MySQL binlog, plain INSERT) already
//     drops the in-scope tables first — for the adjacent dup-key reason — and
//     therefore never had the problem. The asymmetry was the bug.
//
// Clearing the target fixes it, and for the OPERATOR'S explicit
// `--restart-from-scratch` that is unambiguously the right reading of the
// request. For the AUTOMATIC auto-resnapshot it is not: that fires without
// operator involvement on a live sync, and dropping the target would open an
// empty-target window nobody asked for. So the automatic path warns instead,
// naming what it cannot reconcile.
//
// The zero value is the safe, common one: an ordinary cold-start that is NOT a
// forced re-copy and therefore keeps the Bug-9 refusal.
type freshCopyReason uint8

const (
	// freshCopyNone is an ordinary cold-start. The populated-target
	// preflight applies in full.
	freshCopyNone freshCopyReason = iota
	// freshCopyOperatorRestart is the operator's explicit
	// `--restart-from-scratch`. "From scratch" is taken at its word: the
	// in-scope target tables are cleared first on EITHER reader, so the
	// re-copy reproduces the source exactly rather than merging onto
	// whatever was there.
	freshCopyOperatorRestart
	// freshCopyAutoResnapshot is the automatic ADR-0093 fall-through, taken
	// when a warm resume finds its persisted position purged from the
	// source's binlog/GTID window. It is not operator-initiated and runs on
	// a live stream, so it does NOT clear the target; on an idempotent
	// reader it warns that source-side DELETEs from the gap cannot be
	// reconciled by an UPSERT re-copy.
	freshCopyAutoResnapshot
)

// forcesFresh reports whether this reason is a deliberate re-copy onto an
// expectedly-populated target, and so suppresses the Bug-9 refusal. Both
// non-zero reasons do.
func (r freshCopyReason) forcesFresh() bool { return r != freshCopyNone }

// clearsTarget reports whether the in-scope target tables should be emptied
// before the re-copy on an IDEMPOTENT reader. Only the operator's explicit
// restart does; see the type comment for why the automatic path does not.
func (r freshCopyReason) clearsTarget() bool { return r == freshCopyOperatorRestart }

// autoResnapshotReason maps the ADR-0093 per-source eligibility to a reason.
// A source that is NOT eligible for the automatic fall-through gets
// freshCopyNone, so the cold-start gate refuses LOUDLY — the ADR-0075
// Phase 2b deliberate-recovery contract for a Postgres logical-slot source.
func autoResnapshotReason(src ir.Engine) freshCopyReason {
	if sourceAutoResnapshotOnInvalidPosition(src) {
		return freshCopyAutoResnapshot
	}
	return freshCopyNone
}

// restartReason maps the operator's --restart-from-scratch flag to a reason.
func restartReason(restartFromScratch bool) freshCopyReason {
	if restartFromScratch {
		return freshCopyOperatorRestart
	}
	return freshCopyNone
}
