// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// IsRowDMLChange reports whether c is a row-level DML change — an
// [Insert], [Update], or [Delete]. These are the ONLY events the CDC
// "rows applied" counter ([StreamStatus.RowsApplied], persisted in the
// per-target sluice_cdc_state control table) counts: one increment per
// row-level change durably applied to the target.
//
// [Truncate], [SchemaSnapshot], [TxBegin], and [TxCommit] are NOT
// row-level DML and are excluded — a TRUNCATE is a whole-table
// statement (not a per-row change), a SchemaSnapshot is metadata, and
// the transaction-boundary markers carry no row data. Keeping the
// predicate here (rather than re-deriving the type switch at each apply
// site) gives the count semantics a single owner shared by the serial
// batch loop (internal/appliershared), the concurrent key-hash
// orchestrator (internal/laneapply), and both engines' per-change apply
// paths, so "what counts as a row applied" cannot drift across them.
func IsRowDMLChange(c Change) bool {
	switch c.(type) {
	case Insert, Update, Delete:
		return true
	default:
		return false
	}
}

// RowsAppliedDelta is the rows_applied contribution of a single change:
// 1 for a row-level DML change ([IsRowDMLChange]), 0 otherwise. The
// engines' serial per-change apply paths pass it straight to the
// position write so the counter advances exactly one per DML change
// committed atomically with its position.
func RowsAppliedDelta(c Change) int64 {
	if IsRowDMLChange(c) {
		return 1
	}
	return 0
}
