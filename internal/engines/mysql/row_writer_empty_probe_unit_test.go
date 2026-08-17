// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"testing"
)

// TestEmptyOnConfirmedAbsentTable_Bug253 pins the safety property of the
// emptiness-probe disambiguation: an unclassified probe error is treated as
// "empty" ONLY when the reliable information_schema existence check CONFIRMS
// the table is absent — and an existence probe that itself ERRORS must NEVER
// be guessed as absent (it would let a populated-target cold copy clobber, or
// mask a real failure). This is the refuse-to-guess class shared with SL-1.
func TestEmptyOnConfirmedAbsentTable_Bug253(t *testing.T) {
	// Confirmed absent (probe succeeded, table not found) -> empty, so the
	// cold-start / add-table flow reaches the create-phase sharded-target door
	// that emits SLUICE-E-SCHEMA-TARGET-KEYSPACE-SHARDED instead of the opaque
	// vtgate Error 1105.
	if !emptyOnConfirmedAbsentTable(false, nil) {
		t.Error("a confirmed-absent table must read as empty (proceed to the create-phase door)")
	}
	// Present -> NOT empty via this path; the original probe error surfaces.
	if emptyOnConfirmedAbsentTable(true, nil) {
		t.Error("a present table must not be classified empty by the disambiguation")
	}
	// Bug 253 safety: an existence-probe ERROR must NEVER yield empty.
	probeErr := errors.New("dial tcp 10.0.0.1:3306: i/o timeout")
	if emptyOnConfirmedAbsentTable(false, probeErr) {
		t.Error("an existence-probe error must NOT be guessed as absent — surface the original error")
	}
	if emptyOnConfirmedAbsentTable(true, probeErr) {
		t.Error("an existence-probe error must NOT yield empty even if exists happened to be true")
	}
}
