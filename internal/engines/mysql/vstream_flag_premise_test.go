// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestMySQLFlagAutoIncrementBoundToVitessCarrier is the code-scoped half of the
// PREM-VSTREAM-AUTOINC premise (audit-2026-08-19 value-fidelity follow-up): it
// binds sluice's flag-512 constant AND its consumption to the CARRIER'S OWN
// vocabulary — vitess.io/vitess's querypb.MySqlFlag enum, the type whose value
// vtgate puts on query.Field.Flags — so the two facts ("sluice reads bit 512"
// and "the carrier defines AUTO_INCREMENT as bit 512") are held together by a
// test rather than by two independent assertions that could drift apart (the
// exact two-facts-pinned-argument-unpinned shape the premise-naming rule
// warns about).
//
// Scope, stated per the gate-enumeration rule: this binds sluice's constant
// and classifier to the vendored carrier's DEFINITION. It cannot and does not
// verify that a live vtgate POPULATES the flag on the VStream wire for an
// auto-increment column — that half needs a real cluster/vttestserver run and
// remains the UNVERIFIED PREMISE named at [mysqlFlagAutoIncrement].
func TestMySQLFlagAutoIncrementBoundToVitessCarrier(t *testing.T) {
	carrier := uint32(querypb.MySqlFlag_AUTO_INCREMENT_FLAG)
	if carrier == 0 {
		t.Fatal("anti-vacuity: the carrier's AUTO_INCREMENT_FLAG is zero — the binding would hold vacuously")
	}
	if carrier != uint32(mysqlFlagAutoIncrement) {
		t.Fatalf("mysqlFlagAutoIncrement = %d but the vendored Vitess carrier defines AUTO_INCREMENT_FLAG = %d; "+
			"the VStream schema mapping AND the tinyint(1) value decode both read the wrong bit",
			mysqlFlagAutoIncrement, carrier)
	}

	// Bind the CONSUMPTION, not just the constant: a field flagged with the
	// carrier's own enum value must flip the tinyint(1) bool classification
	// (auto-increment tinyint(1) is an integer, never a bool) through the real
	// VStream classifier — the path whose value decode depends on this premise
	// since v0.130.1.
	autoInc := &querypb.Field{ColumnType: "tinyint(1)", Flags: uint32(querypb.MySqlFlag_AUTO_INCREMENT_FLAG)}
	if vstreamTinyint1IsBool(autoInc) {
		t.Fatal("a field carrying the carrier's AUTO_INCREMENT_FLAG classified as bool — " +
			"the flag the classifier reads is not the flag the carrier defines")
	}
	plain := &querypb.Field{ColumnType: "tinyint(1)"}
	if !vstreamTinyint1IsBool(plain) {
		t.Fatal("anti-vacuity: a plain signed tinyint(1) must classify as bool, or the auto-inc cell proves nothing")
	}
}
