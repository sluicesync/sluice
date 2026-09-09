// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import "testing"

// TestRowInCaptureScope_DDLMarkersAreNotTableScoped pins the fix for the
// silent-loss regression the v0.145.0 pre-tag value-fidelity review found
// in the S-2 layer-1 scope check — a defect that shipped with no test for
// this file at all.
//
// THE TWO ROW SHAPES, which is the whole finding. The change log's
// `table_name` column is written by four arms and carries two different
// shapes:
//
//   - row trigger (I/U/D) and TRUNCATE (T): a BARE `TG_TABLE_NAME`.
//   - `ddl_command_end` and `sql_drop` (X): `object_identity`, which is
//     SCHEMA-QUALIFIED — "public.orders".
//
// The pipeline's scope predicate matches bare names, so grading an X row
// through it asked `path.Match("orders", "public.orders")`, which is
// false. Every DDL marker for a CAPTURED table was dropped whenever
// `--include-table` was set.
//
// THE HARM IS THE D-1 CLASS, SILENTLY REOPENED. A `DROP TABLE` on a
// synced table writes an X row; the drop marker was discarded; the
// observed-DDL refusal never fired; the stream ran on at exit 0 with the
// target holding the dropped table's rows forever — while the WARN said
// the relation was "not synced", which was false.
//
// The no-filter case was unaffected (an empty filter allows everything),
// which is exactly why a representative test would have missed it.
func TestRowInCaptureScope_DDLMarkersAreNotTableScoped(t *testing.T) {
	// A scope predicate shaped like the pipeline's: bare-name matching,
	// this stream syncing only "orders".
	onlyOrders := func(_, table string) bool { return table == "orders" }

	r := &CDCReader{schema: "public", scopeAllowed: onlyOrders}

	t.Run("a DDL marker for a captured table is NOT dropped", func(t *testing.T) {
		// The exact value the sql_drop arm writes for `DROP TABLE
		// public.orders` — object_identity, schema-qualified.
		if !r.rowInCaptureScope("X", "public", "public.orders", 0) {
			t.Fatal("the DDL marker for a table this stream DOES sync was dropped. Its table_name is " +
				"object_identity (schema-qualified) while the scope predicate matches bare names, so the " +
				"drop marker is discarded, the observed-DDL refusal never fires, and the stream runs at " +
				"exit 0 with the target holding the dropped table's rows forever — the D-1 class reopened.")
		}
	})

	t.Run("a DDL marker from another schema is still dropped", func(t *testing.T) {
		// The security half must survive the exemption: the table-scope
		// check is skipped for X rows, the SCHEMA check is not. Without
		// this, a decoy relation elsewhere could halt the stream.
		if r.rowInCaptureScope("X", "evil", "evil.orders", 0) {
			t.Fatal("a DDL marker from a foreign schema was accepted; the schema half must still apply " +
				"to X rows even though the table half does not")
		}
	})

	t.Run("row events are still table-scoped", func(t *testing.T) {
		// The exemption must not leak to data-bearing ops — those are the
		// rows that can forge a write, and they carry BARE names, so the
		// predicate works on them.
		if !r.rowInCaptureScope("I", "public", "orders", 0) {
			t.Error("an INSERT for a synced table was dropped")
		}
		if r.rowInCaptureScope("I", "public", "secrets", 0) {
			t.Fatal("an INSERT for a table OUTSIDE this stream's scope was accepted; the table-scope " +
				"half must still apply to data-bearing ops")
		}
		if r.rowInCaptureScope("I", "evil", "orders", 0) {
			t.Fatal("an INSERT from a foreign schema was accepted — this is the S-2 attack itself")
		}
	})

	t.Run("with no scope predicate, nothing is table-dropped", func(t *testing.T) {
		// The non-streamer construction: scopeAllowed is nil and only the
		// schema half applies. A gate that failed here would break every
		// direct-API and test consumer.
		bare := &CDCReader{schema: "public"}
		for _, op := range []string{"I", "U", "D", "T", "X"} {
			if !bare.rowInCaptureScope(op, "public", "anything", 0) {
				t.Errorf("op %q was dropped with no scope predicate set", op)
			}
		}
	})
}

// TestRowInCaptureScope_AMovedCapturedTableStaysInScopeByOID pins audit
// 2026-09-09 A0909-PG-MEDIUM-1: an `ALTER TABLE ... SET SCHEMA` on a
// captured table writes its DDL marker under the NEW schema, and the
// schema check used to discard it before the DDL arm could refuse -- a
// loud halt turned into a silent permanent stall. The relation's OID
// survives the move, so a foreign-schema marker is in scope exactly when
// its OID is one this stream captured at open; a decoy relation in
// another schema never was, and a marker with no OID (an older capture
// function) matches nothing.
func TestRowInCaptureScope_AMovedCapturedTableStaysInScopeByOID(t *testing.T) {
	r := &CDCReader{schema: "public", capturedRelIDs: map[uint32]bool{16385: true}}
	if !r.rowInCaptureScope("X", "other", "other.orders", 16385) {
		t.Fatal("the DDL marker for a captured table moved to another schema was discarded; the stream would " +
			"stall silently instead of halting on the observed DDL")
	}
	if r.rowInCaptureScope("X", "evil", "evil.orders", 99999) {
		t.Fatal("a DDL marker for a relation this stream never captured was accepted from a foreign schema; " +
			"a decoy could halt the stream")
	}
	if r.rowInCaptureScope("X", "other", "other.orders", 0) {
		t.Fatal("a foreign-schema marker with NO recorded OID was accepted; an older capture function must not widen scope")
	}
	if r.rowInCaptureScope("I", "other", "orders", 16385) {
		t.Fatal("a ROW event from a foreign schema was accepted because its relation is captured; the OID rule is for DDL markers only (the S-2 security half stays)")
	}
	none := &CDCReader{schema: "public"}
	if none.rowInCaptureScope("X", "other", "other.orders", 16385) {
		t.Fatal("with no captured-OID set loaded, a foreign-schema marker was accepted")
	}
}

func TestDecodeDDLMarker_ReadsTheRelationOID(t *testing.T) {
	m := decodeDDLMarker(`{"command_tag":"ALTER TABLE","object_type":"table","objid":"16385"}`)
	if m.relID != 16385 || m.tag != "ALTER TABLE" {
		t.Fatalf("decoded %+v; want relID 16385 and the tag", m)
	}
	if old := decodeDDLMarker(`{"command_tag":"ALTER TABLE","object_type":"table"}`); old.relID != 0 || old.tag != "ALTER TABLE" {
		t.Fatalf("a pre-OID marker decoded as %+v; want relID 0 with the tag intact", old)
	}
}
