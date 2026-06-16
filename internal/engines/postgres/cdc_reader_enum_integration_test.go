//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// catalog Bug 151 CDC enum pin. Before this, the pgoutput path's oidToType
// had no case for a user-defined ENUM (its OID is dynamic, assigned at
// CREATE TYPE time), so a PG source table with an enum column wedged the
// sync stream on the first enum DML ("unsupported column type OID <dyn>
// (typmod -1)") — enums cold-start (COPY) fine but could not be
// continuous-synced PG→PG. Same class as the Bug 144 array / Bug 147
// geometry oidToType gaps. This pin resolves the runtime enum OIDs
// (ensureEnumTypeOIDs / buildRelationCacheEntry → bare ir.Enum) and drives
// real pgoutput INSERT/UPDATE/DELETE through the live replication stream,
// asserting the decoded ir.Row value is the enum's text LABEL.
//
// Coverage: two DISTINCT enum types in the same table (proves the resolved
// set carries more than one dynamic OID); the UPDATE after-image; a NULL
// enum value; a DELETE before-image on a NO-PK REPLICA IDENTITY FULL table
// (where the before-image carries the full row incl. the enum label, unlike
// a PK table whose before-image narrows to the key).
//
// Enum ARRAY columns (enum[]) are intentionally NOT covered: their array OID
// is neither a builtin array OID nor typtype='e', so they stay loud-refused
// (a tracked follow-up, same posture as geography for geometry).

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_Enum_Bug151(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	// Two distinct enum types so the resolved enumOIDs set must carry more
	// than one dynamic OID (a single-OID scalar like geometryOID wouldn't
	// suffice for enums).
	// One label carries a comma + space + embedded single-quote ("it's a,
	// test") to pin that a scalar enum value rides pgoutput text format as
	// raw wire bytes straight to decodeString — there is NO array-style
	// escaping at the scalar level, so special chars must round-trip verbatim.
	applyPGSQL(t, dsn, `
		CREATE TYPE order_status AS ENUM ('draft','live','void','it''s a, test');
		CREATE TYPE priority     AS ENUM ('low','high');
		CREATE TABLE orders (
			id     BIGINT PRIMARY KEY,
			status order_status,
			prio   priority,
			note   text
		);
		ALTER TABLE orders REPLICA IDENTITY FULL;
		CREATE TABLE orders_nopk (
			tag    text,
			status order_status
		);
		ALTER TABLE orders_nopk REPLICA IDENTITY FULL;
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// orders: INSERT of TWO rows (a multi-row INSERT emits one event per row
	// → 2 events; two enum types + a NULL enum) + UPDATE (after-image new
	// label) + DELETE (PK before-image narrows to {id}). orders_nopk: INSERT
	// + DELETE (FULL, no PK → before-image carries the enum label). 6 row
	// events; their arrival proves the enum doesn't wedge (Bug 151).
	applyPGSQL(t, dsn, `
		INSERT INTO orders VALUES (1, 'live', 'high', 'a'), (2, 'it''s a, test', NULL, 'b');
		UPDATE orders SET status = 'void' WHERE id = 1;
		DELETE FROM orders WHERE id = 2;
		INSERT INTO orders_nopk VALUES ('x', 'draft');
		DELETE FROM orders_nopk WHERE tag = 'x';
	`)

	const wantChanges = 6
	got := drainChanges(t, ctx, changes, wantChanges, 60*time.Second)
	if len(got) != wantChanges {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want %d (enum must not wedge the stream — Bug 151; stream error: %v)", len(got), wantChanges, streamErr)
			}
		}
		t.Fatalf("got %d changes; want %d (enum must not wedge the stream — Bug 151)", len(got), wantChanges)
	}

	var ins1, ins2 ir.Insert
	var upd ir.Update
	var del ir.Delete
	var nopkIns ir.Insert
	var nopkDel ir.Delete
	for _, c := range got {
		switch v := c.(type) {
		case ir.Insert:
			if v.Table == "orders_nopk" {
				nopkIns = v
			} else if id, _ := v.Row["id"].(int64); id == 1 {
				ins1 = v
			} else {
				ins2 = v
			}
		case ir.Update:
			upd = v
		case ir.Delete:
			if v.Table == "orders_nopk" {
				nopkDel = v
			} else {
				del = v
			}
		default:
			t.Fatalf("unexpected change type %T", c)
		}
	}

	asLabel := func(label string, v any) string {
		t.Helper()
		s, ok := v.(string)
		if !ok {
			t.Fatalf("%s: decoded enum = %T(%v); want string label", label, v, v)
		}
		return s
	}

	// --- INSERT: both enum types decode to their text label ---
	if ins1.Row == nil {
		t.Fatal("orders INSERT id=1 missing")
	}
	if l := asLabel("ins1.status", ins1.Row["status"]); l != "live" {
		t.Errorf("ins1.status = %q; want live", l)
	}
	if l := asLabel("ins1.prio", ins1.Row["prio"]); l != "high" {
		t.Errorf("ins1.prio = %q; want high", l)
	}
	// NULL enum: decodeTuple maps 'n' → nil before decodeValue, so the column
	// lands as a present nil (not absent, not garbage).
	if ins2.Row == nil {
		t.Fatal("orders INSERT id=2 missing")
	}
	// Special-char label (comma + space + embedded single-quote) decodes verbatim.
	if l := asLabel("ins2.status", ins2.Row["status"]); l != "it's a, test" {
		t.Errorf("ins2.status = %q; want %q (special chars must round-trip verbatim)", l, "it's a, test")
	}
	if v, present := ins2.Row["prio"]; !present || v != nil {
		t.Errorf("ins2.prio = (present=%v, %v); want present nil", present, v)
	}

	// --- UPDATE after-image enum decodes to the new label ---
	if upd.After == nil {
		t.Fatal("orders UPDATE after-image missing")
	}
	if l := asLabel("upd.status", upd.After["status"]); l != "void" {
		t.Errorf("upd.after.status = %q; want void", l)
	}

	// --- DELETE on a PK table: before-image narrows to {id} (FULL+PK), so
	// the enum is absent — pin the narrowing reality. ---
	if del.Before == nil {
		t.Fatal("orders DELETE before-image missing")
	}
	if id, _ := del.Before["id"].(int64); id != 2 {
		t.Errorf("del.before.id = %v; want 2", del.Before["id"])
	}
	if _, present := del.Before["status"]; present {
		t.Errorf("del.before.status present; want narrowed away (FULL+PK before-image is key-only)")
	}

	// --- DELETE on a NO-PK FULL table: the before-image carries the full
	// row, so an enum IN the before-image must decode to its label. ---
	if nopkIns.Row == nil || nopkDel.Before == nil {
		t.Fatalf("orders_nopk INSERT/DELETE missing (ins=%v del=%v)", nopkIns.Row != nil, nopkDel.Before != nil)
	}
	if l := asLabel("nopk.ins.status", nopkIns.Row["status"]); l != "draft" {
		t.Errorf("nopk.ins.status = %q; want draft", l)
	}
	if l := asLabel("nopk.del.before.status", nopkDel.Before["status"]); l != "draft" {
		t.Errorf("nopk.del.before.status = %q; want draft (enum in a FULL before-image must decode)", l)
	}
}
