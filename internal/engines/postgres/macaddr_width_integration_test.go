//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The `macaddr` / `macaddr8` width pins (roadmap item 117 / Bug 225).
//
// Two facts about a real Postgres server hold up two pieces of code, and both
// were argued rather than checked before this file existed:
//
//  1. The reader must distinguish the two widths, or the emitter narrows a
//     `macaddr8` column to MACADDR and the COPY fails on the first 8-byte
//     value. TestSchemaReader_MacaddrWidths reads them from a real catalog.
//  2. Postgres WIDENS a 6-byte EUI-48 input to EUI-64 in a macaddr8 column, by
//     inserting FF:FE in the middle. rowpredicate's canonical-spelling refusal
//     computes that widened form and tells the operator to write it, so if the
//     rule were different the refusal would name the wrong literal — a loud
//     error with wrong advice, on the exact path built to stop a silent one.
//     That is a safety argument resting on an environmental fact, so per the
//     premise-naming rule it gets a check against the server rather than a
//     comment. TestMacaddr8WidensEUI48OnInput is it.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApplier_MacaddrWidthMatrix is the CDC-side half of the write-path
// enumeration, and it exists because the COPY half needed a fix and this half
// did not — a claim that is worth nothing unmeasured.
//
// The four cells {macaddr, macaddr8} × {scalar, array} are the same matrix the
// COPY path takes. The applier binds parameterised INSERTs rather than binary
// COPY, so PG's own type input function parses the text and the missing pgx
// `_macaddr8` array codec never comes into play. Measured, on a real PG 16:
// all four pass with no registration, which is why registerPGMacaddr8ArrayCodec
// is wired into copyFromOnSQLConn ONLY. If that ever stops being true, this
// fails here rather than in the field.
func TestApplier_MacaddrWidthMatrix(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE macs (
		id BIGINT PRIMARY KEY, m6 MACADDR, m8 MACADDR8, a6 MACADDR[], a8 MACADDR8[]);`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	pumpBatchedChanges(t, ctx, applier, []ir.Change{
		ir.Insert{
			Position: ir.Position{Engine: engineNamePostgres, Token: "m1"},
			Schema:   "public", Table: "macs",
			Row: ir.Row{
				"id": int64(1),
				"m6": "08:00:2b:01:02:03",
				"m8": "08:00:2b:ff:fe:01:02:03",
				"a6": []any{"08:00:2b:01:02:03", nil},
				"a8": []any{"08:00:2b:ff:fe:01:02:03", nil},
			},
		},
		// The UPDATE arm too: it binds through a different statement shape.
		ir.Update{
			Position: ir.Position{Engine: engineNamePostgres, Token: "m2"},
			Schema:   "public", Table: "macs",
			Before: ir.Row{"id": int64(1)},
			After: ir.Row{
				"id": int64(1),
				"m6": "aa:bb:cc:dd:ee:ff",
				"m8": "01:02:03:04:05:06:07:08",
				"a6": []any{"aa:bb:cc:dd:ee:ff"},
				"a8": []any{"01:02:03:04:05:06:07:08"},
			},
		},
	}, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var m6, m8, a6, a8 string
	if err := db.QueryRowContext(
		ctx,
		`SELECT m6::text, m8::text, a6::text, a8::text FROM macs WHERE id = 1`,
	).Scan(&m6, &m8, &a6, &a8); err != nil {
		t.Fatalf("read applied row: %v", err)
	}
	for _, tc := range []struct{ name, got, want string }{
		{"macaddr", m6, "aa:bb:cc:dd:ee:ff"},
		{"macaddr8", m8, "01:02:03:04:05:06:07:08"},
		{"macaddr[]", a6, "{aa:bb:cc:dd:ee:ff}"},
		{"macaddr8[]", a8, "{01:02:03:04:05:06:07:08}"},
	} {
		if tc.got != tc.want {
			t.Errorf("applied %s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

// TestSchemaReader_MacaddrWidths ground-truths the text-keyed reader against a
// real catalog: `macaddr` and `macaddr8` are two different data_type strings
// and must produce two different IR widths, scalars and arrays alike.
func TestSchemaReader_MacaddrWidths(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, `
		CREATE TABLE devices (
			id        BIGINT PRIMARY KEY,
			mac6      MACADDR,
			mac8      MACADDR8,
			mac6_arr  MACADDR[],
			mac8_arr  MACADDR8[]
		);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	var tbl *ir.Table
	for _, tt := range schema.Tables {
		if tt.Name == "devices" {
			tbl = tt
		}
	}
	if tbl == nil {
		t.Fatal("table devices not found in the read schema")
	}

	want := map[string]ir.Type{
		"mac6":     ir.Macaddr{Width: ir.MacaddrEUI48},
		"mac8":     ir.Macaddr{Width: ir.MacaddrEUI64},
		"mac6_arr": ir.Array{Element: ir.Macaddr{Width: ir.MacaddrEUI48}},
		"mac8_arr": ir.Array{Element: ir.Macaddr{Width: ir.MacaddrEUI64}},
	}
	for _, c := range tbl.Columns {
		w, tracked := want[c.Name]
		if !tracked {
			continue
		}
		if c.Type != w {
			t.Errorf("column %q read as %#v, want %#v — a macaddr8 column read as a 6-byte "+
				"ir.Macaddr is emitted as MACADDR on the target and the COPY fails (Bug 225)",
				c.Name, c.Type, w)
		}
		delete(want, c.Name)
	}
	for name := range want {
		t.Errorf("column %q was not present in the read schema at all", name)
	}
}

// TestMacaddr8WidensEUI48OnInput is the PREMISE CHECK behind
// rowpredicate.widenEUI48 and the truth table in checkMACLiteralWidth's doc.
//
// It asserts the two halves the safety argument needs, against the server:
// a 6-byte input into a macaddr8 column comes back WIDENED (so a 6-byte
// literal on such a column can never equal the delivered value — the silent
// C1 residual), and the widening is FF:FE insertion with no universal/local
// bit flip (so the literal the refusal tells the operator to write is the one
// the column actually delivers). The `macaddr8_set7bit` row is the control:
// it shows the bit flip exists as an explicit function and is NOT what input
// coercion does.
func TestMacaddr8WidensEUI48OnInput(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE mac_widening (label TEXT PRIMARY KEY, v MACADDR8);
		INSERT INTO mac_widening VALUES
			('from_eui48', '08:00:2b:01:02:03'),
			('from_eui64', '08:00:2b:ff:fe:01:02:03');
	`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for _, tc := range []struct {
		label string
		want  string
	}{
		// The load-bearing row: the 6-byte spelling an operator naturally
		// writes is stored and delivered as the 8-byte one.
		{"from_eui48", "08:00:2b:ff:fe:01:02:03"},
		{"from_eui64", "08:00:2b:ff:fe:01:02:03"},
	} {
		var got string
		if err := db.QueryRowContext(ctx,
			`SELECT v::text FROM mac_widening WHERE label = $1`, tc.label).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", tc.label, err)
		}
		if got != tc.want {
			t.Errorf("macaddr8 %s delivered %q, want %q.\n"+
				"rowpredicate.widenEUI48 computes the canonical spelling a --where literal must use on a "+
				"macaddr8 column from THIS rule. If the server's rule changed, that refusal now names a "+
				"literal the column does not deliver — loud, and wrong.", tc.label, got, tc.want)
		}
	}

	// The control. `macaddr8_set7bit` flips the universal/local bit; input
	// coercion does not. If these two ever agreed, the assertion above would
	// no longer distinguish the two rules and would stop pinning the one
	// widenEUI48 implements.
	var flipped string
	if err := db.QueryRowContext(ctx,
		`SELECT macaddr8_set7bit('08:00:2b:01:02:03'::macaddr8)::text`).Scan(&flipped); err != nil {
		t.Fatalf("macaddr8_set7bit: %v", err)
	}
	if flipped == "08:00:2b:ff:fe:01:02:03" {
		t.Errorf("macaddr8_set7bit produced the same value as plain input coercion (%q) — the control "+
			"is vacuous and the widening assertion above no longer pins WHICH rule applies", flipped)
	}
}
