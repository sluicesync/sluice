//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The UUID canonical-form premise pin (2026-08-22 invariant sweep — the
// missing member of the identifier-literal family matrix).
//
// rowpredicate's identifier-literal lens rests its whole client-evaluator
// argument on a sentence at identifier_literal.go: "the DECODED value ...
// is always canonical (decodeUUID lowercases and hyphenates, ...)". The
// NETWORK and MAC members of that family earned real-server bindings when
// their premises turned out wrong in live-confirmed ways (the S2 inet/cidr
// mirror bugs, the C1 dotted-quad rendering, the MAC width) —
// network_rendering_integration_test.go and
// macaddr_width_integration_test.go — and the temporal member has
// rowpredicate's own realdb suite. UUID had only self-referential unit
// pins: rowpredicate asserting what rowpredicate produces. Nothing bound
// the CLIENT evaluator's canonical form to what the engine's decoders
// actually deliver — the two-facts-pinned-argument-unpinned shape CLAUDE.md
// names with the VStream FLOAT carrier.
//
// The harm model if the premise broke is the SL-3 one exactly: a --where
// filter on a uuid column compiles (the literal IS canonical per
// rowpredicate), the snapshot leg copies the rows (server-evaluated), and
// the CDC leg's client evaluator then scores every change against a
// spelling the decoder never produces — every change to a matching row
// dropped at exit 0.
//
// What this pins, against a live PG:
//
//   - the server accepts NON-canonical spellings (uppercase, braced,
//     unhyphenated) and both sluice legs — the snapshot/bulk-copy read and
//     the pgoutput CDC read — deliver the SAME canonical lowercase
//     8-4-4-4-12 string;
//   - the REAL resolver + compiler accept the delivered spelling as
//     canonical (the argument-binding half, mirroring the network test);
//   - the compiled predicate's Eval scores the CDC-delivered row IN scope
//     (the client evaluator itself, run against the real decode).
//
// A failure here means the decoder and the lens disagree — the silent-drop
// shape — not that the test is brittle.

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
)

// uuidCanonical is the one spelling the decoders are documented to
// produce, for the value every stored variant below spells.
const uuidCanonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

func TestUUIDCanonicalForm_BothLegsDeliverTheEvaluatorsForm(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE uuids (
			id      BIGINT PRIMARY KEY,
			u_upper uuid,
			u_brace uuid,
			u_bare  uuid
		);
		ALTER TABLE uuids REPLICA IDENTITY FULL;
	`)

	// Three spellings PG itself accepts for the SAME value, none canonical.
	// If any row errors at INSERT the premise landscape changed server-side
	// and the test should say so loudly rather than skip.
	applyPGSQL(t, dsn, `
		INSERT INTO uuids VALUES (
			1,
			'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
			'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}',
			'a0eebc999c0b4ef8bb6d6bb9bd380a11'
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tbl := &ir.Table{
		Schema: "public",
		Name:   "uuids",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "u_upper", Type: ir.UUID{}},
			{Name: "u_brace", Type: ir.UUID{}},
			{Name: "u_bare", Type: ir.UUID{}},
		},
	}
	uuidCols := []string{"u_upper", "u_brace", "u_bare"}

	// ---- Leg 1: the snapshot / bulk-copy read ----
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() {
		if c, ok := rr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	snapRows := drainAllRows(t, ctx, rr, tbl)
	if len(snapRows) != 1 {
		t.Fatalf("snapshot leg: got %d rows; want 1", len(snapRows))
	}
	snap := snapRows[0]

	// ---- Leg 2: the CDC read (pgoutput text) ----
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

	applyPGSQL(t, dsn, `
		INSERT INTO uuids VALUES (
			2,
			'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
			'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}',
			'a0eebc999c0b4ef8bb6d6bb9bd380a11'
		);
	`)

	got := drainChanges(t, ctx, changes, 1, 60*time.Second)
	if len(got) != 1 {
		t.Fatalf("cdc leg: got %d changes; want 1", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("cdc leg: got %T; want ir.Insert", got[0])
	}
	cdc := ins.Row

	// ---- (a)+(b): the legs agree, and both deliver THE canonical form ----
	for _, col := range uuidCols {
		snapVal, sok := snap[col].(string)
		cdcVal, cok := cdc[col].(string)
		if !sok || !cok {
			t.Errorf("%s: non-string decode (snapshot %T, cdc %T)", col, snap[col], cdc[col])
			continue
		}
		if snapVal != cdcVal {
			t.Errorf("%s: THE TWO LEGS DISAGREE — snapshot delivers %q, CDC delivers %q. No single --where "+
				"literal can be correct on both; uuid would have to move to the refuse-outright bucket.",
				col, snapVal, cdcVal)
			continue
		}
		if snapVal != uuidCanonical {
			t.Errorf("%s: delivered %q; want the canonical %q. identifier_literal.go's 'decodeUUID lowercases "+
				"and hyphenates' premise is FALSE of the real decode, so a canonical --where literal never "+
				"equals the delivered value and every CDC change to a matching row is silently dropped.",
				col, snapVal, uuidCanonical)
		}
	}

	// ---- The argument binding: real resolver → real compiler → real Eval ----
	// Compile with the DELIVERED spelling (must be accepted as canonical) and
	// evaluate against the CDC-delivered row (must score IN scope). This is
	// the client evaluator itself running over the real decode — the half no
	// unit pin on either side can supply.
	infos := rowpredicate.ColumnInfosFromIR(pgCollationResolver{}, tbl.Columns, false)
	for _, col := range uuidCols {
		delivered, _ := snap[col].(string)
		expr := col + " = '" + delivered + "'"
		pred, err := rowpredicate.Compile("uuids", expr, infos)
		if err != nil {
			t.Errorf("%s: server delivered %q but `--where %q` is REFUSED as non-canonical: %v", col, delivered, expr, err)
			continue
		}
		if !pred.Eval(cdc) {
			t.Errorf("%s: the compiled canonical predicate %q scores the CDC-delivered row OUT of scope — "+
				"the client evaluator and the decoder disagree; every change to this row would be dropped at exit 0.",
				col, expr)
		}
	}

	// Anti-vacuity control for the canonical check itself: the uppercase
	// spelling must REFUSE at compile on this same ColumnInfo set (proving
	// the canonicaliser is actually engaged for these columns, so the
	// acceptances above are meaningful).
	if _, err := rowpredicate.Compile("uuids", "u_upper = 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'", infos); err == nil {
		t.Error("control: an UPPERCASE uuid literal compiled without refusal — the canonical-spelling check is not engaged for these columns, so this test's acceptances prove nothing")
	} else if !strings.Contains(err.Error(), "canonical") {
		t.Errorf("control: uppercase literal refused with %v; want the not-canonical refusal", err)
	}
}
