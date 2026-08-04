//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The inet/cidr delivered-rendering premise pin (audit 2026-08-01 S2).
//
// `pgCollationResolver.ResolveNetworkLiteralRendering` declares how this
// engine renders network values, and a `--where` filter's whole correctness
// rests on that declaration being TRUE of what the server actually delivers.
// That is a fact about the environment, so per the premise-naming rule it owes
// a named test rather than a comment, and this is it.
//
// This test EXISTS because reasoning got it wrong twice, in opposite
// directions, and only a live server settled it:
//
//   - The audit filed one Postgres bug, in one direction.
//   - The first fix inverted that direction after reading pgx v5.10.0's
//     InetCodec.DecodeValue, which scans unconditionally into a netip.Prefix
//     and would render "10.0.0.1/32". That is real code and it is not the
//     path this program takes: the row reader goes through database/sql,
//     where the value arrives as Postgres's own text output.
//   - A `::text` cast in psql shows the mask at every width, which looks like
//     confirmation of the masked reading and is not what the wire carries.
//
// What the server actually does, and what this pins:
//
//	inet '10.0.0.1'    → "10.0.0.1"      (full-width mask DROPPED)
//	inet '10.0.0.0/24' → "10.0.0.0/24"   (narrower mask kept)
//	cidr '10.0.0.1/32' → "10.0.0.1/32"   (mask ALWAYS kept)
//
// So inet and cidr have OPPOSITE canonical spellings for the same address —
// which makes S2 two mirror-image bugs, not one, and is why the rendering is
// resolved per network type rather than per engine.
//
// It also pins that the two legs AGREE. The snapshot read and the pgoutput
// CDC read reach the same decodeNetwork by different routes, and if they
// disagreed no single literal could be correct on both — putting these
// columns in the refuse-outright bucket beside fractional TIME rather than
// the canonicalise bucket. A failure here means the declaration is wrong, not
// that the test is brittle.

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
)

// networkRenderingCase is one stored value and the form the server delivers.
type networkRenderingCase struct {
	col string
	// stored is the literal handed to INSERT.
	stored string
	// wantDelivered is what BOTH legs must return. These are ground truth
	// from a live PG 16 reading the column — NOT a `::text` cast, which
	// always shows the mask and is the trap the first cut of this fix fell
	// into.
	wantDelivered string
}

// TestNetworkRendering_BothLegsAgree_AndMatchTheDeclaredRendering is the S2
// premise pin. It covers the family, not one representative: v4 host, v6 host,
// v4 network, v6 network, across both inet and cidr.
func TestNetworkRendering_BothLegsAgree_AndMatchTheDeclaredRendering(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE nets (
			id        BIGINT PRIMARY KEY,
			v4_host   inet,
			v6_host   inet,
			v4_net    inet,
			v6_net    inet,
			c4_host   cidr,
			c4_net    cidr,
			c6_net    cidr,
			v6_compat inet,
			v6_high   inet
		);
		ALTER TABLE nets REPLICA IDENTITY FULL;
	`)

	cases := []networkRenderingCase{
		// inet DROPS a full-width mask. This is S2 bug one: a literal written
		// '10.0.0.1/32' — which PG itself accepts — can never equal this.
		{col: "v4_host", stored: "10.0.0.1", wantDelivered: "10.0.0.1"},
		{col: "v6_host", stored: "2001:db8::1", wantDelivered: "2001:db8::1"},
		// inet KEEPS a narrower mask, including with host bits set.
		{col: "v4_net", stored: "10.0.0.0/24", wantDelivered: "10.0.0.0/24"},
		{col: "v6_net", stored: "2001:db8::/32", wantDelivered: "2001:db8::/32"},
		// cidr ALWAYS keeps the mask, even at full width — the opposite rule
		// on the sibling type. This is S2 bug two: here the BARE literal is
		// the one that can never match.
		{col: "c4_host", stored: "10.0.0.1/32", wantDelivered: "10.0.0.1/32"},
		{col: "c4_net", stored: "10.0.0.0/24", wantDelivered: "10.0.0.0/24"},
		{col: "c6_net", stored: "2001:db8::/32", wantDelivered: "2001:db8::/32"},
		// IPv4-COMPATIBLE v6: PG renders the trailing 32 bits as a dotted
		// quad (BSD inet_ntop6); Go's netip renders hex groups, so a literal
		// canonicalised through netip.String would be `::102:304` and could
		// never equal this (audit 2026-08-04 C1). Every other v6 row above
		// uses a 2001:db8:: shape, where the two agree -- which is why they
		// were all green while the divergence was live.
		{col: "v6_compat", stored: "::1.2.3.4", wantDelivered: "::1.2.3.4"},
		{col: "v6_high", stored: "::255.255.255.255", wantDelivered: "::255.255.255.255"},
	}

	applyPGSQL(t, dsn, `
		INSERT INTO nets VALUES (
			1, '10.0.0.1', '2001:db8::1', '10.0.0.0/24', '2001:db8::/32',
			'10.0.0.1/32', '10.0.0.0/24', '2001:db8::/32',
			'::1.2.3.4', '::255.255.255.255'
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// ---- Leg 1: the snapshot / bulk-copy read (pgx codec) ----
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() {
		if c, ok := rr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	tbl := &ir.Table{
		Schema: "public",
		Name:   "nets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v4_host", Type: ir.Inet{}},
			{Name: "v6_host", Type: ir.Inet{}},
			{Name: "v4_net", Type: ir.Inet{}},
			{Name: "v6_net", Type: ir.Inet{}},
			{Name: "c4_host", Type: ir.Cidr{}},
			{Name: "c4_net", Type: ir.Cidr{}},
			{Name: "c6_net", Type: ir.Cidr{}},
		},
	}
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
		INSERT INTO nets VALUES (
			2, '10.0.0.1', '2001:db8::1', '10.0.0.0/24', '2001:db8::/32',
			'10.0.0.1/32',
			'10.0.0.0/24', '2001:db8::/32'
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

	// ---- The assertions ----
	//
	// (a) The two legs must agree. A disagreement means no single --where
	//     literal is correct on both, which would put inet/cidr in the
	//     refuse-outright bucket beside fractional TIME.
	// (b) Both must equal what the canonicaliser would produce under the
	//     rendering pgCollationResolver DECLARES for that column's type. That
	//     is the actual contract: the declaration is what rowpredicate
	//     canonicalises literals under, so if the delivered value differs from
	//     the canonical spelling, every matching row is silently dropped.
	for _, tc := range cases {
		snapVal, sok := snap[tc.col].(string)
		cdcVal, cok := cdc[tc.col].(string)
		if !sok || !cok {
			t.Errorf("%s: non-string decode (snapshot %T, cdc %T)", tc.col, snap[tc.col], cdc[tc.col])
			continue
		}

		if snapVal != cdcVal {
			t.Errorf("%s (stored %q): THE TWO LEGS DISAGREE — snapshot delivers %q, CDC delivers %q. "+
				"No single --where literal can be correct on both, so this column family would have to be "+
				"refused outright (the fractional-TIME shape), NOT canonicalised. The declared rendering is "+
				"unsound as written.",
				tc.col, tc.stored, snapVal, cdcVal)
			continue
		}

		if snapVal != tc.wantDelivered {
			t.Errorf("%s (stored %q): delivered %q; want %q. This is the value a --where literal is compared "+
				"against, so a mismatch here means the declared rendering canonicalises to a spelling the "+
				"server never sends, and every change to a matching row is silently dropped.",
				tc.col, tc.stored, snapVal, tc.wantDelivered)
		}
	}

	// ---- Bind the two facts together ----
	//
	// Everything above pins what the SERVER sends. pgCollationResolver's
	// declaration is pinned by its own unit matrix. Neither pins the
	// ARGUMENT that connects them — the exact gap CLAUDE.md names with the
	// VStream FLOAT carrier ("two facts can each be pinned and still leave
	// the argument unpinned"). So: drive the REAL resolver through the REAL
	// compiler and require that the value the server just delivered is
	// accepted as canonical. If the declaration drifts from the server, this
	// fails even though both halves above still pass.
	infos := rowpredicate.ColumnInfosFromIR(pgCollationResolver{}, tbl.Columns, false)
	for _, tc := range cases {
		delivered, _ := snap[tc.col].(string)
		expr := tc.col + " = '" + delivered + "'"
		if _, err := rowpredicate.Compile("nets", expr, infos); err != nil {
			t.Errorf("%s: the server delivered %q, but `--where %q` is REFUSED as non-canonical: %v\n"+
				"The declared rendering and the server disagree. An operator writing the value their own "+
				"database shows them cannot filter on it.",
				tc.col, delivered, expr, err)
		}
	}
}
