//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MariaDB native-network delivered-rendering premise pin
// (audit 2026-08-01 S2, third rendering).
//
// `mysqlCollationResolver.ResolveNetworkLiteralRendering` declares
// [ir.NetworkLiteralRenderingAddressOnly], and every `--where` literal on a
// native inet4/inet6 column is canonicalised under that declaration. It is a
// fact about a real server, so per the premise-naming rule it owes a named
// test rather than a comment.
//
// It exists because this was the ONE arm of the S2 fix that shipped
// code-read. The Postgres arms got a live two-leg pin; MariaDB's was declared
// from reading formatMariaDBInet, and the v0.109.0 regression cycle correctly
// reported it as the gap it could not cover — its rig had no MariaDB. CI's
// required engines-mysql shard boots both LTS lines, so the check belongs
// here rather than on a rig, where it runs every PR instead of once.
//
// Ground truth from mariadb:11.4, and the second half is stronger than the
// declaration assumed:
//
//	inet4 '10.0.0.1'      → "10.0.0.1"       bare, no mask
//	inet6 '2001:db8::1'   → "2001:db8::1"    bare, no mask
//	inet4 '10.0.0.0/24'   → REJECTED at INSERT
//	inet4 '10.0.0.1/32'   → REJECTED at INSERT, even at full width
//
// So these columns cannot hold ANY masked value — which is what makes
// refusing a network literal outright correct rather than merely strict: no
// stored value could ever equal one.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
)

func TestMariaDBNetworkRendering_IsAddressOnly_AndBindsToTheDeclaration(t *testing.T) {
	cases := []struct {
		col    string
		typ    string
		stored string
		want   string
	}{
		{"v4", "inet4", "10.0.0.1", "10.0.0.1"},
		{"v6", "inet6", "2001:db8::1", "2001:db8::1"},
		// ADR-0171's trailing-zero-stripping shapes: the binlog delivers these
		// with trailing 0x00 removed, so they are the ones a width-from-len
		// decode would get wrong.
		{"v4zero", "inet4", "0.0.0.0", "0.0.0.0"},
		{"v6zero", "inet6", "::", "::"},
		{"v6trunc", "inet6", "2001:db8::", "2001:db8::"},
		// IPv4-MAPPED. netip and MariaDB agree on this one, so it does NOT
		// guard the renderer choice — it is kept as the control that proves
		// the agreeing case still works.
		//
		// The comment here used to claim this was "IPv4-compatible v6, where
		// MariaDB's BSD inet_ntop6 and Go's netip disagree". It is not: the
		// case is even named `v6mapped`. That mislabel is why this pin was
		// green while the divergence it named was live (audit 2026-08-04 C1).
		{"v6mapped", "inet6", "::ffff:10.0.0.1", "::ffff:10.0.0.1"},

		// IPv4-COMPATIBLE, which is the shape that actually diverges: MariaDB
		// renders the trailing 32 bits as a dotted quad, Go's netip renders
		// hex groups (`::102:304`). These are the rows that fail if the
		// renderer ever reverts to netip.String.
		{"v6compat", "inet6", "::1.2.3.4", "::1.2.3.4"},
		{"v6compatHigh", "inet6", "::255.255.255.255", "::255.255.255.255"},
	}

	for _, image := range mariadbLTSImages() {
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "net_rendering")
			db, err := sql.Open("mysql", dsn+"&multiStatements=true")
			if err != nil {
				t.Fatalf("open %s: %v", image, err)
			}
			defer func() { _ = db.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			cols, vals := "", ""
			for _, c := range cases {
				cols += ", `" + c.col + "` " + c.typ
				vals += ", '" + c.stored + "'"
			}
			if _, err := db.ExecContext(ctx,
				"CREATE TABLE nets (id INT PRIMARY KEY"+cols+")"); err != nil {
				t.Fatalf("create table: %v", err)
			}
			if _, err := db.ExecContext(ctx,
				"INSERT INTO nets VALUES (1"+vals+")"); err != nil {
				t.Fatalf("insert: %v", err)
			}

			// (a) A masked value cannot be STORED at all — the property that
			//     makes refusing a network literal correct rather than strict.
			for _, bad := range []string{"10.0.0.0/24", "10.0.0.1/32"} {
				if _, err := db.ExecContext(ctx,
					"INSERT INTO nets (id, `v4`) VALUES (99, ?)", bad); err == nil {
					t.Errorf("MariaDB accepted %q into an inet4 column. The rendering declaration assumes "+
						"these columns hold an address and never a network, and the canonicaliser refuses a "+
						"network literal on that basis — if a mask can be stored, that refusal drops a "+
						"filter the source would have matched.", bad)
				}
			}

			// (b) What the server actually delivers, through sluice's reader.
			tbl := &ir.Table{
				Schema: "net_rendering", Name: "nets",
				Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}},
			}
			for _, c := range cases {
				tbl.Columns = append(tbl.Columns, &ir.Column{Name: c.col, Type: ir.Inet{}})
			}

			eng := Engine{}
			rr, err := eng.OpenRowReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenRowReader: %v", err)
			}
			defer func() {
				if cl, ok := rr.(interface{ Close() error }); ok {
					_ = cl.Close()
				}
			}()

			ch, err := rr.ReadRows(ctx, tbl)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			var row ir.Row
			for r := range ch {
				row = r
			}
			if row == nil {
				t.Fatal("no row read back")
			}

			for _, c := range cases {
				got, ok := row[c.col].(string)
				if !ok {
					t.Errorf("%s: non-string decode (%T)", c.col, row[c.col])
					continue
				}
				if got != c.want {
					t.Errorf("%s (stored %q): delivered %q, want %q — bare, with no prefix length",
						c.col, c.stored, got, c.want)
				}
			}

			// (c) BIND the delivered value to the DECLARED rendering. Pinning
			//     what the server sends and pinning the engine's declaration
			//     separately would leave the argument between them unpinned —
			//     the gap CLAUDE.md names with the VStream FLOAT carrier. So
			//     drive the REAL resolver through the REAL compiler and require
			//     the value just delivered to be accepted as canonical.
			resolver := mysqlCollationResolver{flavor: FlavorMariaDB}
			if got := resolver.ResolveNetworkLiteralRendering(false); got != ir.NetworkLiteralRenderingAddressOnly {
				t.Fatalf("this engine declares %v for a network column; this pin is written against AddressOnly", got)
			}
			infos := rowpredicate.ColumnInfosFromIR(resolver, tbl.Columns, false)
			for _, c := range cases {
				delivered, _ := row[c.col].(string)
				expr := c.col + " = '" + delivered + "'"
				if _, err := rowpredicate.Compile("nets", expr, infos); err != nil {
					t.Errorf("%s: the server delivered %q, but `--where %q` is REFUSED as non-canonical: %v\n"+
						"The declared rendering and the server disagree; an operator filtering on the value "+
						"their own database shows them cannot.", c.col, delivered, expr, err)
				}
			}

			// (d) And a NETWORK literal must be refused — no stored value can
			//     equal one, per (a).
			if _, err := rowpredicate.Compile("nets", "v4 = '10.0.0.0/24'", infos); err == nil {
				t.Error("a network literal compiled against a MariaDB inet4 column; the column cannot hold a " +
					"network, so this silently matches nothing")
			}
		})
	}
}
