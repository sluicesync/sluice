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
	"strings"
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

		// A longest zero run of length ONE, which MariaDB COMPRESSES and RFC
		// 5952 §4.2.2 forbids — so Postgres and Go's netip both leave these
		// uncompressed and MariaDB does not. These are the rows that separate
		// the two renderings; without them the per-engine split in
		// ir.RenderNetworkAddr would be indistinguishable from a shared rule.
		//
		// This is also the shape that was live with NO --where involved:
		// ir.Inet emits VARCHAR on a MySQL-family target, so a cold copy wrote
		// the server's compressed spelling while a later CDC UPDATE of the same
		// row wrote the uncompressed one, and the target held a different
		// string depending on whether the row had ever been updated. Interior,
		// leading and trailing are all pinned — the trailing one exercises the
		// renderer's separate end-of-address colon.
		{"v6single", "inet6", "2001:db8:0:1:2:3:4:5", "2001:db8::1:2:3:4:5"},
		{"v6lead", "inet6", "0:1:2:3:4:5:6:7", "::1:2:3:4:5:6:7"},
		{"v6trail", "inet6", "1:2:3:4:5:6:7:0", "1:2:3:4:5:6:7::"},
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
				// The FAMILY comes from the declared column type, exactly as
				// the schema reader's mariadbNativeDataTypes registry sets it
				// (roadmap item 133). A bare ir.Inet{} here would refuse every
				// literal in step (c) for the undeclared-family reason and
				// make this whole pin vacuous.
				family := ir.InetFamilyIPv4
				if c.typ == "inet6" {
					family = ir.InetFamilyIPv6
				}
				tbl.Columns = append(tbl.Columns,
					&ir.Column{Name: c.col, Type: ir.Inet{Family: family}})
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

// TestMariaDBInet6WidensIPv4_AndTheWhereGateNamesIt is the roadmap-item-133
// pin: the live proof that a MariaDB INET6 column stores and delivers an IPv4
// address in its IPv4-MAPPED form while the server still matches the bare
// literal, and that sluice's `--where` gate now refuses the bare spelling and
// names the mapped one.
//
// # Why this is a two-LEG test rather than a rendering check
//
// The harm is a DISAGREEMENT between the legs, and each leg alone looks
// correct. The cold copy pushes the predicate into the source SELECT, where
// MariaDB coerces `'10.0.0.1'` to the column type and matches — so the row is
// copied. The CDC leg evaluates the same predicate client-side against the
// DECODED value, which is `::ffff:10.0.0.1` — so every change to that row
// scores out of scope and is dropped. The stream then sits caught-up, healthy,
// at exit 0, with the target row frozen at its cold-copy contents. A test that
// pinned only the delivered rendering, or only the server's match, would have
// been green throughout.
//
// The INET4 half is the control that keeps the refusal from being a blanket
// "refuse anything v6-looking": there the server matches NOTHING for a v6
// literal, so both legs already agree (on empty) and the refusal is about the
// operator's intent rather than about divergence.
//
// # The coercion is VERSION-DEPENDENT, and that is measured here rather than
// # assumed
//
// Ground truth across the LTS lines this suite boots:
//
//	mariadb 10.11  INSERT INET6 '10.0.0.1' -> errno 1292 (Incorrect inet6 value)
//	               WHERE ip = '10.0.0.1'   -> 0 rows
//	mariadb 11.4+  INSERT INET6 '10.0.0.1' -> stored/delivered ::ffff:10.0.0.1
//	               WHERE ip = '10.0.0.1'   -> MATCHES
//
// So 10.11 has no silent divergence at all — the bare literal is simply
// rejected everywhere — and 11.4 onwards is where the cold copy and the CDC
// leg disagree. sluice's answer is the SAME on both, which is why the fix
// carries no version split: on 11.4+ the refusal names the spelling the column
// actually delivers, and on 10.11 it names the only spelling the column can
// hold. The test asserts whichever shape the image in hand exhibits, and a
// floor below requires at least one image to still exhibit the coercing one —
// otherwise this pin would go quietly vacuous if the behaviour were reverted.
func TestMariaDBInet6WidensIPv4_AndTheWhereGateNamesIt(t *testing.T) {
	coercing := 0
	for _, image := range mariadbLTSImages() {
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "inet_family")
			db, err := sql.Open("mysql", dsn+"&multiStatements=true")
			if err != nil {
				t.Fatalf("open %s: %v", image, err)
			}
			defer func() { _ = db.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			if _, err := db.ExecContext(ctx,
				"CREATE TABLE hosts (id INT PRIMARY KEY, ip6 INET6, ip4 INET4)"); err != nil {
				t.Fatalf("create table: %v", err)
			}
			// Does THIS server widen a bare IPv4 literal into the column, or
			// refuse it? Both answers are legal MariaDB; the row must exist
			// either way, so fall back to the mapped spelling.
			_, bareErr := db.ExecContext(ctx,
				"INSERT INTO hosts VALUES (1, '10.0.0.1', '10.0.0.1')")
			widensOnInsert := bareErr == nil
			if !widensOnInsert {
				if _, err := db.ExecContext(ctx,
					"INSERT INTO hosts VALUES (1, '::ffff:10.0.0.1', '10.0.0.1')"); err != nil {
					t.Fatalf("insert (mapped fallback, after %v): %v", bareErr, err)
				}
			}

			// (a) THE SERVER-SIDE LEG — the independent evidence for what the
			//     cold copy would have done under each literal. Without it,
			//     "the CDC leg drops the row" would just be "the row was never
			//     in scope".
			countWhere := func(col, lit string) int {
				t.Helper()
				var n int
				if err := db.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM hosts WHERE `"+col+"` = ?", lit).Scan(&n); err != nil {
					t.Fatalf("count %s = %q: %v", col, lit, err)
				}
				return n
			}
			bareMatches := countWhere("ip6", "10.0.0.1")
			switch {
			case widensOnInsert:
				coercing++
				if bareMatches != 1 {
					t.Fatalf("this server WIDENS a bare IPv4 literal on INSERT but matched %d rows for the "+
						"same literal in a WHERE. The finding rests on those two agreeing — a server that "+
						"stores it and then does not match it is a shape nobody has derived.", bareMatches)
				}
			default:
				if bareMatches != 0 {
					t.Fatalf("this server REJECTS a bare IPv4 literal on INSERT but matched %d rows for it "+
						"in a WHERE; that combination is not one of the two derived shapes", bareMatches)
				}
			}
			if got := countWhere("ip6", "::ffff:10.0.0.1"); got != 1 {
				t.Fatalf("MariaDB did not match the IPv4-MAPPED literal on an INET6 column (%d rows). That "+
					"spelling is the remedy sluice names, so it must match server-side too or the "+
					"operator is sent to a predicate that copies nothing.", got)
			}
			// The INET4 control: no coercion in the other direction.
			if got := countWhere("ip4", "::ffff:10.0.0.1"); got != 0 {
				t.Errorf("MariaDB matched an IPv4-mapped literal on an INET4 column (%d rows); the "+
					"canonicaliser refuses that literal on the premise that it can never match", got)
			}

			// (b) THE DELIVERED VALUE, through sluice's own reader.
			tbl := &ir.Table{
				Schema: "inet_family", Name: "hosts",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 32}},
					{Name: "ip6", Type: ir.Inet{Family: ir.InetFamilyIPv6}},
					{Name: "ip4", Type: ir.Inet{Family: ir.InetFamilyIPv4}},
				},
			}
			eng := Engine{Flavor: FlavorMariaDB}
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
			if got, _ := row["ip6"].(string); got != "::ffff:10.0.0.1" {
				t.Fatalf("INET6 column delivered %q for a stored `10.0.0.1`; the family coercion is "+
					"derived from it being `::ffff:10.0.0.1`", got)
			}
			if got, _ := row["ip4"].(string); got != "10.0.0.1" {
				t.Fatalf("INET4 column delivered %q, want the bare dotted quad", got)
			}

			// (c) THE SCHEMA READ must carry the family, or the compiler below
			//     is being handed a hand-built type rather than the engine's
			//     own answer. This is the binding step: the two facts (what
			//     the server delivers, what sluice's gate does) are useless
			//     apart if nothing ties them to the real reader.
			sr, err := eng.OpenSchemaReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer func() {
				if cl, ok := sr.(interface{ Close() error }); ok {
					_ = cl.Close()
				}
			}()
			schema, err := sr.ReadSchema(ctx)
			if err != nil {
				t.Fatalf("ReadSchema: %v", err)
			}
			var read *ir.Table
			for _, tt := range schema.Tables {
				if tt.Name == "hosts" {
					read = tt
				}
			}
			if read == nil {
				t.Fatal("schema read did not return the hosts table")
			}
			wantFamily := map[string]ir.InetFamily{
				"ip6": ir.InetFamilyIPv6,
				"ip4": ir.InetFamilyIPv4,
			}
			for _, c := range read.Columns {
				want, ok := wantFamily[c.Name]
				if !ok {
					continue
				}
				got, isInet := c.Type.(ir.Inet)
				if !isInet {
					t.Fatalf("column %q read back as %T, want ir.Inet", c.Name, c.Type)
				}
				if got.Family != want {
					t.Fatalf("column %q read back with family %d, want %d — the schema reader is not "+
						"carrying the discriminant the --where gate depends on", c.Name, got.Family, want)
				}
			}

			// (d) THE CLIENT-SIDE LEG, compiled from the schema the reader
			//     just produced.
			infos := rowpredicate.ColumnInfosFromIR(
				mysqlCollationResolver{flavor: FlavorMariaDB}, read.Columns, false,
			)

			_, err = rowpredicate.Compile("hosts", "ip6 = '10.0.0.1'", infos)
			if err == nil {
				t.Fatal("`--where ip6 = '10.0.0.1'` compiled against a MariaDB INET6 column. The server " +
					"matches it (step a) so the cold copy takes the row, and the CDC leg compares against " +
					"`::ffff:10.0.0.1` and drops every change to it — the sync freezes, caught up, at " +
					"exit 0 (roadmap item 133)")
			}
			if !strings.Contains(err.Error(), "::ffff:10.0.0.1") {
				t.Errorf("the refusal must name the spelling to write; got: %v", err)
			}

			// The named remedy must compile AND evaluate TRUE against the row
			// the reader actually delivered — the end-to-end closure. A remedy
			// that compiles and then scores the row out of scope would be the
			// same bug wearing a different literal.
			p, err := rowpredicate.Compile("hosts", "ip6 = '::ffff:10.0.0.1'", infos)
			if err != nil {
				t.Fatalf("the spelling the refusal names must compile: %v", err)
			}
			if !p.Eval(row) {
				t.Errorf("`ip6 = '::ffff:10.0.0.1'` compiled but scored the delivered row %v OUT of scope; "+
					"the CDC leg would still drop every change to it", row["ip6"])
			}

			// The INET4 control on the client side: a v6 literal is refused
			// rather than silently unmapped into one the server would match.
			if _, err := rowpredicate.Compile("hosts", "ip4 = '::ffff:10.0.0.1'", infos); err == nil {
				t.Error("`ip4 = '::ffff:10.0.0.1'` compiled against a MariaDB INET4 column; the server " +
					"matches nothing for it (step a), so this filter can only ever be empty")
			}
			// And the bare literal on INET4 still works — the refusal must not
			// have become a blanket one.
			p4, err := rowpredicate.Compile("hosts", "ip4 = '10.0.0.1'", infos)
			if err != nil {
				t.Fatalf("`ip4 = '10.0.0.1'` must still compile on an INET4 column: %v", err)
			}
			if !p4.Eval(row) {
				t.Errorf("`ip4 = '10.0.0.1'` scored the delivered row %v out of scope", row["ip4"])
			}
		})
	}

	// The anti-vacuity floor for the version split above: if NO image still
	// widens a bare IPv4 literal, the silent-divergence shape this pin was
	// built for no longer exists in the matrix, and every subtest has been
	// quietly running the harmless 10.11 arm.
	if coercing == 0 {
		t.Errorf("no MariaDB image in %v widened a bare IPv4 literal on an INET6 column. That is the "+
			"behaviour roadmap item 133 exists for; if it is genuinely gone from every supported line, "+
			"re-derive the finding rather than leaving this pin asserting nothing.", mariadbLTSImages())
	}
}
