// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// tbl is a one-column table with the given name — the whole fixture this
// refusal reads.
func tbl(name string) *ir.Table {
	return &ir.Table{
		Schema:  "public",
		Name:    name,
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
}

// TestServerTableNameFold pins the predicate that decides whether ANY refusal
// is possible, in both directions.
//
// The nil row is the load-bearing one: lct=0 is the stock Linux default, and a
// fold there would refuse `orders` + `Orders` on servers where that pair is two
// ordinary tables. Every lct != 0 row must fold, including 2 — see
// [Engine.lowerCaseTableNames] for why the predicate reads the EFFECTIVE value
// rather than the operator's configured intent.
func TestServerTableNameFold(t *testing.T) {
	if fold := serverTableNameFold(0); fold != nil {
		t.Errorf("lct=0 returned a fold (%q for \"Orders\"); a case-SENSITIVE server must produce no "+
			"refusal at all, and this is the direction that breaks stock Linux MySQL migrations",
			fold("Orders"))
	}
	for _, lct := range []int{1, 2} {
		fold := serverTableNameFold(lct)
		if fold == nil {
			t.Fatalf("lct=%d returned no fold; the server compares table names case-insensitively there, "+
				"so two spellings silently merge", lct)
		}
		if got := fold("Orders"); got != "orders" {
			t.Errorf("lct=%d folded %q to %q; want %q", lct, "Orders", got, "orders")
		}
	}
}

func TestValidateMySQLTableNameFold(t *testing.T) {
	cases := []struct {
		name    string
		lct     int
		tables  []*ir.Table
		refuses bool
		// mentions are substrings the refusal must carry; an operator acts on
		// the two names and the identifier they share.
		mentions []string
	}{
		{
			// THE DEFECT, measured on real mysql:8 and mariadb:11.4 under
			// lct=1: Note 1050, one table, both tables' rows in it.
			name:     "ASCII case pair on a folding server",
			lct:      1,
			tables:   []*ir.Table{tbl("orders"), tbl("Orders")},
			refuses:  true,
			mentions: []string{`"orders"`, `"Orders"`, "lower_case_table_names=1", "Note 1050"},
		},
		{
			// THE CONTROL, and the reason this check needs a connection at all.
			// The same pair on the stock Linux default is two tables, and
			// refusing it would break every migration that works today.
			name:    "the same pair on a case-SENSITIVE server",
			lct:     0,
			tables:  []*ir.Table{tbl("orders"), tbl("Orders")},
			refuses: false,
		},
		{
			name:     "lct=2 folds for collision purposes",
			lct:      2,
			tables:   []*ir.Table{tbl("orders"), tbl("Orders")},
			refuses:  true,
			mentions: []string{"lower_case_table_names=2"},
		},
		{
			name:    "distinct names on a folding server",
			lct:     1,
			tables:  []*ir.Table{tbl("orders"), tbl("order_items")},
			refuses: false,
		},
		{
			// Measured: `é` then CREATE TABLE IF NOT EXISTS `É` gives Note 1050
			// on both mysql:8 and mariadb:11.4 under lct=1. The server folds
			// non-ASCII case, so this refusal is correct rather than the
			// over-refusal the first draft of the doc claimed it was.
			name:     "non-ASCII case pair on a folding server",
			lct:      1,
			tables:   []*ir.Table{tbl("é"), tbl("É")},
			refuses:  true,
			mentions: []string{`"é"`, `"É"`},
		},
		{
			name:    "the non-ASCII pair on a case-SENSITIVE server",
			lct:     0,
			tables:  []*ir.Table{tbl("é"), tbl("É")},
			refuses: false,
		},
		{
			// The reported prior claimant is the FIRST, because first-claim-wins
			// matches the emit order and the first is the table that survives.
			name:     "three-way collision names the first claimant",
			lct:      1,
			tables:   []*ir.Table{tbl("orders"), tbl("ORDERS"), tbl("Orders")},
			refuses:  true,
			mentions: []string{`"orders"`, `"ORDERS"`},
		},
		{
			name:    "one table cannot collide with itself",
			lct:     1,
			tables:  []*ir.Table{tbl("Orders")},
			refuses: false,
		},
		{
			name:    "nil and nameless entries are skipped, not claimed",
			lct:     1,
			tables:  []*ir.Table{nil, tbl(""), tbl(""), tbl("orders")},
			refuses: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateMySQLTableNameFold(c.lct, c.tables)
			if !c.refuses {
				if err != nil {
					t.Fatalf("refused a schema it must accept: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a schema whose two tables land on one MySQL name; the second CREATE " +
					"TABLE IF NOT EXISTS returns a WARNING and creates nothing, and the copy then INSERTs " +
					"its rows into the table that won the name, at exit 0")
			}
			ce, coded := sluicecode.FromError(err)
			if !coded || ce.Code != sluicecode.CodeSchemaTableNameCollision {
				t.Errorf("refusal must carry %s (operators route on the code, and the thing lost here is "+
					"ROWS); got %v", sluicecode.CodeSchemaTableNameCollision, err)
			}
			for _, want := range c.mentions {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not mention %q; got: %v", want, err)
				}
			}
		})
	}
}

// TestPreflightTableNameFoldSkipsTheProbe pins that the schemas which cannot
// collide never reach the server — asserted with a DSN that cannot possibly
// connect, so a probe would be visible as an error rather than inferred.
//
// This is what makes the call at the `add-table` entry point free: that path
// scopes the schema to one table.
func TestPreflightTableNameFoldSkipsTheProbe(t *testing.T) {
	const unconnectable = "root:pw@tcp(127.0.0.1:1)/sluice_no_such_server"

	for _, c := range []struct {
		name   string
		schema *ir.Schema
	}{
		{"no tables", &ir.Schema{}},
		{"one table", &ir.Schema{Tables: []*ir.Table{tbl("Orders")}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := (Engine{}).PreflightTableNameFold(context.Background(), unconnectable, c.schema); err != nil {
				t.Errorf("probed the server for a schema that cannot collide: %v", err)
			}
		})
	}

	t.Run("nil schema", func(t *testing.T) {
		if err := (Engine{}).PreflightTableNameFold(context.Background(), unconnectable, nil); err == nil {
			t.Error("a nil schema must be a loud programming error, not a silent pass")
		}
	})

	// The other direction, which is what stops the three cases above from being
	// evidence of nothing: a schema that CAN collide does reach the server.
	t.Run("two tables do probe", func(t *testing.T) {
		schema := &ir.Schema{Tables: []*ir.Table{tbl("orders"), tbl("Orders")}}
		err := (Engine{}).PreflightTableNameFold(context.Background(), unconnectable, schema)
		if err == nil {
			t.Fatal("a two-table schema did not probe the server; the fold is a property of the SERVER, " +
				"so a check that answers without asking is answering from nothing")
		}
	})
}
