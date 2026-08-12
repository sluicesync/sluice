// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// foldPair is one cell of the identifier matrix: two names the migration would
// emit, and what a real SQLite target does with them.
type foldPair struct {
	name string
	a, b string
	// why records what the cell is evidence FOR, so a failure says which claim
	// stopped being true rather than only which strings differed.
	why string
}

// foldMatrix is the class this fold dispatches on: not "a name" but every
// CHARACTER FAMILY an identifier can differ in — ASCII case, non-ASCII case in
// several scripts, mixed ASCII/non-ASCII, and the three Unicode special-casing
// shapes where Go's fold famously does something SQLite's cannot.
//
// Pinning one representative would have missed item 150 exactly as it was
// missed: `idx_v`/`IDX_V` was pinned green, and `idx_é`/`idx_É` — the same
// sluice code path, a different character family — was refused wrongly for two
// releases.
var foldMatrix = []foldPair{
	{
		name: "identical names",
		a:    "orders", b: "orders",
		why: "the plain collision; if this stopped folding the whole refusal is dead",
	},
	{
		name: "ASCII case",
		a:    "orders", b: "Orders",
		why: "SQLite folds A-Z even inside double quotes — the reason the fold exists at all",
	},
	{
		name: "ASCII case, other direction",
		a:    "ORDERS", b: "orders",
		why: "the fold is symmetric; a one-directional lower-caser would pass the row above alone",
	},
	{
		name: "distinct ASCII names",
		a:    "orders", b: "order_items",
		why: "the control: a check that refused everything would satisfy every other row",
	},
	{
		name: "non-ASCII case (Latin-1)",
		a:    "é", b: "É",
		why: "THE HEADLINE of roadmap item 150 — strings.ToLower folds this pair and SQLite does not",
	},
	{
		name: "non-ASCII case (Cyrillic)",
		a:    "Ж", b: "ж",
		why: "the fold stops at Z for every script, not only for Latin-1",
	},
	{
		name: "non-ASCII case (Greek)",
		a:    "Σ", b: "σ",
		why: "same, on the script whose lower-casing is context-dependent in Unicode",
	},
	{
		name: "mixed ASCII and non-ASCII, differing in both",
		a:    "Café_Order", b: "CAFÉ_ORDER",
		why: "the partly-ASCII shape: the ASCII letters fold, the É does not, so the pair survives",
	},
	{
		name: "mixed ASCII and non-ASCII, differing only in ASCII",
		a:    "Café_Order", b: "café_Order",
		why: "the same shape the other way — a non-ASCII byte in the name must not disable the ASCII fold",
	},
	{
		name: "Kelvin sign versus ASCII k",
		a:    "idx_k", b: "idx_K",
		why: "strings.ToLower maps U+212A KELVIN SIGN to ASCII 'k'; SQLite's table does not",
	},
	{
		name: "capital sharp S versus sharp S",
		a:    "straße", b: "STRAẞE",
		why: "strings.ToLower maps U+1E9E to U+00DF, and the ASCII letters around it fold too",
	},
	{
		name: "dotted capital I versus i",
		a:    "i", b: "İ",
		why: "strings.ToLower expands U+0130 to TWO runes, so a fold here would change the name's LENGTH",
	},
}

// TestFoldSQLiteIdentifierMatchesRealSQLite is the premise-naming step for
// [foldSQLiteIdentifier] and for all THREE walks that key on it.
//
// # The independent expected value
//
// It is the target's own catalog. For each pair the test creates the first
// object and then emits sluice's own `IF NOT EXISTS` form for the second, then
// counts what `sqlite_schema` holds — one row means SQLite folded the pair, two
// means it did not. Nothing in the expectation column is written by hand and
// nothing derives from [foldSQLiteIdentifier]; the fold is then REQUIRED to
// agree with what the engine did, cell by cell, for tables, views and indexes
// alike.
//
// # Why all three walks and not one
//
// The table walk, the view walk (both off [scanSQLiteObjectNamespace]) and the
// index walk ([validateSQLiteIndexNamespace]) each pass their own fold into
// [namecollide]. Item 149's roster found how easily one of a set of siblings
// gets missed, and item 150 was itself two call sites with identical comments.
// A pin on one of the three would have been satisfied by fixing one of them.
func TestFoldSQLiteIdentifierMatchesRealSQLite(t *testing.T) {
	// Anti-vacuity for the matrix itself: the reverted-to-Unicode mutation must
	// be able to FAIL this test. Counting the cells where strings.ToLower
	// disagrees with the engine is what proves the matrix contains the
	// regression rather than only the shapes that were always right.
	toLowerDisagreements := 0

	for i, tc := range foldMatrix {
		t.Run(tc.name, func(t *testing.T) {
			collides := sqliteFoldsPair(t, i, tc.a, tc.b)

			if got := foldSQLiteIdentifier(tc.a) == foldSQLiteIdentifier(tc.b); got != collides {
				t.Errorf("foldSQLiteIdentifier says collide=%v for %q/%q; real SQLite says %v (%s). "+
					"sluice's fold has to be the ENGINE's rule: folding more over-refuses a pair the "+
					"target would keep, folding less silently merges two objects into one.",
					got, tc.a, tc.b, collides, tc.why)
			}
			// NOT strings.EqualFold: the claim being measured is what
			// strings.ToLower — the fold the walks used to key on — would have
			// answered, and EqualFold is a third rule again.
			//nolint:gocritic,staticcheck // equalFold/SA6005: ToLower is the SUBJECT of this comparison, not a slow spelling of EqualFold
			if (strings.ToLower(tc.a) == strings.ToLower(tc.b)) != collides {
				toLowerDisagreements++
			}

			// The three walks, each keyed on the same fold, each required to
			// refuse exactly when the engine would actually have merged.
			assertWalkAgrees(t, "table", collides, tc.why,
				validateSQLiteTableNamespace([]*ir.Table{vnsTable(tc.a), vnsTable(tc.b)}))
			assertWalkAgrees(t, "view", collides, tc.why,
				validateSQLiteViewNamespace(nil, []*ir.View{vnsView(tc.a), vnsView(tc.b)}))
			assertWalkAgrees(t, "index", collides, tc.why, validateSQLiteIndexNamespace([]*ir.Table{
				nsTable("t_one", &ir.Index{Name: tc.a, Columns: idxCol("v")}),
				nsTable("t_two", &ir.Index{Name: tc.b, Columns: idxCol("v")}),
			}))
		})
	}

	if toLowerDisagreements < 4 {
		t.Errorf("strings.ToLower disagrees with real SQLite on only %d of %d cells; the matrix is "+
			"supposed to CONTAIN the regression roadmap item 150 fixed (é/É, the Kelvin sign, the "+
			"capital sharp S, the mixed Café_Order pair). With fewer than 4 such cells, reverting the "+
			"fold to strings.ToLower would leave this test green.", toLowerDisagreements, len(foldMatrix))
	}
}

// assertWalkAgrees requires one namespace walk to refuse exactly when SQLite
// would have merged the pair.
func assertWalkAgrees(t *testing.T, walk string, collides bool, why string, err error) {
	t.Helper()
	switch {
	case collides && err == nil:
		t.Errorf("the %s walk ACCEPTED a pair real SQLite merges into one object (%s) — that is the "+
			"silent loss the walk exists to refuse", walk, why)
	case !collides && err != nil:
		t.Errorf("the %s walk REFUSED a pair real SQLite keeps as two objects (%s): %v. That is an "+
			"over-refusal: the migration used to work and the rename it demands is unnecessary.",
			walk, why, err)
	}
}

// sqliteFoldsPair reports whether a real SQLite database treats a and b as one
// name, measured three ways — as tables, as views, and as indexes — because the
// three walks are three separate call sites and nothing but measurement says
// SQLite applies one rule to all three. A disagreement between them is itself a
// finding and fails here rather than being averaged away.
//
// Each kind gets its OWN database. SQLite keeps tables, views and indexes in
// one flat namespace, so measuring them in one file would have the "distinct
// names" control fail for an unrelated reason: `CREATE INDEX "orders"` beside a
// table `orders` is a loud error about the table, not evidence about the fold.
func sqliteFoldsPair(t *testing.T, cell int, a, b string) bool {
	t.Helper()
	q := func(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

	measure := func(kind string, setup, first, second string) int {
		db, err := sql.Open("sqlite", fmt.Sprintf("file:foldmatrix_%s_%d?mode=memory&cache=shared", kind, cell))
		if err != nil {
			t.Fatalf("open %s db: %v", kind, err)
		}
		defer func() { _ = db.Close() }()
		if setup != "" {
			mustExec(t, db, setup)
		}
		mustExec(t, db, first)
		// The second statement is sluice's own emit form, which is what makes
		// a collision silent rather than loud.
		mustExec(t, db, second)
		var n int
		if err := db.QueryRowContext(
			context.Background(),
			`SELECT COUNT(*) FROM sqlite_schema WHERE type = ? AND name IN (?, ?)`, kind, a, b,
		).Scan(&n); err != nil {
			t.Fatalf("count %ss: %v", kind, err)
		}
		return n
	}

	tables := measure("table", "",
		`CREATE TABLE `+q(a)+` (v TEXT)`,
		`CREATE TABLE IF NOT EXISTS `+q(b)+` (v TEXT)`)
	views := measure("view", "",
		`CREATE VIEW `+q(a)+` AS SELECT 1 AS x`,
		`CREATE VIEW IF NOT EXISTS `+q(b)+` AS SELECT 1 AS x`)
	// Two base tables, because the index walk is about two source TABLES
	// carrying index names that resolve to one.
	indexes := measure("index",
		`CREATE TABLE zz_base_one (v TEXT); CREATE TABLE zz_base_two (v TEXT)`,
		`CREATE INDEX `+q(a)+` ON zz_base_one (v)`,
		`CREATE INDEX IF NOT EXISTS `+q(b)+` ON zz_base_two (v)`)

	if tables != views || tables != indexes {
		t.Fatalf("SQLite folded %q/%q differently per object kind (tables=%d views=%d indexes=%d). The "+
			"three walks share one fold on the premise that the rule is one rule; if that premise has "+
			"broken, the walks need three folds, not a tweak to this test.", a, b, tables, views, indexes)
	}
	if tables != 1 && tables != 2 {
		t.Fatalf("expected 1 or 2 objects named %q/%q, got %d", a, b, tables)
	}
	return tables == 1
}

func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

// TestFoldSQLiteIdentifierTouchesOnlyASCIIUpperCase is the byte-level half:
// the matrix above proves the fold agrees with SQLite on the pairs a schema
// produces, and this proves it does so by touching NOTHING outside A-Z — the
// property that keeps a non-UTF-8 identifier from being rewritten into a key it
// could share with a different name.
func TestFoldSQLiteIdentifierTouchesOnlyASCIIUpperCase(t *testing.T) {
	for b := 0; b < 256; b++ {
		in := string([]byte{byte(b)})
		want := in
		if b >= 'A' && b <= 'Z' {
			want = string([]byte{byte(b) + ('a' - 'A')})
		}
		if got := foldSQLiteIdentifier(in); got != want {
			t.Errorf("byte 0x%02X folded to %q, want %q — SQLite's UpperToLower table maps A-Z and "+
				"nothing else", b, got, want)
		}
	}

	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"Orders", "orders"},
		{"ORDER_ITEMS_9", "order_items_9"},
		{"Café_Order", "café_order"},
		{"CAFÉ_ORDER", "cafÉ_order"},
		// Invalid UTF-8 must survive byte-exactly. A rune-wise fold
		// (strings.Map and friends) replaces each bad byte with U+FFFD, which
		// would hand namecollide one key for two different broken names.
		{"\xff\xfeAB", "\xff\xfeab"},
		{"\x80Z", "\x80z"},
		// strings.ToLower turns this into two runes; the fold must not change
		// an identifier's length.
		{"İ", "İ"},
		{"K", "K"},
	} {
		got := foldSQLiteIdentifier(tc.in)
		if got != tc.want {
			t.Errorf("foldSQLiteIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if len(got) != len(tc.in) {
			t.Errorf("foldSQLiteIdentifier(%q) changed the byte length (%d -> %d); SQLite's fold is a "+
				"per-byte table and cannot change a name's length", tc.in, len(tc.in), len(got))
		}
		if again := foldSQLiteIdentifier(got); again != got {
			t.Errorf("foldSQLiteIdentifier is not idempotent on %q: %q -> %q", tc.in, got, again)
		}
	}
}

// TestTargetTableNameFold_UsesSQLitesOwnFold pins that the pre-create shape
// gate's fold surface (audit 2026-08-11, PRF-2) returns SQLite's OWN
// identifier rule — folding ASCII case so `Orders` meets a pre-existing
// `orders`, but NOT the non-ASCII case strings.ToLower would fold (the
// item-150 over-refusal). A gate wired to ToLower instead would refuse legal
// `Café`/`CAFÉ` pairs a real SQLite target keeps distinct.
func TestTargetTableNameFold_UsesSQLitesOwnFold(t *testing.T) {
	fold, err := Engine{}.TargetTableNameFold(context.Background(), "")
	if err != nil {
		t.Fatalf("TargetTableNameFold: %v", err)
	}
	if fold == nil {
		t.Fatal("SQLite folds unconditionally; a nil (identity) fold would miss `Orders`/`orders`")
	}
	if got := fold("Orders"); got != "orders" {
		t.Errorf("fold(%q) = %q; want the ASCII fold `orders` so the source table meets a stored `orders`", "Orders", got)
	}
	// The non-ASCII case must NOT fold: SQLite lowers the ASCII letters and
	// leaves É byte-exact (`cafÉ_order`), where strings.ToLower would yield
	// `café_order`. This is the item-150 regression guard.
	if got := fold("CAFÉ_ORDER"); got != "cafÉ_order" {
		t.Errorf("fold(%q) = %q; want SQLite's ASCII-only fold `cafÉ_order` (strings.ToLower would give `café_order` — Go's rule, not SQLite's)", "CAFÉ_ORDER", got)
	}
}
