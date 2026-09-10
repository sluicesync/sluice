// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The SECOND number of the D1 mangle bracket, pinned WITHOUT live credentials
// (audit A0909-SLP-MEDIUM-2).
//
// THE GAP THIS CLOSES. D1 stores invalid-UTF-8 TEXT intact but rewrites every
// MAXIMAL INVALID SUBPART to U+FFFD in its /query JSON response, SERVER-SIDE —
// so the mangled cell arrives as valid UTF-8 and no client-side inspection can
// see it. The reader lane bracketed that with ONE number, the server's own
// summed byte length of its text cells, on the reasoning that three bytes
// replace one. But a maximal invalid subpart is one, two OR THREE bytes, and
// at exactly three the substitution is byte-length PRESERVING: the sums agree
// over a cell that was rewritten. A 4-byte UTF-8 sequence severed at a byte
// boundary IS a three-byte maximal subpart — which is what fixed-width
// truncation does to an emoji — so the blind case is the ordinary one.
//
// The d1-trigger change-log poll grew the count half first
// (checkD1CapturedImageBytes); the reader lane and its staging sibling never
// did. These cells are that gate.

// severedEmoji is 'A' followed by the first three bytes of a 4-byte emoji
// (U+1F618, F0 9F 98 98) — the realistic shape, produced by truncating a text
// column at a byte boundary. It STORES four bytes and zero U+FFFD.
const severedEmojiHex = "41F09F98"

// deliveredSeveredEmoji is what D1 hands back for [severedEmojiHex]: the
// three-byte maximal invalid subpart collapsed to one U+FFFD, which is itself
// three bytes. Four bytes in, four bytes out — the byte bracket sees nothing.
const deliveredSeveredEmoji = "A�"

// TestReplacementCountExpr_GroundTruthOnRealSQLite is the independent
// expected value this whole bracket rests on: the numbers are computed by a
// REAL SQLite executing the production expressions, not asserted from Go's
// idea of what they should be.
//
// It is the closest available stand-in for live D1 (which needs credentials —
// see TestD1Verify_MangledTextIsRefusedNotCopied behind the d1verify tag).
// What it grades is the SERVER half: that textBytesExpr and
// replacementCountExpr, run over the same table, report the stored byte length
// and the stored U+FFFD count for every value family a text column can hold,
// and that they agree on WHICH cells they weigh.
//
// The matrix is the family matrix, not a representative: ASCII, valid
// multi-byte, a severed sequence (the length-preserving case), a value that
// LEGITIMATELY stores U+FFFD (the false-refusal control), the empty string and
// SQL NULL — plus the non-text storage classes and a generated column, which
// must contribute to NEITHER sum.
func TestReplacementCountExpr_GroundTruthOnRealSQLite(t *testing.T) {
	for _, tc := range []struct {
		name             string
		insert           string
		wantBytes        int64
		wantReplacements int64
	}{
		{"ascii", `'plain'`, 5, 0},
		{"valid multi-byte", `'€'`, 3, 0},
		{"the empty string", `''`, 0, 0},
		{
			// The cell the byte bracket cannot see. Four stored bytes, zero
			// stored replacements; delivered as four bytes carrying one.
			"a severed 4-byte sequence", `CAST(x'` + severedEmojiHex + `' AS TEXT)`, 4, 0,
		},
		{
			// The control that keeps this from being a blanket ban on
			// U+FFFD: a source may legitimately store the replacement
			// character, and then both sides count it.
			"a legitimately stored U+FFFD", `'A' || char(65533)`, 4, 1,
		},
		{"two legitimately stored U+FFFD", `char(65533) || char(65533)`, 6, 2},
		{"SQL NULL", `NULL`, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := &ir.Table{
				Name: "t",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "v", Type: ir.Text{}},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
			}
			db := openSeeded(t,
				`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
				`INSERT INTO t (id, v) VALUES (1, `+tc.insert+`)`)

			gotBytes := scanInt64(t, db, "SELECT "+textBytesExpr(table)+" FROM t")
			gotFFFD := scanInt64(t, db, "SELECT "+replacementCountExpr(table)+" FROM t")
			if gotBytes != tc.wantBytes || gotFFFD != tc.wantReplacements {
				t.Fatalf("real SQLite reports (bytes=%d, U+FFFD=%d); want (%d, %d)",
					gotBytes, gotFFFD, tc.wantBytes, tc.wantReplacements)
			}
		})
	}

	t.Run("the two sums weigh the same cells", func(t *testing.T) {
		// The alignment that makes the numbers comparable at all. A cell
		// counted on one side and not the other is noise in whichever sum
		// carries it — that is how the byte half first diverged, on a
		// VIRTUAL column duplicating another text column
		// (TestStageD1Table_RowidShadowMatrix). Non-text storage classes
		// contribute to neither; a generated column is excluded from both.
		table := &ir.Table{
			Name: "mixed",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "n", Type: ir.Integer{Width: 64}},
				{Name: "r", Type: ir.Float{Precision: ir.FloatDouble}},
				{Name: "b", Type: ir.Blob{}},
				{Name: "v", Type: ir.Text{}},
				{Name: "g", Type: ir.Text{}, GeneratedExpr: "v"},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
		}
		db := openSeeded(t,
			`CREATE TABLE mixed (id INTEGER PRIMARY KEY, n INTEGER, r REAL, b BLOB, v TEXT, `+
				`g TEXT GENERATED ALWAYS AS (v) VIRTUAL)`,
			`INSERT INTO mixed (id, n, r, b, v) VALUES (1, 42, 1.5, x'00FF', 'A' || char(65533))`)

		// Only `v` is text: 4 bytes, 1 stored U+FFFD. The generated column
		// duplicates it and must NOT double either number.
		if got := scanInt64(t, db, "SELECT "+textBytesExpr(table)+" FROM mixed"); got != 4 {
			t.Errorf("text-byte sum = %d; want 4 (only the one text column, generated one excluded)", got)
		}
		if got := scanInt64(t, db, "SELECT "+replacementCountExpr(table)+" FROM mixed"); got != 1 {
			t.Errorf("stored-U+FFFD sum = %d; want 1 (only the one text column, generated one excluded)", got)
		}
	})
}

// TestD1RowReader_ReplacementCountBracket is the crux: the reader lane must
// refuse a length-preserving rewrite, and must NOT refuse a value that
// legitimately stores U+FFFD.
//
// Both arms deliver the IDENTICAL payload — "A" followed by U+FFFD. What
// separates them is the source's own numbers, and those come from a REAL
// SQLite executing the production bracket query over the two different
// sources. That is the independent expected value: nothing in the verdict is
// re-derived from the payload being graded.
func TestD1RowReader_ReplacementCountBracket(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	delivered := []map[string]any{dataRow(table, map[string]cell{
		"id": ival("1"),
		"v":  tval(deliveredSeveredEmoji),
	})}
	if len(deliveredSeveredEmoji) != 4 {
		t.Fatalf("fixture is not length-preserving: delivered is %d bytes, want 4", len(deliveredSeveredEmoji))
	}

	for _, tc := range []struct {
		name        string
		stored      string // the SQL value the source actually holds
		wantRefusal bool
	}{
		{
			// Stored: 4 bytes, 0 U+FFFD. Delivered: 4 bytes, 1 U+FFFD.
			// The byte halves AGREE — that is the whole point — so only
			// the count half can see this.
			name: "a length-preserving rewrite is refused", stored: `CAST(x'` + severedEmojiHex + `' AS TEXT)`,
			wantRefusal: true,
		},
		{
			// Stored: 4 bytes, 1 U+FFFD. Delivered: the same. Nothing was
			// rewritten; the source simply contains the replacement
			// character. Refusing here would be a false accusation against
			// the operator's data.
			name: "a value that legitimately stores U+FFFD passes", stored: `'A' || char(65533)`,
			wantRefusal: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &D1RowReader{client: startMockD1(t, honestBracketMangledPage(t, tc.stored, delivered))}
			out, err := r.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			for range out {
			}
			readErr := r.Err()

			var coded *sluicecode.CodedError
			gotRefusal := errors.As(readErr, &coded) && coded.Code == sluicecode.CodeD1TextMangled
			if gotRefusal != tc.wantRefusal {
				t.Fatalf("refused=%v, want %v; err = %v", gotRefusal, tc.wantRefusal, readErr)
			}
			if !tc.wantRefusal {
				if readErr != nil {
					t.Fatalf("clean read failed: %v", readErr)
				}
				return
			}
			// An operator has to be able to act on this: the refusal names
			// both numbers, says the byte counts agreed (so they do not go
			// looking for a length difference that is not there), and
			// points at hex(col).
			for _, want := range []string{`"t"`, "1 U+FFFD", "stores 0", "byte counts in agreement", "hex(col)"} {
				if !strings.Contains(readErr.Error(), want) {
					t.Errorf("refusal missing %q; got: %v", want, readErr)
				}
			}
			if !strings.Contains(coded.Hint, "hex(col)") {
				t.Errorf("remedy does not tell the operator how to see the real bytes; got: %q", coded.Hint)
			}
		})
	}
}

// TestStageD1Table_ReplacementCountBracket is the sibling half. fetchPages has
// TWO consumers and every previous mangle number reached only one of them at
// first — the byte sum landed on the reader and had to be brought to staging
// afterwards (v0.140.0 HIGH-1). `--infer-types` against D1 engages staging
// AUTOMATICALLY and then swaps the source to the staged file, so a rewrite
// baked in here is undetectable from that point on.
func TestStageD1Table_ReplacementCountBracket(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	delivered := []map[string]any{dataRow(table, map[string]cell{
		"id": ival("1"),
		"v":  tval(deliveredSeveredEmoji),
	})}

	for _, tc := range []struct {
		name        string
		stored      string
		inScope     bool
		wantRefusal bool
	}{
		{
			"a length-preserving rewrite refuses before the file is trusted",
			`CAST(x'` + severedEmojiHex + `' AS TEXT)`, true, true,
		},
		{
			"a legitimately stored U+FFFD stages cleanly",
			`'A' || char(65533)`, true, false,
		},
		{
			// Bug 265's rule applies to this number too: staging copies
			// the whole database before the table filter is consulted, so
			// a mangled table the run will never read warns instead of
			// failing the run.
			"a length-preserving rewrite OUT OF SCOPE warns instead of failing the run",
			`CAST(x'` + severedEmojiHex + `' AS TEXT)`, false, false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := &D1RowReader{client: startMockD1(t, honestBracketMangledPage(t, tc.stored, delivered))}
			db := openStageDest(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
			_, err := stageD1Table(context.Background(), rr, db, table, tc.inScope, slog.Default())

			var coded *sluicecode.CodedError
			gotRefusal := errors.As(err, &coded) && coded.Code == sluicecode.CodeD1TextMangled
			if gotRefusal != tc.wantRefusal {
				t.Fatalf("staging refused=%v, want %v; err = %v", gotRefusal, tc.wantRefusal, err)
			}
			if !tc.wantRefusal && err != nil {
				t.Fatalf("clean staging failed: %v", err)
			}
		})
	}
}

// TestD1TextMangle_BothNumbersAndTheAbstentions grades the shared verdict
// directly — the predicate both consumers ride, so a divergence between them
// is impossible by construction rather than by review.
func TestD1TextMangle_BothNumbersAndTheAbstentions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		got       d1DeliveredText
		src       d1SourceTotals
		quiescent bool
		wantWord  string // "" means the verdict must be clean
	}{
		{
			name: "agreeing numbers are clean",
			got:  d1DeliveredText{bytes: 5, replacements: 0},
			src:  d1SourceTotals{textBytes: 5}, quiescent: true,
		},
		{
			name: "a longer delivery is the byte half",
			got:  d1DeliveredText{bytes: 5},
			src:  d1SourceTotals{textBytes: 3}, quiescent: true, wantWord: "bytes where the source stores 3",
		},
		{
			name: "equal bytes with an extra replacement is the count half",
			got:  d1DeliveredText{bytes: 4, replacements: 1},
			src:  d1SourceTotals{textBytes: 4}, quiescent: true, wantWord: "1 U+FFFD where the source stores 0",
		},
		{
			// Both halves compare with != rather than >. A delivered total
			// BELOW the stored one is equally unexplained on a quiescent
			// table, and the byte half has refused in both directions
			// since it shipped.
			name: "fewer replacements than stored is also unexplained",
			got:  d1DeliveredText{bytes: 4, replacements: 0},
			src:  d1SourceTotals{textBytes: 4, replacements: 1}, quiescent: true,
			wantWord: "0 U+FFFD where the source stores 1",
		},
		{
			// The false-refusal floor. A concurrent write moves the source
			// numbers legitimately; the row-count bracket already speaks
			// for a moving table, and refusing here would make a live D1
			// unmigratable.
			name: "a non-quiescent table abstains",
			got:  d1DeliveredText{bytes: 4, replacements: 1},
			src:  d1SourceTotals{textBytes: 4}, quiescent: false,
		},
		{
			// One sentinel, not two: a transport that cannot weigh its own
			// text disarms BOTH halves rather than leaving one comparing
			// against a fabricated zero.
			name: "the no-byte-evidence sentinel disarms the count half too",
			got:  d1DeliveredText{bytes: 4, replacements: 1},
			src:  d1SourceTotals{textBytes: -1, replacements: -1}, quiescent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := d1TextMangle(tc.got, tc.src, tc.quiescent)
			if tc.wantWord == "" {
				if got != "" {
					t.Fatalf("verdict = %q; want clean", got)
				}
				return
			}
			if !strings.Contains(got, tc.wantWord) {
				t.Fatalf("verdict = %q; want it to name %q", got, tc.wantWord)
			}
		})
	}
}

// TestD1RowReader_MissingBracketNumberRefuses is the bracket's own liveness.
// A server number that is ABSENT must not read as clean: an absent column
// means the query shape drifted, and silently skipping the comparison is how a
// bracket becomes decoration — which is the failure mode it exists to stop.
// The d1-trigger lane refuses on the same grounds
// (checkD1CapturedImageBytes's "not optional" arm); this is the reader lane's.
//
// Note the asymmetry with the -1 SENTINEL, which is a DELIBERATE, explicit "I
// cannot weigh my own text" answer and does disarm. Absence is not an answer.
func TestD1RowReader_MissingBracketNumberRefuses(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	page := []map[string]any{dataRow(table, map[string]cell{"id": ival("1"), "v": tval("plain")})}

	for _, tc := range []struct {
		name   string
		answer map[string]any
	}{
		{"no stored-U+FFFD count", map[string]any{"n": "1", "b": "5"}},
		{"no text-byte sum", map[string]any{"n": "1", "f": "0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := func(sqlStr string, _ []string) (int, []byte) {
				if isD1CountQuery(sqlStr) {
					return http.StatusOK, d1OK([]map[string]any{tc.answer})
				}
				if isD1WidthProbe(sqlStr) {
					return http.StatusOK, d1WidthProbeAnswer(0)
				}
				return http.StatusOK, d1OK(page)
			}
			r := &D1RowReader{client: startMockD1(t, h)}
			out, err := r.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			for range out {
			}
			if r.Err() == nil {
				t.Fatal("a bracket reading with a missing server number was treated as a clean read — " +
					"the mangle comparison is then inoperative and nothing on this transport can see a " +
					"server-side rewrite")
			}
		})
	}
}

// TestD1ReplacementCountExpr_IsTheSharedSeamExpression pins that both D1 lanes
// render the same server-side expression. The d1-trigger change-log poll grew
// this number first and the reader lane inherited the gap; hoisting it to the
// seam is what makes "both lanes" a fact rather than a promise.
func TestD1ReplacementCountExpr_IsTheSharedSeamExpression(t *testing.T) {
	got := D1ReplacementCountExpr("v")
	for _, want := range []string{"length(CAST(v AS BLOB))", "replace(v, char(65533), '')", ") / 3"} {
		if !strings.Contains(got, want) {
			t.Errorf("expression missing %q — it must measure the STORED value server-side, "+
				"not the delivered text; got: %s", want, got)
		}
	}
	// The reader's per-table sum must be built from it, over the PROJECTED
	// expression rather than the bare column, exactly as textBytesExpr is.
	table := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{}}},
	}
	sum := replacementCountExpr(table)
	if !strings.Contains(sum, D1ReplacementCountExpr(CapturedValueExpr(quoteIdent("v")))) {
		t.Errorf("replacementCountExpr does not sum the shared expression over the projected value; got: %s", sum)
	}
	if !strings.Contains(sum, CapturedTypeofExpr(quoteIdent("v"))+"='text'") {
		t.Errorf("replacementCountExpr does not carry textBytesExpr's typeof guard, so the two sums "+
			"would weigh different cells; got: %s", sum)
	}
}

// honestBracketMangledPage answers the bracket's COUNT/byte/U+FFFD query from
// a REAL SQLite holding stored, and serves page as the data — the mangle a
// canned transport cannot produce on its own.
//
// This is the shape that keeps the test honest: the source numbers are
// computed by SQLite executing the production expression over the real bytes,
// so the expected value does not come from the same place as the value being
// graded. (Go's encoding/json cannot stand in for D1's rewrite: it replaces
// each invalid BYTE with its own U+FFFD, where D1 — like every WHATWG
// decoder — replaces each maximal invalid SUBPART with one. That difference is
// exactly the case under test, so the delivered page is written by hand.)
func honestBracketMangledPage(t *testing.T, stored string, page []map[string]any) d1Handler {
	t.Helper()
	db := openSeeded(t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO t (id, v) VALUES (1, `+stored+`)`)
	honest := execD1Handler(db)
	return func(sqlStr string, params []string) (int, []byte) {
		if isD1CountQuery(sqlStr) {
			return honest(sqlStr, params)
		}
		if isD1WidthProbe(sqlStr) {
			return http.StatusOK, d1WidthProbeAnswer(0)
		}
		return http.StatusOK, d1OK(page)
	}
}

// openSeeded opens a real temp SQLite file seeded with stmts.
func openSeeded(t *testing.T, stmts ...string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", seedDB(t, stmts...))
	if err != nil {
		t.Fatalf("open seeded db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// scanInt64 runs a one-value query against db.
func scanInt64(t *testing.T, db *sql.DB, q string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("query %s: %v", q, err)
	}
	return n
}
