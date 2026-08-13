// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestEscapeExprLiteralBackslashes is the emit half of the 2026-08-11
// escaping contract (option (a) of the decision memo): the IR carries
// expression literals in the SQL-standard spelling — apostrophes doubled,
// backslashes BARE — and the MySQL boundary re-escapes backslashes when
// (and only when) the target session's sql_mode treats backslash as an
// escape introducer. Without it, `'a\d'` emitted to a default-mode MySQL
// stores as `'ad'`: silent value corruption, the exact inverse of the
// read-side defect the same chunk fixed.
func TestEscapeExprLiteralBackslashes(t *testing.T) {
	cases := []struct {
		name             string
		in               string
		backslashEscapes bool
		want             string
	}{
		{
			"literal backslash doubled under standard mode",
			`regexp_like(c,'^a\d$')`, true, `regexp_like(c,'^a\\d$')`,
		},
		{
			"untouched under NO_BACKSLASH_ESCAPES",
			`regexp_like(c,'^a\d$')`, false, `regexp_like(c,'^a\d$')`,
		},
		{
			"doubled apostrophes ride through, backslash still escaped",
			`concat(c,'''s\x')`, true, `concat(c,'''s\\x')`,
		},
		{
			"value ending in a backslash does not swallow the closing quote",
			`c <> 'a\'`, true, `c <> 'a\\'`,
		},
		{
			"backslash outside any literal is copied verbatim",
			`a \ b`, true, `a \ b`,
		},
		{
			"backtick identifier span is not scanned as a literal",
			"`a'b` = 'x\\y'", true, "`a'b` = 'x\\\\y'",
		},
		{
			"no backslash anywhere is the fast path",
			`c IN ('x','y')`, true, `c IN ('x','y')`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := escapeExprLiteralBackslashes(c.in, c.backslashEscapes); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEscapeExprLiteralBackslashes_RoundTripsThroughTheReader pins the
// two halves against each other: what the emitter writes under standard
// mode, a MySQL server normalizes and information_schema re-renders, and
// the reader's decoder must recover the identical portable spelling. The
// server's side of the trip is simulated with the measured
// transformations (SHOW CREATE keeps the standard-mode escapes; I_S
// escapes ' and \ once more), which the integration matrix ground-truths
// against a real server.
func TestEscapeExprLiteralBackslashes_RoundTripsThroughTheReader(t *testing.T) {
	portable := `regexp_like(c,'^o''brien\d$')`
	emitted := escapeExprLiteralBackslashes(portable, true)
	if want := `regexp_like(c,'^o''brien\\d$')`; emitted != want {
		t.Fatalf("emitted = %q; want %q", emitted, want)
	}
	// The server stores the expression and SHOW CREATE re-renders the
	// interior apostrophe in backslash form: '' and \' denote the same
	// value, and MySQL's normalizer prefers \'.
	showCreate := `regexp_like(c,'^o\'brien\\d$')`
	if got := normalizeShowCreateExpressionText(showCreate); got != portable {
		t.Fatalf("SHOW CREATE round trip = %q; want %q", got, portable)
	}
	// information_schema applies its measured layer on top: ' → \', \ → \\.
	isForm := `regexp_like(c,_utf8mb4\'^o\\\'brien\\\\d$\')`
	if got := normalizeMySQLExpressionText(isForm); got != `regexp_like(c,'^o''brien\d$')` {
		t.Fatalf("I_S round trip = %q; want %q", got, portable)
	}
}

// TestEscapeExprLiteralBackslashes_PreV0120DoubledRecordingReDoubles_Premise
// binds the premise the backup recorded-schema gate's residue arm rests
// on (internal/pipeline/backup, recordedByPreEscapeFixMySQLReader): a
// pre-v0.120.0 chain records a literal backslash in MySQL's DOUBLED
// spelling, and this emitter — correct for the bare IR contract — knows
// nothing of eras, so the doubled recording re-doubles: `'a\d'` (one
// intended backslash) emits as `'a\\d'`, which MySQL lexes to TWO.
// The same-engine restore round trip silently gains a backslash, which
// is what makes the gate's refusal TARGET-INDEPENDENT (the Bug 243
// residue filing assumed a MySQL-target restore stays correct; this pin
// is the corrected premise). If this emitter ever learns to detect and
// de-double old spellings, this pin flips and the gate's era keying
// must be revisited with it.
func TestEscapeExprLiteralBackslashes_PreV0120DoubledRecordingReDoubles_Premise(t *testing.T) {
	oldRecording := `code <> 'a\d'`
	got := escapeExprLiteralBackslashes(oldRecording, true)
	want := `code <> 'a\\d'`
	if got != want {
		t.Fatalf("doubled recording emitted %q; want %q — if the emitter now compensates for pre-v0.120.0 spellings, revisit the backup gate's residue arm (recordedByPreEscapeFixMySQLReader) in the same change", got, want)
	}
}
