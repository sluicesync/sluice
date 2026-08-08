// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package refusalgate

import (
	"slices"
	"strings"
	"testing"
)

// shippedV0117Hint and shippedV0117Body are the EXACT texts sluice
// v0.117.0 printed for the generated-identity replica-identity refusal,
// frozen here verbatim.
//
// This is the anti-vacuity floor that matters most, and it is the one
// shape a hand-written fixture cannot fake: the checker has to flag the
// real defect, not a stylised version of it. A rewrite of the extractor
// or the window that stops catching this is a regression regardless of
// what the rest of the suite says.
const (
	shippedV0117Hint = "on the SOURCE, either make the table's key immediate " +
		"(ALTER TABLE … DROP CONSTRAINT …, then re-add it WITHOUT DEFERRABLE), or set a replica identity " +
		"(ALTER TABLE … REPLICA IDENTITY FULL, or REPLICA IDENTITY USING INDEX <an immediate NOT NULL UNIQUE index>), " +
		"or take the table out of scope with --exclude-table"

	shippedV0117Body = `postgres: source table(s) "public"."t" have no usable replica identity ` +
		`(its replica identity includes GENERATED column(s) "g", which PostgreSQL does not publish; ` +
		`REPLICA IDENTITY FULL is NOT a fix here (a generated column is unpublished under FULL too) — ` +
		`the table needs a row identity made of non-generated columns), and this sync is about to add them ` +
		`to its publication with UPDATE and DELETE publishing.`
)

func TestContradictions_FlagsTheShippedV0117Defect(t *testing.T) {
	got := Contradictions(shippedV0117Body, shippedV0117Hint)
	if !slices.Contains(got, "REPLICA IDENTITY FULL") {
		t.Fatalf("the checker did not flag the defect it was built for: got %v", got)
	}
	// And it must not shotgun everything else in the hint — a gate that
	// flags every phrase is one nobody can act on.
	for _, phrase := range []string{"ALTER TABLE", "REPLICA IDENTITY USING INDEX", "--exclude-table"} {
		if slices.Contains(got, phrase) {
			t.Errorf("checker flagged %q, which the body does not deny: got %v", phrase, got)
		}
	}
}

func TestContradictions_CleanMessages(t *testing.T) {
	cases := []struct {
		name       string
		body, hint string
	}{
		{
			name: "the fixed generated-identity shape: hint names no FULL",
			body: `REPLICA IDENTITY FULL is NOT a fix for these tables: PostgreSQL leaves a generated column ` +
				`unpublished under FULL as well`,
			hint: "on the SOURCE, give the table a row identity made of NON-GENERATED columns — or point " +
				"`ALTER TABLE … REPLICA IDENTITY USING INDEX` at an immediate NOT NULL UNIQUE index",
		},
		{
			name: "the deferrable shape: the body endorses the remedy",
			body: "REPLICA IDENTITY FULL is sufficient here — it publishes the whole old row",
			hint: "set a replica identity (ALTER TABLE … REPLICA IDENTITY FULL)",
		},
		{
			name: "a denial of a DIFFERENT remedy in the same paragraph",
			body: "REPLICA IDENTITY FULL makes it publishable as-is; note that --reset-target-data does not help",
			hint: "set ALTER TABLE … REPLICA IDENTITY FULL",
		},
		{
			name: "a hint with no remedy phrase at all",
			body: "REPLICA IDENTITY FULL is NOT a fix here",
			hint: "fix each table named above with the remedy its own note gives",
		},
		{
			// The false positive the postgres gate's first run produced:
			// the denial belongs to the NEXT sentence, not to the flag
			// that happens to end this one.
			name: "a denial one sentence later is not attributed backwards",
			body: "take it out of scope with --exclude-table. REPLICA IDENTITY FULL is NOT a fix for these tables",
			hint: "or take the table out of scope with --exclude-table",
		},
		{name: "empty", body: "", hint: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Contradictions(tc.body, tc.hint); len(got) > 0 {
				t.Errorf("false positive: %v", got)
			}
		})
	}
}

// TestContradictions_EveryDenialCueIsReachable is the gate-on-the-gate:
// a cue nobody can trigger is a line of prose, not a check. Each cue is
// exercised against the same remedy so a typo in the list fails here.
func TestContradictions_EveryDenialCueIsReachable(t *testing.T) {
	for _, cue := range DenialCues {
		body := "REPLICA IDENTITY FULL " + cue + " for this shape"
		got := Contradictions(body, "run ALTER TABLE … REPLICA IDENTITY FULL")
		if !slices.Contains(got, "REPLICA IDENTITY FULL") {
			t.Errorf("denial cue %q does not fire: body=%q got=%v", cue, body, got)
		}
	}
	if len(DenialCues) == 0 {
		t.Fatal("DenialCues is empty; the checker denies nothing")
	}
}

// TestContradictions_FlagRemediesAreGraded covers the second remedy
// vocabulary. A flag is exactly the kind of remedy a body rules out
// ("--reset-target-data does not help here") and it carries no capitals.
func TestContradictions_FlagRemediesAreGraded(t *testing.T) {
	got := Contradictions("dropping the index does not fix it, and --exclude-table does not help either",
		"re-run with --exclude-table")
	if !slices.Contains(got, "--exclude-table") {
		t.Errorf("a denied flag remedy was not flagged: got %v", got)
	}
}

// TestContradictions_WindowIsTightEnough pins the measured separation
// the window exists for: a remedy phrase mentioned well before an
// unrelated denial in the same sentence must NOT be attributed to it.
func TestContradictions_WindowIsTightEnough(t *testing.T) {
	body := `the table already carries an immediate NOT NULL UNIQUE index "uq_email" over non-generated ` +
		`columns, so ` + "`ALTER TABLE public.t REPLICA IDENTITY USING INDEX \"uq_email\";`" + ` is the ` +
		`narrowest fix — note REPLICA IDENTITY FULL is NOT a fix here`
	got := Contradictions(body, "point ALTER TABLE … REPLICA IDENTITY USING INDEX at an immediate NOT NULL UNIQUE index")
	if len(got) != 0 {
		t.Errorf("the window attributed a distant denial to an unrelated remedy: %v", got)
	}
	// Sanity: the same body DOES deny FULL, so the fixture is not simply
	// denial-free.
	if got := Contradictions(body, "run ALTER TABLE … REPLICA IDENTITY FULL"); len(got) != 1 {
		t.Errorf("the fixture's real denial was not caught: %v", got)
	}
}

func TestRemedies_Extraction(t *testing.T) {
	cases := []struct {
		name string
		hint string
		want []string
	}{
		{
			name: "sql phrases and a flag",
			hint: "make the key immediate (ALTER TABLE … DROP CONSTRAINT …, then re-add it WITHOUT DEFERRABLE), " +
				"or take the table out of scope with --exclude-table",
			want: []string{"ALTER TABLE", "DROP CONSTRAINT", "WITHOUT DEFERRABLE", "--exclude-table"},
		},
		{
			name: "a lone capitalised word is not a remedy",
			hint: "the table needs a NEW identity",
			want: nil,
		},
		{
			name: "backticked phrase",
			hint: "point `ALTER TABLE … REPLICA IDENTITY USING INDEX` at an index",
			want: []string{"ALTER TABLE", "REPLICA IDENTITY USING INDEX"},
		},
		{
			name: "a bare word is not a flag",
			hint: "pass -x or --a",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Remedies(tc.hint)
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Errorf("Remedies(%q) = %v; want %v", tc.hint, got, tc.want)
			}
		})
	}
}
