// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/refusalgate"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The durable gate for Bug 235.
//
// The defect was not a typo. `SLUICE-E-SOURCE-REPLICA-IDENTITY` grew a
// FOURTH shape (a GENERATED identity column) whose body was written
// correctly for it, while the code's SHARED `hint=` — written for the
// three item-93 shapes, where `REPLICA IDENTITY FULL` genuinely is the
// remedy — was never re-derived. So the field an operator ACTS on
// prescribed exactly what the field two over said, twice, was not a fix.
//
// Editing the string would close the instance. What closes the class is
// grading the PROPERTY over every shape the refusal can render:
// [refusalgate.Contradictions] extracts the remedies a hint prescribes
// and reports the ones the body denies. That is the Bug 231 gate's idea
// (a claim graded against the evidence printed beside it) generalised
// from one sentence to a rule, and moved into a package so the next
// refusal that grows a shape can be wired in with two lines.
//
// SCOPE, stated so the name cannot be read as broader than the truth:
// this grades the refusals [errUnusableReplicaIdentity] renders and
// nothing else. Postgres's other coded refusals are not graded here, and
// the reader-side [refuseUnpublishedGeneratedIdentity] — whose hint was
// already correct, and is what the fixed preflight hint was derived
// from — has its own pins.

// replicaIdentityRefusalShapes enumerates every gap shape the preflight
// can produce, derived by running the real classifier over the catalog
// states that yield one rather than by hand-writing reasons. A shape the
// classifier stops producing, or starts producing, changes this list.
func replicaIdentityRefusalShapes(t *testing.T) []replicaIdentityGap {
	t.Helper()
	rows := []replicaIdentityRow{
		{Table: "d_deferrable", Identity: "d", PrimaryKeyIndex: "t_pkey"},
		{Table: "d_deferrable_cand", Identity: "d", PrimaryKeyIndex: "t_pkey", CandidateIndex: "t_alt_key"},
		{Table: "d_keyless", Identity: "d"},
		{Table: "d_keyless_cand", Identity: "d", CandidateIndex: "t_alt_key"},
		{Table: "n_nothing", Identity: "n"},
		{Table: "i_dropped", Identity: "i"},
		{Table: "i_unusable", Identity: "i", ChosenIndex: "t_ri_idx"},
		{Table: "unknown_letter", Identity: "z"},
		{Table: "gen_default", Identity: "d", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: true, IdentityGeneratedCols: []string{"g"}},
		{Table: "gen_default_cand", Identity: "d", PrimaryKeyIndex: "t_pkey", PrimaryKeyUsable: true, CandidateIndex: "t_alt_key", IdentityGeneratedCols: []string{"g"}},
		{Table: "gen_full", Identity: "f", IdentityGeneratedCols: []string{"g", "h"}},
		{Table: "gen_using_index", Identity: "i", ChosenIndex: "t_ri_idx", ChosenIndexUsable: true, IdentityGeneratedCols: []string{"g"}},
	}
	gaps := make([]replicaIdentityGap, 0, len(rows))
	for _, row := range rows {
		usable, reason := replicaIdentityUsable(row)
		if usable {
			t.Fatalf("fixture row %q is USABLE; it was chosen because it is not — the classifier has changed", row.Table)
		}
		gaps = append(gaps, replicaIdentityGap{
			Table:             row.Table,
			Reason:            reason,
			CandidateIndex:    row.CandidateIndex,
			GeneratedIdentity: len(row.IdentityGeneratedCols) > 0,
		})
	}
	return gaps
}

func TestReplicaIdentityRefusalHintNeverContradictsItsBody(t *testing.T) {
	shapes := replicaIdentityRefusalShapes(t)

	// The refusal aggregates, so the combinations matter as much as the
	// singletons: a MIXED run is the one where no single blanket remedy
	// can be right, and it is the case a per-shape hint is easiest to get
	// wrong.
	runs := make([][]replicaIdentityGap, 0, len(shapes)*len(shapes)+2)
	for i := range shapes {
		runs = append(runs, []replicaIdentityGap{shapes[i]})
		for j := range shapes {
			if i != j {
				runs = append(runs, []replicaIdentityGap{shapes[i], shapes[j]})
			}
		}
	}
	runs = append(runs, shapes)

	var graded, withRemedies, withDenials int
	for _, gaps := range runs {
		err := errUnusableReplicaIdentity("public", gaps)
		coded, ok := sluicecode.FromError(err)
		if !ok {
			t.Fatalf("refusal for %s is not coded: %v", gapLabels(gaps), err)
		}
		graded++
		body := err.Error()

		if len(refusalgate.Remedies(coded.Hint)) > 0 {
			withRemedies++
		}
		if bodyCarriesADenial(body) {
			withDenials++
		}

		if bad := refusalgate.Contradictions(body, coded.Hint); len(bad) > 0 {
			t.Errorf("the refusal for %s prescribes %v in its hint while its own body rules that out — "+
				"a message must not recommend what its own evidence denies (Bug 235)\nhint: %s\nbody: %s",
				gapLabels(gaps), bad, coded.Hint, body)
		}
	}

	// Anti-vacuity, three floors. Without them a hint that named no
	// remedies at all, or a body that denied nothing, would pass this
	// gate while grading nothing.
	if graded < len(shapes)*len(shapes) {
		t.Errorf("graded only %d refusals over %d shapes — the matrix has gone vacuous", graded, len(shapes))
	}
	if withRemedies == 0 {
		t.Error("no graded hint contained a remedy phrase; refusalgate.Remedies is matching nothing here, so the check is inert")
	}
	if withDenials == 0 {
		t.Error("no graded body contained a denial cue; the half of the check that reads the body is grading nothing")
	}
}

// bodyCarriesADenial reports whether a rendered body rules ANY remedy
// out. Used only as the anti-vacuity floor above.
func bodyCarriesADenial(body string) bool {
	lower := strings.ToLower(body)
	for _, cue := range refusalgate.DenialCues {
		if strings.Contains(lower, cue) {
			return true
		}
	}
	return false
}

func gapLabels(gaps []replicaIdentityGap) string {
	names := make([]string, len(gaps))
	for i, g := range gaps {
		names[i] = g.Table
	}
	return "[" + strings.Join(names, " + ") + "]"
}

// TestReplicaIdentityHintIsPerShape pins the three hints against the
// three populations, from the operator's side — the property gate above
// says the hint does not contradict the body, and this says it is the
// RIGHT hint. A hint that named no remedy at all would satisfy the
// property and help nobody.
func TestReplicaIdentityHintIsPerShape(t *testing.T) {
	deferrable := replicaIdentityGap{Table: "dpk", Reason: "its PRIMARY KEY index is DEFERRABLE"}
	generated := replicaIdentityGap{Table: "gpk", Reason: "its replica identity includes GENERATED column(s)", GeneratedIdentity: true}

	cases := []struct {
		name    string
		gaps    []replicaIdentityGap
		want    string
		wantNot []string
	}{
		{
			name:    "deferrable/keyless only — FULL is the remedy and the hint says so",
			gaps:    []replicaIdentityGap{deferrable},
			want:    replicaIdentityHint,
			wantNot: nil,
		},
		{
			name: "generated only — the hint must NOT send the operator to FULL",
			gaps: []replicaIdentityGap{generated},
			want: replicaIdentityGeneratedHint,
			// The exact remedy Bug 235 was filed over.
			wantNot: []string{"REPLICA IDENTITY FULL"},
		},
		{
			name:    "mixed — no blanket remedy can be right, so the hint names none",
			gaps:    []replicaIdentityGap{deferrable, generated},
			want:    replicaIdentityMixedHint,
			wantNot: []string{"REPLICA IDENTITY FULL", "REPLICA IDENTITY USING INDEX"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coded, ok := sluicecode.FromError(errUnusableReplicaIdentity("public", tc.gaps))
			if !ok {
				t.Fatal("refusal is not coded")
			}
			if coded.Hint != tc.want {
				t.Errorf("hint = %q; want %q", coded.Hint, tc.want)
			}
			for _, banned := range tc.wantNot {
				if strings.Contains(coded.Hint, banned) {
					t.Errorf("hint prescribes %q for this population:\n%s", banned, coded.Hint)
				}
			}
			// Every hint keeps the escape hatch: it is the one remedy that
			// works for every shape.
			if !strings.Contains(coded.Hint, "--exclude-table") {
				t.Errorf("hint drops the --exclude-table escape:\n%s", coded.Hint)
			}
		})
	}
}

// TestReplicaIdentityRefusalQuotesTheServerWordingForItsShape is the
// other half of Bug 235: sluice quoted the MISSING-replica-identity
// wording for a shape PostgreSQL refuses with a different message, so an
// operator grepping their log for the quoted line found nothing.
//
// The generated wording is not transcribed here — it is
// [pgGeneratedIdentityRefusalDetail], the same literal
// [isUnpublishedGeneratedIdentityRefusal] matches against a live server
// in TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity. That
// binding is what makes the quote a measurement.
func TestReplicaIdentityRefusalQuotesTheServerWordingForItsShape(t *testing.T) {
	const missingWording = "does not have a replica identity and publishes updates"

	deferrable := errUnusableReplicaIdentity("public", []replicaIdentityGap{
		{Table: "dpk", Reason: "its PRIMARY KEY index is DEFERRABLE"},
	}).Error()
	if !strings.Contains(deferrable, missingWording) {
		t.Errorf("the deferrable-shape refusal no longer quotes the wording PostgreSQL uses for it:\n%s", deferrable)
	}
	if strings.Contains(deferrable, pgGeneratedIdentityRefusalDetail) {
		t.Errorf("the deferrable-shape refusal quotes the GENERATED-column DETAIL, which PostgreSQL does not emit here:\n%s", deferrable)
	}

	generated := errUnusableReplicaIdentity("public", []replicaIdentityGap{
		{Table: "gpk", Reason: "its replica identity includes GENERATED column(s)", GeneratedIdentity: true},
	}).Error()
	if !strings.Contains(generated, pgGeneratedIdentityRefusalDetail) {
		t.Errorf("the generated-shape refusal does not quote the DETAIL PostgreSQL actually emits:\n%s", generated)
	}
	if strings.Contains(generated, missingWording) {
		t.Errorf("the generated-shape refusal still quotes the MISSING-replica-identity wording, which PostgreSQL "+
			"does not emit for this shape — an operator grepping their log for it finds nothing (Bug 235):\n%s", generated)
	}

	// A mixed run refuses both shapes, so it must quote both.
	mixed := errUnusableReplicaIdentity("public", []replicaIdentityGap{
		{Table: "dpk", Reason: "its PRIMARY KEY index is DEFERRABLE"},
		{Table: "gpk", Reason: "its replica identity includes GENERATED column(s)", GeneratedIdentity: true},
	}).Error()
	for _, want := range []string{missingWording, pgGeneratedIdentityRefusalDetail} {
		if !strings.Contains(mixed, want) {
			t.Errorf("a mixed refusal omits the wording %q:\n%s", want, mixed)
		}
	}
}
