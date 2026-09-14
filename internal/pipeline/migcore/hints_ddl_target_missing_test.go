// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestDDLPhasesCodeAMissingRelation pins that the index and constraint phases
// carry a code and a hint for a missing relation or column, which they did not.
//
// # Where this came from
//
// The v0.152.1 regression cycle measured the copy lane refusing a missing
// target relation in 0.18s with `SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING` and a
// hint naming the likely cause — and the index and constraint phases refusing
// the IDENTICAL condition, one phase later, with a bare
// `pipeline: create indexes: … (SQLSTATE 42P01)`: no code, no hint.
//
// The cycle graded it before filing and correctly declined to file it as a
// defect: the refusal is loud, immediate, and names the SQLSTATE, the relation
// and the phase. It is a quality gap, and it is the kind that is invisible
// until someone puts the two messages side by side.
//
// # Why the hint is NOT the copy lane's
//
// The same SQLSTATE means a different thing at a different phase, and this is
// the part worth pinning rather than just the presence of a code.
//
// At COPY time, a missing relation means schema-apply failed or wrote into a
// different schema — the copy is the first thing to touch the table after it
// is created, so its absence indicts the phase before it.
//
// By the INDEX phase the table demonstrably existed and took rows. So the same
// error means something later and rarer: it was dropped or altered BETWEEN the
// copy and the DDL, or the index names a column schema-apply never created.
// Handing that operator the copy lane's text would send them to audit a phase
// that provably worked, which is worse than saying nothing.
func TestDDLPhasesCodeAMissingRelation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		phase    string
		err      error
		wantCode sluicecode.Code
	}{
		{
			name:     "postgres index phase, relation gone",
			phase:    PhaseIndexes,
			err:      errors.New(`pipeline: create indexes: ERROR: relation "events" does not exist (SQLSTATE 42P01)`),
			wantCode: sluicecode.CodeIndexTargetMissing,
		},
		{
			name:     "postgres index phase, column gone",
			phase:    PhaseIndexes,
			err:      errors.New(`pipeline: create indexes: ERROR: column "created_at" does not exist (SQLSTATE 42703)`),
			wantCode: sluicecode.CodeIndexTargetMissing,
		},
		{
			name:     "mysql index phase, table gone",
			phase:    PhaseIndexes,
			err:      errors.New(`pipeline: create indexes: Error 1146: Table 'app.events' doesn't exist`),
			wantCode: sluicecode.CodeIndexTargetMissing,
		},
		{
			name:     "postgres constraint phase, parent gone",
			phase:    PhaseConstraints,
			err:      errors.New(`pipeline: create constraints: ERROR: relation "tenants" does not exist (SQLSTATE 42P01)`),
			wantCode: sluicecode.CodeConstraintTargetMissing,
		},
		{
			name:     "mysql constraint phase, parent gone",
			phase:    PhaseConstraints,
			err:      errors.New(`pipeline: create constraints: Error 1146: Table 'app.tenants' doesn't exist`),
			wantCode: sluicecode.CodeConstraintTargetMissing,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wrapped := WrapWithHint(tc.phase, tc.err)

			ce, ok := sluicecode.FromError(wrapped)
			if !ok || ce.Code != tc.wantCode {
				t.Fatalf("phase %q carries code %q (found=%v), want %q.\n\nerror: %v\n\n"+
					"The copy lane has carried a code for this exact condition since forever; these two "+
					"phases produced a bare SQLSTATE one phase later, so an operator hitting the same "+
					"problem got a searchable code or not depending on which phase they reached.",
					tc.phase, codeOf(ce), ok, tc.wantCode, wrapped)
			}

			hint := hintFor(tc.phase, tc.err)
			if hint == "" {
				t.Fatalf("phase %q produced a code but NO hint for %v — the code makes it searchable, "+
					"the hint is what tells the operator where to look", tc.phase, tc.err)
			}

			// The diagnosis must be the DDL-phase one, not the copy lane's.
			// This is the assertion that would fail if someone "simplified"
			// these entries by pointing them at CodeBulkCopyTargetMissing.
			if strings.Contains(hint, "schema-apply phase fail") {
				t.Fatalf("phase %q reuses the COPY lane's diagnosis: %q\n\n"+
					"At copy time a missing relation indicts schema-apply. By this phase the table "+
					"existed and took rows, so schema-apply provably worked — sending the operator to "+
					"audit it is worse than saying nothing.", tc.phase, hint)
			}
			if !strings.Contains(hint, "not a schema-apply failure") {
				t.Fatalf("phase %q's hint does not rule OUT the copy-lane cause: %q\n\n"+
					"Ruling it out is most of the hint's value — the operator's first instinct at a "+
					"missing relation is to suspect schema creation, and here that is the wrong place.",
					tc.phase, hint)
			}
		})
	}
}

// TestDDLPhaseHintsDoNotSwallowOtherFailures is the anti-vacuity half: the new
// entries match on "does not exist", which is a common substring, and the
// registry is first-match-wins. An entry placed or worded too broadly would
// shadow the more specific PlanetScale wall entries that share these phases and
// carry much more actionable remedies.
func TestDDLPhaseHintsDoNotSwallowOtherFailures(t *testing.T) {
	t.Parallel()

	// The PlanetScale statement-wall errors must keep their own codes.
	walls := []struct {
		phase string
		err   error
		want  sluicecode.Code
	}{
		{
			phase: PhaseIndexes,
			err:   errors.New("pipeline: create indexes: Error 3024: Query execution was interrupted, maximum statement execution time exceeded"),
			want:  sluicecode.CodeIndexStatementTimeLimit,
		},
		{
			phase: PhaseConstraints,
			err:   errors.New("pipeline: create constraints: Error 3024: Query execution was interrupted, maximum statement execution time exceeded"),
			want:  sluicecode.CodeConstraintStatementTimeLimit,
		},
	}
	for _, w := range walls {
		ce, ok := sluicecode.FromError(WrapWithHint(w.phase, w.err))
		if !ok || ce.Code != w.want {
			t.Fatalf("the statement-wall error on phase %q now codes as %q (found=%v), want %q — a "+
				"missing-relation entry is shadowing it, and the wall's remedy (--resume, the "+
				"deploy-request fallback, --skip-foreign-keys) is far more actionable than the "+
				"missing-relation one", w.phase, codeOf(ce), ok, w.want)
		}
	}

	// And an unrelated failure on these phases must still carry NO code, so
	// the new entries cannot be accused of coding everything they see.
	for _, phase := range []string{PhaseIndexes, PhaseConstraints} {
		plain := fmt.Errorf("pipeline: %s: connection reset by peer", phase)
		if ce, ok := sluicecode.FromError(WrapWithHint(phase, plain)); ok {
			t.Fatalf("an unrelated %s failure picked up code %q — the missing-relation entry is "+
				"matching too broadly", phase, codeOf(ce))
		}
	}
}

// codeOf renders a possibly-nil CodedError's code for a failure message, so a
// "no code at all" failure prints something legible rather than panicking on
// the nil it is reporting.
func codeOf(ce *sluicecode.CodedError) sluicecode.Code {
	if ce == nil {
		return "(none)"
	}
	return ce.Code
}
