// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 143 gates, Postgres side — the twin of
// internal/engines/mysql/grow_evidence_test.go, which carries the full
// rationale. Stated as its own file rather than shared because the face corpus
// is engine-specific and a shared corpus would be a corpus for neither.
//
// The sibling-sweep point, restated because it is the half that gets missed:
// this file is here because item 143's finding is about a CLASS of claim, and
// the class has two implementors. A verdict gated on one engine would be a
// gate whose coverage is narrower than its name.

package postgres

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

type pgGrowEvidenceCase struct {
	name string
	err  error
	want ir.GrowEvidence
}

// pgGrowEvidenceCorpus is the PG face matrix. The grow-face half is small on
// purpose: Postgres's storage-grow vocabulary genuinely is small, and padding
// it with codes that merely co-occur with a grow is the over-claiming this
// item exists to stop.
func pgGrowEvidenceCorpus() []pgGrowEvidenceCase {
	return []pgGrowEvidenceCase{
		// ---- Storage-grow / serving-transition faces.
		{
			name: "53100 disk_full — could not extend file",
			err:  &pgconn.PgError{Code: "53100", Message: `could not extend file "base/16384/1259": No space left on device`},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "XX000 cluster is read-only (the PG twin of MySQL 1290)",
			err:  &pgconn.PgError{Code: "XX000", Message: "pg_readonly: invalid statement because cluster is read-only"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "57P01 admin_shutdown",
			err:  &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "57P03 cannot_connect_now",
			err:  &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "unstructured 'database system is starting up'",
			err:  errors.New("failed to connect: the database system is starting up"),
			want: ir.GrowEvidenceTargetFace,
		},

		// ---- No grow evidence.
		{
			name: "53200 out_of_memory (memory, not storage)",
			err:  &pgconn.PgError{Code: "53200", Message: "out of memory"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "53000 insufficient_resources (the generic class root)",
			err:  &pgconn.PgError{Code: "53000", Message: "insufficient resources"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "08006 connection_failure (the generic connection class)",
			err:  &pgconn.PgError{Code: "08006", Message: "connection failure"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "08003 connection_does_not_exist",
			err:  &pgconn.PgError{Code: "08003", Message: "connection does not exist"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "40001 serialization_failure (contention)",
			err:  &pgconn.PgError{Code: "40001", Message: "could not serialize access"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "40P01 deadlock_detected (contention)",
			err:  &pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "42703 undefined_column (schema drift)",
			err:  &pgconn.PgError{Code: "42703", Message: `column "x" does not exist`},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "unexpected EOF mid-COPY (transport)",
			err:  fmt.Errorf("copy from stdin: %w", io.ErrUnexpectedEOF),
			want: ir.GrowEvidenceNone,
		},
		{
			name: "bare transport text",
			err:  errors.New("read tcp 10.0.0.1:5432: connection reset by peer"),
			want: ir.GrowEvidenceNone,
		},
		{
			name: "XX000 with unrelated message (the generic catch-all must not over-match)",
			err:  &pgconn.PgError{Code: "XX000", Message: "internal error"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "terminal 23505 quoting read-only text (the echo shape)",
			err:  &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "notes_pkey": DETAIL: Key (note)=(cluster is read-only) already exists.`},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "nil error",
			err:  nil,
			want: ir.GrowEvidenceNone,
		},
	}
}

// TestPGGrowEvidence_FaceMatrix is the direct verdict pin over the corpus.
func TestPGGrowEvidence_FaceMatrix(t *testing.T) {
	for _, tc := range pgGrowEvidenceCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			if got := growEvidenceOf(tc.err); got != tc.want {
				t.Errorf("growEvidenceOf(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

// TestPGGrowEvidence_CorpusExercisesBothVerdicts is the anti-vacuity floor —
// see the MySQL twin for why "everything is transport" is the answer that must
// not pass silently.
func TestPGGrowEvidence_CorpusExercisesBothVerdicts(t *testing.T) {
	var faces, none int
	for _, tc := range pgGrowEvidenceCorpus() {
		switch tc.want {
		case ir.GrowEvidenceTargetFace:
			faces++
		case ir.GrowEvidenceNone:
			none++
		case ir.GrowEvidenceTelemetry:
			t.Errorf("%s: telemetry is not an ERROR verdict; growEvidenceOf must never return it", tc.name)
		}
	}
	if faces < 4 || none < 5 {
		t.Fatalf("the face corpus must exercise BOTH verdicts non-trivially: %d grow-face, %d no-evidence", faces, none)
	}
}

// TestPGGrowEvidence_IsDerivedFromTheRetriableClassifier binds the evidence
// verdict to retriability in the one direction that must hold: a face this
// engine calls grow evidence is a face the chunked-COPY retry rides out.
func TestPGGrowEvidence_IsDerivedFromTheRetriableClassifier(t *testing.T) {
	for _, tc := range pgGrowEvidenceCorpus() {
		if tc.err == nil || growEvidenceOf(tc.err) != ir.GrowEvidenceTargetFace {
			continue
		}
		var re ir.RetriableError
		if !errors.As(classifyApplierError(tc.err), &re) || !re.Retriable() {
			t.Errorf(
				"%s: growEvidenceOf says target-grow-face but classifyApplierError says TERMINAL — "+
					"the trip log would name a grow window for an error that ends the copy", tc.name,
			)
		}
	}
}

// TestGrowEvidence_SharesTheReadOnlyPredicateWithTheClassifier is the
// anti-drift gate named in [pgReadOnlyClusterSubstrings]'s doc.
//
// Retriability and the evidence verdict both hinge on the same wording, and
// before item 143 the classifier spelled it inline. Two spellings of one fact
// is the 2026-07-28 shape; this asserts there is one. Every literal in the
// shared list must make an XX000 BOTH retriable and grow-evidenced — so a
// literal added to the list without the classifier seeing it fails here.
func TestGrowEvidence_SharesTheReadOnlyPredicateWithTheClassifier(t *testing.T) {
	if len(pgReadOnlyClusterSubstrings) == 0 {
		t.Fatal("the read-only wording list is empty — this gate would be vacuous")
	}
	for _, sub := range pgReadOnlyClusterSubstrings {
		err := &pgconn.PgError{Code: "XX000", Message: "server says: " + strings.ToUpper(sub)}
		var re ir.RetriableError
		if !errors.As(classifyApplierError(err), &re) || !re.Retriable() {
			t.Errorf("%q does not make an XX000 retriable — the classifier is not reading the shared list", sub)
		}
		if got := growEvidenceOf(err); got != ir.GrowEvidenceTargetFace {
			t.Errorf("%q yields evidence %s, want target-grow-face", sub, got)
		}
	}
}

// TestPGGrowEvidence_TripSitePassesTheDerivedVerdict reaches the CALL SITE via
// the real chunked-COPY retry loop — the item-136 M4 lesson applied here: the
// predicate being right says nothing about whether the lane hands it over. It
// requires the two faces to produce DIFFERENT verdicts, so a site hard-coding
// either constant fails.
func TestPGGrowEvidence_TripSitePassesTheDerivedVerdict(t *testing.T) {
	withFastPGCopyBackoff(t)

	for _, tc := range []struct {
		name string
		err  error
		want ir.GrowEvidence
	}{
		{"grow face", &pgconn.PgError{Code: "53100", Message: "could not extend file: No space left on device"}, ir.GrowEvidenceTargetFace},
		{"transport drop", fmt.Errorf("copy: %w", io.ErrUnexpectedEOF), ir.GrowEvidenceNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := &recordingGrowGate{}
			w := &RowWriter{growGate: gate}
			calls := 0
			err := w.copyChunkWithRetry(t.Context(), pgKeyedPinTable("t"), 10, func(context.Context) error {
				calls++
				if calls == 1 {
					return tc.err
				}
				return nil
			})
			if err != nil {
				t.Fatalf("copyChunkWithRetry: %v", err)
			}
			if gate.trips.Load() == 0 {
				t.Fatal("the chunk loop never tripped the gate — the pin below would be vacuous")
			}
			if got := gate.LastEvidence(); got != tc.want {
				t.Errorf("chunk trip claimed %s, want %s (the call site is not passing the derived verdict)", got, tc.want)
			}
		})
	}
}

// TestPGGrowGateTrip_IsReachedOnlyThroughTheDerivingHelper is the PG half of
// the sibling sweep — see the MySQL twin for the reasoning.
func TestPGGrowGateTrip_IsReachedOnlyThroughTheDerivingHelper(t *testing.T) {
	callers := pgGrowGateTripCallers(t, ".")
	if len(callers) == 0 {
		t.Fatal("found no call to the raw gate Trip method at all — the walk is broken, so this gate proves nothing")
	}
	for _, fn := range callers {
		if fn != "tripGrowGate" {
			t.Errorf(
				"%s calls growGate.Trip directly; every trip must route through tripGrowGate so the "+
					"item-143 evidence verdict is DERIVED from the error rather than claimed per site", fn,
			)
		}
	}
}

// pgGrowGateTripCallers mirrors the MySQL walker; see its doc. Uses this
// package's existing os.ReadDir + parser.ParseFile idiom rather than the
// deprecated parser.ParseDir.
func pgGrowGateTripCallers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		file, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if perr != nil {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Trip" {
					return true
				}
				var buf strings.Builder
				if perr := printer.Fprint(&buf, fset, sel.X); perr != nil {
					return true
				}
				if strings.Contains(buf.String(), "growGate") {
					out = append(out, fn.Name.Name)
				}
				return true
			})
		}
	}
	sort.Strings(out)
	return out
}
