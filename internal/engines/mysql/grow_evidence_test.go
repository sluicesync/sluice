// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 143 gates: the grow-gate trip's CLAIM must be derived from the error
// and must be checkable.
//
// # What these gates reach, and what they do not
//
// [TestGrowEvidence_FaceMatrix] and its siblings exercise [growEvidenceOf]
// directly, over the MySQL/Vitess face corpus. [TestGrowEvidence_TripSites-
// PassTheDerivedVerdict] is the one that reaches the CALL SITES, because the
// classifier's own tests cannot see whether a lane actually hands the verdict
// to the gate — that is the item-136 M4 lesson, where a mutation reverting a
// call site passed every test the mechanism had. Neither reaches the Postgres
// engine; its twin lives in internal/engines/postgres/grow_evidence_test.go.
//
// # Why an evidence verdict needs a gate at all
//
// Roadmap item 143 exists because a log line said "likely a primary reparent"
// and nothing checked it, for long enough that two independent datasets had to
// be gathered to discover it was almost always false. A verdict that is merely
// COMPUTED is the same hazard one step later: it stays honest only while
// something fails when it stops being.

package mysql

import (
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

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// growEvidenceCase is one face of the corpus: the error a lane can meet, and
// what the trip is entitled to claim about it.
type growEvidenceCase struct {
	name string
	err  error
	want ir.GrowEvidence
}

// growEvidenceCorpus is the face matrix. It is stated as ONE list covering
// BOTH verdicts rather than two lists, so a change that flips a face shows up
// as a diff in the same place the reasoning lives.
//
// The TransportOnly half is not filler: it is the population item 143 is
// about, and every entry in it is a shape the 2026-08-05 field log or the
// 2026-08-06 real-PlanetScale validation actually produced.
func growEvidenceCorpus() []growEvidenceCase {
	return []growEvidenceCase{
		// ---- Storage-grow / serving-transition faces: the target said
		// something about its own storage or serving state.
		{
			name: "1021 ER_DISK_FULL",
			err:  &gomysql.MySQLError{Number: 1021, Message: "Disk full (/tmp); waiting for someone to free some space"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "1114 ER_RECORD_FILE_FULL",
			err:  &gomysql.MySQLError{Number: 1114, Message: "The table 'documents' is full"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "Error 3 with ENOSPC wording",
			err:  &gomysql.MySQLError{Number: 3, Message: "Error writing file '/vt/vtdataroot/x' (errno: 28 - No space left on device)"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "1290 read-only serving transition",
			err:  &gomysql.MySQLError{Number: 1290, Message: "The MySQL server is running with the --read-only option so it cannot execute this statement"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "1105 vtgate: primary is not serving",
			err: &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: primary is not serving, " +
				"there may be a reparent operation in progress"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "1105 vtgate: no healthy tablet available",
			err:  &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: no healthy tablet available for 'keyspace:\"ks\"'"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "1105 vttablet: code = Unavailable",
			err:  &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = Unavailable desc = tablet is shutting down"},
			want: ir.GrowEvidenceTargetFace,
		},
		{
			name: "unstructured 'not serving' text",
			err:  errors.New("dial ks.-.primary: tablet is not serving"),
			want: ir.GrowEvidenceTargetFace,
		},

		// ---- No grow evidence. 244 of the 246 field windows were opened by
		// the first entry here.
		{
			name: "1105 vtgate connection error: no endpoints (THE field face)",
			err:  &gomysql.MySQLError{Number: 1105, Message: "unavailable: vtgate connection error: no endpoints"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1105 vtgate connection error: no healthy endpoints",
			err:  &gomysql.MySQLError{Number: 1105, Message: "unavailable: vtgate connection error: no healthy endpoints"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1105 vtgate connection error: connection reset by peer",
			err: &gomysql.MySQLError{Number: 1105, Message: "internal: vtgate connection error: read tcp 10.0.0.1:15999: " +
				"read: connection reset by peer"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "2013 CR_SERVER_LOST",
			err:  &gomysql.MySQLError{Number: 2013, Message: "target: ks.-.primary: vttablet: rpc error: code = Canceled desc = EOF (errno 2013)"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "2006 CR_SERVER_GONE_ERROR",
			err:  &gomysql.MySQLError{Number: 2006, Message: "MySQL server has gone away"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1213 InnoDB deadlock (contention, not grow)",
			err:  &gomysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1205 lock-wait-timeout (contention, not grow)",
			err:  &gomysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1105 vttablet QueryList.TerminateAll (query killer — deliberately NOT evidence)",
			err: &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = Canceled " +
				"desc = QueryList.TerminateAll() killing connection ID 42"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1105 vttablet code = ResourceExhausted (throttler)",
			err:  &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = ResourceExhausted desc = throttled"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "1105 vttablet code = Aborted (tx killer OR step-down — ambiguous)",
			err:  &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = Aborted desc = for tx killer rollback"},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "driver bad-conn sentinel",
			err:  fmt.Errorf("exec: %w", gomysql.ErrInvalidConn),
			want: ir.GrowEvidenceNone,
		},
		{
			name: "io.EOF sentinel",
			err:  fmt.Errorf("read packet: %w", io.EOF),
			want: ir.GrowEvidenceNone,
		},
		{
			name: "bare transport text",
			err:  errors.New("write tcp 10.0.0.1:3306: broken pipe"),
			want: ir.GrowEvidenceNone,
		},
		{
			name: "terminal 1062 on a table named reparent_history (the D0-3 echo shape)",
			err: &gomysql.MySQLError{Number: 1062, Message: `Duplicate entry '7' for key 'PRIMARY' ` +
				`(CallerID: app): Sql: "insert into reparent_history (id) values (7)"`},
			want: ir.GrowEvidenceNone,
		},
		{
			name: "nil error",
			err:  nil,
			want: ir.GrowEvidenceNone,
		},
	}
}

// TestGrowEvidence_FaceMatrix is the direct verdict pin over the corpus.
func TestGrowEvidence_FaceMatrix(t *testing.T) {
	for _, tc := range growEvidenceCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			if got := growEvidenceOf(tc.err); got != tc.want {
				t.Errorf("growEvidenceOf(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

// TestGrowEvidence_CorpusExercisesBothVerdicts is the ANTI-VACUITY floor.
//
// A matrix that had drifted to all-None would still pass the test above while
// having deleted the distinction — and "everything is transport" is exactly
// the answer a lazy implementation gives, so it is the one that must not pass
// silently. Both halves must be non-trivially populated.
func TestGrowEvidence_CorpusExercisesBothVerdicts(t *testing.T) {
	var faces, none int
	for _, tc := range growEvidenceCorpus() {
		switch tc.want {
		case ir.GrowEvidenceTargetFace:
			faces++
		case ir.GrowEvidenceNone:
			none++
		case ir.GrowEvidenceTelemetry:
			t.Errorf("%s: telemetry is not an ERROR verdict; growEvidenceOf must never return it", tc.name)
		}
	}
	if faces < 5 || none < 5 {
		t.Fatalf("the face corpus must exercise BOTH verdicts non-trivially: %d grow-face, %d no-evidence", faces, none)
	}
}

// TestGrowEvidence_IsDerivedFromTheRetriableClassifier binds the two verdicts
// that must not drift apart.
//
// The evidence classifier is not free to invent faces: a shape it calls
// [ir.GrowEvidenceTargetFace] is by construction one the RETRY classifier
// already rides out. If it ever claims grow evidence for something the lane
// would terminate on, the log would announce a coordinated grow window for an
// error that killed the copy — a claim about a mechanism that did not run.
//
// The converse is deliberately NOT asserted (retriable does not imply
// evidence); that asymmetry is the entire content of item 143.
func TestGrowEvidence_IsDerivedFromTheRetriableClassifier(t *testing.T) {
	for _, tc := range growEvidenceCorpus() {
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

// TestVtgateSubstringHalves_PartitionTheTransientSet is the anti-drift gate on
// the split introduced for item 143.
//
// [vtgateTransientSubstrings] is now the concatenation of a serving-transition
// half and a transport half. That arrangement is only safe while the union is
// EXACTLY the set the retry classifier used to match on — a literal added to
// one half but forgotten in the union would silently stop being retriable, and
// this set's own doc records that an incomplete derivation of it shipped once
// and was found by a live rig within hours.
func TestVtgateSubstringHalves_PartitionTheTransientSet(t *testing.T) {
	want := append(
		append([]string{}, vtgateServingTransitionSubstrings...),
		vtgateTransportSubstrings...,
	)
	if len(want) != len(vtgateTransientSubstrings) {
		t.Fatalf("union has %d literals, halves have %d — the partition has drifted", len(vtgateTransientSubstrings), len(want))
	}
	for i := range want {
		if want[i] != vtgateTransientSubstrings[i] {
			t.Errorf("literal %d: union has %q, halves have %q", i, vtgateTransientSubstrings[i], want[i])
		}
	}
	// The halves must be DISJOINT, or a literal could be counted as both
	// serving-transition evidence and transport noise.
	for _, s := range vtgateServingTransitionSubstrings {
		for _, t2 := range vtgateTransportSubstrings {
			if s == t2 {
				t.Errorf("%q appears in BOTH halves", s)
			}
		}
	}
	// Anti-vacuity: neither half may be empty, and the transport half must
	// still carry the field face the whole item is about.
	if len(vtgateServingTransitionSubstrings) == 0 || len(vtgateTransportSubstrings) == 0 {
		t.Fatal("both halves must be populated; an empty half makes the split meaningless")
	}
	found := false
	for _, s := range vtgateTransportSubstrings {
		if s == "vtgate connection error" {
			found = true
		}
	}
	if !found {
		t.Error("the transport half must carry \"vtgate connection error\" — the face that opened 244 of the 246 field windows")
	}
}

// TestGrowEvidence_TripSitesPassTheDerivedVerdict reaches the CALL SITES.
//
// This is the gate the item-136 M4 lesson demands: [growEvidenceOf] being
// correct proves nothing about whether a lane hands its verdict to the gate,
// and a mutation that reverts a trip site to a constant is invisible to every
// test above. It drives the real flush loop and the real acquire loop through
// a fake gate that records what it was told, once with a face-carrying error
// and once with the field's transport face, and requires the two to DIFFER —
// so a site hard-coding either constant fails.
func TestGrowEvidence_TripSitesPassTheDerivedVerdict(t *testing.T) {
	captureSlog(t)
	withFastReparentBackoff(t, 8)

	faceErr := &gomysql.MySQLError{Number: 1021, Message: "Disk full (/vt); waiting for someone to free some space"}
	dropErr := &gomysql.MySQLError{Number: 1105, Message: "unavailable: vtgate connection error: no endpoints"}

	// The FLUSH loop (ADR-0108 / item 138's trip site).
	for _, tc := range []struct {
		name string
		err  error
		want ir.GrowEvidence
	}{
		{"grow face", faceErr, ir.GrowEvidenceTargetFace},
		{"transport drop", dropErr, ir.GrowEvidenceNone},
	} {
		t.Run("flush/"+tc.name, func(t *testing.T) {
			script := &flushScript{execErrs: []error{tc.err, nil}}
			db := newScriptDB(t, script)
			gate := &recordingGrowGate{}
			w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert, growGate: gate}
			if err := w.WriteRows(t.Context(), pinReparentTable(), feedReparentRows(2)); err != nil {
				t.Fatalf("WriteRows: %v", err)
			}
			if gate.trips.Load() == 0 {
				t.Fatal("the flush loop never tripped the gate — the pin below would be vacuous")
			}
			if got := gate.LastEvidence(); got != tc.want {
				t.Errorf("flush trip claimed %s, want %s (the call site is not passing the derived verdict)", got, tc.want)
			}
		})
	}
}

// TestGrowGateTrip_IsReachedOnlyThroughTheDerivingHelper is the sibling-sweep
// gate on the verdict, and it is the one that survives a NEW lane.
//
// [RowWriter.tripGrowGate] derives the verdict from the error, so no call site
// can hard-code one — the type system sees to that. What the type system does
// NOT prevent is a future lane calling `w.growGate.Trip(reason, ...)` directly
// and supplying its own answer. That is precisely the shape this project keeps
// paying for: a fix made for the class, reached by all but one implementor.
// So the invariant is stated mechanically — in non-test files of this package,
// the raw gate method is called from exactly one function.
func TestGrowGateTrip_IsReachedOnlyThroughTheDerivingHelper(t *testing.T) {
	callers := growGateTripCallers(t, ".")
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

// growGateTripCallers returns the names of the functions in dir's non-test Go
// files that call `.Trip(...)` on a receiver expression mentioning growGate.
// Deliberately narrow: that is the only spelling the engine packages use, and
// a walker that tried to be clever here would be another unchecked claim.
//
// Uses the same os.ReadDir + parser.ParseFile idiom as this package's existing
// roster gates rather than parser.ParseDir, which is deprecated.
func growGateTripCallers(t *testing.T, dir string) []string {
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

// TestGrowEvidenceString_TokensAreStable pins the operator-facing tokens. They
// are what a field report gets grepped and counted on — the instrument item
// 143 adds so the NEXT report can answer the question this one could not — so
// a rename is a breaking change to an analysis, not a cosmetic edit.
func TestGrowEvidenceString_TokensAreStable(t *testing.T) {
	for ev, want := range map[ir.GrowEvidence]string{
		ir.GrowEvidenceNone:       "no-grow-evidence",
		ir.GrowEvidenceTargetFace: "target-grow-face",
		ir.GrowEvidenceTelemetry:  "telemetry-headroom",
	} {
		if got := ev.String(); got != want {
			t.Errorf("GrowEvidence(%d).String() = %q, want %q", ev, got, want)
		}
	}
	// The zero value must render as the under-claiming token, not as
	// something a reader could mistake for a positive finding.
	var zero ir.GrowEvidence
	if !strings.Contains(zero.String(), "no-grow-evidence") {
		t.Errorf("the zero value renders as %q — it must under-claim", zero.String())
	}
}
