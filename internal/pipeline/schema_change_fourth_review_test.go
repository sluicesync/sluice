// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// GC-44 F5 fourth review: the unit pins.
//
//   - Finding 1 (HIGH, silent): a narrow-back to a shape the table held
//     before was exempt from judgement — "a recorded shape, possibly a
//     replay" — even where the source's positions PROVE the boundary lies
//     after the prior. On a MySQL source that kept the target's unconverted
//     values at exit 0, live and after a restart against a history holding
//     both versions. A boundary proven after its prior is now judged
//     whatever shape it shows, at all three consumers of the old exemption:
//     the unforwarded check ([acceptedBoundary.evidenceFor]) and the
//     forward and Shape A first boundaries ([firstBoundaryWitness.historyPriorAt]).
//   - Finding 2 (loud, destructive advice): on Postgres, where nothing
//     orders a boundary against the one-version history, a narrowing or
//     DROP judged against it is a replay OR a source change. It still
//     refuses, but as AMBIGUOUS-SCHEMA-BOUNDARY: both readings named, how to
//     tell them apart, and the replay's exit — the one-shot
//     --accept-unforwarded-schema-change=<fingerprint>.
//   - Finding 3 (LOW, loud): varchar(n)[] against a text[] target refused
//     every start; char(n)[] stays refused (GC-44 F25).
//
// The independent expected value of every case is the table written here
// from what the source did, not derived from the check.

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fourthReviewDeps is a refuse-intercept dependency set over one table
// whose target holds target. history, when non-nil, is the retained
// version anchored at anchor, with recorded every version the stream's
// history holds for the table.
func fourthReviewDeps(target, history *ir.Table, anchor int, recorded ...*ir.Table) unforwardedBoundaryDeps {
	w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": target}}, "postgres", "postgres")
	w.orderer = numericOrderer{}
	if history != nil {
		w.history = []*ir.Table{history}
		w.retained = retainedHistory{history: {anchor: testPos(anchor), recorded: recorded}}
	}
	return unforwardedBoundaryDeps{
		witnessFor: func(string) *firstBoundaryWitness { return w },
		orderer:    numericOrderer{}, why: "--schema-changes=refuse",
	}
}

// TestInterceptSchemaChangeRefuse_NarrowBackProvenAfterThePrior is Finding
// 1 at the unforwarded check, every family [witnessWidthOrder] orders: the
// source widened (the drained model ran the target ahead first) and then
// narrowed BACK to the shape the table held before. Positions order it
// (a MySQL-family source), so it is judged and refused as a narrowing —
// not AMBIGUOUS — in process and after a restart against a history holding
// both versions. The DROP-back sibling likewise.
func TestInterceptSchemaChangeRefuse_NarrowBackProvenAfterThePrior(t *testing.T) {
	t.Parallel()
	families := narrowingFamilies()
	if len(families) < 15 {
		t.Fatalf("matrix holds %d families; floor 15", len(families))
	}
	for _, f := range families {
		wide, narrow := witnessTable(wcol("v", f.wide)), witnessTable(wcol("v", f.narrow))
		for _, tc := range []struct {
			name  string
			deps  unforwardedBoundaryDeps
			snaps []ir.SchemaSnapshot
			// passed is how many boundaries go downstream before the refusal.
			passed int
		}{
			{"in process", fourthReviewDeps(wide, nil, 0), []ir.SchemaSnapshot{refuseAt(narrow, 1), refuseAt(wide, 2), refuseAt(narrow, 3)}, 2},
			{"after a restart, history holding both versions", fourthReviewDeps(wide, wide, 10, narrow, wide), []ir.SchemaSnapshot{refuseAt(narrow, 20)}, 0},
		} {
			out, err := runRefuseIntercept(t, tc.deps, tc.snaps...)
			if !errors.Is(err, ir.ErrSchemaChangeRefused) || !strings.Contains(err.Error(), "narrowed from") {
				t.Errorf("%s / %s: err %v; want the narrow-back refused as a narrowing", f.name, tc.name, err)
				continue
			}
			if strings.Contains(err.Error(), ambiguousBoundaryMarker) {
				t.Errorf("%s / %s: a narrowing the positions prove was called %s: %v", f.name, tc.name, ambiguousBoundaryMarker, err)
			}
			if len(out) != tc.passed {
				t.Errorf("%s / %s: %d boundaries downstream; want %d", f.name, tc.name, len(out), tc.passed)
			}
		}
	}

	a := wcol("a", ir.Integer{Width: 32})
	withA, withoutA := witnessTable(a), witnessTable()
	t.Run("a DROP back to a shape the table held, proven after", func(t *testing.T) {
		_, err := runRefuseIntercept(t, fourthReviewDeps(withA, nil, 0), refuseAt(withoutA, 1), refuseAt(withA, 2), refuseAt(withoutA, 3))
		if !errors.Is(err, ir.ErrSchemaChangeRefused) || !strings.Contains(err.Error(), "DROP COLUMN a") || strings.Contains(err.Error(), ambiguousBoundaryMarker) {
			t.Fatalf("err %v; want the DROP refused, not ambiguous", err)
		}
	})

	// The residual, stated: where the order is unknown, a boundary showing a
	// shape the stream recorded may be a replay, and is kept with the WARN.
	// On a Postgres source this sequence needs one reader session, where
	// the reader's gate refuses the second type change first.
	t.Run("unknown order (Postgres in-process positions): a recorded shape is kept", func(t *testing.T) {
		wide := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 4}))
		narrow := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
		out, err := runRefuseIntercept(t, fourthReviewDeps(wide, nil, 0), refuseAt(narrow, 0), refuseAt(wide, 0), refuseAt(narrow, 0))
		if err != nil || len(out) != 3 {
			t.Fatalf("out %d, err %v; want the documented residual (all kept)", len(out), err)
		}
	})
}

// TestFirstBoundaryWitness_NarrowBackProvenAfterTheHistory is Finding 1 at
// the forward and Shape A first boundaries, which share
// [firstBoundaryWitness.verdict]: a narrowing back to a shape the history
// also holds, made while the stream was stopped and proven after the
// resolved version, is forwarded as the narrowing it is (it was kept with
// the WARN). At the anchor — a Postgres 0/0 boundary — nothing is proven
// and the WARN-keep stands (GC-44 F23).
func TestFirstBoundaryWitness_NarrowBackProvenAfterTheHistory(t *testing.T) {
	t.Parallel()
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
	w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": wide}}, "postgres", "postgres")
	w.orderer = numericOrderer{}
	w.history = []*ir.Table{wide}
	w.retained = retainedHistory{wide: {anchor: testPos(10), recorded: []*ir.Table{narrow, wide}}}
	ctx := context.Background()
	if v, err := w.verdict(ctx, narrow, testPos(20)); err != nil || v.kind != witnessForwardAlter {
		t.Errorf("proven after: verdict %d (%v); want the narrowing forwarded", v.kind, err)
	}
	if v, err := w.verdict(ctx, narrow, testPos(10)); err != nil || v.kind != witnessTargetWider {
		t.Errorf("at the anchor: verdict %d (%v); want the WARN-keep", v.kind, err)
	}
}

// ackRE extracts the fingerprint an AMBIGUOUS refusal offers.
var ackRE = regexp.MustCompile(`--accept-unforwarded-schema-change=([0-9a-f]{12})`)

// ambiguousFingerprint returns the fingerprint err offers, failing the test
// when it offers none.
func ambiguousFingerprint(t *testing.T, err error) string {
	t.Helper()
	m := ackRE.FindStringSubmatch(err.Error())
	if m == nil {
		t.Fatalf("the refusal offers no acknowledgement: %v", err)
	}
	return m[1]
}

// assertAmbiguous checks the shape of an AMBIGUOUS refusal: the marker, both
// readings, how to tell them apart, and the destructive remedy confined to
// the source-change reading.
func assertAmbiguous(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ir.ErrSchemaChangeRefused) {
		t.Fatalf("err %v; want %s", err, schemaChangeRefusedMarker)
	}
	msg := err.Error()
	for _, want := range []string{ambiguousBoundaryMarker, "REPLAY", "(2) a change made on the source", "CURRENT definition", "change NOTHING on the target", "Drained-model recovery"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the AMBIGUOUS refusal lacks %q: %s", want, msg)
		}
	}
	// "apply the same change" is the source-change remedy: it must follow
	// the sentence that confines it to that case.
	if two, apply := strings.Index(msg, "it is (2):"), strings.Index(msg, "apply the same change"); two < 0 || apply < two {
		t.Errorf("the destructive remedy is not confined to the source-change reading: %s", msg)
	}
}

// TestInterceptSchemaChangeRefuse_AmbiguousBoundary is Finding 2: the
// reviewer's crash replays against the history a Postgres source really
// keeps (one version, the latest, at 0/0). Each refuses as AMBIGUOUS with a
// fingerprint; the fingerprint, passed back, accepts the replay; a
// fingerprint of another boundary does not.
func TestInterceptSchemaChangeRefuse_AmbiguousBoundary(t *testing.T) {
	t.Parallel()
	a := wcol("a", ir.Integer{Width: 32})
	withA, withoutA := witnessTable(a), witnessTable()
	acked := func(d unforwardedBoundaryDeps, fp string) unforwardedBoundaryDeps {
		d.acknowledged = fp
		return d
	}

	for _, f := range narrowingFamilies() {
		wide, narrow := witnessTable(wcol("v", f.wide)), witnessTable(wcol("v", f.narrow))
		// The replay of {DML; ALTER widen; DML}: pre-ALTER relation first.
		replay := []ir.SchemaSnapshot{refuseAt(narrow, 0), refuseAt(wide, 0)}
		d := fourthReviewDeps(wide, wide, 0, wide)
		_, err := runRefuseIntercept(t, d, replay...)
		if err == nil {
			t.Errorf("%s: the replay was accepted; want the AMBIGUOUS refusal", f.name)
			continue
		}
		assertAmbiguous(t, err)
		if !strings.Contains(err.Error(), "narrowed from") {
			t.Errorf("%s: the refusal does not name the narrowing: %v", f.name, err)
		}
		fp := ambiguousFingerprint(t, err)
		if out, err := runRefuseIntercept(t, acked(d, fp), replay...); err != nil || len(out) != 2 {
			t.Errorf("%s: acknowledged: out %d, err %v; want the replay accepted", f.name, len(out), err)
		}
	}

	t.Run("the replay of an ADD COLUMN transaction", func(t *testing.T) {
		d := fourthReviewDeps(withA, withA, 0, withA)
		// A single-database refuse-mode stream, which a proven refusal
		// would point at --schema-changes=forward.
		d.forwardRemedy = true
		replay := []ir.SchemaSnapshot{refuseAt(withoutA, 0), refuseAt(withA, 0)}
		_, err := runRefuseIntercept(t, d, replay...)
		if err == nil {
			t.Fatal("accepted; want the AMBIGUOUS refusal")
		}
		assertAmbiguous(t, err)
		if !strings.Contains(err.Error(), "DROP COLUMN a") {
			t.Errorf("the refusal does not name the DROP: %v", err)
		}
		// Forward mode's first boundary cannot prove the DROP on this source
		// either (F23), so in the source-change case it would WARN and keep
		// the target: never offered for an AMBIGUOUS refusal.
		if strings.Contains(err.Error(), "--schema-changes=forward") {
			t.Errorf("the AMBIGUOUS refusal offers --schema-changes=forward: %v", err)
		}
		fp := ambiguousFingerprint(t, err)
		if out, err := runRefuseIntercept(t, acked(d, fp), replay...); err != nil || len(out) != 2 {
			t.Fatalf("acknowledged: out %d, err %v; want both boundaries accepted", len(out), err)
		}
	})

	t.Run("the fingerprint names the table and the columns", func(t *testing.T) {
		d := fourthReviewDeps(withA, withA, 0, withA)
		_, e1 := runRefuseIntercept(t, d, refuseAt(withoutA, 0))
		_, e2 := runRefuseIntercept(t, d, refuseAt(withoutA, 0))
		if ambiguousFingerprint(t, e1) != ambiguousFingerprint(t, e2) {
			t.Error("the same boundary printed two fingerprints; a restart could never acknowledge it")
		}
		// The same table, a different dropped column (b).
		b := wcol("b", ir.Integer{Width: 32})
		withAB := witnessTable(a, b)
		_, e3 := runRefuseIntercept(t, fourthReviewDeps(withAB, withAB, 0, withAB), refuseAt(withA, 0))
		if ambiguousFingerprint(t, e1) == ambiguousFingerprint(t, e3) {
			t.Error("two different boundaries share a fingerprint")
		}
		// The acknowledgement of the first does not accept the second.
		if _, err := runRefuseIntercept(t, acked(fourthReviewDeps(withAB, withAB, 0, withAB), ambiguousFingerprint(t, e1)), refuseAt(withA, 0)); err == nil ||
			!strings.Contains(err.Error(), "does not name this boundary") {
			t.Errorf("another boundary's fingerprint: err %v; want refused, saying the acknowledgement was not applied", err)
		}
	})

	t.Run("positions that order the boundary: not ambiguous, no acknowledgement", func(t *testing.T) {
		d := fourthReviewDeps(withA, withA, 10, withA)
		_, err := runRefuseIntercept(t, d, refuseAt(withoutA, 20))
		if !errors.Is(err, ir.ErrSchemaChangeRefused) || strings.Contains(err.Error(), ambiguousBoundaryMarker) || ackRE.MatchString(err.Error()) {
			t.Fatalf("err %v; want the proven DROP refused with no acknowledgement offered", err)
		}
	})

	t.Run("a replay that also carries a column the target lacks cannot be acknowledged", func(t *testing.T) {
		// A RENAME COLUMN v -> w taken by the target: history and target
		// hold w, the replay shows v.
		v := witnessTable(wcol("v", ir.Text{Size: ir.TextLong}))
		w := witnessTable(wcol("w", ir.Text{Size: ir.TextLong}))
		d := fourthReviewDeps(w, w, 0, w)
		_, err := runRefuseIntercept(t, d, refuseAt(v, 0))
		if err == nil || !strings.Contains(err.Error(), ambiguousBoundaryMarker) || !strings.Contains(err.Error(), "cannot be acknowledged") || ackRE.MatchString(err.Error()) {
			t.Fatalf("err %v; want AMBIGUOUS without an acknowledgement on offer", err)
		}
		// Even the boundary's own fingerprint, passed anyway, does not
		// accept it: the column the target lacks is not a replay's to explain.
		j := unforwardedJudgement{
			refused: []witnessColumnDiff{{"w", "(dropped on the source)", "Text"}, {"v", "Text", "(absent)"}},
			onPrior: 1, ambiguous: true,
		}
		fp := newAmbiguousBoundary("src.w", renderWitnessDiffs(j.refused), "", false).fingerprint
		err = j.settle(context.Background(), "src.w", unforwardedBoundaryDeps{acknowledged: fp})
		if !errors.Is(err, ir.ErrSchemaChangeRefused) {
			t.Fatalf("a non-acknowledgeable boundary was accepted on its own fingerprint (err %v)", err)
		}
	})

	t.Run("the unwitnessed fallback refuses as AMBIGUOUS too", func(t *testing.T) {
		wide := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 4}))
		narrow := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{}}, "postgres", "postgres")
		w.orderer = numericOrderer{}
		w.history = []*ir.Table{wide}
		w.retained = retainedHistory{wide: {anchor: testPos(0), recorded: []*ir.Table{wide}}}
		d := unforwardedBoundaryDeps{witnessFor: func(string) *firstBoundaryWitness { return w }, orderer: numericOrderer{}, why: "--schema-changes=refuse"}
		_, err := runRefuseIntercept(t, d, refuseAt(narrow, 0))
		if err == nil || !strings.Contains(err.Error(), ambiguousBoundaryMarker) {
			t.Fatalf("err %v; want AMBIGUOUS", err)
		}
		d.acknowledged = ambiguousFingerprint(t, err)
		if out, err := runRefuseIntercept(t, d, refuseAt(narrow, 0)); err != nil || len(out) != 1 {
			t.Fatalf("acknowledged: out %d, err %v; want accepted", len(out), err)
		}
	})
}

// TestWitnessWidthOrder_ArrayElementAcrossFamilies is Finding 3, decided
// arm by arm: varchar(n)[] against text[] is kept (it matched before the
// third review threaded the element modifier, and refused every start
// after); char(n)[] against text[] refuses (bpchar padding, GC-44 F25); no
// other scalar cross-family arm reaches an array element.
func TestWitnessWidthOrder_ArrayElementAcrossFamilies(t *testing.T) {
	t.Parallel()
	arr := func(e ir.Type) ir.Type { return ir.Array{Element: e} }
	text := ir.Text{Size: ir.TextLong}
	for _, tc := range []struct {
		name             string
		target, snapshot ir.Type
		want             widthOrder
	}{
		{"text[] holds varchar(16)[]", arr(text), arr(ir.Varchar{Length: 16}), targetWider},
		{"varchar(16)[] does not hold text[]", arr(ir.Varchar{Length: 16}), arr(text), notComparable},
		{"tinytext[] does not cover varchar(255)[] at 4 bytes a character", arr(ir.Text{Size: ir.TextTiny}), arr(ir.Varchar{Length: 255}), notComparable},
		{"char(5)[] against text[]: refused (F25)", arr(text), arr(ir.Char{Length: 5}), notComparable},
		{"int[] against numeric[]: not extended", arr(ir.Decimal{Unconstrained: true}), arr(ir.Integer{Width: 32}), notComparable},
		{"same family still ordered", arr(ir.Varchar{Length: 64}), arr(ir.Varchar{Length: 16}), targetWider},
		{"a narrower element still refuses as mixed", arr(ir.Varchar{Length: 16}), arr(ir.Varchar{Length: 64}), mixedWidth},
	} {
		if got := witnessWidthOrder(witnessCompareType(tc.target), witnessCompareType(tc.snapshot)); got != tc.want {
			t.Errorf("%s: order %d, want %d", tc.name, got, tc.want)
		}
	}

	// Through the refuse check and the forward first boundary.
	src := witnessTable(wcol("tags", arr(ir.Varchar{Length: 16})))
	tgt := witnessTable(wcol("tags", arr(text)))
	if out, err := runRefuseIntercept(t, fourthReviewDeps(tgt, nil, 0), refuseSnap(src)); err != nil || len(out) != 1 {
		t.Errorf("refuse check, varchar(16)[] -> text[]: out %d, err %v; want kept", len(out), err)
	}
	if v := classifyWitness(src, tgt, witnessOptions{}); v.kind != witnessTargetWider {
		t.Errorf("forward first boundary, varchar(16)[] -> text[]: verdict %d, want the WARN-keep", v.kind)
	}
	charSrc := witnessTable(wcol("tags", arr(ir.Char{Length: 5})))
	if _, err := runRefuseIntercept(t, fourthReviewDeps(tgt, nil, 0), refuseSnap(charSrc)); !errors.Is(err, ir.ErrSchemaChangeRefused) {
		t.Errorf("refuse check, char(5)[] -> text[]: err %v; want refused", err)
	}
}
