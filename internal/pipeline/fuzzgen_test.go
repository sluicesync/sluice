// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the fuzz harness's pure logic (registry / generator /
// oracle). No build tag — these run in the bare `go test ./...` gate so
// the registry-coverage and classifier-correctness invariants are
// verified without Docker. The integration driver
// (migrate_fuzz_roundtrip_integration_test.go) exercises the same code
// against real databases.

package pipeline

import (
	"database/sql"
	"math/rand"
	"strings"
	"testing"
)

// TestRegistry_CoversEveryBugClass is the "verify, do not trust" guard
// (design-contract review focus + the Bug 74 class): the registry MUST
// contain every family that produced a v0.69.x bug, and each must be
// present at the shapes that exercised the defect.
func TestRegistry_CoversEveryBugClass(t *testing.T) {
	byName := map[string]*family{}
	for _, f := range registry() {
		byName[f.name] = f
	}

	// (b) of the report: enumerate every required family and the bug
	// shape it pins.
	required := map[string]struct {
		needsArray bool
		bug        string
	}{
		"int8":                  {true, "ints all widths"},
		"int16":                 {true, "ints all widths"},
		"int24":                 {true, "ints all widths (mediumint)"},
		"int32":                 {true, "ints all widths"},
		"int64":                 {true, "ints all widths"},
		"uint8":                 {false, "unsigned widen"},
		"uint16":                {false, "unsigned widen"},
		"uint32":                {false, "unsigned widen"},
		"uint64":                {false, "Bug 11 unsigned-bigint narrowing"},
		"numeric_15_4":          {true, "constrained decimal"},
		"numeric_unconstrained": {true, "Bug 69 unconstrained numeric"},
		"float4":                {true, "float"},
		"float8":                {true, "float"},
		"bool":                  {true, "bool"},
		// char/varchar are scalar-only by design (documented PG
		// array-element-length emit gap — see strFamily).
		"char":         {false, "char"},
		"varchar":      {false, "varchar"},
		"varchar_wide": {false, "Bug 72 wide varchar >16383"},
		"text":         {true, "text"},
		"varbinary":    {false, "binary/varbinary"},
		"blob":         {false, "blob"},
		"bit8":         {false, "Bug 75 bit silent corruption"},
		"varbit":       {false, "Bug 75 varbit"},
		"date":         {true, "date"},
		"time":         {true, "time"},
		"timetz":       {true, "Bug 71 timetz / Bug 73 timetz[] loud-refuse"},
		"timestamp":    {true, "timestamp"},
		"timestamptz":  {true, "timestamptz"},
		"json":         {false, "json"},
		"uuid":         {true, "Bug 73/74 uuid[] element class"},
		"inet":         {true, "Bug 73/74 inet[] element class"},
		"cidr":         {true, "Bug 73/74 cidr[] element class"},
		"macaddr":      {true, "Bug 73/74 macaddr[] element class"},
		"enum":         {false, "enum"},
	}

	for name, req := range required {
		f, ok := byName[name]
		if !ok {
			t.Errorf("registry MISSING family %q (%s)", name, req.bug)
			continue
		}
		if req.needsArray {
			has1D, has2D := false, false
			for _, s := range f.shapes {
				if s == shape1DArray {
					has1D = true
				}
				if s == shapeMultiDim {
					has2D = true
				}
			}
			if !has1D || !has2D {
				t.Errorf("family %q (%s) must cover 1-D AND multi-dim array shapes; shapes=%v",
					name, req.bug, f.shapes)
			}
		}
	}
}

// TestRegistry_TimetzArrayIsLoudRefuse pins the load-bearing
// classification: PG→PG timetz[] (1-D and ≥2-D) is a DOCUMENTED
// loud-refuse (migrate_bug7374_integration_test.go), while scalar
// timetz PG→PG is faithful. The harness must treat the refusal as a
// PASS, not a FAIL.
func TestRegistry_TimetzArrayIsLoudRefuse(t *testing.T) {
	var tz *family
	for _, f := range registry() {
		if f.name == "timetz" {
			tz = f
		}
	}
	if tz == nil {
		t.Fatal("timetz family missing")
	}
	pgpg := direction{enginePG, enginePG}
	if got := tz.expect(pgpg, shapeScalar); got != outcomeFaithful {
		t.Errorf("timetz scalar PG→PG: got %s; want faithful", got)
	}
	if got := tz.expect(pgpg, shape1DArray); got != outcomeLoudRefuse {
		t.Errorf("timetz[] PG→PG: got %s; want loud-refuse (Bug 73)", got)
	}
	if got := tz.expect(pgpg, shapeMultiDim); got != outcomeLoudRefuse {
		t.Errorf("timetz[][] PG→PG: got %s; want loud-refuse (Bug 73)", got)
	}
}

// TestRegistry_NoFalsePositiveOnDocumentedLossy guards the v0.69.0 #16
// false-positive class: documented cross-engine degradations
// (unconstrained numeric → MySQL, PG array → MySQL JSON, uuid/inet →
// MySQL) MUST classify lossy-documented (NOT compared for equality, NOT
// a refusal), so the harness never FAILs a documented transformation.
func TestRegistry_NoFalsePositiveOnDocumentedLossy(t *testing.T) {
	cases := []struct {
		fam string
		d   direction
		s   shape
		exp outcome
	}{
		{"numeric_unconstrained", direction{enginePG, engineMySQL}, shapeScalar, outcomeLossyDocument},
		{"numeric_unconstrained", direction{enginePG, enginePG}, shapeScalar, outcomeFaithful},
		{"uuid", direction{enginePG, engineMySQL}, shapeScalar, outcomeLossyDocument},
		{"uuid", direction{enginePG, enginePG}, shapeScalar, outcomeFaithful},
		{"inet", direction{enginePG, engineMySQL}, shape1DArray, outcomeLossyDocument},
		{"varchar_wide", direction{enginePG, engineMySQL}, shapeScalar, outcomeLossyDocument},
		{"varchar_wide", direction{enginePG, enginePG}, shapeScalar, outcomeFaithful},
		{"json", direction{enginePG, engineMySQL}, shapeScalar, outcomeLossyDocument},
		// Phase-1 scope: cross-engine canonical text is engine-specific,
		// so even a lossless int round-trip is lossy-documented
		// cross-engine (the #16-safe stance — see the scope note in
		// fuzzgen_registry.go). Same-engine stays faithful.
		{"int32", direction{engineMySQL, enginePG}, shapeScalar, outcomeLossyDocument},
		{"int32", direction{enginePG, enginePG}, shapeScalar, outcomeFaithful},
		{"int32", direction{engineMySQL, engineMySQL}, shapeScalar, outcomeFaithful},
		{"bool", direction{engineMySQL, enginePG}, shapeScalar, outcomeLossyDocument},
		{"varbinary", direction{engineMySQL, enginePG}, shapeScalar, outcomeLossyDocument},
		{"int8", direction{enginePG, engineMySQL}, shape1DArray, outcomeLossyDocument},
	}
	byName := map[string]*family{}
	for _, f := range registry() {
		byName[f.name] = f
	}
	for _, c := range cases {
		f := byName[c.fam]
		if f == nil {
			t.Errorf("family %q missing", c.fam)
			continue
		}
		if got := f.expect(c.d, c.s); got != c.exp {
			t.Errorf("%s %s %s: got %s; want %s", c.fam, c.d, c.s, got, c.exp)
		}
	}
}

// TestGenerator_Deterministic proves the seed IS the fixture: the same
// (seed, idx, dir) regenerates a byte-identical script (design
// decision #4 — reproducible replay).
func TestGenerator_Deterministic(t *testing.T) {
	for _, d := range allDirections() {
		for i := 0; i < 25; i++ {
			a := generateCase(1234567, i, d)
			b := generateCase(1234567, i, d)
			if a.ddl != b.ddl {
				t.Fatalf("non-deterministic [%s case %d]:\nA:\n%s\nB:\n%s", d, i, a.ddl, b.ddl)
			}
			if a.ddl == "" {
				t.Fatalf("empty script [%s case %d]", d, i)
			}
		}
	}
}

// TestGenerator_LoudRefuseArrayForcesNonNull pins the Findings 1 & 2
// fix: a loud-refuse array column must never render a whole-column
// NULL or an all-NULL array, or the loud-refuse assertion is vacuous
// (migrate legitimately exits 0 because nothing traverses the absent
// codec) — a guaranteed false-positive on the documented loud-refuse
// set. timetz[]/timetz[][] PG→PG is that load-bearing case.
func TestGenerator_LoudRefuseArrayForcesNonNull(t *testing.T) {
	var timetz *family
	for _, f := range registry() {
		if f.name == "timetz" {
			timetz = f
			break
		}
	}
	if timetz == nil {
		t.Fatal("timetz family missing from registry")
	}

	// (a) renderCell mechanism: a forced loud-refuse array column never
	// emits NULL and always carries ≥1 real timetz literal (a quoted
	// value). 500 draws covers gen's ~1/6 NULL probability deeply.
	for _, shp := range []shape{shape1DArray, shapeMultiDim} {
		c := genColumn{
			name: "c_timetz", fam: timetz, shp: shp,
			forceNonNullLeaf: true,
		}
		r := rand.New(rand.NewSource(1))
		for i := 0; i < 500; i++ {
			out := renderCell(r, &c, enginePG)
			if out == "NULL" {
				t.Fatalf("[%s] forced loud-refuse array rendered whole-column NULL at draw %d (vacuous-FP class)", shp, i)
			}
			if !strings.Contains(out, "'") {
				t.Fatalf("[%s] forced loud-refuse array has no non-NULL element at draw %d: %q", shp, i, out)
			}
		}
	}

	// (b) wiring: generateCase must SET forceNonNullLeaf for every
	// timetz array column in the PG→PG direction, and none of that
	// column's rendered values may be NULL / all-NULL.
	var pgpg direction
	for _, d := range allDirections() {
		if d.String() == "postgres->postgres" {
			pgpg = d
		}
	}
	sawTimetzArray := false
	for idx := 0; idx < 120; idx++ {
		gc := generateCase(987654321, idx, pgpg)
		for _, c := range gc.columns {
			if c.fam.name != "timetz" || c.shp == shapeScalar {
				continue
			}
			sawTimetzArray = true
			if !c.forceNonNullLeaf {
				t.Errorf("idx %d col %s: timetz array PG→PG must have forceNonNullLeaf set", idx, c.name)
			}
			for ri, v := range c.values {
				if v == "NULL" || !strings.Contains(v, "'") {
					t.Errorf("idx %d col %s row %d: vacuous loud-refuse value %q (no real element)", idx, c.name, ri, v)
				}
			}
		}
	}
	if !sawTimetzArray {
		t.Fatal("no timetz array column generated across 120 PG→PG cases — coverage gap")
	}

	// (c) over-correction guard: a faithful (non-loud-refuse) array
	// family must still be ABLE to render NULL — the fix must not
	// globally suppress NULL coverage.
	var faithfulArr *family
	for _, f := range registry() {
		if f.canSource(enginePG, shape1DArray) && f.expect(pgpg, shape1DArray) != outcomeLoudRefuse {
			faithfulArr = f
			break
		}
	}
	if faithfulArr == nil {
		t.Fatal("no faithful array-capable family found")
	}
	c := genColumn{name: "c_faithful", fam: faithfulArr, shp: shape1DArray}
	r := rand.New(rand.NewSource(2))
	sawNull := false
	for i := 0; i < 500 && !sawNull; i++ {
		if renderCell(r, &c, enginePG) == "NULL" {
			sawNull = true
		}
	}
	if !sawNull {
		t.Errorf("faithful array family %q never rendered NULL in 500 draws — fix over-suppressed NULLs", faithfulArr.name)
	}
}

// TestGenerator_SourceDialectShape sanity-checks the emitted script:
// MySQL source never emits array columns / PG `::` casts; PG source
// emits a CREATE TABLE; both always include the id PK and ≥1 column.
func TestGenerator_SourceDialectShape(t *testing.T) {
	for _, d := range allDirections() {
		gc := generateCase(99, 7, d)
		if !strings.Contains(gc.ddl, "CREATE TABLE "+gc.tableNm) {
			t.Errorf("[%s] missing CREATE TABLE: %s", d, gc.ddl)
		}
		if !strings.Contains(gc.ddl, "id INT") {
			t.Errorf("[%s] missing id PK: %s", d, gc.ddl)
		}
		if len(gc.columns) == 0 {
			t.Errorf("[%s] zero columns generated", d)
		}
		if d.src == engineMySQL {
			for _, c := range gc.columns {
				if c.shp != shapeScalar {
					t.Errorf("[%s] MySQL source got non-scalar column %s shape=%s", d, c.name, c.shp)
				}
			}
			if strings.Contains(gc.ddl, "::") || strings.Contains(gc.ddl, "ARRAY[") {
				t.Errorf("[%s] MySQL source script contains PG syntax:\n%s", d, gc.ddl)
			}
			if !strings.Contains(gc.ddl, "ENGINE=InnoDB") {
				t.Errorf("[%s] MySQL source script missing ENGINE clause", d)
			}
		}
	}
}

// TestOracle_ClassifyTruthTable exercises every branch of the
// three-outcome classifier — the load-bearing logic the design
// contract calls out for adversarial review.
func TestOracle_ClassifyTruthTable(t *testing.T) {
	mkCase := func() *genCase { return &genCase{tableNm: "t", dir: direction{enginePG, enginePG}} }
	ns := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

	t.Run("loud-refuse expected, refused, target absent → PASS", func(t *testing.T) {
		ce := caseExpectation{loudRefuse: true, reason: "timetz[]"}
		v, msg := classify(mkCase(), ce, errBoom, -1, nil, nil)
		if v != verdictPass {
			t.Errorf("got FAIL (%s); want PASS", msg)
		}
	})
	t.Run("loud-refuse expected, refused, EMPTY target → PASS (documented refuse-at-copy)", func(t *testing.T) {
		// The load-bearing new logic: timetz[] refuses in the COPY
		// writer, AFTER create-tables, so an empty table necessarily
		// exists. The Bug 73 battle fixture accepts exactly this; an
		// empty table is NOT a partial-data target.
		ce := caseExpectation{loudRefuse: true, reason: "timetz[]"}
		v, msg := classify(mkCase(), ce, errBoom, 0, nil, nil)
		if v != verdictPass {
			t.Errorf("empty target after documented refuse-at-copy must PASS; got FAIL (%s)", msg)
		}
	})
	t.Run("loud-refuse expected, migrate SUCCEEDED → FAIL (silent)", func(t *testing.T) {
		ce := caseExpectation{loudRefuse: true, reason: "timetz[]"}
		v, _ := classify(mkCase(), ce, nil, 3, nil, nil)
		if v != verdictFail {
			t.Error("expected FAIL on unexpected success of a loud-refuse case")
		}
	})
	t.Run("loud-refuse expected, refused but PARTIAL DATA (rows>0) → FAIL", func(t *testing.T) {
		ce := caseExpectation{loudRefuse: true, reason: "timetz[]"}
		v, msg := classify(mkCase(), ce, errBoom, 2, nil, nil)
		if v != verdictFail || !strings.Contains(msg, "PARTIAL TARGET DATA") {
			t.Errorf("want FAIL/PARTIAL TARGET DATA on rows after refusal; got %v %q", v, msg)
		}
	})
	t.Run("faithful expected, unexpected refusal → FAIL (#16 class)", func(t *testing.T) {
		ce := caseExpectation{faithfulCols: []string{"c"}}
		v, msg := classify(mkCase(), ce, errBoom, -1, nil, nil)
		if v != verdictFail || !strings.Contains(msg, "UNEXPECTED REFUSAL") {
			t.Errorf("want FAIL/UNEXPECTED REFUSAL; got %v %q", v, msg)
		}
	})
	t.Run("faithful, src==dst → PASS", func(t *testing.T) {
		gc := mkCase()
		gc.columns = []genColumn{{name: "c"}}
		ce := caseExpectation{faithfulCols: []string{"c"}}
		src := map[string][]sql.NullString{"c": {ns("a"), ns("b")}}
		dst := map[string][]sql.NullString{"c": {ns("a"), ns("b")}}
		v, msg := classify(gc, ce, nil, 2, src, dst)
		if v != verdictPass {
			t.Errorf("got FAIL (%s); want PASS", msg)
		}
	})
	t.Run("faithful, src!=dst → FAIL (silent loss)", func(t *testing.T) {
		gc := mkCase()
		gc.columns = []genColumn{{name: "c"}}
		ce := caseExpectation{faithfulCols: []string{"c"}}
		src := map[string][]sql.NullString{"c": {ns("a"), ns("b")}}
		dst := map[string][]sql.NullString{"c": {ns("a"), ns("X")}}
		v, msg := classify(gc, ce, nil, 2, src, dst)
		if v != verdictFail || !strings.Contains(msg, "MISMATCH") {
			t.Errorf("want FAIL/MISMATCH; got %v %q", v, msg)
		}
	})
	t.Run("cross-engine rowcount pseudo-column mismatch → FAIL", func(t *testing.T) {
		// The cross-engine regime (no faithful columns — every SQLite
		// direction lives here) reports row presence under
		// fuzzRowCountKey; a skew is a silent ROW loss.
		gc := mkCase()
		gc.columns = []genColumn{{name: "c"}}
		src := map[string][]sql.NullString{fuzzRowCountKey: {{}, {}, {}}}
		dst := map[string][]sql.NullString{fuzzRowCountKey: {{}, {}}}
		v, msg := classify(gc, caseExpectation{}, nil, 2, src, dst)
		if v != verdictFail || !strings.Contains(msg, "ROW COUNT MISMATCH") {
			t.Errorf("want FAIL/ROW COUNT MISMATCH; got %v %q", v, msg)
		}
	})
	t.Run("cross-engine rowcount pseudo-column equal → PASS", func(t *testing.T) {
		gc := mkCase()
		gc.columns = []genColumn{{name: "c"}}
		src := map[string][]sql.NullString{fuzzRowCountKey: {{}, {}}}
		dst := map[string][]sql.NullString{fuzzRowCountKey: {{}, {}}}
		v, msg := classify(gc, caseExpectation{}, nil, 2, src, dst)
		if v != verdictPass {
			t.Errorf("got FAIL (%s); want PASS", msg)
		}
	})
	t.Run("faithful, row count mismatch → FAIL", func(t *testing.T) {
		gc := mkCase()
		gc.columns = []genColumn{{name: "c"}}
		ce := caseExpectation{faithfulCols: []string{"c"}}
		src := map[string][]sql.NullString{"c": {ns("a"), ns("b")}}
		dst := map[string][]sql.NullString{"c": {ns("a")}}
		v, _ := classify(gc, ce, nil, 1, src, dst)
		if v != verdictFail {
			t.Error("expected FAIL on row-count mismatch")
		}
	})
}

// TestOracle_ExpectationFor reduces a multi-column case correctly: one
// loud-refuse column makes the whole case expect a refusal; faithful
// columns are collected; lossy-documented columns are neither.
func TestOracle_ExpectationFor(t *testing.T) {
	byName := map[string]*family{}
	for _, f := range registry() {
		byName[f.name] = f
	}
	gc := &genCase{
		dir: direction{enginePG, enginePG},
		columns: []genColumn{
			{name: "a", fam: byName["int32"], shp: shapeScalar},   // faithful
			{name: "b", fam: byName["timetz"], shp: shape1DArray}, // loud-refuse
			{name: "c", fam: byName["int8"], shp: shapeScalar},    // faithful
		},
	}
	ce := expectationFor(gc)
	if !ce.loudRefuse {
		t.Error("a case containing timetz[] PG→PG must expect a loud refusal")
	}
	if ce.reason == "" {
		t.Error("loud-refuse expectation must carry a reason")
	}

	// Cross-engine (PG→MySQL): every column is lossy-documented per the
	// Phase-1 scope, so NO faithful columns and NOT a refusal — the
	// oracle will only assert migrate-succeeds + column-exists.
	gc2 := &genCase{
		dir: direction{enginePG, engineMySQL},
		columns: []genColumn{
			{name: "a", fam: byName["int32"], shp: shapeScalar},
			{name: "b", fam: byName["numeric_unconstrained"], shp: shapeScalar},
		},
	}
	ce2 := expectationFor(gc2)
	if ce2.loudRefuse {
		t.Error("PG→MySQL unconstrained numeric is lossy-documented, NOT a refusal")
	}
	if len(ce2.faithfulCols) != 0 {
		t.Errorf("faithfulCols = %v; want empty (cross-engine is lossy-documented in Phase 1)", ce2.faithfulCols)
	}

	// Same-engine (PG→PG): faithful columns ARE compared; the lossy/
	// refuse families collapse to faithful same-engine (except a
	// genuine same-engine loud-refuse like timetz[]).
	gc3 := &genCase{
		dir: direction{enginePG, enginePG},
		columns: []genColumn{
			{name: "a", fam: byName["int32"], shp: shapeScalar},
			{name: "b", fam: byName["numeric_unconstrained"], shp: shapeScalar},
		},
	}
	ce3 := expectationFor(gc3)
	if ce3.loudRefuse {
		t.Error("PG→PG int32 + unconstrained numeric must NOT be a refusal")
	}
	if len(ce3.faithfulCols) != 2 {
		t.Errorf("faithfulCols = %v; want both columns (same-engine faithful)", ce3.faithfulCols)
	}
}

// TestOracle_FaithfulColumnsFor pins which columns the oracle compares
// src==dst: only outcomeFaithful columns. A loud-refuse or
// lossy-documented column must NOT be in the compared set (comparing a
// documented degradation is the #16 false-positive class).
func TestOracle_FaithfulColumnsFor(t *testing.T) {
	byName := map[string]*family{}
	for _, f := range registry() {
		byName[f.name] = f
	}
	gc := &genCase{
		dir: direction{enginePG, engineMySQL},
		columns: []genColumn{
			{name: "a", fam: byName["int32"], shp: shapeScalar},
			{name: "b", fam: byName["numeric_unconstrained"], shp: shapeScalar},
			{name: "c", fam: byName["text"], shp: shapeScalar},
			{name: "d", fam: byName["timetz"], shp: shape1DArray},
		},
	}
	// Cross-engine: NOTHING is text-compared (every family is
	// lossy-documented in Phase 1 — the #16-safe stance).
	if got := faithfulColumnsFor(gc); len(got) != 0 {
		t.Errorf("PG→MySQL faithfulColumnsFor = %v; want empty (cross-engine lossy-documented)", got)
	}

	// Same-engine PG→PG: the non-refuse columns ARE compared; timetz[]
	// is a loud-refuse and must be excluded from the compared set.
	gc.dir = direction{enginePG, enginePG}
	got := faithfulColumnsFor(gc)
	want := map[string]bool{"a": true, "b": true, "c": true}
	if len(got) != len(want) {
		t.Fatalf("PG→PG faithfulColumnsFor = %v; want %v (timetz[] excluded as loud-refuse)", got, want)
	}
	for _, c := range got {
		if c == "d" {
			t.Error("timetz[] PG→PG is loud-refuse; must not be a compared column")
		}
		if !want[c] {
			t.Errorf("unexpected compared column %q", c)
		}
	}
}

// TestRegistry_SQLiteSourceScope pins the SQLite-SOURCE family set: the
// storage-class core (int64/float8/bool/text/blob + the ADR-0129
// declared date/time/timestamp), scalar-only (SQLite has no arrays),
// and NOTHING else — every excluded family is a documented asymmetry
// (see the registry() doc), so a family silently gaining or losing an
// sqType is a review event, not drift.
func TestRegistry_SQLiteSourceScope(t *testing.T) {
	want := map[string]bool{
		"int64": true, "float8": true, "bool": true, "text": true,
		"blob": true, "date": true, "time": true, "timestamp": true,
	}
	for _, f := range registry() {
		if want[f.name] {
			if !f.canSource(engineSQLite, shapeScalar) {
				t.Errorf("family %q must be a SQLite source (scalar)", f.name)
			}
			if f.canSource(engineSQLite, shape1DArray) || f.canSource(engineSQLite, shapeMultiDim) {
				t.Errorf("family %q: SQLite has no array type — scalar only", f.name)
			}
			continue
		}
		if f.sqType != "" || f.canSource(engineSQLite, shapeScalar) {
			t.Errorf("family %q must NOT be a SQLite source (documented exclusion — see registry())", f.name)
		}
	}
}

// TestRegistry_SQLiteExpectations pins expectedOutcome over the SQLite
// directions — the truth table the whole oracle keys on, sourced from
// ADR-0134 §1 (the writer's emit-refusal list) and the Phase-1
// cross-engine scope note.
func TestRegistry_SQLiteExpectations(t *testing.T) {
	byName := map[string]*family{}
	for _, f := range registry() {
		byName[f.name] = f
	}
	sqToPG := direction{engineSQLite, enginePG}
	sqToMy := direction{engineSQLite, engineMySQL}
	pgToSQ := direction{enginePG, engineSQLite}
	myToSQ := direction{engineMySQL, engineSQLite}

	// SQLite as a SOURCE is always cross-engine here → lossy-documented
	// for every family (never text-compared, never a refusal).
	for _, fam := range []string{"int64", "float8", "bool", "text", "blob", "date", "time", "timestamp"} {
		for _, d := range []direction{sqToPG, sqToMy} {
			if got := expectedOutcome(byName[fam], d, shapeScalar); got != outcomeLossyDocument {
				t.Errorf("%s %s scalar: got %s; want lossy-documented (cross-engine scope)", fam, d, got)
			}
		}
	}

	// SQLite as a TARGET: the ADR-0134 emit-refusal classes are LOUD.
	for _, fam := range []string{"bit8", "varbit", "inet", "cidr", "macaddr"} {
		f := byName[fam]
		d := pgToSQ
		if f.pgType == "" {
			d = myToSQ
		}
		if got := expectedOutcome(f, d, shapeScalar); got != outcomeLoudRefuse {
			t.Errorf("%s %s scalar: got %s; want loud-refuse (ADR-0134 §1)", fam, d, got)
		}
	}
	if got := expectedOutcome(byName["bit8"], myToSQ, shapeScalar); got != outcomeLoudRefuse {
		t.Errorf("bit8 mysql->sqlite: got %s; want loud-refuse", got)
	}
	// EVERY array shape into SQLite refuses (ir.Array is on the emit list)
	// — including families that are faithful/lossy at scalar.
	for _, tc := range []struct {
		fam string
		s   shape
	}{
		{"int8", shape1DArray},
		{"text", shapeMultiDim},
		{"uuid", shape1DArray},
		{"timetz", shapeMultiDim},
	} {
		if got := expectedOutcome(byName[tc.fam], pgToSQ, tc.s); got != outcomeLoudRefuse {
			t.Errorf("%s[] pg->sqlite %s: got %s; want loud-refuse (ir.Array, ADR-0134 §1)", tc.fam, tc.s, got)
		}
	}
	// Non-refused scalars into SQLite are lossy-documented — the trap
	// case is timetz, whose OWN closure falls through to faithful for a
	// non-PG/MySQL target; expectedOutcome must shield it (the value is
	// carried verbatim as TIME text — the ADR-0134 tz edge, a documented
	// transformation, never text-compared).
	for _, fam := range []string{"timetz", "text", "int64", "json", "numeric_unconstrained", "varchar_wide", "uint64", "enum"} {
		f := byName[fam]
		d := pgToSQ
		if f.pgType == "" {
			d = myToSQ
		}
		if got := expectedOutcome(f, d, shapeScalar); got != outcomeLossyDocument {
			t.Errorf("%s %s scalar: got %s; want lossy-documented", fam, d, got)
		}
	}

	// And the PG/MySQL matrix is untouched by the central lookup: it
	// delegates to the per-family closures.
	if got := expectedOutcome(byName["int32"], direction{enginePG, enginePG}, shapeScalar); got != outcomeFaithful {
		t.Errorf("int32 pg->pg through expectedOutcome: got %s; want faithful", got)
	}
	if got := expectedOutcome(byName["timetz"], direction{enginePG, enginePG}, shape1DArray); got != outcomeLoudRefuse {
		t.Errorf("timetz[] pg->pg through expectedOutcome: got %s; want loud-refuse", got)
	}
}

// TestGenerator_SQLiteDialect pins the emitted source scripts for the
// SQLite directions: SQLite sources speak SQLite (no PG casts/arrays, no
// MySQL ENGINE clause, blobs as x'..', bools as 0/1), and every
// generated column is scalar.
func TestGenerator_SQLiteDialect(t *testing.T) {
	for idx := 0; idx < 40; idx++ {
		for _, d := range []direction{{engineSQLite, enginePG}, {engineSQLite, engineMySQL}} {
			gc := generateCase(424242, idx, d)
			if len(gc.columns) == 0 {
				t.Fatalf("[%s case %d] zero columns generated (fallback broken for the narrower SQLite registry)", d, idx)
			}
			for _, c := range gc.columns {
				if c.shp != shapeScalar {
					t.Errorf("[%s case %d] non-scalar SQLite source column %s", d, idx, c.name)
				}
				for ri, v := range c.values {
					switch c.fam.name {
					case "blob":
						if v != "NULL" && !strings.HasPrefix(v, "x'") {
							t.Errorf("[%s case %d] blob row %d not a SQLite x'..' literal: %q", d, idx, ri, v)
						}
					case "bool":
						if v != "NULL" && v != "0" && v != "1" {
							t.Errorf("[%s case %d] bool row %d not 0/1 (ADR-0129 INTEGER bool): %q", d, idx, ri, v)
						}
					}
				}
			}
			if strings.Contains(gc.ddl, "::") || strings.Contains(gc.ddl, "ARRAY[") ||
				strings.Contains(gc.ddl, "ENGINE=") {
				t.Errorf("[%s case %d] SQLite source script contains foreign dialect:\n%s", d, idx, gc.ddl)
			}
		}
	}
}

// TestGenerator_SQLiteTargetBias pins the X→sqlite down-weighting: the
// documented loud-refuse classes (arrays, bit/net families — ADR-0134)
// must still be GENERATED (the refusal stays pinned), but must not
// dominate — a healthy share of cases must expect SUCCESS, or the SQLite
// write path is never value-fuzzed.
func TestGenerator_SQLiteTargetBias(t *testing.T) {
	for _, d := range []direction{{enginePG, engineSQLite}, {engineMySQL, engineSQLite}} {
		refuse, success := 0, 0
		for idx := 0; idx < 200; idx++ {
			gc := generateCase(31337, idx, d)
			if expectationFor(&gc).loudRefuse {
				refuse++
			} else {
				success++
			}
		}
		if refuse == 0 {
			t.Errorf("[%s] no loud-refuse case in 200 — the ADR-0134 refusal class is unpinned", d)
		}
		if success < 50 {
			t.Errorf("[%s] only %d/200 success cases — refusals dominate and the SQLite write path is barely value-fuzzed", d, success)
		}
	}
}

type boomErr struct{}

func (boomErr) Error() string { return "simulated loud refusal" }

var errBoom error = boomErr{}
