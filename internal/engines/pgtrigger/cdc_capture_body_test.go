// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the capture-shape door's BODY arm (audit 2026-08-31 SL-5).
//
// The matrix is the CLASS, not a representative: every capture function
// pgtrigger installs — row (× all three ADR-0068 payload modes), truncate,
// the ddl_command_end arm and the sql_drop arm — × every drift shape the
// door decides on: identical, body edited, GUC pin missing, SECURITY
// DEFINER dropped, and the capture-defeat body. That matters here for the
// same reason it mattered for Bug 74: the four functions do not share a
// render, the three payload modes differ inside the row body, and the GUC
// pins are carried by only some of them — so a green test on the row
// function says nothing about the truncate one.
package pgtrigger

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

const testSchema = "public"

func testChangeLogRef() string {
	return quoteIdent(testSchema) + "." + quoteIdent(ChangeLogTable)
}

func testMetaRef() string {
	return quoteIdent(testSchema) + "." + quoteIdent(ChangeLogMetaTable)
}

// renderedCaptureFunction is one family × variant cell of the pin matrix.
type renderedCaptureFunction struct {
	name string
	stmt string
}

// renderedCaptureFunctions is every capture-function definition this binary
// can install, keyed by a label naming the family × variant cell.
func renderedCaptureFunctions() map[string]renderedCaptureFunction {
	type cell = renderedCaptureFunction
	return map[string]cell{
		"row/full":    {CaptureFunctionRow, renderCaptureRowFunction(testSchema, testChangeLogRef(), CapturePayloadFull)},
		"row/changed": {CaptureFunctionRow, renderCaptureRowFunction(testSchema, testChangeLogRef(), CapturePayloadChanged)},
		"row/minimal": {CaptureFunctionRow, renderCaptureRowFunction(testSchema, testChangeLogRef(), CapturePayloadMinimal)},
		"truncate":    {CaptureFunctionTruncate, renderCaptureTruncateFunction(testSchema, testChangeLogRef())},
		"ddl":         {CaptureFunctionDDL, renderCaptureDDLFunction(testSchema, testChangeLogRef(), testMetaRef())},
		"drop":        {CaptureFunctionDrop, renderCaptureDropFunction(testSchema, testChangeLogRef(), testMetaRef())},
	}
}

// The extractor's anti-vacuity floor: every render must parse, carry a
// body that records, and carry the SECURITY DEFINER + search_path pin the
// SEC-1 door also grades. A renderer whose shape drifts out of the
// extractor's reach would otherwise silently make the whole body arm
// unable to build an expectation.
func TestCaptureFunctionShape_ParsesEveryRender(t *testing.T) {
	t.Parallel()
	for label, cell := range renderedCaptureFunctions() {
		shape, ok := captureFunctionShapeOfRender(cell.name, cell.stmt)
		if !ok {
			t.Fatalf("%s: the rendered CREATE OR REPLACE does not parse; the body arm cannot build an expectation for it", label)
		}
		if !shape.definer {
			t.Errorf("%s: parsed as SECURITY INVOKER", label)
		}
		if !shape.recordsIntoChangeLog() {
			t.Errorf("%s: the parsed body does not record into the change log — the capture-defeat predicate would refuse a healthy install", label)
		}
		if len(shape.settings) == 0 {
			t.Errorf("%s: parsed no SET clauses; the proconfig half of the comparison is inert", label)
		}
		for _, want := range []string{"search_path=pg_catalog, pg_temp"} {
			if !containsString(shape.settings, want) {
				t.Errorf("%s: settings %v missing %q", label, shape.settings, want)
			}
		}
		if strings.Contains(shape.body, "$sluice$") {
			t.Errorf("%s: the extracted body still carries its dollar quotes", label)
		}
	}
	// The row function's three payload modes must produce three DIFFERENT
	// bodies: if they collapsed, matching "any mode" would be vacuous.
	seen := map[string]bool{}
	for _, payload := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		shape, _ := captureFunctionShapeOfRender(CaptureFunctionRow, renderCaptureRowFunction(testSchema, testChangeLogRef(), payload))
		seen[shape.digest()] = true
	}
	if len(seen) != 3 {
		t.Errorf("the three capture-payload modes render %d distinct bodies; want 3", len(seen))
	}
}

// installedFrom builds the door's "installed" view out of rendered
// statements — the same three pieces PostgreSQL stores, so the unit pins
// grade the exact comparison the catalog read feeds.
func installedFrom(t *testing.T, cells map[string]string) map[string]captureFunctionShape {
	t.Helper()
	out := map[string]captureFunctionShape{}
	for name, stmt := range cells {
		shape, ok := captureFunctionShapeOfRender(name, stmt)
		if !ok {
			t.Fatalf("%s: test fixture does not parse", name)
		}
		out[name] = shape
	}
	return out
}

func containsString(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

// A healthy install of any payload mode must grade clean and silent, with
// and without recorded provenance.
func TestGradeCaptureFunctionShapes_HealthyInstallIsSilent(t *testing.T) {
	t.Parallel()
	for _, payload := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		rendered := map[string]string{
			CaptureFunctionRow:      renderCaptureRowFunction(testSchema, testChangeLogRef(), payload),
			CaptureFunctionTruncate: renderCaptureTruncateFunction(testSchema, testChangeLogRef()),
			CaptureFunctionDDL:      renderCaptureDDLFunction(testSchema, testChangeLogRef(), testMetaRef()),
			CaptureFunctionDrop:     renderCaptureDropFunction(testSchema, testChangeLogRef(), testMetaRef()),
		}
		installed := installedFrom(t, rendered)
		for _, meta := range []installMeta{
			{}, // a pre-v5 install with no provenance at all
			{captureFnDigest: captureFunctionDigests(rendered), schemaVersion: ChangeLogSchemaVer},
		} {
			drift, err := gradeCaptureFunctionShapes(testSchema, installed, meta)
			if err != nil {
				t.Fatalf("payload=%s meta=%+v: healthy install refused: %v", payload, meta, err)
			}
			if len(drift) != 0 {
				t.Errorf("payload=%s meta=%+v: healthy install reported drift %v", payload, meta, drift)
			}
		}
	}
}

// The class matrix: every function × every drift shape, against both
// provenance states. This is the pin the fix exists for.
func TestGradeCaptureFunctionShapes_EveryFunctionEveryDriftShape(t *testing.T) {
	t.Parallel()
	// Mutators, expressed on the rendered statement so each produces a
	// definition PostgreSQL would really store.
	mutations := []struct {
		name string
		// mutate returns the edited statement, or "" if the shape does not
		// apply to this function (e.g. dropping a GUC pin it never had).
		mutate       func(stmt string) string
		wantCategory string // "refuse-defeat" | "provenance"
	}{
		{
			name:         "body gutted to a no-op (the attack)",
			mutate:       gutBody,
			wantCategory: "refuse-defeat",
		},
		{
			name: "a line inserted into the body (a partial edit that still records)",
			mutate: func(stmt string) string {
				return strings.Replace(stmt, "\nBEGIN\n", "\nBEGIN\n    -- edited\n", 1)
			},
			wantCategory: "provenance",
		},
		{
			name: "a GUC pin dropped (the pre-v0.113 bytea_output / Bug 194 vintage)",
			mutate: func(stmt string) string {
				for _, pin := range []string{"SET bytea_output = hex\n", "SET extra_float_digits = 3\n", "SET search_path = pg_catalog, pg_temp\n"} {
					if strings.Contains(stmt, pin) {
						return strings.Replace(stmt, pin, "", 1)
					}
				}
				return ""
			},
			wantCategory: "provenance",
		},
		{
			name: "SECURITY DEFINER dropped",
			mutate: func(stmt string) string {
				return strings.Replace(stmt, "SECURITY DEFINER\n", "", 1)
			},
			wantCategory: "provenance",
		},
	}

	for label, cell := range renderedCaptureFunctions() {
		for _, m := range mutations {
			edited := m.mutate(cell.stmt)
			if edited == "" || edited == cell.stmt {
				t.Fatalf("%s/%s: the mutation did not change the statement — a mutation that did not mutate reads exactly like a gate that missed", label, m.name)
			}
			// Baseline: everything else in the install is healthy, so the
			// verdict is about this function alone.
			rendered := map[string]string{
				CaptureFunctionRow:      renderCaptureRowFunction(testSchema, testChangeLogRef(), CapturePayloadFull),
				CaptureFunctionTruncate: renderCaptureTruncateFunction(testSchema, testChangeLogRef()),
				CaptureFunctionDDL:      renderCaptureDDLFunction(testSchema, testChangeLogRef(), testMetaRef()),
				CaptureFunctionDrop:     renderCaptureDropFunction(testSchema, testChangeLogRef(), testMetaRef()),
			}
			recorded := captureFunctionDigests(rendered)
			rendered[cell.name] = edited
			installed := installedFrom(t, rendered)

			t.Run(label+"/"+m.name+"/no recorded provenance (pre-v5 install)", func(t *testing.T) {
				drift, err := gradeCaptureFunctionShapes(testSchema, installed, installMeta{})
				switch m.wantCategory {
				case "refuse-defeat":
					if err == nil {
						t.Fatalf("a function that records NOTHING passed on a pre-v5 install: %v", drift)
					}
					if !strings.Contains(err.Error(), cell.name) || !strings.Contains(err.Error(), "records NOTHING") {
						t.Errorf("refusal does not name the function and the defect:\n%v", err)
					}
				default:
					if err != nil {
						t.Fatalf("a drift with no provenance must WARN, not refuse — an operator's untouched pre-v5 install would be stranded: %v", err)
					}
					if len(drift) != 1 || drift[0].name != cell.name {
						t.Fatalf("drift = %v; want exactly %s", drift, cell.name)
					}
					if !strings.Contains(drift[0].why, "CANNOT be told apart") {
						t.Errorf("the WARN reason must say the provenance is unknown; got %q", drift[0].why)
					}
				}
			})

			t.Run(label+"/"+m.name+"/provenance recorded (v5 install)", func(t *testing.T) {
				meta := installMeta{captureFnDigest: recorded, schemaVersion: ChangeLogSchemaVer}
				_, err := gradeCaptureFunctionShapes(testSchema, installed, meta)
				if err == nil {
					t.Fatal("a definition that changed AFTER setup recorded it must refuse")
				}
				if !strings.Contains(err.Error(), cell.name) {
					t.Errorf("refusal does not name the function:\n%v", err)
				}
			})
		}
	}
}

// gutBody replaces everything between the dollar quotes with a body that
// records nothing — the exact `CREATE OR REPLACE … RETURN NULL` an
// operator (or an attacker with DDL rights) can install while every trigger
// stays in place and correctly named.
func gutBody(stmt string) string {
	head, rest, ok := strings.Cut(stmt, "\nAS $sluice$\n")
	if !ok {
		return ""
	}
	_ = rest
	return head + "\nAS $sluice$\nBEGIN\n    RETURN NULL;\nEND\n$sluice$;"
}

// The vintage/tamper split is what makes the refusals safe to ship, so it
// gets its own pin: an install whose definitions still match what SETUP
// recorded — but not what this binary renders — is an upgrade, not an
// attack, and must not strand the running sync.
func TestGradeCaptureFunctionShapes_OldButUntamperedWarnsRatherThanRefuses(t *testing.T) {
	t.Parallel()
	older := map[string]string{
		// Stand-in for an older sluice's rendering: the same function
		// without the bytea_output pin (which really did arrive in
		// v0.113.0) — it still records, it is simply not what this binary
		// would install today.
		CaptureFunctionRow: strings.Replace(
			renderCaptureRowFunction(testSchema, testChangeLogRef(), CapturePayloadFull),
			"SET bytea_output = hex\n", "", 1,
		),
		CaptureFunctionTruncate: renderCaptureTruncateFunction(testSchema, testChangeLogRef()),
		CaptureFunctionDDL:      renderCaptureDDLFunction(testSchema, testChangeLogRef(), testMetaRef()),
		CaptureFunctionDrop:     renderCaptureDropFunction(testSchema, testChangeLogRef(), testMetaRef()),
	}
	installed := installedFrom(t, older)
	meta := installMeta{captureFnDigest: captureFunctionDigests(older), schemaVersion: ChangeLogSchemaVer}

	drift, err := gradeCaptureFunctionShapes(testSchema, installed, meta)
	if err != nil {
		t.Fatalf("an install that still matches its own recorded provenance was refused — every binary upgrade would strand its sync: %v", err)
	}
	if len(drift) != 1 || drift[0].name != CaptureFunctionRow {
		t.Fatalf("drift = %v; want exactly %s", drift, CaptureFunctionRow)
	}
	if !strings.Contains(drift[0].why, "DIFFERENT sluice binary") {
		t.Errorf("the WARN must say the install is older, not edited; got %q", drift[0].why)
	}

	// ... and the SAME state with the version regressed by an older
	// binary's setup run must fall back to "cannot tell", never to a
	// refusal: that is the downgrade-then-upgrade path.
	stale := installMeta{captureFnDigest: captureFunctionDigests(older), schemaVersion: captureDigestMinSchemaVer - 1}
	if !stale.captureDigestTrusted() {
		drift, err := gradeCaptureFunctionShapes(testSchema, installed, stale)
		if err != nil {
			t.Fatalf("a regressed schema_version must not make a stale digest refuse: %v", err)
		}
		if len(drift) != 1 || !strings.Contains(drift[0].why, "CANNOT be told apart") {
			t.Errorf("drift = %v; want the provenance-unknown reason", drift)
		}
	} else {
		t.Error("captureDigestTrusted believes a digest recorded under an older schema_version")
	}
}

// TestCaptureDigestTrust_FloorIsFrozenNotTheCurrentSchemaVersion pins the one
// property that makes the refusal arm survive a future meta-table migration:
// the trust floor is the version the digest was INTRODUCED at, not whatever
// ChangeLogSchemaVer happens to be.
//
// Written as `>= ChangeLogSchemaVer`, the next unrelated bump to 6 would make
// every correctly-set-up v5 install's digest untrusted, silently dropping the
// door from REFUSE to WARN for the tamper case — a security regression caused
// by an unrelated change, which is exactly the kind nothing would have caught.
// SHAPE, and why the behavioural cells alone are NOT the gate. The two
// constants are equal today, so `>= ChangeLogSchemaVer` and
// `>= captureDigestMinSchemaVer` are behaviourally identical in every input
// this test can construct — the divergence only appears after a future bump,
// which a test cannot fabricate for a compile-time constant. Mutation-run
// confirmed exactly that: restoring the moving constant PASSED the
// behavioural cells. So the load-bearing arm reads the SOURCE and requires
// captureDigestTrusted to compare against the frozen floor, with the
// behavioural cells kept below as the anti-vacuity half (they catch a floor
// that is nonsense in the other directions).
func TestCaptureDigestTrust_FloorIsFrozenNotTheCurrentSchemaVersion(t *testing.T) {
	if captureDigestMinSchemaVer > ChangeLogSchemaVer {
		t.Fatalf("the digest trust floor (%d) is above the current schema version (%d) — "+
			"no install can ever satisfy it", captureDigestMinSchemaVer, ChangeLogSchemaVer)
	}

	assertCaptureDigestFloorIsFrozenInSource(t)

	// An install recorded exactly AT the floor is trusted, and stays trusted
	// however far ChangeLogSchemaVer later moves.
	for _, ver := range []int{captureDigestMinSchemaVer, captureDigestMinSchemaVer + 1, ChangeLogSchemaVer + 3} {
		m := installMeta{captureFnDigest: "some-digest", schemaVersion: ver}
		if !m.captureDigestTrusted() {
			t.Errorf("schema_version %d: digest not trusted; the floor is %d and a version at or "+
				"above it must stay trusted no matter how far ChangeLogSchemaVer (%d) advances — "+
				"otherwise an unrelated meta-table bump silently turns this door's tamper REFUSAL "+
				"into a WARN", ver, captureDigestMinSchemaVer, ChangeLogSchemaVer)
		}
	}

	// Below the floor is untrusted (the downgrade-then-upgrade path), and an
	// absent digest is untrusted at any version. Without these the test would
	// pass for a captureDigestTrusted that simply returned true.
	if (installMeta{captureFnDigest: "some-digest", schemaVersion: captureDigestMinSchemaVer - 1}).captureDigestTrusted() {
		t.Error("a digest recorded below the floor must not be trusted")
	}
	if (installMeta{captureFnDigest: "", schemaVersion: ChangeLogSchemaVer}).captureDigestTrusted() {
		t.Error("an absent digest must not be trusted at any version")
	}
}

// assertCaptureDigestFloorIsFrozenInSource walks captureDigestTrusted's body
// and requires the version comparison to name the frozen floor rather than
// the moving schema constant. It carries its own anti-vacuity floor: the
// function must be found and must reference the floor, so a rename that made
// the walk match nothing fails instead of passing silently.
func assertCaptureDigestFloorIsFrozenInSource(t *testing.T) {
	t.Helper()
	const fn = "captureDigestTrusted"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "preflight_replica_role.go", nil, 0)
	if err != nil {
		t.Fatalf("parse preflight_replica_role.go: %v", err)
	}

	var body *ast.BlockStmt
	ast.Inspect(file, func(n ast.Node) bool {
		if fd, ok := n.(*ast.FuncDecl); ok && fd.Name.Name == fn {
			body = fd.Body
		}
		return body == nil
	})
	if body == nil {
		t.Fatalf("%s not found in preflight_replica_role.go — if it moved or was renamed, move this "+
			"gate with it rather than deleting it; it is the only thing pinning the frozen floor", fn)
	}

	var sawFloor, sawMovingConst bool
	ast.Inspect(body, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			switch id.Name {
			case "captureDigestMinSchemaVer":
				sawFloor = true
			case "ChangeLogSchemaVer":
				sawMovingConst = true
			}
		}
		return true
	})
	if !sawFloor {
		t.Errorf("%s does not reference captureDigestMinSchemaVer", fn)
	}
	if sawMovingConst {
		t.Errorf("%s compares against ChangeLogSchemaVer. The trust floor is the version the digest "+
			"was INTRODUCED at and never moves; ChangeLogSchemaVer advances on every future "+
			"meta-table migration, and the next bump would make every correctly-set-up install's "+
			"digest untrusted — silently turning this door's tamper REFUSAL into a WARN. Compare "+
			"against captureDigestMinSchemaVer.", fn)
	}
}

// The digest is per FUNCTION: a plan that installs fewer functions than the
// schema carries must leave the others' provenance unknown rather than
// making them all look replaced.
func TestCaptureFunctionDigests_ArePerFunction(t *testing.T) {
	t.Parallel()
	polled := map[string]string{
		CaptureFunctionRow:      renderCaptureRowFunction(testSchema, testChangeLogRef(), CapturePayloadFull),
		CaptureFunctionTruncate: renderCaptureTruncateFunction(testSchema, testChangeLogRef()),
	}
	recorded := parseCaptureFunctionDigests(captureFunctionDigests(polled))
	if len(recorded) != 2 {
		t.Fatalf("recorded %d digests; want 2", len(recorded))
	}
	for _, name := range []string{CaptureFunctionDDL, CaptureFunctionDrop} {
		if _, ok := recorded[name]; ok {
			t.Errorf("%s has provenance recorded by a plan that never installed it", name)
		}
	}
	// An event-tier function left over from an earlier install, at an old
	// vintage, must WARN (unknown provenance) rather than refuse.
	installed := installedFrom(t, map[string]string{
		CaptureFunctionRow:      polled[CaptureFunctionRow],
		CaptureFunctionTruncate: polled[CaptureFunctionTruncate],
		CaptureFunctionDDL: strings.Replace(
			renderCaptureDDLFunction(testSchema, testChangeLogRef(), testMetaRef()),
			"SET search_path = pg_catalog, pg_temp\n", "", 1,
		),
	})
	meta := installMeta{captureFnDigest: captureFunctionDigests(polled), schemaVersion: ChangeLogSchemaVer}
	drift, err := gradeCaptureFunctionShapes(testSchema, installed, meta)
	if err != nil {
		t.Fatalf("a function outside the recorded set was treated as replaced: %v", err)
	}
	if len(drift) != 1 || drift[0].name != CaptureFunctionDDL {
		t.Fatalf("drift = %v; want exactly %s", drift, CaptureFunctionDDL)
	}
	if !strings.Contains(drift[0].why, "outside the set") {
		t.Errorf("the WARN must say WHY the provenance is missing for this one function, not blame the schema version; got %q", drift[0].why)
	}
}

// Body normalization must absorb the transforms a hand-applied plan
// undergoes and NOTHING else — a change inside a literal must still read
// as drift.
func TestNormalizeFunctionBody_AbsorbsOnlyTheHarmlessTransforms(t *testing.T) {
	t.Parallel()
	base := renderCaptureTruncateFunction(testSchema, testChangeLogRef())
	shape, _ := captureFunctionShapeOfRender(CaptureFunctionTruncate, base)

	crlf, _ := captureFunctionShapeOfRender(CaptureFunctionTruncate, strings.ReplaceAll(base, "\n", "\r\n"))
	if !shape.equal(crlf) {
		t.Error("a CRLF-normalized plan (the Windows psql paste) reads as drift")
	}
	trailing, _ := captureFunctionShapeOfRender(CaptureFunctionTruncate, strings.ReplaceAll(base, "\nBEGIN\n", "\nBEGIN   \n"))
	if !shape.equal(trailing) {
		t.Error("trailing whitespace reads as drift")
	}
	// The load-bearing negative: a change inside the recorded op literal
	// ('T' → 'I') is a semantic change and must NOT be normalized away.
	semantic, _ := captureFunctionShapeOfRender(CaptureFunctionTruncate, strings.Replace(base, "         'T',", "         'I',", 1))
	if shape.equal(semantic) {
		t.Error("a changed op literal compares EQUAL — the normalization is too loose")
	}
	// So is a re-indentation, which no paste produces and an editor does.
	indented, _ := captureFunctionShapeOfRender(CaptureFunctionTruncate, strings.Replace(base, "    INSERT INTO ", "        INSERT INTO ", 1))
	if shape.equal(indented) {
		t.Error("leading-whitespace changes are normalized away — the comparison would miss a re-written body")
	}
}
