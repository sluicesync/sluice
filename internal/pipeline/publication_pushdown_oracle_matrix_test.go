// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ADR-0176 §4 oracle matrix DATA, deliberately in an UNTAGGED test
// file (audit 2026-07-23 TEST-2 / G-12): the real-PG runner that
// executes the cells lives behind the `integration` build tag
// (publication_pushdown_oracle_integration_test.go), but the matrix
// itself must be visible to the unit-level coupling gates below — while
// it lived in the tagged file, the envelope pin and the matrix literally
// could not cross-reference, so nothing mechanical forced an oracle
// extension when the envelope widened. Now the coupling runs on every
// bare `go test`:
//
//   - TestPushdownOracleMatrix_EnvelopeCoupling — every column type the
//     classifier admits maps to >=1 oracle family, and every oracle
//     family's type is still admitted (no stale families);
//   - TestPushdownOracleMatrix_OpCompleteness — every leaf operator the
//     rowpredicate grammar compiles (rowpredicate.GrammarLeafOps) appears
//     in >=1 cell, every cell predicate compiles under its family's
//     ColumnInfo, and the AND/OR/NOT composition shapes are present.

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
)

// oracleCell is one matrix cell: a predicate plus the literals that define
// in-scope (in0/in1) and out-of-scope (out0/out1) values for it. Literals
// are SQL fragments ("NULL" allowed) interpolated into the fixed workload
// script; in0 doubles as the sentinel value, so it must genuinely satisfy
// the predicate.
type oracleCell struct {
	name string
	pred string
	in0  string
	in1  string
	out0 string
	out1 string
}

// oracleFamily is one value family: the column type of `v` plus its cells.
// irType is the ir.Type this family's cells certify for the push-down
// envelope — the coupling key TestPushdownOracleMatrix_EnvelopeCoupling
// matches against pgPushdownEligibleColumn's admissions, so the envelope
// and the oracle can only move together (G-12).
type oracleFamily struct {
	name    string
	colType string
	irType  ir.Type
	cells   []oracleCell
}

// oracleMatrix is the ADR-0176 §4 family matrix. The value-family axis
// mirrors the classifier envelope EXACTLY (int, numeric, bool, text
// default-collation, text COLLATE "C", date, timestamp-naive); the
// predicate-shape axis covers =, !=/<>, all four ordering ops (full set on
// int, one representative elsewhere), IN / NOT IN, IS NULL / IS NOT NULL,
// and AND/OR/NOT compositions incl. the NOT(… OR …) three-valued case.
// Every workload carries NULL rows (the fixed script's id=5 arc), so
// NULL-in-every-position rides every cell.
var oracleMatrix = []oracleFamily{
	{
		name: "int", colType: "int", irType: ir.Integer{},
		cells: []oracleCell{
			{name: "eq", pred: "v = 5", in0: "5", in1: "5", out0: "6", out1: "4"},
			{name: "ne", pred: "v != 5", in0: "6", in1: "4", out0: "5", out1: "5"},
			{name: "lt", pred: "v < 5", in0: "4", in1: "-3", out0: "5", out1: "6"},
			{name: "le", pred: "v <= 5", in0: "5", in1: "4", out0: "6", out1: "100"},
			{name: "gt", pred: "v > 5", in0: "6", in1: "100", out0: "5", out1: "4"},
			{name: "ge", pred: "v >= 5", in0: "5", in1: "6", out0: "4", out1: "-3"},
			{name: "in", pred: "v IN (5, 7)", in0: "5", in1: "7", out0: "6", out1: "8"},
			{name: "notin", pred: "v NOT IN (5, 7)", in0: "6", in1: "8", out0: "5", out1: "7"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "5", out1: "6"},
			{name: "isnotnull", pred: "v IS NOT NULL", in0: "5", in1: "6", out0: "NULL", out1: "NULL"},
			{name: "and", pred: "v >= 5 AND v < 10", in0: "5", in1: "9", out0: "10", out1: "4"},
			{name: "or", pred: "v = 5 OR v = 7", in0: "5", in1: "7", out0: "6", out1: "8"},
			{name: "not_or_3vl", pred: "NOT (v = 5 OR v > 10)", in0: "6", in1: "10", out0: "5", out1: "11"},
		},
	},
	{
		name: "numeric", colType: "numeric(12,4)", irType: ir.Decimal{},
		cells: []oracleCell{
			// 10.5000 == 10.5 numerically: scale must not break equality on
			// either side.
			{name: "eq", pred: "v = 10.5", in0: "10.5", in1: "10.5000", out0: "10.5001", out1: "10"},
			{name: "ne", pred: "v <> 10.5", in0: "10.5001", in1: "10", out0: "10.5", out1: "10.5000"},
			{name: "gt", pred: "v > 10.5", in0: "10.5001", in1: "99999999.9999", out0: "10.5", out1: "0.0001"},
			{name: "in", pred: "v IN (10.5, 20.25)", in0: "10.5", in1: "20.25", out0: "10.4999", out1: "20"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "10.5", out1: "0"},
			{name: "not_or_3vl", pred: "NOT (v = 10.5 OR v > 100)", in0: "99.9999", in1: "0", out0: "10.5", out1: "100.0001"},
		},
	},
	{
		// Unconstrained NUMERIC with the non-finite specials (review F2
		// belt): PG's NUMERIC stores NaN and (PG 14+) ±Infinity — NaN above
		// everything, and ±Infinity only in an UNCONSTRAINED numeric column
		// (numeric(p,s) refuses it with "numeric field overflow"; observed
		// live) — and numeric is INSIDE the envelope, so the server
		// evaluates the pushed filter on those rows and the client belt
		// must agree. One ordering + one negated-equality cell put
		// NaN/±Infinity through the pushed prqual, real decode, and the
		// client belt. (The server-as-oracle gate for the evaluator class
		// is TestWhereCDC_PGNumericNonFinite — a client-evaluator bug drops
		// on BOTH of this oracle's legs identically, so these cells pin the
		// pushed path's delivery, not the evaluator.)
		name: "numeric_nonfinite", colType: "numeric", irType: ir.Decimal{},
		cells: []oracleCell{
			{name: "gt_nonfinite", pred: "v > 10.5", in0: "'NaN'::numeric", in1: "'Infinity'::numeric", out0: "'-Infinity'::numeric", out1: "0"},
			{name: "ne_nonfinite", pred: "v != 10.5", in0: "'NaN'::numeric", in1: "'-Infinity'::numeric", out0: "10.5", out1: "10.5000"},
		},
	},
	{
		name: "bool", colType: "boolean", irType: ir.Boolean{},
		cells: []oracleCell{
			{name: "eq", pred: "v = TRUE", in0: "TRUE", in1: "TRUE", out0: "FALSE", out1: "FALSE"},
			{name: "ne", pred: "v != FALSE", in0: "TRUE", in1: "TRUE", out0: "FALSE", out1: "FALSE"},
			// IN / NOT IN on bool: the grammar compiles it and the classifier
			// admits it, so it needs cells (audit 2026-07-23 TEST-4).
			{name: "in", pred: "v IN (TRUE)", in0: "TRUE", in1: "TRUE", out0: "FALSE", out1: "FALSE"},
			{name: "notin", pred: "v NOT IN (TRUE)", in0: "FALSE", in1: "FALSE", out0: "TRUE", out1: "TRUE"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "TRUE", out1: "FALSE"},
			{name: "isnotnull", pred: "v IS NOT NULL", in0: "TRUE", in1: "FALSE", out0: "NULL", out1: "NULL"},
		},
	},
	{
		name: "text", colType: "text", irType: ir.Text{},
		cells: []oracleCell{
			// Case variant + shared prefix out-values: byte-exact equality on
			// both sides, no case folding, no prefix confusion.
			{name: "eq", pred: "v = 'alpha'", in0: "'alpha'", in1: "'alpha'", out0: "'Alpha'", out1: "'alphax'"},
			// Trailing space: text is NO-PAD on both sides ('alpha ' ≠ 'alpha').
			{name: "eq_trailing_space", pred: "v = 'alpha '", in0: "'alpha '", in1: "'alpha '", out0: "'alpha'", out1: "'alpha  '"},
			{name: "ne", pred: "v != 'alpha'", in0: "'Alpha'", in1: "'beta'", out0: "'alpha'", out1: "'alpha'"},
			// Embedded quote in an IN member: the doubled-quote escape must
			// survive the single-sourced rendering into the publication DDL.
			{name: "in_quote", pred: "v IN ('alpha', 'ga''mma')", in0: "'alpha'", in1: "'ga''mma'", out0: "'gamma'", out1: "'beta'"},
			// Empty string is in-scope and distinct from NULL.
			{name: "notin", pred: "v NOT IN ('alpha', 'beta')", in0: "'x'", in1: "''", out0: "'alpha'", out1: "'beta'"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "''", out1: "'alpha'"},
			{name: "not_or_3vl", pred: "NOT (v = 'alpha' OR v = 'zeta')", in0: "'beta'", in1: "''", out0: "'alpha'", out1: "'zeta'"},
		},
	},
	{
		name: "text_collate_c", colType: `text COLLATE "C"`, irType: ir.Text{Collation: "C"},
		cells: []oracleCell{
			{name: "eq", pred: "v = 'alpha'", in0: "'alpha'", in1: "'alpha'", out0: "'Alpha'", out1: "'alphax'"},
			{name: "ne", pred: "v != 'alpha'", in0: "'Alpha'", in1: "'beta'", out0: "'alpha'", out1: "'alpha'"},
			{name: "in", pred: "v IN ('alpha', 'beta')", in0: "'alpha'", in1: "'beta'", out0: "'ALPHA'", out1: "'gamma'"},
		},
	},
	{
		name: "date", colType: "date", irType: ir.Date{},
		cells: []oracleCell{
			{name: "eq", pred: "v = '2026-01-15'", in0: "'2026-01-15'", in1: "'2026-01-15'", out0: "'2026-01-14'", out1: "'2026-01-16'"},
			// !=, NOT IN, and the 3VL negation — the shapes where a
			// server-stricter divergence would surface (audit 2026-07-23
			// TEST-4).
			{name: "ne", pred: "v != '2026-01-15'", in0: "'2026-01-16'", in1: "'1999-12-31'", out0: "'2026-01-15'", out1: "'2026-01-15'"},
			{name: "ge", pred: "v >= '2026-01-15'", in0: "'2026-01-15'", in1: "'2027-03-01'", out0: "'2026-01-14'", out1: "'1999-12-31'"},
			{name: "in", pred: "v IN ('2026-01-15', '2026-02-01')", in0: "'2026-01-15'", in1: "'2026-02-01'", out0: "'2026-01-16'", out1: "'2026-02-02'"},
			{name: "notin", pred: "v NOT IN ('2026-01-15', '2026-02-01')", in0: "'2026-01-16'", in1: "'2026-02-02'", out0: "'2026-01-15'", out1: "'2026-02-01'"},
			{name: "not_or_3vl", pred: "NOT (v = '2026-01-15' OR v > '2026-06-01')", in0: "'2026-01-16'", in1: "'2026-06-01'", out0: "'2026-01-15'", out1: "'2026-06-02'"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "'2026-01-15'", out1: "'2026-01-16'"},
			// TIME-BEARING literals on a DATE column (audit 2026-07-23 D0-5,
			// Q1 re-admission): PG casts the literal to date — time-of-day
			// TRUNCATED — inside both the pushed prqual and the snapshot
			// SELECT, and Compile normalizes the client literal identically,
			// so these are equivalence cells now. The in/out values are the
			// TRUNCATED semantics ('v < ... 12:00:00' ≡ 'v < 2026-01-15').
			{name: "eq_time_bearing", pred: "v = '2026-01-15 08:30:00'", in0: "'2026-01-15'", in1: "'2026-01-15'", out0: "'2026-01-14'", out1: "'2026-01-16'"},
			{name: "lt_time_bearing", pred: "v < '2026-01-15 12:00:00'", in0: "'2026-01-14'", in1: "'1999-12-31'", out0: "'2026-01-15'", out1: "'2026-01-16'"},
			{name: "ne_time_bearing", pred: "v != '2026-01-15 12:00:00'", in0: "'2026-01-16'", in1: "'1999-12-31'", out0: "'2026-01-15'", out1: "'2026-01-15'"},
			{name: "in_time_bearing", pred: "v IN ('2026-01-15 08:30', '2026-02-01')", in0: "'2026-01-15'", in1: "'2026-02-01'", out0: "'2026-01-16'", out1: "'2026-02-02'"},
		},
	},
	{
		name: "timestamp", colType: "timestamp", irType: ir.DateTime{},
		cells: []oracleCell{
			{name: "eq", pred: "v = '2026-01-15 08:30:00'", in0: "'2026-01-15 08:30:00'", in1: "'2026-01-15 08:30:00'", out0: "'2026-01-15 08:30:01'", out1: "'2026-01-15 08:29:59.999999'"},
			{name: "lt", pred: "v < '2026-01-15 08:30:00'", in0: "'2026-01-15 08:29:59.999999'", in1: "'1999-01-01 00:00:00'", out0: "'2026-01-15 08:30:00'", out1: "'2026-01-15 08:30:00.000001'"},
			{name: "ne", pred: "v != '2026-01-15 08:30:00'", in0: "'2026-01-15 08:30:00.000001'", in1: "'2020-05-05 05:05:05'", out0: "'2026-01-15 08:30:00'", out1: "'2026-01-15 08:30:00'"},
			// IN / NOT IN with µs-boundary members. Audit 2026-07-23 TEST-4.
			{name: "in", pred: "v IN ('2026-01-15 08:30:00', '2026-02-01 00:00:00.000001')", in0: "'2026-01-15 08:30:00'", in1: "'2026-02-01 00:00:00.000001'", out0: "'2026-01-15 08:30:01'", out1: "'2026-02-01 00:00:00'"},
			{name: "notin", pred: "v NOT IN ('2026-01-15 08:30:00', '2026-02-01 00:00:00')", in0: "'2026-01-15 08:30:01'", in1: "'2020-01-01 00:00:00'", out0: "'2026-01-15 08:30:00'", out1: "'2026-02-01 00:00:00'"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "'2026-01-15 08:30:00'", out1: "'2026-01-16 00:00:00'"},
			// SUB-MICROSECOND literals (audit 2026-07-23 D0-5, Q1
			// re-admission): PG rounds >6 fractional digits to µs by its
			// DOUBLE-MEDIATED rule (rint(strtod·10⁶) — review F1) in both
			// server-side legs, and Compile normalizes the client literal
			// with the same rule — equivalence cells. The .1234565 half
			// pins the rounding mode end to end (half-up would put in0/out0
			// on the wrong sides); the .0001255/.0001265 pair pins that the
			// rule is DOUBLE-mediated (exact decimal half-even gives
			// .000126 for BOTH — the review-F1 divergence, RED on the
			// exact-decimal implementation); .9999995 pins the carry.
			{name: "eq_7digit", pred: "v = '2026-01-15 08:30:00.1234567'", in0: "'2026-01-15 08:30:00.123457'", in1: "'2026-01-15 08:30:00.123457'", out0: "'2026-01-15 08:30:00.123456'", out1: "'2026-01-15 08:30:00.123458'"},
			{name: "eq_half_boundary", pred: "v = '2026-01-15 08:30:00.1234565'", in0: "'2026-01-15 08:30:00.123456'", in1: "'2026-01-15 08:30:00.123456'", out0: "'2026-01-15 08:30:00.123457'", out1: "'2026-01-15 08:30:00.123455'"},
			{name: "eq_dblmediated_down", pred: "v = '2026-01-15 08:30:00.0001255'", in0: "'2026-01-15 08:30:00.000125'", in1: "'2026-01-15 08:30:00.000125'", out0: "'2026-01-15 08:30:00.000126'", out1: "'2026-01-15 08:30:00.000124'"},
			{name: "eq_dblmediated_up", pred: "v = '2026-01-15 08:30:00.0001265'", in0: "'2026-01-15 08:30:00.000127'", in1: "'2026-01-15 08:30:00.000127'", out0: "'2026-01-15 08:30:00.000126'", out1: "'2026-01-15 08:30:00.000128'"},
			{name: "eq_7digit_carry", pred: "v = '2026-01-15 08:30:00.9999995'", in0: "'2026-01-15 08:30:01'", in1: "'2026-01-15 08:30:01'", out0: "'2026-01-15 08:30:00.999999'", out1: "'2026-01-15 08:30:01.000001'"},
		},
	},
}

// pushdownOracleFamilyKey maps a column type to its oracle-family axis
// key, "" when the type has no family (i.e. must not be eligible). The
// key granularity is EXACTLY the envelope's decision granularity: type
// family, plus the collation split on text and the tz split on
// timestamps.
func pushdownOracleFamilyKey(t ir.Type) string {
	textKey := func(collation string) string {
		switch collation {
		case "":
			return "text-default"
		case "C":
			return "text-c"
		}
		return ""
	}
	switch tt := t.(type) {
	case ir.Integer:
		return "integer"
	case ir.Decimal:
		return "numeric"
	case ir.Boolean:
		return "bool"
	case ir.Date:
		return "date"
	case ir.DateTime:
		return "timestamp-naive"
	case ir.Timestamp:
		if tt.WithTimeZone {
			return ""
		}
		return "timestamp-naive"
	case ir.Varchar:
		return textKey(tt.Collation)
	case ir.Text:
		return textKey(tt.Collation)
	}
	return ""
}

// pushdownEnvelopeProbeTypes is the type universe the coupling gate
// classifies: every ir type (zero value, via the ir.AllTypes registry)
// plus the envelope's variant axes on top (collation on text/varchar;
// the naive/tz split rides Timestamp's zero value vs the envelope pin's
// explicit tz case).
func pushdownEnvelopeProbeTypes() []ir.Type {
	return append(
		ir.AllTypes(),
		ir.Varchar{Collation: "C"},
		ir.Text{Collation: "C"},
	)
}

// TestPushdownOracleMatrix_EnvelopeCoupling is the unit-level half of the
// G-12 coupling (the integration-tagged oracle runner is the other): every
// type the classifier ADMITS must be certified by >=1 oracle family, and
// every oracle family's type must still be admitted — widening the envelope
// without oracle cells, or excluding a family while its cells still claim
// to certify it, fails here on every bare `go test`.
func TestPushdownOracleMatrix_EnvelopeCoupling(t *testing.T) {
	certified := map[string]bool{}
	for _, fam := range oracleMatrix {
		key := pushdownOracleFamilyKey(fam.irType)
		if key == "" {
			t.Errorf("oracle family %q certifies irType %T, which maps to no envelope family key — the family is stale or the key fn lags", fam.name, fam.irType)
			continue
		}
		if ok, reason := pgPushdownEligibleColumn(&ir.Column{Name: "v", Type: fam.irType}); !ok {
			t.Errorf("oracle family %q certifies irType %#v, which the classifier no longer admits (%s) — drop or re-key the family together with the envelope pin", fam.name, fam.irType, reason)
		}
		certified[key] = true
	}

	for _, typ := range pushdownEnvelopeProbeTypes() {
		ok, _ := pgPushdownEligibleColumn(&ir.Column{Name: "v", Type: typ})
		if !ok {
			continue
		}
		key := pushdownOracleFamilyKey(typ)
		if key == "" {
			t.Errorf("classifier admits %#v but pushdownOracleFamilyKey has no key for it — extend the key fn together with the oracle", typ)
			continue
		}
		if !certified[key] {
			t.Errorf("classifier admits %#v (family key %q) but NO oracle family certifies it — widening the envelope requires real-PG oracle cells in the same change (ADR-0176; audit 2026-07-23 G-12)", typ, key)
		}
	}

	// Non-vacuity: the matrix must keep its full family axis.
	if len(oracleMatrix) < 8 {
		t.Errorf("oracleMatrix has %d families; want >= 8 — a shrunk matrix means lost coverage, not a passing gate", len(oracleMatrix))
	}
}

// TestPushdownOracleMatrix_OpCompleteness compiles every oracle cell's
// predicate through the REAL rowpredicate parser (with the PG engine's
// temporal lens, like production) and asserts (a) every cell compiles, (b)
// the union of leaf operators across the matrix covers the grammar's full
// inventory (rowpredicate.GrammarLeafOps — a new grammar operator cannot
// ship without an oracle cell exercising it on real PG), and (c) the
// AND/OR/NOT composition shapes are present. Audit 2026-07-23 TEST-2 / G-12.
func TestPushdownOracleMatrix_OpCompleteness(t *testing.T) {
	infoFor := func(familyName string) rowpredicate.ColumnInfo {
		switch familyName {
		case "int", "numeric", "numeric_nonfinite":
			return rowpredicate.ColumnInfo{Family: rowpredicate.FamilyNumeric}
		case "bool":
			return rowpredicate.ColumnInfo{Family: rowpredicate.FamilyBool}
		case "text", "text_collate_c":
			return rowpredicate.ColumnInfo{Family: rowpredicate.FamilyString, Faithful: true}
		case "date":
			return rowpredicate.ColumnInfo{Family: rowpredicate.FamilyTemporal, TemporalSemantics: ir.TemporalLiteralCastToColumn, TemporalDateOnly: true}
		case "timestamp":
			return rowpredicate.ColumnInfo{Family: rowpredicate.FamilyTemporal, TemporalSemantics: ir.TemporalLiteralCastToColumn}
		}
		t.Fatalf("oracle family %q has no ColumnInfo mapping — extend infoFor", familyName)
		return rowpredicate.ColumnInfo{}
	}

	seenOps := map[string]bool{}
	var sawAnd, sawOr, sawNot bool
	cellCount := 0
	for _, fam := range oracleMatrix {
		infos := map[string]rowpredicate.ColumnInfo{"v": infoFor(fam.name)}
		for _, cell := range fam.cells {
			cellCount++
			p, err := rowpredicate.Compile("t", cell.pred, infos)
			if err != nil {
				t.Errorf("family %s cell %s: predicate %q does not compile: %v", fam.name, cell.name, cell.pred, err)
				continue
			}
			for _, term := range p.PushdownTerms() {
				if term.Unrecognized != "" {
					t.Errorf("family %s cell %s: unrecognized term %q", fam.name, cell.name, term.Unrecognized)
					continue
				}
				seenOps[term.Op] = true
			}
			sawAnd = sawAnd || strings.Contains(cell.pred, " AND ")
			sawOr = sawOr || strings.Contains(cell.pred, " OR ")
			sawNot = sawNot || strings.Contains(cell.pred, "NOT (")
		}
	}

	inventory := rowpredicate.GrammarLeafOps()
	for _, op := range inventory {
		if !seenOps[op] {
			t.Errorf("grammar leaf operator %q appears in NO oracle cell — add a real-PG cell for it (audit 2026-07-23 G-12: the oracle's op axis must cover what the grammar compiles)", op)
		}
	}
	known := map[string]bool{}
	for _, op := range inventory {
		known[op] = true
	}
	for op := range seenOps {
		if !known[op] {
			t.Errorf("oracle cells emit operator %q, which GrammarLeafOps does not list — the inventory rotted", op)
		}
	}
	if !sawAnd || !sawOr || !sawNot {
		t.Errorf("composition coverage: AND=%v OR=%v NOT=%v — the matrix must keep all three composition shapes", sawAnd, sawOr, sawNot)
	}
	// Non-vacuity: the per-cell loop must actually have walked the matrix.
	if cellCount < 40 {
		t.Errorf("oracle matrix has %d cells; want >= 40 — a shrunk matrix means lost coverage", cellCount)
	}
}
