// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The independent oracle for the generative round-trip fuzz harness
// (Track 2, Phase 1). Design decision #3: a three-outcome classifier.
//
// The oracle reads BOTH databases directly through the per-engine
// canonical text form — exactly the battle-test approach: PG columns
// are projected `col::text` (NULL-element survival and array
// dimensionality become observable in a single string compare, see
// migrate_bug7374_integration_test.go); MySQL columns are read raw
// (the driver returns the canonical rendering, and the Bug 75 fixture
// shows the BIN()/format adornments where needed). The source is
// established by the database itself (raw DDL/DML applied directly),
// never by sluice's writers — so a writer bug cannot mask a mismatch.
//
// Verdict logic (the load-bearing part):
//
//	migrate err == nil  &&  src==dst canonical text  → PASS  (faithful)
//	migrate err == nil  &&  case is lossy-documented  → PASS  (assert
//	                                                     only: migrate
//	                                                     succeeded +
//	                                                     column exists)
//	migrate err != nil  &&  case is a known loud-refuse → PASS
//	migrate err != nil  &&  case is NOT a known refuse  → FAIL
//	                                          (unexpected refusal — the
//	                                           v0.69.0 #16 hazard class)
//	migrate err == nil  &&  src!=dst (faithful expected) → FAIL (silent
//	                                          loss / mismatch / flatten)
//	partial target after a refusal                      → FAIL
//
// No build tag on the pure classification helpers; the DB reads use
// database/sql only and are invoked from the integration driver.

package pipeline

import (
	"database/sql"
	"fmt"
)

// caseExpectation is the reduced, whole-case expectation derived from
// the per-column families: the worst (most permissive) outcome that any
// column in the case justifies, plus the set of columns that are
// faithfully comparable.
type caseExpectation struct {
	// loudRefuse is true iff at least one column's expected outcome for
	// this direction+shape is outcomeLoudRefuse. A single loud-refuse
	// column makes the whole migrate refuse (it aborts before any
	// target rows) — so the whole case is expected to refuse.
	loudRefuse bool

	// faithfulCols are the column names whose expected outcome is
	// outcomeFaithful and which must therefore compare src==dst exactly.
	faithfulCols []string

	// reason explains, for a loud-refuse case, which column+family
	// justifies it (used in the failure/diagnostic message).
	reason string
}

// expectationFor reduces a genCase to its whole-case expectation. This
// is the known-loud-refuse SET, derived entirely from the per-family
// expect() closures (which are themselves sourced from
// docs/type-mapping.md + the catalogued cross-engine limitations — see
// fuzzgen_registry.go). The harness never hard-codes a refuse list
// here; the registry is the single source of truth.
func expectationFor(gc *genCase) caseExpectation {
	var ce caseExpectation
	for _, c := range gc.columns {
		switch c.fam.expect(gc.dir, c.shp) {
		case outcomeLoudRefuse:
			ce.loudRefuse = true
			if ce.reason == "" {
				ce.reason = fmt.Sprintf("%s %s (%s) is a documented loud-refuse for %s",
					c.name, c.fam.name, c.shp, gc.dir)
			}
		case outcomeFaithful:
			ce.faithfulCols = append(ce.faithfulCols, c.name)
		case outcomeLossyDocument:
			// Intentionally not compared for value-equality: asserting
			// equality on a documented degradation is the #16
			// false-positive class. We only require migrate to succeed
			// and (checked by the driver) the column to exist.
		}
	}
	return ce
}

// verdict is the harness's classification of one executed case.
type verdict int

const (
	verdictPass verdict = iota
	verdictFail
)

// classify applies the three-outcome truth table. migErr is the error
// returned by Migrator.Run (nil == exit 0). src/dst are the canonical
// text reads keyed by column name → per-row values; targetRowCount is
// the number of rows present in the target table (<0 == table absent),
// used to distinguish a clean loud-refuse from a *data* partial target.
//
// "Partial target" precision (a deliberate, documented oracle call):
// sluice's pipeline orders create-tables BEFORE bulk-copy, and the
// timetz[] refusal (the documented Bug 73 loud-refuse) fires in the
// COPY row-writer — so an EMPTY target table necessarily exists after
// that documented refusal. The Bug 73 battle fixture
// (migrate_bug7374_integration_test.go) accepts exactly this refuse-at-
// copy behaviour. So for a known-loud-refuse case the FAIL condition is
// a target with ROWS (real partial data — the corruption signature),
// NOT the mere existence of an empty table. An empty table after a
// documented refuse-at-copy is the contracted behaviour, not a defect.
func classify(
	gc *genCase,
	ce caseExpectation,
	migErr error,
	targetRowCount int,
	src, dst map[string][]sql.NullString,
) (v verdict, diag string) {
	if ce.loudRefuse {
		// Expected a loud refusal.
		if migErr == nil {
			return verdictFail, fmt.Sprintf(
				"UNEXPECTED SUCCESS: %s — expected a loud refusal but migrate exited 0 (silent corruption risk)",
				ce.reason,
			)
		}
		if targetRowCount > 0 {
			return verdictFail, fmt.Sprintf(
				"PARTIAL TARGET DATA: %s — migrate refused (good) but %d row(s) reached the target (partial data is a FAIL)",
				ce.reason, targetRowCount,
			)
		}
		return verdictPass, "loud-refuse as documented (no partial data): " + ce.reason
	}

	// Not a loud-refuse case → migrate must succeed.
	if migErr != nil {
		return verdictFail, fmt.Sprintf(
			"UNEXPECTED REFUSAL: migrate errored on a case with no documented loud-refuse column (the v0.69.0 #16 false-positive class): %v",
			migErr,
		)
	}

	// Faithful columns must compare src==dst exactly via canonical text.
	if len(src) == 0 && len(gc.columns) > 0 {
		return verdictFail, "migrate exited 0 but the source read returned no rows (harness/source error)"
	}
	if len(dst) != len(src) {
		return verdictFail, fmt.Sprintf(
			"ROW COUNT MISMATCH: src has %d rows, dst has %d (silent loss)", len(src), len(dst),
		)
	}
	for _, col := range ce.faithfulCols {
		sv, ok := src[col]
		if !ok {
			continue
		}
		dv := dst[col]
		if len(dv) != len(sv) {
			return verdictFail, fmt.Sprintf(
				"col %s: src %d cells, dst %d cells (silent loss)", col, len(sv), len(dv),
			)
		}
		for i := range sv {
			if sv[i] != dv[i] {
				return verdictFail, fmt.Sprintf(
					"MISMATCH col %s row %d: src=%q dst=%q "+
						"(silent loss / flatten / corruption — faithful round-trip expected)",
					col, i, nullStr(sv[i]), nullStr(dv[i]),
				)
			}
		}
	}
	return verdictPass, ""
}

func nullStr(n sql.NullString) string {
	if !n.Valid {
		return "<NULL>"
	}
	return n.String
}

// faithfulColumnsFor returns the subset of gc's columns whose expected
// outcome is faithful for gc.dir — the columns the oracle compares
// src==dst. Pure logic (unit-tested); the DB read that consumes it
// lives in the integration driver.
func faithfulColumnsFor(gc *genCase) []string {
	var out []string
	for _, c := range gc.columns {
		if c.fam.expect(gc.dir, c.shp) == outcomeFaithful {
			out = append(out, c.name)
		}
	}
	return out
}
