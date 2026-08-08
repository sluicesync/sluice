// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The fail-by-default roster over every geometry-dispatch site in an
// engine package — the durable half of audit 2026-08-05 finding C-14.
//
// # Why a roster and not another table test
//
// C-14 was one sentence, repeated verbatim in three decoders: "per-row
// SRID is intentionally dropped — SRID is a per-COLUMN property". That
// is a true statement about sluice's IR and a FALSE one about both
// engines. PostGIS's unconstrained `geometry` and MySQL's spatial types
// without the `SRID n` attribute both accept a different SRID per row, so
// three separate decoders threw one away and each writer stamped the
// column's back on. Valid geometry, wrong place, exit 0.
//
// Every one of those three sites was written independently, with the same
// reasoning, over three different releases. So the gate is not "does the
// geometry decoder still work" — that is pinned per engine against real
// servers. The gate is: **a new geometry-dispatch arm in any engine
// package fails the build until someone states what happens to the row's
// own SRID there.**
//
// # What it reaches, stated rather than implied
//
// It walks `internal/engines/**` non-test sources for case clauses that
// dispatch on a geometry type — `case ir.Geometry:` (including multi-type
// clauses) and Vitess's `case query.Type_GEOMETRY:` — keyed by package
// directory + enclosing function. That is every place an engine decides
// what a geometry VALUE becomes.
//
// It does NOT reach, and both limits are structural rather than chosen:
//
//   - Both row writers' geometry arms, which are type ASSERTIONS
//     (`if geom, ok := t.(ir.Geometry); ok`) rather than case clauses, so
//     the walker cannot see them. That happens to coincide with a
//     deliberate exclusion — a writer re-frames with [ir.Geometry.SRID]
//     by construction and has no row SRID available to lose, so the loss
//     is always upstream of it — but the coincidence is worth naming,
//     because the writers ARE where a future lossless-carriage change
//     would need matching work (see [ir.CheckGeometryRowSRID]) and this
//     gate would not ask for it.
//   - `internal/pipeline`. No geometry value is decoded there; the backup
//     change codec carries whatever the engine reader produced.
//
// The roster therefore contains SCHEMA-side arms as well as value-side
// ones — DDL emit, CDC type normalisation. Those are listed with the
// `arm-carries-no-value` verdict rather than filtered out by the walker,
// deliberately: "this arm never sees a value" is a claim worth writing
// down once and re-reading when the arm changes, and a walker clever
// enough to filter them would be a walker that could be wrong quietly.
//
// # How the verdicts are checked
//
// The walker records WHERE a geometry arm is, not what it does — proving
// "this arm reaches the guard" would mean re-implementing call-graph
// analysis. The verdict is a human statement, and it is the behavioural
// tests that hold it honest:
// TestDecodeMySQLGeometry_SRIDFamilyMatrix,
// TestDecodePGGeometry_SRIDFamilyMatrix, TestDecodeVStreamCellGeometry,
// and the two migrate integration pins. That split is the same one the
// hex roster makes, for the same reason.
package engines

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// geometrySRIDVerdict is what a geometry-dispatch arm does with the SRID
// the value's own framing carries.
type geometrySRIDVerdict struct {
	handling string
	reason   string
}

const (
	// sridCompared: the arm reads the row's SRID and compares it against
	// the column's declared one, refusing a mismatch
	// ([ir.CheckGeometryRowSRID]).
	sridCompared = "compared-against-the-column"
	// sridNotCarried: the arm never sees a geometry VALUE — it dispatches
	// on the type for some other purpose (shaping a literal, choosing a
	// projection). Requires a reason saying what it does instead.
	sridNotCarried = "arm-carries-no-value"
)

// geometrySRIDRoster is the fail-by-default map. A geometry-dispatch arm
// that is not listed fails the build.
var geometrySRIDRoster = map[string]geometrySRIDVerdict{
	"postgres.decodeValue": {
		sridCompared,
		"forwards the column's ir.Geometry.SRID into decodePGGeometry, which compares it " +
			"against the value's EWKB SRID (ewkbSRID) before ewkbToWKB strips the framing. " +
			"Reaches the cold-start scan AND the pgoutput CDC tuple decode.",
	},
	"mysql.decodeValue": {
		sridCompared,
		"forwards the column's ir.Geometry.SRID into decodeMySQLGeometry, which compares it " +
			"against the value's 4-byte LE prefix before stripping. Reaches bulk copy, binlog " +
			"CDC (decodeBinlogRow), the flatfile shim, and mydumper (via mysql.DecodeRowValue).",
	},
	"mysql.decodeVStreamCell": {
		sridCompared,
		"compares against 0, which IS the column SRID on this path: columnMetaFromField " +
			"hardcodes SrsID 0 because VStream's FieldEvent carries no per-column SRID. Returns " +
			"the vstreamGeometrySRIDError sentinel (the cell decoder has no error channel); " +
			"decodeVStreamRow converts it to a loud, column-named stream error.",
	},
	"mydumper.literalToDriverShape": {
		sridNotCarried,
		"shapes a lexed dump literal into the raw form mysql.DecodeRowValue expects — the " +
			"bytes pass through untouched here. The SRID comparison happens one call later, in " +
			"mysql.decodeValue, which literalToRowValue funnels every value through.",
	},
	"mysql.emitColumnType": {
		sridNotCarried,
		"DDL emit: turns the IR type into MySQL type text. It DOES read ir.Geometry.SRID (the " +
			"mariadb REF_SYSTEM_ID spelling), which is the per-COLUMN carriage working as " +
			"designed; no value passes through.",
	},
	"mysql.mysqlForbidsDefault": {
		sridNotCarried,
		"schema policy: answers whether MySQL permits a DEFAULT on this type family.",
	},
	"postgres.emitColumnType": {
		sridNotCarried,
		"DDL emit: renders geometry(<Subtype>,<SRID>) from the IR column type. Per-COLUMN " +
			"carriage; no value passes through.",
	},
	"postgres.normalizeTypeForCDCComparison": {
		sridNotCarried,
		"schema-drift comparison: projects a bare ir.Geometry so a pgoutput RelationMessage " +
			"(which carries no subtype/SRID) compares equal to the catalog-read type. Types " +
			"only — no value reaches it.",
	},
}

// geometrySiteFloor is the anti-vacuity floor. A walker that stops finding
// arms — a parse failure, a moved directory, a renamed type — would
// otherwise report a clean roster, which is the shape that makes a gate
// defend the defect. Deliberately below the current count so an ordinary
// deletion does not fail the build, and far enough above zero that a
// broken walk cannot pass.
const geometrySiteFloor = 3

// geometrySRIDEnginesFloor requires the walk to reach both engines that
// can carry geometry. C-14 existed in three places at once precisely
// because nobody looked across engines.
var geometrySRIDEnginesFloor = []string{"postgres", "mysql"}

// TestGeometryDispatchArmsDeclareTheirRowSRID is the roster gate.
func TestGeometryDispatchArmsDeclareTheirRowSRID(t *testing.T) {
	found, err := findGeometryDispatchArms(".")
	if err != nil {
		t.Fatalf("walk engine sources: %v", err)
	}

	if len(found) < geometrySiteFloor {
		t.Fatalf(
			"discovered only %d geometry-dispatch arms (floor %d) — the walker is broken, not "+
				"the tree. Found: %v", len(found), geometrySiteFloor, sortedKeys(found),
		)
	}
	for _, pkg := range geometrySRIDEnginesFloor {
		if !anyKeyInPackage(found, pkg) {
			t.Fatalf("no geometry-dispatch arm found in the %q engine; the walker is not reaching it", pkg)
		}
	}

	for _, key := range sortedKeys(found) {
		verdict, listed := geometrySRIDRoster[key]
		if !listed {
			t.Errorf(
				"%s (%s) dispatches on a geometry type and is NOT on the roster.\n"+
					"A geometry value's own SRID is what makes a coordinate pair mean a place, and\n"+
					"both engines let it differ per ROW from what the column declares. Audit C-14\n"+
					"found three decoders that each dropped it and let the writer stamp the column's\n"+
					"back on — silently. Add an entry to geometrySRIDRoster saying either that this\n"+
					"arm compares the row's SRID against the column's (ir.CheckGeometryRowSRID), or\n"+
					"that it never sees a value and why.",
				key, found[key],
			)
			continue
		}
		switch verdict.handling {
		case sridCompared, sridNotCarried:
		default:
			t.Errorf("%s: unknown verdict %q", key, verdict.handling)
		}
		if strings.TrimSpace(verdict.reason) == "" {
			t.Errorf("%s: roster entry has no reason; the reason IS the gate", key)
		}
	}

	for key := range geometrySRIDRoster {
		if _, ok := found[key]; !ok {
			t.Errorf(
				"roster lists %s but no such geometry-dispatch arm exists any more — remove the "+
					"entry. A stale blessing is how a roster starts covering less than its name "+
					"implies.", key,
			)
		}
	}
}

// findGeometryDispatchArms walks root for non-test Go sources and returns
// "<package dir>.<enclosing func>" → "file:line" for every case clause
// that dispatches on a geometry type.
//
// Keyed by the DIRECTORY name for the same reason the hex roster is: the
// `-trigger` engines' package clauses differ from their directories.
//
// Function-keyed with first-occurrence dedup, exactly like
// [findHexDecodeSites] — a SECOND geometry arm inside an already-rostered
// function inherits its verdict. Every rostered function today has exactly
// one, and a decoder with two geometry arms would be strange enough to
// notice; the floor catches a wholesale break rather than that shape.
func findGeometryDispatchArms(root string) (map[string]string, error) {
	out := map[string]string{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			ast.Inspect(fn.Body, func(inner ast.Node) bool {
				clause, ok := inner.(*ast.CaseClause)
				if !ok {
					return true
				}
				for _, expr := range clause.List {
					if !isGeometryDispatchExpr(expr) {
						continue
					}
					key := pkgDir + "." + fn.Name.Name
					if _, seen := out[key]; !seen {
						out[key] = fmt.Sprintf("%s:%d", path, fset.Position(clause.Pos()).Line)
					}
					break
				}
				return true
			})
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// isGeometryDispatchExpr reports whether a case-clause expression selects
// a geometry type: `ir.Geometry` (the IR type-switch form) or
// `query.Type_GEOMETRY` (Vitess's wire enum).
//
// Selector-based, so an aliased import of either package evades it — the
// same known limit the hex roster carries, and no engine aliases them
// today. The floor catches a wholesale break, not one aliased arm.
func isGeometryDispatchExpr(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	base, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	switch {
	case base.Name == "ir" && sel.Sel.Name == "Geometry":
		return true
	case base.Name == "query" && sel.Sel.Name == "Type_GEOMETRY":
		return true
	}
	return false
}
