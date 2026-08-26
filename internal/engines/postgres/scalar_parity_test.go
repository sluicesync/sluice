// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// The Postgres engine carries TWO parallel scalar type registries: the schema
// reader's text-keyed translateScalarType switch (types.go, information_schema
// data_type spellings) and the CDC relation decoder's OID-keyed oidToType
// switch (cdc_relations.go, pgoutput wire OIDs). They have drifted at least six
// times (Bug 97 verbatim families, Bug 144 arrays, macaddr8/Bug 225, interval,
// timetz/Bug 71, and most recently bit/varbit — b08212e6, where a bit(n)
// column migrated fine and then killed the sync stream on the FIRST DML with
// "unsupported column type OID 1560"). TestOIDToType_ArrayParity guards the
// ARRAY element families only; this file is the audit-2026-08-26 A-1 gate for
// the SCALAR level.
//
// Scope (stated per the gate-enumeration rule): the builtin scalar families —
// the translateScalarType case arms ↔ the oidToType case arms — plus the
// verbatim-carry pair (coreVerbatimEligibleTypes ↔ coreVerbatimCDCOIDs).
// It does NOT reach: array families (TestOIDToType_ArrayParity), or
// dynamic-OID user types — enum / domain / PostGIS geometry — which have no
// static arm on either side and are reconciled in resolveWireColumnType
// (pinned by TestBuildRelationCacheEntry_EnumOID_Bug151 / _DomainOID_Bug254 /
// the geometry SRID tests).
//
// Universe derivation: both switches' case arms are read from the package
// SOURCE via go/ast — not from a hand-list of what the switches "should"
// contain — so adding a scalar arm to EITHER side without updating the
// canonical scalarFamilies table below is a build-blocking failure, in both
// directions. The behavioral leg then resolves every family through both
// registries and requires the same IR type family out of each.

// scalarFamilies is the canonical correspondence between a scalar family's
// information_schema data_type spelling (as translateScalarType dispatches on
// it) and its pgoutput wire OID (as oidToType dispatches on it). One row per
// (spelling, OID) pair; families with two spellings (numeric/decimal,
// time/time without time zone) or two OIDs per spelling (character ↔
// bpchar + "char") appear once per pair. oidName is the case-expression
// spelling in oidToType's source, held against the AST-derived arm set.
var scalarFamilies = []struct {
	dataType string     // translateScalarType case spelling
	oidName  string     // oidToType case-expression spelling
	oid      uint32     // the wire OID that spelling denotes
	typmod   int32      // representative wire typmod
	meta     columnMeta // representative schema-reader meta (DataType filled from dataType; AttTypmod defaulted to -1)
}{
	{dataType: "boolean", oidName: "pgtype.BoolOID", oid: pgtype.BoolOID, typmod: -1},
	{dataType: "smallint", oidName: "pgtype.Int2OID", oid: pgtype.Int2OID, typmod: -1},
	{dataType: "integer", oidName: "pgtype.Int4OID", oid: pgtype.Int4OID, typmod: -1},
	{dataType: "bigint", oidName: "pgtype.Int8OID", oid: pgtype.Int8OID, typmod: -1},
	{dataType: "real", oidName: "pgtype.Float4OID", oid: pgtype.Float4OID, typmod: -1},
	{dataType: "double precision", oidName: "pgtype.Float8OID", oid: pgtype.Float8OID, typmod: -1},
	{dataType: "numeric", oidName: "pgtype.NumericOID", oid: pgtype.NumericOID, typmod: -1},
	{dataType: "decimal", oidName: "pgtype.NumericOID", oid: pgtype.NumericOID, typmod: -1},
	{dataType: "character varying", oidName: "pgtype.VarcharOID", oid: pgtype.VarcharOID, typmod: 14, meta: columnMeta{CharMaxLen: i64p(10)}},
	{dataType: "character", oidName: "pgtype.BPCharOID", oid: pgtype.BPCharOID, typmod: 14, meta: columnMeta{CharMaxLen: i64p(10)}},
	// PG's internal one-byte "char" (QCharOID). The schema reader reaches the
	// "character" arm for it via builtinArrayElement's `_char` mapping.
	{dataType: "character", oidName: "pgtype.QCharOID", oid: pgtype.QCharOID, typmod: -1, meta: columnMeta{CharMaxLen: i64p(1)}},
	{dataType: "text", oidName: "pgtype.TextOID", oid: pgtype.TextOID, typmod: -1},
	{dataType: "bytea", oidName: "pgtype.ByteaOID", oid: pgtype.ByteaOID, typmod: -1},
	// Bit typmods carry the raw length, no +4 offset (see both arms' docs).
	{dataType: "bit", oidName: "pgtype.BitOID", oid: pgtype.BitOID, typmod: 3, meta: columnMeta{CharMaxLen: i64p(3)}},
	{dataType: "bit varying", oidName: "pgtype.VarbitOID", oid: pgtype.VarbitOID, typmod: 3, meta: columnMeta{CharMaxLen: i64p(3)}},
	{dataType: "date", oidName: "pgtype.DateOID", oid: pgtype.DateOID, typmod: -1},
	{dataType: "interval", oidName: "pgtype.IntervalOID", oid: pgtype.IntervalOID, typmod: -1},
	{dataType: "time without time zone", oidName: "pgtype.TimeOID", oid: pgtype.TimeOID, typmod: -1},
	{dataType: "time", oidName: "pgtype.TimeOID", oid: pgtype.TimeOID, typmod: -1},
	{dataType: "time with time zone", oidName: "pgtype.TimetzOID", oid: pgtype.TimetzOID, typmod: -1},
	{dataType: "timestamp without time zone", oidName: "pgtype.TimestampOID", oid: pgtype.TimestampOID, typmod: -1},
	{dataType: "timestamp", oidName: "pgtype.TimestampOID", oid: pgtype.TimestampOID, typmod: -1},
	{dataType: "timestamp with time zone", oidName: "pgtype.TimestamptzOID", oid: pgtype.TimestamptzOID, typmod: -1},
	{dataType: "json", oidName: "pgtype.JSONOID", oid: pgtype.JSONOID, typmod: -1},
	{dataType: "jsonb", oidName: "pgtype.JSONBOID", oid: pgtype.JSONBOID, typmod: -1},
	{dataType: "uuid", oidName: "pgtype.UUIDOID", oid: pgtype.UUIDOID, typmod: -1},
	{dataType: "inet", oidName: "pgtype.InetOID", oid: pgtype.InetOID, typmod: -1},
	{dataType: "cidr", oidName: "pgtype.CIDROID", oid: pgtype.CIDROID, typmod: -1},
	{dataType: "macaddr", oidName: "pgtype.MacaddrOID", oid: pgtype.MacaddrOID, typmod: -1},
	{dataType: "macaddr8", oidName: "pgtype.Macaddr8OID", oid: pgtype.Macaddr8OID, typmod: -1},
}

// scalarParityExempt lists translateScalarType case spellings that are
// DELIBERATELY absent from the CDC decoder, each with a written reason.
// Empty today: every scalar family the schema reader accepts must decode on
// the CDC path, or a column of that family migrates fine and then kills the
// sync stream on its first DML. Add an entry only with an architectural
// reason for why the CDC side cannot receive the family.
var scalarParityExempt = map[string]string{}

func i64p(n int64) *int64 { return &n }

// Anti-vacuity floors. The derived universes hold 29 spellings / 27 OID arms
// today; these only ever rise. A drop below the floor means the AST scan
// stopped finding the switch (file moved, function renamed) — fail loudly
// instead of passing on an empty universe.
const (
	scalarSpellingFloor = 25
	scalarOIDArmFloor   = 20
)

// parityFuncDecl parses one source file of this package and returns the named
// function's declaration. The test runs in the package directory, so bare
// filenames resolve.
func parityFuncDecl(t *testing.T, filename, funcName string) *ast.FuncDecl {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == funcName {
			return fd
		}
	}
	t.Fatalf("%s: no function %q — the registry moved; repoint this gate at its new home", filename, funcName)
	return nil
}

// caseExprSpellings collects every case-clause expression in fn's body,
// rendered as source-ish spellings: string literals unquoted ("boolean"),
// selector expressions dotted ("pgtype.BoolOID"), identifiers and other
// literals verbatim — so an arm added under ANY spelling lands in the derived
// universe rather than being silently invisible to the gate.
func caseExprSpellings(t *testing.T, fn *ast.FuncDecl) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, e := range cc.List {
			switch e := e.(type) {
			case *ast.BasicLit:
				if e.Kind == token.STRING {
					s, err := strconv.Unquote(e.Value)
					if err != nil {
						t.Fatalf("unquote case literal %s: %v", e.Value, err)
					}
					out[s] = true
				} else {
					out[e.Value] = true
				}
			case *ast.SelectorExpr:
				if x, ok := e.X.(*ast.Ident); ok {
					out[x.Name+"."+e.Sel.Name] = true
				}
			case *ast.Ident:
				out[e.Name] = true
			default:
				t.Fatalf("%s: unrecognized case expression shape %T — extend caseExprSpellings so the gate keeps seeing every arm", fn.Name.Name, e)
			}
		}
		return true
	})
	return out
}

// TestScalarTypeRegistryParity is the audit-2026-08-26 A-1 dual-registry gate:
// every scalar family must have BOTH a schema-reader arm and a CDC decode arm,
// resolving to the same IR type family, with the two arm sets derived from the
// source so neither side can grow (or shrink) without this failing.
func TestScalarTypeRegistryParity(t *testing.T) {
	schemaArms := caseExprSpellings(t, parityFuncDecl(t, "types.go", "translateScalarType"))
	cdcArms := caseExprSpellings(t, parityFuncDecl(t, "cdc_relations.go", "oidToType"))
	if len(schemaArms) < scalarSpellingFloor {
		t.Fatalf("derived only %d translateScalarType case spellings (floor %d) — the AST scan went vacuous; fix it before trusting this gate", len(schemaArms), scalarSpellingFloor)
	}
	if len(cdcArms) < scalarOIDArmFloor {
		t.Fatalf("derived only %d oidToType case arms (floor %d) — the AST scan went vacuous; fix it before trusting this gate", len(cdcArms), scalarOIDArmFloor)
	}

	canonSpellings := map[string]bool{}
	canonOIDNames := map[string]bool{}
	for _, row := range scalarFamilies {
		canonSpellings[row.dataType] = true
		canonOIDNames[row.oidName] = true
	}

	// Schema reader ↔ canon, both directions.
	for s := range schemaArms {
		if !canonSpellings[s] && scalarParityExempt[s] == "" {
			t.Errorf("translateScalarType accepts %q but scalarFamilies has no row for it — a column of this family would migrate fine and then kill the CDC stream on its first DML (the bit/varbit b08212e6 shape). Add the row with its wire OID (and the oidToType arm), or record a scalarParityExempt reason", s)
		}
	}
	for s := range canonSpellings {
		if !schemaArms[s] {
			t.Errorf("scalarFamilies row %q has no translateScalarType arm — phantom family in the canonical list; remove the row or add the schema-reader arm", s)
		}
	}
	for s, reason := range scalarParityExempt {
		if !schemaArms[s] {
			t.Errorf("scalarParityExempt entry %q (%s) matches no translateScalarType arm — stale exemption; remove it", s, reason)
		}
	}

	// CDC decoder ↔ canon, both directions.
	for s := range cdcArms {
		if !canonOIDNames[s] {
			t.Errorf("oidToType has arm %s with no scalarFamilies row — the CDC decoder accepts a family the schema reader never produces (the reverse-drift half of Bug 97); add the row or remove the arm", s)
		}
	}
	for s := range canonOIDNames {
		if !cdcArms[s] {
			t.Errorf("scalarFamilies expects oidToType arm %s and the switch has none — a column of this family migrates fine and then kills the sync stream on its first DML. Add the CDC arm", s)
		}
	}

	// Behavioral leg: both registries resolve each row to the same IR family.
	for _, row := range scalarFamilies {
		row := row
		t.Run(fmt.Sprintf("%s/%s", row.dataType, row.oidName), func(t *testing.T) {
			meta := row.meta
			meta.DataType = row.dataType
			if meta.AttTypmod == 0 {
				meta.AttTypmod = -1
			}
			st, err := translateScalarType(meta)
			if err != nil {
				t.Fatalf("translateScalarType(%q): %v", row.dataType, err)
			}
			ct, err := oidToType(row.oid, row.typmod)
			if err != nil {
				t.Fatalf("oidToType(%s=%d): %v", row.oidName, row.oid, err)
			}
			if reflect.TypeOf(st) != reflect.TypeOf(ct) {
				t.Errorf("family split: schema reader resolves %q to %T, CDC resolves %s to %T — the two registries disagree on this family's IR shape", row.dataType, st, row.oidName, ct)
			}
		})
	}
}

// TestCoreVerbatimRegistryParity holds the verbatim-carry halves of the same
// dual registry to each other: the schema reader's coreVerbatimEligibleTypes
// keys (data_type spellings) and the CDC coreVerbatimCDCOIDs values (typname
// spellings) are the same names by construction — ADR-0051/0070 families take
// no parameters, so data_type == typname for every member. Bug 97 WAS this
// exact drift (families eligible at schema-read, absent from the CDC side,
// stream killed on first DML); the v0.92.0 hotfix reconciled them manually and
// this pins the reconciliation.
func TestCoreVerbatimRegistryParity(t *testing.T) {
	if len(coreVerbatimEligibleTypes) < 10 {
		t.Fatalf("coreVerbatimEligibleTypes has only %d entries — the allowlist moved or shrank; fix this gate's floor deliberately if that was intended", len(coreVerbatimEligibleTypes))
	}
	cdcNames := map[string]bool{}
	for oid, name := range coreVerbatimCDCOIDs {
		if cdcNames[name] {
			t.Errorf("coreVerbatimCDCOIDs maps two OIDs to %q (second: %d) — duplicate entry", name, oid)
		}
		cdcNames[name] = true
	}
	for name := range coreVerbatimEligibleTypes {
		if !cdcNames[name] {
			t.Errorf("%q is verbatim-eligible at schema-read but has no coreVerbatimCDCOIDs entry — first DML on such a column kills the sync stream (Bug 97)", name)
		}
	}
	for name := range cdcNames {
		if !coreVerbatimEligibleTypes[name] {
			t.Errorf("coreVerbatimCDCOIDs carries %q but the schema reader's coreVerbatimEligibleTypes does not — the CDC path would translate events for a column type the migration refused", name)
		}
	}
}
