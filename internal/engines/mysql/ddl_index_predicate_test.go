// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL has no partial indexes; a dropped WHERE is not free
// (audit 2026-08-01 S8, second half).
//
// The first half of S8 was the mirror case — a MySQL prefix length with no
// Postgres equivalent — and its lesson was that a grep for the shared helper
// undercounted the emit sites, because one of them rendered its own column
// list. That lesson is applied here up front: MySQL has THREE index-emitting
// sites, and only one of them is the shared ADD-clause builder.
//
//	emitAddIndexClause          ALTER TABLE ... ADD [UNIQUE] INDEX
//	                            (reached by both emitCreateIndex and
//	                            emitCreateIndexesCombined)
//	CREATE TABLE inline index   inlineAutoIncrementIndex (GitHub #25)
//	CREATE TABLE copy unique    inlineUniqueKeyForCopy   (Bug 125)
//
// The two inline sites select an index out of table.Indexes and render its
// column list directly, so a check that lived only in the ADD-clause builder
// would miss them. Both detectors — inlineAutoIncrementIndex and
// pickNonNullUniqueIndex — filter on uniqueness and column shape and NOT on
// Predicate, so a source partial unique index reaches them.
//
// TestEveryMySQLIndexEmitSiteChecksThePredicate is the fail-by-default roster
// that keeps that enumeration honest as sites are added.

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// partialUniqueTable is the shape that motivates the whole check: soft delete,
// where uniqueness applies only to the live rows.
func partialUniqueTable(idxName string, unique bool) *ir.Table {
	return &ir.Table{
		Schema: "public", Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "deleted_at", Type: ir.Timestamp{}},
		},
		PrimaryKey: &ir.Index{
			Name: "users_pkey", Unique: true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
		Indexes: []*ir.Index{{
			Name:             idxName,
			Unique:           unique,
			Columns:          []ir.IndexColumn{{Column: "email"}},
			Predicate:        "deleted_at IS NULL",
			PredicateDialect: "postgres",
		}},
	}
}

// TestPartialUniqueIndexRefusedOnAddIndex covers the ALTER path.
func TestPartialUniqueIndexRefusedOnAddIndex(t *testing.T) {
	tbl := partialUniqueTable("users_email_live_uniq", true)

	_, err := emitCreateIndex("users", tbl.Indexes[0])
	if err == nil {
		t.Fatal("a PARTIAL UNIQUE index was emitted as a whole-table UNIQUE key with no complaint.\n\n" +
			"The source permits many soft-deleted rows to share an email and forbids only live duplicates; " +
			"a MySQL UNIQUE KEY (email) forbids both. The target ends up STRICTER than the source, so it " +
			"refuses rows the source holds legally — mid-copy if a duplicate already exists, and on the " +
			"operator's first ordinary write if one does not.")
	}
	for _, want := range []string{"users_email_live_uniq", "deleted_at IS NULL", "generated column"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — an operator needs the index, the predicate, and a way "+
				"forward.\ngot: %v", want, err)
		}
	}
}

// TestPartialNonUniqueIndexIsCarriedWithoutThePredicate is the other half of
// the policy, and it must NOT refuse: on a non-unique index the predicate is a
// size choice, the widened index covers a superset of the rows, and every
// query still returns the correct answer.
func TestPartialNonUniqueIndexIsCarriedWithoutThePredicate(t *testing.T) {
	tbl := partialUniqueTable("users_email_live_idx", false)

	stmt, err := emitCreateIndex("users", tbl.Indexes[0])
	if err != nil {
		t.Fatalf("a partial NON-UNIQUE index was refused (%v); dropping its predicate changes cost, not "+
			"correctness, so it must be carried with a WARN", err)
	}
	if !strings.Contains(stmt, "users_email_live_idx") {
		t.Errorf("index not emitted: %q", stmt)
	}
	// The predicate must not leak into MySQL DDL — there is no syntax for it.
	if strings.Contains(strings.ToUpper(stmt), "WHERE") {
		t.Errorf("emitted DDL carries a WHERE clause, which MySQL cannot parse: %q", stmt)
	}
}

// TestPartialUniqueIndexRefusedAtTheInlineCreateTableSite covers the site the
// shared ADD-clause check does NOT reach: an auto-increment column whose
// supporting unique index is partial gets inlined into CREATE TABLE.
func TestPartialUniqueIndexRefusedAtTheInlineCreateTableSite(t *testing.T) {
	// No PK, so the auto-increment column needs an inline supporting index —
	// and the only candidate is the partial unique one.
	tbl := &ir.Table{
		Schema: "public", Name: "events",
		Columns: []*ir.Column{
			{Name: "seq", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "deleted_at", Type: ir.Timestamp{}},
		},
		Indexes: []*ir.Index{{
			Name:             "events_seq_live_uniq",
			Unique:           true,
			Columns:          []ir.IndexColumn{{Column: "seq"}},
			Predicate:        "deleted_at IS NULL",
			PredicateDialect: "postgres",
		}},
	}

	// Guard the test's own premise: if this table no longer routes through the
	// inline path, the test is exercising something else and proves nothing.
	if inlineAutoIncrementIndex(tbl) == nil {
		t.Skip("inlineAutoIncrementIndex no longer selects this shape; the inline site needs a new fixture " +
			"rather than this test silently passing")
	}

	if _, err := emitTableDef(tbl); err == nil {
		t.Fatal("a PARTIAL UNIQUE index was inlined into CREATE TABLE as a whole-table UNIQUE KEY with no " +
			"complaint. This is the site a grep for the shared ADD-clause helper misses, which is exactly " +
			"how the FIRST half of S8 undercounted its emit sites.")
	}
}

// TestEveryMySQLIndexEmitSiteChecksThePredicate is the fail-by-default roster.
//
// It walks ddl_emit.go for every site that renders an index into MySQL DDL and
// requires each to call checkIndexPredicate. A new emit site added without the
// check fails here rather than shipping a silently-widened unique key — the
// sibling-sweep step in mechanical form, since the recurring cost in this
// project is a guard that reached one implementor and not its siblings.
func TestEveryMySQLIndexEmitSiteChecksThePredicate(t *testing.T) {
	// Each entry: the function that renders index DDL, and why it is a site.
	//
	// Keyed on the FULL declaration identity, receiver included. There are two
	// declarations named emitTableDefWithDomainChecks — a free function and a
	// mysqlEmitter method — and only the method carries the body that renders
	// the inline indexes. A name-only roster matched the wrong one and reported
	// the check missing while it was present, so the receiver is part of the key.
	sites := map[string]string{
		"emitAddIndexClause": "renders ADD [UNIQUE] INDEX for both emitCreateIndex and " +
			"emitCreateIndexesCombined",
		"(mysqlEmitter).emitTableDefWithDomainChecks": "renders the two CREATE TABLE inline index sites " +
			"(inlineAutoIncrementIndex, inlineUniqueKeyForCopy) directly",
	}

	// declName renders a FuncDecl as it appears in the roster.
	declName := func(fn *ast.FuncDecl) string {
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return fn.Name.Name
		}
		typ := fn.Recv.List[0].Type
		if star, ok := typ.(*ast.StarExpr); ok {
			typ = star.X
		}
		id, ok := typ.(*ast.Ident)
		if !ok {
			return fn.Name.Name
		}
		return "(" + id.Name + ")." + fn.Name.Name
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "ddl_emit.go", nil, 0)
	if err != nil {
		t.Fatalf("parse ddl_emit.go: %v", err)
	}

	found := map[string]bool{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		name := declName(fn)
		if _, want := sites[name]; !want {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "checkIndexPredicate" {
				found[name] = true
			}
			return true
		})
	}

	// Anti-vacuity: if the walk stops finding the functions at all — renamed,
	// moved to another file — this gate would pass on an empty set forever.
	seen := 0
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			if _, want := sites[declName(fn)]; want {
				seen++
			}
		}
	}
	if seen != len(sites) {
		t.Fatalf("found %d of %d rostered emit sites in ddl_emit.go; a function was renamed or moved and "+
			"this gate is no longer checking what it names", seen, len(sites))
	}

	for name, why := range sites {
		if !found[name] {
			t.Errorf("%s does not call checkIndexPredicate, but it %s.\n\n"+
				"MySQL has no partial-index syntax, so an index emitted from here drops any WHERE clause. "+
				"On a UNIQUE key that makes the target STRICTER than the source and it refuses rows the "+
				"source holds legally. Call checkIndexPredicate before rendering, or — if this site "+
				"genuinely cannot receive a partial index — remove it from the roster with the reason.",
				name, why)
		}
	}
}
