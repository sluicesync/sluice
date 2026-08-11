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
	"regexp"
	"strconv"
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

	_, err := emitCreateIndex("users", tbl.Indexes[0], true)
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

	stmt, err := emitCreateIndex("users", tbl.Indexes[0], true)
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

// indexEmitExempt names functions the DISCOVERY rule below flags which are not
// actually emit sites, each with the reason. A function here is a DECISION; a
// function merely absent from the discovered set is invisible to this gate,
// which is why discovery must not be a curated list.
var indexEmitExempt = map[string]string{
	"emitIndexColumnList": "the shared column-list renderer itself — it receives " +
		"[]ir.IndexColumn and never sees the ir.Index, so it cannot inspect a predicate; " +
		"its CALLERS are the sites",
	"emitIndexColumnListWithPrefix": "the prefix-aware half of the same shared renderer, " +
		"same reason",
}

// TestEveryMySQLIndexEmitSiteChecksThePredicate is the fail-by-default roster.
//
// It walks ddl_emit.go for every site that renders an index into MySQL DDL and
// requires each to call checkIndexPredicate. A new emit site added without the
// check fails here rather than shipping a silently-widened unique key — the
// sibling-sweep step in mechanical form, since the recurring cost in this
// project is a guard that reached one implementor and not its siblings.
//
// # This gate previously could not fail on an added site, which is the one
// # thing its own doc promises
//
// It was a CURATED two-entry allowlist, and non-members were `continue`d past.
// So it verified that the two known sites still call the check — useful — while
// being structurally incapable of noticing a THIRD site, which is the entire
// scenario the paragraph above describes. A gate whose coverage is narrower
// than its name is worse than no gate, because it stops anyone from looking;
// that this one was built as the mechanical form of the sibling-sweep step
// makes it the sharpest instance of the class yet found here.
//
// It is now DISCOVERY-based: a function is an emit site if it renders an
// index's column list (calls emitIndexColumnList / …WithPrefix) or writes an
// index-DDL keyword literal. Both signals are used, because the first half of
// S8 undercounted precisely by assuming every site goes through the shared
// helper — a future site that renders its own column list is caught by the
// literal. Anything discovered must either call checkIndexPredicate or be in
// indexEmitExempt with a reason.
func TestEveryMySQLIndexEmitSiteChecksThePredicate(t *testing.T) {
	// Calling one of these means the function renders an index column list.
	renderers := map[string]bool{
		"emitIndexColumnList":           true,
		"emitIndexColumnListWithPrefix": true,
	}
	// Writing one of these means the function renders index DDL directly.
	ddlKeyword := regexp.MustCompile(`^\s*(UNIQUE |FULLTEXT |SPATIAL )?(INDEX|KEY) $`)

	// declName renders a FuncDecl as it appears in the roster.
	//
	// Keyed on the FULL declaration identity, receiver included. There are two
	// declarations named emitTableDefWithDomainChecks — a free function and a
	// mysqlEmitter method — and only the method carries the body that renders
	// the inline indexes. A name-only roster matched the wrong one and reported
	// the check missing while it was present, so the receiver is part of the key.
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

	type site struct{ rendersList, writesDDL, checks bool }
	discovered := map[string]*site{}

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		s := &site{}
		ast.Inspect(fn, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.CallExpr:
				if id, ok := v.Fun.(*ast.Ident); ok {
					switch {
					case id.Name == "checkIndexPredicate":
						s.checks = true
					case renderers[id.Name]:
						s.rendersList = true
					}
				}
			case *ast.BasicLit:
				if v.Kind == token.STRING {
					if lit, err := strconv.Unquote(v.Value); err == nil && ddlKeyword.MatchString(lit) {
						s.writesDDL = true
					}
				}
			}
			return true
		})
		if s.rendersList || s.writesDDL {
			discovered[declName(fn)] = s
		}
	}

	// Anti-vacuity floor. Discovery returning nothing — or losing the two sites
	// S8 was filed for — would let this pass on an empty set forever, which is
	// the failure mode of the version this replaces.
	for _, must := range []string{"emitAddIndexClause", "(mysqlEmitter).emitTableDefWithDomainChecks"} {
		if _, ok := discovered[must]; !ok {
			t.Fatalf("discovery did not find %s in ddl_emit.go. It is a known index emit site, so either it "+
				"was renamed/moved (update the floor) or the discovery signals no longer match how index DDL "+
				"is rendered — in which case this gate is checking nothing.", must)
		}
	}
	if len(discovered) <= len(indexEmitExempt) {
		t.Fatalf("discovery found %d functions and %d are exempt, leaving nothing checked; the discovery "+
			"rule has stopped working", len(discovered), len(indexEmitExempt))
	}

	for name, s := range discovered {
		if reason, ok := indexEmitExempt[name]; ok {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is exempt with an EMPTY reason; an exemption without a reason is "+
					"indistinguishable from an oversight", name)
			}
			continue
		}
		if s.checks {
			continue
		}
		how := "renders an index column list"
		if s.writesDDL {
			how = "writes index DDL directly"
		}
		t.Errorf("%s does not call checkIndexPredicate, but it %s.\n\n"+
			"MySQL has no partial-index syntax, so an index emitted from here drops any WHERE clause. "+
			"On a UNIQUE key that makes the target STRICTER than the source and it refuses rows the "+
			"source holds legally. Call checkIndexPredicate before rendering, or — if this site "+
			"genuinely cannot receive a partial index — add it to indexEmitExempt with the reason.",
			name, how)
	}
}
