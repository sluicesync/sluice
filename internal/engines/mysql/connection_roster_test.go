// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestMySQLConnectionRoster_EveryPoolGoesThroughOpenDB is the sibling roster
// for the session invariants [openDB] establishes — the injected sql_mode
// (strict + NO_AUTO_VALUE_ON_ZERO), the vstream-param strip, the
// multi-statement hardening and [sessionInvariantsConnector]'s time_zone /
// sql_mode checks. Every one of those is "leak-proof against future Open*
// paths" only while openDB really is the one place a MySQL pool is built,
// and that is a claim about the WHOLE TREE, so this walks the whole tree
// rather than trusting a list.
//
// The universe, derived from the AST of every non-test .go file under
// internal/ and cmd/ (import aliases resolved per file):
//
//   - every call to go-sql-driver's NewConnector;
//   - every call to database/sql's OpenDB (a connector of any driver: the
//     gate cannot see the connector's driver, so every site is classified);
//   - every call to database/sql's Open whose driver name is "mysql" or is
//     not a string literal (a computed name could be "mysql").
//
// Each site must be openDB itself or carry an exemption below with its
// reason. openDB's own sql.OpenDB call must wrap the connector in
// sessionInvariantsConnector, or the post-connect checks are bypassed for
// every pool at once. Anti-vacuity: openDB's two sites must be found (a
// walker that finds nothing passes nothing), and a stale exemption is a
// finding.
//
// Reach, stated: database/sql pools. The go-mysql binlog syncer
// (replication.NewBinlogSyncer, the binlog CDC reader) is not a pool and
// carries no write — it streams the source's binlog — so it is out of this
// gate's universe by kind.
func TestMySQLConnectionRoster_EveryPoolGoesThroughOpenDB(t *testing.T) {
	root := repoRootForRoster(t)
	exempt := map[string]string{
		"internal/planetscale/expandcontract/ddl_exec.go:execBranchDDL": "connects to a just-minted PlanetScale " +
			"branch credential to run the operator's own --expand-ddl/--contract-ddl text; no row value " +
			"sluice carries crosses it, so neither the strict sql_mode nor NO_AUTO_VALUE_ON_ZERO applies",
	}
	const chokepoint = "internal/engines/mysql/connect.go:openDB"

	var findings []string
	seen := map[string]int{}
	chokeWrapped := false
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); name == "testdata" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return perr
			}
			sqlAlias, driverAlias := "", ""
			for _, imp := range f.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				name := ""
				if imp.Name != nil {
					name = imp.Name.Name
				}
				switch p {
				case "database/sql":
					sqlAlias = orDefault(name, "sql")
				case "github.com/go-sql-driver/mysql":
					driverAlias = orDefault(name, "mysql")
				}
			}
			if sqlAlias == "" && driverAlias == "" {
				return nil
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				site := rel + ":" + fn.Name.Name
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					x, ok := sel.X.(*ast.Ident)
					if !ok {
						return true
					}
					var kind string
					switch {
					case driverAlias != "" && x.Name == driverAlias && sel.Sel.Name == "NewConnector":
						kind = "mysql.NewConnector"
					case sqlAlias != "" && x.Name == sqlAlias && sel.Sel.Name == "OpenDB":
						kind = "sql.OpenDB"
						if site == chokepoint && len(call.Args) == 1 {
							if lit, ok := call.Args[0].(*ast.CompositeLit); ok {
								if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "sessionInvariantsConnector" {
									chokeWrapped = true
								}
							}
						}
					case sqlAlias != "" && x.Name == sqlAlias && sel.Sel.Name == "Open" && len(call.Args) > 0:
						lit, isLit := call.Args[0].(*ast.BasicLit)
						if isLit {
							drv, _ := strconv.Unquote(lit.Value)
							if drv != "mysql" {
								return true
							}
						}
						kind = "sql.Open(mysql or computed driver)"
					default:
						return true
					}
					seen[site]++
					if site == chokepoint {
						return true
					}
					if _, ok := exempt[site]; ok {
						return true
					}
					findings = append(findings, site+" calls "+kind+" at line "+
						strconv.Itoa(fset.Position(call.Pos()).Line))
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	if seen[chokepoint] < 2 {
		t.Fatalf("anti-vacuity: found %d pool-constructor calls in %s, want its NewConnector and OpenDB "+
			"(the walker is blind, or openDB moved)", seen[chokepoint], chokepoint)
	}
	if !chokeWrapped {
		t.Errorf("%s's sql.OpenDB does not wrap the connector in sessionInvariantsConnector{...}: every MySQL "+
			"pool would skip the post-connect time_zone and NO_AUTO_VALUE_ON_ZERO checks", chokepoint)
	}
	for site, reason := range exempt {
		if seen[site] == 0 {
			t.Errorf("stale exemption %s (%s): it no longer constructs a pool; remove the entry", site, reason)
		}
	}
	sort.Strings(findings)
	for _, f := range findings {
		t.Errorf("MySQL pool built outside openDB: %s — route it through openDB so it carries the injected "+
			"sql_mode (including NO_AUTO_VALUE_ON_ZERO) and the session-invariant checks, or exempt it here "+
			"with the reason no carried row value crosses it", f)
	}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func repoRootForRoster(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}
