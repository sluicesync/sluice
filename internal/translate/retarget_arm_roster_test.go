// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The retarget rule table's ARM roster, derived from the source rather
// than promised (roadmap item 153).
//
// Every premise check in retarget_provenance_test.go grades the rule
// table through a hand-written probe list, and a hand-written list is
// exactly as complete as whoever last edited it. Item 153 is what that
// costs: `retargetPGtoMySQL` grew no `ir.JSON` arm for two years and no
// probe could have noticed, because the probe list and the rule table
// were written by the same hand at the same time.
//
// This closes the other half — it cannot say an arm is CORRECT, only
// that no arm is UNGRADED. The correctness half is
// TestMigrate_PGToMySQL_RetargetedShapeMatchesTheCatalogReadBack in
// internal/pipeline, which ground-truths every family against a real
// MySQL catalog.

package translate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// retargetRuleFile is the file the roster is derived from. The gate
// fails loudly if the function moves out of it rather than silently
// grading nothing.
const retargetRuleFile = "retarget.go"

// TestRetargetRuleArmsAreAllProbed walks [retargetPGtoMySQL]'s type
// switch and requires every arm to be reachable by some entry of
// [retargetProbes].
//
// # What it reaches, stated so the name cannot be read as broader
//
// The PG→MySQL rule table only — the one table [retargetRuleFor] has.
// A rule table added for a second engine pair is NOT walked, and the
// anti-vacuity floor below will not notice one: a new pair owes this
// gate a second instantiation, which is the finding rather than an
// oversight.
func TestRetargetRuleArmsAreAllProbed(t *testing.T) {
	typeArms, extArms := retargetRuleArms(t)

	// Anti-vacuity: the walk must actually find the switch. A rename or
	// a refactor into helpers would otherwise leave this passing forever
	// on an empty roster.
	if len(typeArms) < 8 {
		t.Fatalf("the walk found only %d type arm(s) in %s::retargetPGtoMySQL; the rule table has been "+
			"restructured and this gate is grading nothing", len(typeArms), retargetRuleFile)
	}
	if len(extArms) < 2 {
		t.Fatalf("the walk found only %d ExtensionType arm(s); the ADR-0032 hstore/citext translators are "+
			"the two that must be there", len(extArms))
	}

	probedTypes := make(map[string]bool, len(retargetProbes))
	probedExtensions := make(map[string]bool)
	for _, p := range retargetProbes {
		probedTypes[fmt.Sprintf("%T", p)] = true
		if ext, isExt := p.(ir.ExtensionType); isExt {
			probedExtensions[ext.Extension] = true
		}
	}

	var missing []string
	for _, arm := range typeArms {
		if !probedTypes[arm] {
			missing = append(missing, arm)
		}
	}
	for _, ext := range extArms {
		if !probedExtensions[ext] {
			missing = append(missing, "ir.ExtensionType{Extension: "+strconv.Quote(ext)+"}")
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("retargetPGtoMySQL has arm(s) no probe reaches: %s.\n\nAdd one entry per arm to "+
			"retargetProbes — an unprobed arm is an unchecked premise for every consumer of the retarget's "+
			"provenance, and an unprobed arm is how roadmap item 153's missing ir.JSON rule went unnoticed",
			strings.Join(missing, ", "))
	}
}

// retargetRuleArms parses the rule file and returns the type names of
// [retargetPGtoMySQL]'s type-switch arms (as `%T` spells them, e.g.
// "ir.UUID") plus the extension names of the nested ExtensionType
// switch's string arms.
func retargetRuleArms(t *testing.T) (typeArms, extensionArms []string) {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, retargetRuleFile, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", retargetRuleFile, err)
	}

	var fn *ast.FuncDecl
	for _, decl := range file.Decls {
		if d, ok := decl.(*ast.FuncDecl); ok && d.Name.Name == "retargetPGtoMySQL" {
			fn = d
		}
	}
	if fn == nil {
		t.Fatalf("%s no longer declares retargetPGtoMySQL; this gate's roster derivation is broken",
			retargetRuleFile)
	}

	ast.Inspect(fn, func(n ast.Node) bool {
		switch sw := n.(type) {
		case *ast.TypeSwitchStmt:
			for _, stmt := range sw.Body.List {
				clause, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range clause.List {
					sel, ok := expr.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					pkg, ok := sel.X.(*ast.Ident)
					if !ok {
						continue
					}
					typeArms = append(typeArms, pkg.Name+"."+sel.Sel.Name)
				}
			}
		case *ast.SwitchStmt:
			// The nested `switch v.Extension` — string cases only.
			for _, stmt := range sw.Body.List {
				clause, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range clause.List {
					lit, ok := expr.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					name, err := strconv.Unquote(lit.Value)
					if err != nil {
						continue
					}
					extensionArms = append(extensionArms, name)
				}
			}
		}
		return true
	})
	return typeArms, extensionArms
}
