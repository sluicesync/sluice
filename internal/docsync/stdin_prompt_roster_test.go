// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

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

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Every read of os.Stdin in cmd/sluice is a destructive-confirmation
// prompt, and every one of them must walk through the terminal door first.
//
// # The class
//
// AGENTS.md tells agents that no sluice command blocks on a prompt and
// that without --yes a destructive command refuses loudly. `slot drop`
// was rebuilt to make that true; `trigger teardown` — the un-enumerated
// sibling — kept prompting, and on a pipe read EOF as "no", printed
// "aborted" and returned nil: exit 0 with the trigger engine still
// installed (audit GC-13). The fix is one door, refuseUnlessTerminal in
// cmd/sluice/confirm_door.go, which every prompt site calls before it
// touches stdin. A site that forgets the door is this class reopening,
// and nothing in the compiler notices.
//
// # What this gate grades
//
// It walks every non-test file in cmd/sluice and, for every call whose
// argument subtree contains the os.Stdin selector, requires (1) a roster
// entry naming the site and the exits it produces, and (2) unless the
// entry is the probe itself, a call to refuseUnlessTerminal EARLIER in the
// same function body. The second check is what makes the roster more than
// an allow-list: registering a new site does not excuse it from the door.
//
// The selector is matched STRUCTURALLY — `*ast.SelectorExpr{X: os, Sel:
// Stdin}` anywhere under the argument — not by rendered text. The shared
// exprText helper renders a placeholder for node kinds it has no case for,
// and a text match would be capped at whatever spellings it happens to
// render (the logcapture gate's first draft was blind for exactly that
// reason). Nested spellings such as `os.Stdin.Fd()` are reached because
// the walk descends into the argument.
//
// # Scope, stated so the name is not read as broader than the truth
//
// cmd/sluice, non-test files, os.Stdin passed as (or inside) a call
// argument. A read through an aliased variable (`in := os.Stdin` and then
// `bufio.NewScanner(in)`) is not seen; that spelling does not exist in the
// tree today, and if one appears, widen the matcher here rather than
// filing a second gate beside it. Nothing under internal/ reads stdin.
//
// # The floor
//
// stdinReadReachFloor is the number of os.Stdin argument sites the walk
// found when this landed — three prompt reads (one per friction tier:
// typed `reset`, typed table name, y/N) plus the probe. The census that
// filed GC-13 counted six raw sites; the fix collapsed each tier's copies
// into one function, which is why the universe is small. There is no
// headroom on purpose: every site is a rostered exemption, so removing
// one is a deliberate edit to this file either way (the stale-entry check
// fires too), and the floor's only job is to tell a closed class from a
// blind matcher.
const stdinReadReachFloor = 4

// stdinReadSite is a rostered os.Stdin read: what it asks, and what the
// process exits with when it cannot ask (stdin is not a terminal) or when
// the operator declines on a terminal.
type stdinReadSite struct {
	// probe marks the one site that reads nothing — the isatty probe the
	// door itself is built on. It is exempt from the door-order check
	// because it IS the door.
	probe bool
	// nonTerminalExit is the exit code the site produces on a piped /
	// EOF / CI stdin without --yes. Documentation for the reader; the
	// cmd/sluice tests (TestDestructivePromptDoor) pin the number.
	nonTerminalExit int
	// declineExit is the exit code an operator's "no" on a terminal
	// produces. errConfirmDeclined is the uncoded generic 1.
	declineExit int
	// prompt names the interactive tier the site keeps on a terminal.
	prompt string
}

// stdinReadRoster classifies every os.Stdin argument site in cmd/sluice.
// Keys are "<rel path>::<func>" with a receiver rendered as "(T).m". A
// site absent from this map fails the build; so does a rostered site
// whose function does not call refuseUnlessTerminal before the read.
var stdinReadRoster = map[string]stdinReadSite{
	"cmd/sluice/confirm_door.go::defaultStdinIsTerminal": {
		probe: true, prompt: "none — isatty(os.Stdin.Fd()), the door's environmental probe",
	},
	"cmd/sluice/confirm_door.go::confirmResetTargetData": {
		nonTerminalExit: sluicecode.ExitRefusal, declineExit: sluicecode.ExitFailure,
		prompt: "--reset-target-data: typed 'reset' (ADR-0023); migrate, sync start, sync from-backup run",
	},
	"cmd/sluice/schema_add_table.go::(SchemaAddTableCmd).Run": {
		nonTerminalExit: sluicecode.ExitRefusal, declineExit: sluicecode.ExitFailure, prompt: "typed table name",
	},
	"cmd/sluice/trigger.go::(TriggerTeardownCmd).confirmTeardown": {
		nonTerminalExit: sluicecode.ExitRefusal, declineExit: sluicecode.ExitFailure, prompt: "y/N (the GC-13 site: declined used to exit 0)",
	},
}

// stdinDoorFunc is the door every prompt site must call before reading.
const stdinDoorFunc = "refuseUnlessTerminal"

func TestStdinPromptRoster_EveryReadIsDoored(t *testing.T) {
	sites := discoverStdinReadSites(t)

	if len(sites) < stdinReadReachFloor {
		t.Fatalf("anti-vacuity: the walk found only %d os.Stdin argument sites in cmd/sluice (floor %d) — "+
			"the AST matcher is broken and this gate is blind, or a prompt site was removed; if the latter, "+
			"lower the floor in the same commit and say why", len(sites), stdinReadReachFloor)
	}

	var problems []string
	for key, site := range sites {
		entry, ok := stdinReadRoster[key]
		if !ok {
			problems = append(problems, key+"  ("+site.pos+"): UNROSTERED os.Stdin read")
			continue
		}
		if entry.probe {
			continue
		}
		if !site.doored {
			problems = append(problems, key+"  ("+site.pos+"): reads os.Stdin without calling "+stdinDoorFunc+" first")
		}
		if entry.nonTerminalExit != sluicecode.ExitRefusal {
			problems = append(problems, key+": roster says a non-terminal stdin exits "+strconv.Itoa(entry.nonTerminalExit)+
				"; the door refuses with "+string(sluicecode.CodeConfirmationRequired)+", which is exit "+strconv.Itoa(sluicecode.ExitRefusal))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("destructive-prompt door (audit GC-13):\n  %s\n\n"+
			"AGENTS.md promises that no sluice command blocks on a prompt and that a destructive command "+
			"without --yes refuses loudly. A read of os.Stdin is a prompt; on a pipe, an EOF or a CI runner it "+
			"reads \"\" as a decline, and `trigger teardown` turned that into exit 0 with the trigger engine still "+
			"installed. Every prompt site calls refuseUnlessTerminal(<action>) BEFORE the read (cmd/sluice/"+
			"confirm_door.go) and adds a row to stdinReadRoster naming its exits; the door refuses with "+
			"SLUICE-E-CONFIRMATION-REQUIRED (exit 3) when stdin is not a terminal, and a decline on a terminal "+
			"returns errConfirmDeclined (exit 1), never nil. Then add the site to TestDestructivePromptDoor.",
			strings.Join(problems, "\n  "))
	}

	// The reverse guard: an exemption whose site vanished is a dead
	// classification, and a roster nobody prunes becomes a permanent allow.
	for key := range stdinReadRoster {
		if _, ok := sites[key]; !ok {
			t.Errorf("stale stdinReadRoster entry %q: no such os.Stdin read remains — delete the entry", key)
		}
	}
}

// stdinReadFinding is one os.Stdin argument site: where it is, and
// whether the door was called earlier in the same function body.
type stdinReadFinding struct {
	pos    string
	doored bool
}

// discoverStdinReadSites walks every non-test .go file under cmd/sluice
// and returns the os.Stdin argument sites keyed "<rel>::<func>".
func discoverStdinReadSites(t *testing.T) map[string]stdinReadFinding {
	t.Helper()

	root := repoRootFromDocsync(t)
	sites := map[string]stdinReadFinding{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(filepath.Join(root, "cmd", "sluice"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %q: %v", path, perr)
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)

		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				// A package-level var/const initializer can call too
				// (`var x = f(os.Stdin)`); key it by the file so it is
				// enumerated rather than skipped.
				recordStdinReads(fset, decl, rel+"::(package-level)", nil, sites)
				continue
			}
			fn := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				fn = "(" + receiverTypeName(fd.Recv.List[0].Type) + ")." + fn
			}
			recordStdinReads(fset, fd, rel+"::"+fn, doorCallPositions(fd), sites)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk cmd/sluice: %v", err)
	}
	return sites
}

// recordStdinReads adds every call under node whose argument subtree
// contains the os.Stdin selector. doors holds the positions of the door
// calls in the same function; a read is doored when one precedes it.
func recordStdinReads(fset *token.FileSet, node ast.Node, key string, doors []token.Pos, sites map[string]stdinReadFinding) {
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		hit := false
		for _, arg := range call.Args {
			if containsOSStdin(arg) {
				hit = true
				break
			}
		}
		if !hit {
			return true
		}
		doored := false
		for _, d := range doors {
			if d < call.Pos() {
				doored = true
				break
			}
		}
		if prev, dup := sites[key]; dup {
			// Two reads in one function: the site is doored only if
			// every read is.
			sites[key] = stdinReadFinding{pos: prev.pos, doored: prev.doored && doored}
			return true
		}
		sites[key] = stdinReadFinding{pos: fset.Position(call.Pos()).String(), doored: doored}
		return true
	})
}

// containsOSStdin reports whether the selector os.Stdin appears anywhere
// under e — resolved on the AST, never on rendered text.
func containsOSStdin(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if found {
			return false
		}
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Stdin" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "os" {
			found = true
			return false
		}
		return true
	})
	return found
}

// doorCallPositions returns the positions of every refuseUnlessTerminal
// call in a function body.
func doorCallPositions(fd *ast.FuncDecl) []token.Pos {
	var out []token.Pos
	if fd.Body == nil {
		return out
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == stdinDoorFunc {
			out = append(out, call.Pos())
		}
		return true
	})
	return out
}
