// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestPublicationExposureSiteRoster derives the call sites of
// warnPublicationExposure from the AST and requires each to name its site
// explicitly.
//
// WHY THIS EXISTS. The warning had two callers and one vocabulary — the
// backup one. `backup full --chain-slot` wrote it, then a second door was
// wired to the same function: any StreamChanges whose publication is missing,
// warm resume included. So a plain `sync start` printed "a chain slot needs a
// database-wide publication" and "--chain-slot keeps the publication after the
// run" to an operator who had run no backup, and never named
// `sluice sync decommission`, which is the only thing that actually retires a
// stream's publication. Filed as Bug 269 by the v0.141.1 regression cycle,
// after v0.141.1's own notes claimed BOTH warnings named that remedy. One did.
//
// This is the sibling-miss shape CLAUDE.md calls "a door that MOVED": the
// enumeration owed was not "which engines" but "which call PATHS reach this,
// and does the message still fit each one?" Nobody asked, because adding a
// caller to an existing helper does not feel like moving a door.
//
// WHAT THIS REACHES, precisely, because a gate whose name outruns its coverage
// is worse than none: it grades the CALL SITES — that every one passes a
// literal `exposureSite*` constant rather than a variable, a helper's return,
// or a value threaded from elsewhere, so the site is readable at the call and
// a new caller cannot inherit another door's wording by omission. It does NOT
// grade the message TEXT; TestPublicationExposureSiteWordingIsSiteSpecific
// below does that, and the two are complementary — this one cannot see a site
// whose wording is wrong, and that one cannot see a caller that passes the
// wrong site.
func TestPublicationExposureSiteRoster(t *testing.T) {
	const fn = "warnPublicationExposure"

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fset := token.NewFileSet()
	type callSite struct {
		file string
		line int
		arg  string
	}
	var sites []callSite

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || id.Name != fn {
				return true
			}
			// The declaration itself is not a call; only real invocations
			// reach here. Signature: (ctx, db, site, covered).
			s := callSite{file: name, line: fset.Position(call.Pos()).Line, arg: "<missing>"}
			if len(call.Args) >= 3 {
				if argID, isID := call.Args[2].(*ast.Ident); isID {
					s.arg = argID.Name
				} else {
					s.arg = "<not a plain identifier>"
				}
			}
			sites = append(sites, s)
			return true
		})
	}

	// Anti-vacuity: the function was renamed, inlined, or the walk broke.
	// Either way "every call site is classified" would be trivially true.
	if len(sites) < 2 {
		t.Fatalf("found only %d call site(s) of %s; the roster derives its universe from the AST, so a "+
			"rename or an inline makes this gate vacuous rather than failing — re-anchor it", len(sites), fn)
	}

	valid := map[string]bool{
		"exposureSiteBackupChainSlot": true,
		"exposureSiteStreamOpen":      true,
	}
	seen := map[string]bool{}
	for _, s := range sites {
		if !valid[s.arg] {
			t.Errorf("%s:%d passes %q as the exposure site; it must be one of the declared "+
				"exposureSite* constants, written literally at the call.\n\n"+
				"A site threaded in from elsewhere is exactly how the backup wording reached the stream "+
				"door (Bug 269) — the call read as if it had no opinion, so nobody noticed it had "+
				"inherited one.", s.file, s.line, s.arg)
			continue
		}
		seen[s.arg] = true
	}

	// Both doors must still be represented. If a future change routes every
	// caller through one site, the wording split this gate protects has
	// silently collapsed back to the single-vocabulary state it came from.
	if len(seen) < 2 {
		t.Errorf("all %d call sites use the same exposure site %v; the two doors this split exists for "+
			"(backup --chain-slot, and a stream recreating a missing publication) are no longer distinct",
			len(sites), seen)
	}
}

// TestPublicationExposureSiteWordingIsSiteSpecific grades the MESSAGE, which
// the roster above deliberately does not.
//
// The defect Bug 269 records is not that a call site was unclassified — there
// were no sites to be unclassified. It is that one message served two doors
// and named the wrong one. So the roster alone would not have caught it, and
// this is the half that would.
func TestPublicationExposureSiteWordingIsSiteSpecific(t *testing.T) {
	stream := exposureSiteStreamOpen
	backup := exposureSiteBackupChainSlot

	// The stream door must not speak backup vocabulary. These are the exact
	// phrases an operator saw during a plain `sync start`.
	for _, bad := range []string{"chain slot", "--chain-slot", "chain's incrementals"} {
		if strings.Contains(stream.why(), bad) || strings.Contains(stream.remedy(), bad) {
			t.Errorf("the stream-open message contains backup vocabulary %q — this is Bug 269 verbatim: "+
				"an operator who ran no backup is told about a flag they did not pass", bad)
		}
	}

	// Only the stream door can honestly name the stream remedy, and it must.
	if !strings.Contains(stream.remedy(), "sluice sync decommission") {
		t.Error("the stream-open remedy does not name `sluice sync decommission`, which is the only " +
			"thing that retires a stream's slot and publication together — v0.141.1's notes claimed it did")
	}
	if strings.Contains(backup.remedy(), "sluice sync decommission") {
		t.Error("the backup remedy names `sluice sync decommission`, which does not apply to a backup's " +
			"chain publication — a remedy that cannot work is worse than none mid-incident")
	}

	// Bug 270: both doors described the post-drop resume as a certainty. It
	// is a fork, and the QUIET branch is the dangerous one, so neither
	// message may assert only the loud outcome.
	for label, r := range map[string]string{"stream": stream.remedy(), "backup": backup.remedy()} {
		if !strings.Contains(r, "widen") {
			t.Errorf("the %s remedy does not mention that dropping the publication can silently WIDEN it "+
				"(Bug 270). Saying only that the stream wedges states the loud outcome as the whole "+
				"truth, and hides the branch where the resume comes back green and database-wide", label)
		}
	}

	// Both doors must still refuse the same core advice.
	for label, r := range map[string]string{"stream": stream.remedy(), "backup": backup.remedy()} {
		if !strings.Contains(r, "REPLICA IDENTITY FULL") {
			t.Errorf("the %s remedy dropped the actual fix (give the table a key)", label)
		}
	}
}
