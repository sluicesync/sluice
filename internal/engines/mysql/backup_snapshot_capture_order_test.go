// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit B-4 / B-4b: the snapshot-vs-anchor capture ORDER, as a roster over
// every site in this package rather than a pin on one of them.
//
// B-4 fixed the cold-start lane. B-4b was the SAME defect, in the same
// package, in the function every FTWRL failure falls back to — and it
// survived the B-4 change because the fix was pinned per-site. A per-site
// pin cannot fail for a site it does not name, which is exactly how the
// second one stayed open with a comment next to it saying so.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth: functions in `internal/engines/mysql` that BOTH open an
// InnoDB consistent-snapshot transaction AND capture a binlog position.
// It does NOT reach:
//
//   - the VStream backup/cold-start lane (backup_snapshot_vstream.go) —
//     vtgate's COPY carries the position in the stream itself; there are no
//     two statements to order, and the strings there are comments;
//   - Postgres (postgres/backup_snapshot.go) — `CREATE_REPLICATION_SLOT …
//     EXPORT_SNAPSHOT` returns the consistent_point LSN and the snapshot
//     name from ONE command, so the ordering algebra does not exist there;
//   - the trigger-CDC engines — no binlog, no consistent-snapshot tx.

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// consistentSnapshotStmt is the statement that freezes the read view. A
// function containing it (directly or through a helper) is opening a
// snapshot.
const consistentSnapshotStmt = "START TRANSACTION WITH CONSISTENT SNAPSHOT"

// positionCaptureSeeds are the package's two position readers. They are
// SEEDS, not the roster: the walker closes over their callers so a capture
// moved behind a new wrapper is still seen as a capture. Both must exist as
// declared functions or the walker has gone stale.
var positionCaptureSeeds = []string{"snapshotMasterStatus", "captureBackupPosition"}

// nonAnchorCaptures read a position that never becomes the recorded anchor.
// They must be named, because the walker cannot tell a probe from an anchor
// by shape and grading a probe is exactly how this gate first passed a
// mutant: the churn probe's near edge sits before the snapshot open by
// construction, so counting it as "the capture" makes the real anchor's
// order invisible. Each name must resolve to a declared function — a stale
// entry silently re-opens that hole.
var nonAnchorCaptures = map[string]string{
	"readBinlogTip": "reads the two EDGES of the unlocked capture window for [reportBackupCaptureWindow]; the " +
		"anchor is whatever captureBackupPosition/snapshotMasterStatus returns, and the probe's near edge " +
		"precedes the snapshot open by construction",
}

// ftwrlStmt / unlockStmt are the two edges of the write freeze. A site
// that captures AFTER opening its snapshot is safe only while both edges
// bracket the capture, so the walker has to be able to see them.
const (
	ftwrlStmt  = "FLUSH TABLES WITH READ LOCK"
	unlockStmt = "UNLOCK TABLES"
)

// ftwrlBracketed lists the sites whose capture may follow their snapshot
// open, each with the reason. It is NOT an exemption: a listed site is
// GRADED on the reason itself (the capture must sit between this
// function's own FTWRL and its UNLOCK, in source order — [gradeBracket]),
// because every entry here says "the order cannot matter, because the
// write freeze is held across both" and that clause is the whole safety
// argument. A free-text exemption would leave it asserted and unchecked:
// audit 2026-08-07 confirmed by mutation that moving
// [acquireConsistentSnapshot]'s position read below its UNLOCK TABLES
// left this gate GREEN, which is exactly ADR-0101 §3's "gaplessness is by
// construction" claim resting on nothing.
//
// Fail-by-default in three directions: a site that is not listed here
// MUST capture first; a listed name that the walker no longer discovers
// fails (a stale entry is how a gate quietly stops covering the thing it
// names); and a listed site whose bracket does not hold fails.
var ftwrlBracketed = map[string]string{
	"openBackupSnapshotCoordinated": "captures on R[0] while this function's own FLUSH TABLES WITH READ LOCK is " +
		"held, so no commit can happen between the read views and the anchor — both orders name the same cut " +
		"(ADR-0088 step 4)",
	"acquireConsistentSnapshot": "captures on conn0 while its own FLUSH TABLES WITH READ LOCK is held; the FTWRL " +
		"is the consistency lynchpin and its failure returns errFTWRLUnavailable rather than proceeding " +
		"unlocked (ADR-0101 §2 step 3, §4)",
}

// TestSnapshotCaptureOrder_EveryUnlockedSiteCapturesBeforeTheSnapshot is
// the B-4/B-4b roster. It derives the site list from the source rather than
// carrying one, so a NEW capture site is covered the day it is written and
// a renamed helper fails the build instead of silently emptying the roster.
//
// The independent expected value is the source's own token order: the gate
// compares AST positions, not anything the code reports about itself. That
// is weaker than the integration pins (which read the server's general log)
// and complements them — those prove two sites really execute in the order
// the source implies; this one proves no THIRD site was added in the losing
// order.
//
// Mutation-verified in six directions: swapping the anchor capture and
// `beginConsistentSnapshotTx` in openBackupSnapshotSerial fails it; deleting
// B-4's pre-snapshot capture from openBinlogSnapshotStreamShared fails it
// (so the gate reaches the OTHER lane too, which is the whole reason it
// exists); dropping an entry from ftwrlBracketed fails it; a stale
// exemption name fails it; moving acquireConsistentSnapshot's
// snapshotMasterStatus below its UNLOCK TABLES fails it; and deleting the
// FTWRL from a bracketed site fails it. The last two are the 2026-08-07
// widening — before it, both were GREEN.
//
// KNOWN LIMIT, stated rather than implied: it grades SOURCE order, not
// reachability. Guarding the pre-capture behind an always-false condition
// leaves the tokens in place and passes here — that shape is caught by the
// general-log integration pins, which observe what the server was actually
// asked to run (TestBackupSnapshotCapturesPositionBeforeSnapshot for the
// backup lane, TestLockFreeSnapshotCapturesPositionBeforeSnapshot for the
// cold-start floor, and
// TestConcurrentSnapshotCapturesPositionUnderTheLock for the ADR-0101
// bracketed lane this gate can only read statically).
func TestSnapshotCaptureOrder_EveryUnlockedSiteCapturesBeforeTheSnapshot(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	funcs := map[string]*ast.FuncDecl{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			funcs[fn.Name.Name] = fn
		}
	}
	if len(funcs) == 0 {
		t.Fatal("parsed no functions from the package — the walker cannot see the source it grades")
	}
	for _, seed := range positionCaptureSeeds {
		if _, ok := funcs[seed]; !ok {
			t.Fatalf("position-capture seed %q is no longer a declared function in this package — the walker is "+
				"stale and would report an empty roster", seed)
		}
	}
	for name := range nonAnchorCaptures {
		if _, ok := funcs[name]; !ok {
			t.Fatalf("nonAnchorCaptures names %q, which is no longer a declared function — a stale entry here "+
				"re-opens the hole it exists to close", name)
		}
	}

	// Direct evidence per function.
	directOpen := map[string]bool{}
	directCapture := map[string]bool{}
	callees := map[string]map[string]bool{}
	for name, fn := range funcs {
		calls := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.BasicLit:
				if v.Kind == token.STRING && strings.Contains(v.Value, consistentSnapshotStmt) {
					directOpen[name] = true
				}
			case *ast.CallExpr:
				if id, ok := v.Fun.(*ast.Ident); ok {
					calls[id.Name] = true
				}
			}
			return true
		})
		callees[name] = calls
	}
	for _, seed := range positionCaptureSeeds {
		directCapture[seed] = true
		for name, calls := range callees {
			if calls[seed] {
				directCapture[name] = true
			}
		}
	}
	for name := range nonAnchorCaptures {
		delete(directCapture, name)
	}

	// A pure HELPER carries one marker and not the other: calling it is
	// evidence of that marker at the call site. A function carrying BOTH is
	// a site in its own right and is deliberately NOT transparent, so a
	// dispatcher that merely calls two real sites doesn't inherit both
	// markers and get graded on a meaningless statement order.
	notAnAnchor := map[string]bool{}
	for name := range directOpen {
		notAnAnchor[name] = true
	}
	for name := range nonAnchorCaptures {
		notAnAnchor[name] = true
	}
	openHelpers := helperClosure(directOpen, directCapture, callees)
	captureHelpers := helperClosure(directCapture, notAnAnchor, callees)

	candidates := map[string]bool{}
	for name := range funcs {
		opens := directOpen[name] || callsAny(callees[name], openHelpers)
		captures := directCapture[name] || callsAny(callees[name], captureHelpers)
		if opens && captures {
			candidates[name] = true
		}
	}
	sites := []string{}
	for name := range candidates {
		inner := false
		for callee := range callees[name] {
			if candidates[callee] {
				inner = true
				break
			}
		}
		if !inner {
			sites = append(sites, name)
		}
	}

	// Anti-vacuity floor. A walker that stops matching must fail loudly
	// rather than pass on an empty roster — and a roster that is entirely
	// exempt proves nothing either, so both halves have a floor.
	const wantSites, wantChecked = 4, 2
	if len(sites) < wantSites {
		t.Fatalf("discovered only %d snapshot capture sites (%v); expected at least %d "+
			"(openBackupSnapshotSerial, openBackupSnapshotCoordinated, openBinlogSnapshotStreamShared, "+
			"acquireConsistentSnapshot). The walker has stopped matching and this gate is vacuous",
			len(sites), sites, wantSites)
	}

	checked, bracketed := 0, 0
	for _, name := range sites {
		fn := funcs[name]
		if reason, ok := ftwrlBracketed[name]; ok {
			bracketed++
			gradeBracket(t, fset, name, reason, fn, earliestMarker(fn, false, captureHelpers, fset, false))
			continue
		}
		checked++
		openPos := earliestMarker(fn, directOpen[name], openHelpers, fset, true)
		capturePos := earliestMarker(fn, false, captureHelpers, fset, false)
		if openPos < 0 || capturePos < 0 {
			t.Errorf("%s: could not locate both markers (open line %d, capture line %d) — the gate cannot grade "+
				"a site it cannot read", name, openPos, capturePos)
			continue
		}
		if capturePos > openPos {
			t.Errorf("%s: the binlog position is captured at line %d, AFTER the consistent snapshot opens at "+
				"line %d. That is the ordering where a commit inside the capture window is above the read view "+
				"and below the recorded position, so it lands in NEITHER — audit B-4/B-4b's silent-loss case. "+
				"Capture first, or add %q to ftwrlBracketed with the reason the order cannot matter (today "+
				"the only such reason is holding FLUSH TABLES WITH READ LOCK across both — and it is GRADED, "+
				"not taken on trust)",
				name, capturePos, openPos, name)
		}
	}
	if checked < wantChecked {
		t.Fatalf("only %d of %d discovered sites were graded capture-first; the rest are FTWRL-bracketed. A roster "+
			"that grades everything by the weaker rule is not a gate", checked, len(sites))
	}
	if bracketed != len(ftwrlBracketed) {
		t.Errorf("graded %d FTWRL brackets but ftwrlBracketed lists %d — the bracket half of this gate is "+
			"partly vacuous", bracketed, len(ftwrlBracketed))
	}
	for name := range ftwrlBracketed {
		if !candidates[name] {
			t.Errorf("ftwrlBracketed names %q, which the walker no longer discovers as a capture site. "+
				"Either it was renamed/removed (drop the entry) or the walker stopped matching it (fix the "+
				"walker) — a stale entry silently shrinks this gate", name)
		}
	}
}

// gradeBracket checks the ONE reason a site is allowed to capture AFTER it
// opens its snapshot: this function's own write freeze is held across both,
// so no commit can land between the read views and the recorded position.
// The capture must sit strictly between a FLUSH TABLES WITH READ LOCK
// literal and an UNLOCK TABLES literal, in source order.
//
// Missing edges FAIL. A bracket the walker cannot see is not a bracket, and
// the two shapes that produce a missing edge are precisely the two that
// break the argument: the capture moved below the UNLOCK (no closing edge
// after it), and the FTWRL removed or made conditional-and-continue (no
// opening edge before it). Both were green under the previous free-text
// exemption.
func gradeBracket(t *testing.T, fset *token.FileSet, name, reason string, fn *ast.FuncDecl, capturePos int) {
	t.Helper()
	if capturePos < 0 {
		t.Errorf("%s: listed in ftwrlBracketed, but the walker cannot locate its position capture — it cannot "+
			"grade a bracket it cannot read", name)
		return
	}
	lockBefore, unlockAfter := -1, -1
	for _, l := range literalLines(fn, ftwrlStmt, fset) {
		if l < capturePos && (lockBefore < 0 || l > lockBefore) {
			lockBefore = l
		}
	}
	for _, l := range literalLines(fn, unlockStmt, fset) {
		if l > capturePos && (unlockAfter < 0 || l < unlockAfter) {
			unlockAfter = l
		}
	}
	if lockBefore < 0 || unlockAfter < 0 {
		t.Errorf("%s: captures its position at line %d, and its stated reason for capturing after the snapshot "+
			"opens is %q — but the write freeze does NOT bracket the capture (%s at line %d, %s at line %d; "+
			"want both, with the lock BEFORE and the unlock AFTER). Outside the freeze, a commit between the "+
			"read views and the recorded position is above the views and below the position, so it lands in "+
			"NEITHER — ADR-0101 §3's gaplessness argument is exactly this bracket and nothing else",
			name, capturePos, reason, ftwrlStmt, lockBefore, unlockStmt, unlockAfter)
		return
	}
	t.Logf("bracketed: %s — %s at line %d < capture at line %d < %s at line %d",
		name, ftwrlStmt, lockBefore, capturePos, unlockStmt, unlockAfter)
}

// literalLines returns the source lines of every string literal in fn that
// contains sub.
func literalLines(fn *ast.FuncDecl, sub string, fset *token.FileSet) []int {
	var out []int
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if v, ok := n.(*ast.BasicLit); ok && v.Kind == token.STRING && strings.Contains(v.Value, sub) {
			out = append(out, fset.Position(v.Pos()).Line)
		}
		return true
	})
	return out
}

// helperClosure returns the functions that carry `marker` and NOT
// `blocked`, transitively: a function joins when it calls a member and does
// not itself carry the opposite marker. Members are transparent — calling
// one is evidence of the marker at the call site.
func helperClosure(marker, blocked map[string]bool, callees map[string]map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range marker {
		if !blocked[k] {
			out[k] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for name, calls := range callees {
			if out[name] || blocked[name] {
				continue
			}
			if callsAny(calls, out) {
				out[name] = true
				changed = true
			}
		}
	}
	return out
}

func callsAny(calls, set map[string]bool) bool {
	for c := range calls {
		if set[c] {
			return true
		}
	}
	return false
}

// earliestMarker returns the source LINE of the first open (wantOpen) or
// capture marker inside fn: the literal itself, or a call to a helper that
// carries only that marker. -1 when the function has none.
func earliestMarker(
	fn *ast.FuncDecl,
	hasLiteral bool,
	helpers map[string]bool,
	fset *token.FileSet,
	wantOpen bool,
) int {
	best := -1
	note := func(p token.Pos) {
		line := fset.Position(p).Line
		if best < 0 || line < best {
			best = line
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BasicLit:
			if wantOpen && hasLiteral && v.Kind == token.STRING && strings.Contains(v.Value, consistentSnapshotStmt) {
				note(v.Pos())
			}
		case *ast.CallExpr:
			if id, ok := v.Fun.(*ast.Ident); ok && helpers[id.Name] {
				note(v.Pos())
			}
		}
		return true
	})
	return best
}
