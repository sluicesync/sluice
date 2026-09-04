// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The AST roster behind [binlogPos.ServerUUID].
//
// WHY THIS EXISTS. ServerUUID is the loud-failure floor for the
// "node replaced / restored from backup" position-loss class: binlog
// FILE NAMES are instance-local and a fresh instance reuses
// mysql-bin.000001 for an unrelated lineage, so the name-only
// [verifyBinlogFilePresent] check false-positives and starts the syncer
// at a byte offset in someone else's binlog. [verifySourceInstanceIdentity]
// turns that into a refusal — but only when the PERSISTED position
// carries a uuid, because it deliberately skips when either side is
// empty.
//
// That skip is what made the field's absence invisible. Three of the
// package's file/pos constructions stamped it and two did not, and the
// two that did not were the BACKUP capturers — whose output becomes
// [irbackup.Manifest.EndPosition], which is exactly what `backup
// incremental` and `sync start --position-from-manifest` resume from. So
// the guard reached three doors of four and missed the one its own
// rationale names. Nothing failed; a cross-instance resume was accepted
// silently and dropped rows at exit 0 (ground-truthed 2026-09-01 on two
// independent MySQL 8.0.46 instances with gtid_mode=OFF, both carrying
// mysql-bin.000001).
//
// SCOPE, stated so the name cannot be read as broader than the truth.
// This gate reaches EVERY `binlogPos` composite literal in this package's
// non-test files — that is the complete universe of constructions of this
// type, because binlogPos is unexported and this package is its only
// author. It asserts a syntactic property (the field is SET), not a
// semantic one (the value is CORRECT): a site that sets
// `ServerUUID: ""` satisfies it. The value's correctness is pinned
// behaviourally by TestBackupPositionStampsServerUUID and
// TestBackupChainResumeRefusesAcrossInstances; this gate's job is only to
// make a NEW capture door unable to join the class silently.
//
// It deliberately does not distinguish persisted from ephemeral
// constructions. One site (the cold-start "from now" position in
// resolveStartPosition) is genuinely ephemeral and stamps the field
// anyway, because a persisted-vs-ephemeral exemption list is precisely
// the kind of hand-maintained roster a future site joins the wrong side
// of — which is the mistake this file exists to prevent, not repeat.

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// filePosUUIDExemptMarker opts a single file/pos construction out of the
// ServerUUID requirement. It must sit on the literal's own line or the
// line immediately above, and it must carry a reason. No site uses it
// today; it exists so that a legitimately-exempt future construction is
// recorded as a DECISION in the code rather than silently absent — and
// so the exemption is counted and reported by this gate instead of
// quietly shrinking its coverage.
const filePosUUIDExemptMarker = "//sluice:filepos-no-uuid"

// filePosUUIDMinSites is the anti-vacuity floor. It exists so a broken
// walker — wrong directory, changed type name, parse failure — fails
// LOUDLY instead of passing with an empty roster, which is the failure
// mode that makes a gate worse than no gate.
//
// FIVE, and that is now the WHOLE universe rather than a margin below
// it. This comment used to say six existed (two backup capturers, the
// per-event emitter, the cold-start resolver, and the two snapshot
// openers) with the floor deliberately set one below, so a consolidation
// would not fail the build. The consolidation then happened: SLM-4
// merged the two snapshot openers into one shared
// snapshotHandoffPosition, and the count became five. The margin did its
// job and then quietly stopped being a margin, which is the state the
// 2026-09-01 audit flagged as "floor 5 < universe 6" — accurate when
// filed, and by the time it was checked the two numbers had met.
//
// Floor EQUAL to the universe is the strongest position available here,
// so it is left there deliberately: any future consolidation now fails
// this test and has to lower the number on purpose, which is the whole
// point — a coverage reduction becomes a decision somebody records
// instead of a margin absorbing it silently. If you are that person:
// lower it, and say in this comment what merged into what.
//
// Verified rather than counted from the comment: the walker reports its
// tally on every run ("checked N file/pos constructions across M
// files"), which is how the drift above was found.
const filePosUUIDMinSites = 5

// filePosUUIDMinFiles guards the other vacuity shape: a walker that
// somehow reads only one file would still clear the site floor if that
// file happened to hold several literals.
const filePosUUIDMinFiles = 4

// TestFilePosPositionsCarryServerUUID_ASTRoster fails when any
// `binlogPos` composite literal in this package's non-test files sets
// Mode to positionModeFilePos without also setting ServerUUID.
func TestFilePosPositionsCarryServerUUID_ASTRoster(t *testing.T) {
	// Files are enumerated and parsed individually rather than via
	// parser.ParseDir (deprecated in Go 1.25): the walker wants EVERY
	// non-test .go file in this directory regardless of build tags, which
	// is exactly the association ParseDir gets wrong and the reason it was
	// deprecated.
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var parsed []*ast.File
	var parsedPaths []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		parsed = append(parsed, f)
		parsedPaths = append(parsedPaths, name)
	}
	if len(parsed) == 0 {
		t.Fatal("anti-vacuity: parsed zero non-test .go files; the walker is looking in the wrong place")
	}

	type site struct {
		file string
		line int
	}
	var (
		checked []site
		missing []site
		exempt  []site
		files   = map[string]bool{}
	)

	// Raw source per file, so the exemption marker can be read off the
	// literal's own line / the line above it. Comment positions inside a
	// composite literal are not attached to the literal by go/ast, so a
	// line scan is the honest way to find them.
	srcLines := map[string][]string{}
	readLines := func(path string) []string {
		if l, ok := srcLines[path]; ok {
			return l
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		l := strings.Split(string(b), "\n")
		srcLines[path] = l
		return l
	}

	for i, f := range parsed {
		path := parsedPaths[i]
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, ok := lit.Type.(*ast.Ident)
			if !ok || id.Name != "binlogPos" {
				return true
			}

			var isFilePos, hasUUID bool
			for _, el := range lit.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				switch key.Name {
				case "Mode":
					if v, ok := kv.Value.(*ast.Ident); ok && v.Name == "positionModeFilePos" {
						isFilePos = true
					}
				case "ServerUUID":
					hasUUID = true
				}
			}
			if !isFilePos {
				return true
			}

			pos := fset.Position(lit.Pos())
			base := filepath.Base(path)
			s := site{file: base, line: pos.Line}

			lines := readLines(path)
			marked := false
			for _, ln := range []int{pos.Line - 1, pos.Line - 2} {
				if ln >= 0 && ln < len(lines) && strings.Contains(lines[ln], filePosUUIDExemptMarker) {
					marked = true
				}
			}
			if marked {
				exempt = append(exempt, s)
				return true
			}

			files[base] = true
			checked = append(checked, s)
			if !hasUUID {
				missing = append(missing, s)
			}
			return true
		})
	}

	for _, m := range missing {
		t.Errorf(
			"%s:%d: binlogPos{Mode: positionModeFilePos, ...} does not set ServerUUID.\n"+
				"  A file/pos position without an instance identity cannot be defended by "+
				"verifySourceInstanceIdentity — it skips when the persisted uuid is empty — so a resume "+
				"against a REPLACED source instance that reuses the same binlog filenames is accepted "+
				"silently and streams from a byte offset in an unrelated lineage.\n"+
				"  Stamp it (backupPositionServerUUID for the capture doors, r.serverUUID inside the CDC "+
				"reader), or, if this construction genuinely cannot carry one, put %q plus a reason on the "+
				"line above.",
			m.file, m.line, filePosUUIDExemptMarker,
		)
	}

	if len(checked) < filePosUUIDMinSites {
		t.Errorf(
			"anti-vacuity: found only %d non-exempt file/pos binlogPos constructions (want >= %d); "+
				"the walker is probably broken (wrong dir, renamed type, or parse failure), not the code. "+
				"Found: %v",
			len(checked), filePosUUIDMinSites, checked,
		)
	}
	if len(files) < filePosUUIDMinFiles {
		t.Errorf(
			"anti-vacuity: non-exempt constructions came from only %d distinct files (want >= %d); "+
				"files seen: %v",
			len(files), filePosUUIDMinFiles, filePosUUIDFileNames(files),
		)
	}
	if len(exempt) > 0 {
		t.Logf("file/pos constructions exempted via %s: %v", filePosUUIDExemptMarker, exempt)
	}
	t.Logf("checked %d file/pos constructions across %d files", len(checked), len(files))
}

// filePosUUIDFileNames renders the walker's file set for the
// anti-vacuity failure message.
func filePosUUIDFileNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
