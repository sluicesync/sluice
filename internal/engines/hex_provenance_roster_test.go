// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The fail-by-default roster over every hex-decode site in an engine
// package — the durable half of audit 2026-08-05 finding B-2.
//
// # Why a roster and not another table test
//
// B-2 and roadmap item 135 are the same defect twice: a decoder deciding
// whether bytes are a HEX RENDERING or the bytes themselves by looking at
// the content. `\xdead` is both, so whichever reading the sniff picks, the
// other one is silently corrupted — six bytes become two, and the bare
// two-byte `\x` becomes ZERO, a value that goes EMPTY rather than wrong.
// It was fixed in the Postgres decoder (item 135) and then found again on
// the MySQL writer (B-2), because nothing enumerated the siblings.
//
// So the gate is not "does the bytea decoder still work" — that is pinned
// elsewhere, per-lane, against real servers. The gate is: **a new
// hex-decode call in any engine package fails the build until someone
// states how that site decides which reading it is looking at.** That is
// the property whose absence let the same bug ship twice.
//
// # What it reaches, stated rather than implied
//
// It walks `internal/engines/**` non-test sources and finds calls to
// `hex.Decode` / `hex.DecodeString` / `decodeHexByteaText`, keyed by
// package + enclosing function. That is:
//
//   - every ENGINE value lane, which is the whole population that can
//     turn a stored value into different bytes;
//   - NOT `internal/pipeline` (lineage signing hex-decodes a MAC, whose
//     grammar is fixed and whose failure is loud), NOT
//     `internal/rowpredicate` (a `--where` literal, decided by the SQL
//     lexer's token kind), and NOT hex ENCODING anywhere. Those are
//     deliberate exclusions, not oversights: none of them is a place
//     where a stored value's reading is in question.
//
// # Two ways this roster USED to be narrower than its name, both closed
// (audit backlog B-2e, closed 2026-08-22; originally stated here rather
// than left implied, because a gate whose coverage is narrower than its
// name is worse than no gate)
//
//   - It was FUNCTION-keyed with first-occurrence dedup, so a SECOND
//     hex-decode added inside an already-blessed function was
//     auto-blessed by the entry covering the first. Now every matched
//     CALL is recorded: a function with one call keys as before
//     ("pkg.func"), and a function with N>1 calls keys each site
//     independently as "pkg.func#1".."pkg.func#N" (source order across
//     the package's files, which [filepath.WalkDir] visits
//     lexically) — so adding a second decode to a blessed function
//     produces an unrostered "#2" key AND makes the un-suffixed entry
//     stale, failing in both directions. Reordering or same-named
//     methods on different receivers renumber and force a re-grade,
//     which is the safe direction. Pinned by
//     [TestFindHexDecodeSites_WalkerCapabilities].
//   - [isHexDecodeCall] required the selector base to be the literal
//     identifier `hex`, so an aliased import (`import ehex
//     "encoding/hex"`) evaded the walk entirely. The import name is now
//     resolved PER FILE from the file's own import of "encoding/hex"
//     (and only that import — a foreign package aliased AS `hex` no
//     longer matches), and a dot-import of encoding/hex fails the walk
//     loudly instead of silently hiding every bare Decode call. Also
//     pinned by [TestFindHexDecodeSites_WalkerCapabilities].
//
// A site's verdict is one of three, and each names the thing that DECIDES
// the reading — never the content:
//
//	decidedByProvenance — a caller-declared lane (item 135's mechanism).
//	decidedByGrammar    — the surrounding format already told us which
//	                      it is (a lexer token kind, a wire format byte,
//	                      a `typeof` tag). No value can be both.
//	structurallyTotal   — the two readings provably cannot collide, with
//	                      the argument written down at the site.
package engines

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// hexSiteVerdict is how one site decides which reading it is looking at.
type hexSiteVerdict struct {
	decidedBy string
	reason    string
}

const (
	decidedByProvenance = "provenance"
	decidedByGrammar    = "grammar"
	structurallyTotal   = "structurally-total"
)

// hexDecodeRoster is the fail-by-default classification. The key is
// "<engine package>.<enclosing function>". A site missing from this map
// fails the test; an entry here with no matching site fails it too, so a
// deleted call cannot leave a stale blessing behind.
var hexDecodeRoster = map[string]hexSiteVerdict{
	// ---- Postgres: the family item 135 was about. ----
	"postgres.decodeHexByteaText": {
		decidedByProvenance,
		"the recognizer itself; it is reached ONLY from decodeByteaFromText, i.e. the " +
			"provText lane. decodeByteaFromBinary copies verbatim and never calls it.",
	},
	"postgres.decodeByteaFromText": {
		decidedByProvenance,
		"provText — a pgoutput tuple column or an array-literal leaf. The caller named " +
			"the lane (decodeValueFromText); an unreadable rendering is refused by name.",
	},
	"postgres.byteaArrayLeaf": {
		decidedByProvenance,
		"the pgtrigger change payload only. to_jsonb renders bytea through bytea_output, " +
			"which the capture clause PINS to hex (pgtrigger/setup.go), so the rendering is " +
			"decided at capture; anything else is refused rather than guessed at.",
	},
	"postgres.decodeGeometryHexOrRaw": {
		structurallyTotal,
		"hex-EWKB is even-length all-ASCII-hex; raw EWKB begins with a byte-order byte " +
			"that ewkbToWKB accepts only as 0x00/0x01, and neither is an ASCII hex digit. " +
			"Pinned by TestGeometryHexDiscriminatorIsTotal in the postgres package.",
	},
	"postgres.DecodeValue": {
		decidedByGrammar,
		"pgx hands the codec the wire FORMAT CODE; the hex branch is the text-format arm " +
			"and the binary arm copies bytes. The format byte is the protocol's own answer.",
	},
	"postgres.Encode": {
		decidedByGrammar,
		"encodePlanGeometryBinaryString is selected by pgx for a `string` value, so the " +
			"input is hex-EWKB by the plan's own type dispatch, not by inspection.",
	},

	// ---- MySQL: the sibling B-2 named. ----
	"mysql.decodeHexByteaText": {
		decidedByProvenance,
		"the recognizer; its one caller (prepareValue) gates it on columnIsNativelyBinary, " +
			"so a column an override MADE binary never reaches it.",
	},
	"mysql.prepareValue": {
		decidedByProvenance,
		"ir.Column.SourceColumnType decides whether a string on a binary column is PG's " +
			"bytea rendering or the source's own bytes (audit B-2). Pinned by " +
			"TestPrepareValue_ByteaProvenanceMatrix + TestByteaProvenance_MySQLWriteCores.",
	},
	"mysql.arrayLeafForJSON": {
		decidedByProvenance,
		"the ARRAY leaf, and the lane is decided by the leaf's Go TYPE, not its content: the " +
			"SQL doors (row scan, pgoutput) hand a bytea element back as []byte, which is " +
			"copied verbatim and never inspected, while ONLY the pgtrigger change payload " +
			"delivers a string — and to_jsonb renders bytea through bytea_output, which the " +
			"capture clause PINS to hex (pgtrigger/setup.go). So a string here is a rendering " +
			"by construction; anything that is not `\\x`+even-hex is refused rather than " +
			"guessed at. Same discipline as postgres.byteaArrayLeaf, which this mirrors on " +
			"the other target. Pinned by " +
			"TestArrayLeafForJSON_TriggerDoorByteaAgreesWithTheSQLDoor (the two doors must " +
			"produce the SAME base64) and the refusal rows of TestArrayLeafForJSON_LoudRefusals.",
	},
	"mysql.decodeMySQLHexToken": {
		decidedByGrammar,
		"a DDL DEFAULT token from SHOW CREATE TABLE, in a grammar position where a `0x` " +
			"prefix can only be MySQL's hex literal. Not a row value, and the alternative " +
			"reading (a quoted string) is a different token the caller dispatches on first.",
	},

	// ---- Dump / file engines. ----
	"mydumper.scanBareHexValue": {
		decidedByGrammar,
		"the INSERT lexer is AT a `0x` literal by dispatch; SQL has no other production " +
			"there, so the token kind (litHex) comes from the grammar, never from the bytes.",
	},
	"mydumper.scanQuotedHexValue": {
		decidedByGrammar,
		"same lexer, the `x'...'` production — token kind, not content.",
	},
	"sqlite.d1StorageValue": {
		decidedByGrammar,
		"the D1 projection returns `typeof(c)` alongside the value; the hex branch is taken " +
			"only for the literal tag \"blob\". An unrecognised tag is refused loudly.",
	},
}

// hexDecodeSiteFloor is the anti-vacuity floor. A walker that silently
// stops finding sites (a parse failure, a moved directory, a renamed
// import) would otherwise report a clean roster — the shape that makes a
// gate defend the defect. The floor is deliberately below the current
// count so an ordinary deletion does not fail the build, but far enough
// above zero that a broken walker cannot pass.
const hexDecodeSiteFloor = 9

// hexDecodeEnginesFloor requires the walk to reach at least these
// packages. B-2 existed because the Postgres fix did not look at MySQL;
// a roster that covered one engine would have the same blind spot.
var hexDecodeEnginesFloor = []string{"postgres", "mysql"}

// TestHexDecodeSitesAreProvenanceDecided is the roster gate.
func TestHexDecodeSitesAreProvenanceDecided(t *testing.T) {
	found, err := findHexDecodeSites(".")
	if err != nil {
		t.Fatalf("walk engine sources: %v", err)
	}

	if len(found) < hexDecodeSiteFloor {
		t.Fatalf(
			"discovered only %d hex-decode sites (floor %d) — the walker is broken, not the tree. "+
				"Found: %v", len(found), hexDecodeSiteFloor, sortedKeys(found),
		)
	}
	for _, pkg := range hexDecodeEnginesFloor {
		if !anyKeyInPackage(found, pkg) {
			t.Fatalf("no hex-decode site found in the %q engine; the walker is not reaching it", pkg)
		}
	}

	for _, key := range sortedKeys(found) {
		verdict, listed := hexDecodeRoster[key]
		if !listed {
			t.Errorf(
				"%s (%s) hex-decodes and is NOT on the roster.\n"+
					"A hex-decode is a place where bytes could be read two ways — that is exactly the\n"+
					"ambiguity item 135 / audit B-2 were about (`\\xdead` is both a 2-byte rendering and\n"+
					"6 ordinary bytes; a bare `\\x` decodes to ZERO). Add an entry to hexDecodeRoster\n"+
					"naming what DECIDES the reading — provenance, grammar, or a written structural\n"+
					"argument. Do not add one that says the content is inspected.",
				key, found[key],
			)
			continue
		}
		switch verdict.decidedBy {
		case decidedByProvenance, decidedByGrammar, structurallyTotal:
		default:
			t.Errorf("%s: unknown verdict %q", key, verdict.decidedBy)
		}
		if strings.TrimSpace(verdict.reason) == "" {
			t.Errorf("%s: roster entry has no reason; the reason IS the gate", key)
		}
	}

	for key := range hexDecodeRoster {
		if _, ok := found[key]; !ok {
			t.Errorf(
				"roster lists %s but no such hex-decode site exists any more — remove the entry. "+
					"A stale blessing is how a roster starts covering less than its name implies.", key,
			)
		}
	}
}

// findHexDecodeSites walks root for non-test Go sources and returns one
// key per CALL SITE of hex.Decode / hex.DecodeString /
// decodeHexByteaText: "<package dir>.<enclosing func>" when the
// function contains exactly one matched call, and
// "<package dir>.<enclosing func>#N" (1-based, source order) when it
// contains more — so every site is graded independently and a second
// decode added to a blessed function cannot inherit its verdict
// (audit backlog B-2e).
//
// Keyed by the DIRECTORY name rather than the package clause so the
// `-trigger` engines (whose package names differ from their dirs) key
// readably; every engine lives in its own directory.
//
// The `hex` selector is resolved against each FILE's own import of
// "encoding/hex" — default name or alias — so an aliased import cannot
// evade the walk, and an unrelated package imported AS `hex` does not
// false-match. A dot-import of encoding/hex is refused loudly: it makes
// every Decode call a bare identifier this walk cannot attribute.
func findHexDecodeSites(root string) (map[string]string, error) {
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
		hexName, dotImported := localHexImportName(file)
		if dotImported {
			return fmt.Errorf(
				"%s dot-imports encoding/hex, which turns every Decode call into a bare identifier "+
					"this provenance walk cannot attribute — use a named import", path,
			)
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			var positions []token.Position
			ast.Inspect(fn.Body, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				if !isHexDecodeCall(call.Fun, hexName) {
					return true
				}
				positions = append(positions, fset.Position(call.Pos()))
				return true
			})
			base := pkgDir + "." + fn.Name.Name
			for i, pos := range positions {
				key := base
				if len(positions) > 1 {
					key = fmt.Sprintf("%s#%d", base, i+1)
				}
				out[key] = fmt.Sprintf("%s:%d", filepath.ToSlash(path), pos.Line)
			}
			return true
		})
		return nil
	})
	return out, err
}

// localHexImportName returns the identifier "encoding/hex" is bound to
// in this file ("" when the file does not import it) and whether it is
// dot-imported. The blank import (`_`) returns "" like a missing import:
// it binds no identifier a call could go through.
func localHexImportName(file *ast.File) (name string, dotImported bool) {
	for _, imp := range file.Imports {
		if imp.Path == nil || imp.Path.Value != `"encoding/hex"` {
			continue
		}
		if imp.Name == nil {
			return "hex", false
		}
		switch imp.Name.Name {
		case ".":
			return "", true
		case "_":
			return "", false
		default:
			return imp.Name.Name, false
		}
	}
	return "", false
}

// isHexDecodeCall reports whether the call target is one of the three
// forms that turn hex text into bytes, with hexName the file-local name
// of the encoding/hex import ("" when the file does not import it).
// hex.EncodeToString and hex.DecodedLen are deliberately NOT matched —
// encoding is never ambiguous, and DecodedLen only sizes a buffer for a
// Decode that is itself matched.
func isHexDecodeCall(fun ast.Expr, hexName string) bool {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		pkg, ok := f.X.(*ast.Ident)
		if !ok || hexName == "" || pkg.Name != hexName {
			return false
		}
		return f.Sel.Name == "Decode" || f.Sel.Name == "DecodeString"
	case *ast.Ident:
		return f.Name == "decodeHexByteaText"
	}
	return false
}

// TestFindHexDecodeSites_WalkerCapabilities pins the two B-2e holes
// closed against synthetic sources, so the closures cannot regress
// silently even between mutation runs:
//
//   - an ALIASED encoding/hex import is walked (and a foreign package
//     merely NAMED hex is not),
//   - a SECOND decode inside one function gets its own key instead of
//     inheriting the first's blessing,
//   - a dot-import of encoding/hex fails the walk loudly,
//   - and the encode-side forms stay unmatched.
func TestFindHexDecodeSites_WalkerCapabilities(t *testing.T) {
	writeSrc := func(t *testing.T, dir, name, src string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("aliased import is walked", func(t *testing.T) {
		root := t.TempDir()
		writeSrc(t, filepath.Join(root, "enginex"), "a.go", `package enginex

import ehex "encoding/hex"

func decodeAliased(s string) ([]byte, error) { return ehex.DecodeString(s) }
`)
		found, err := findHexDecodeSites(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := found["enginex.decodeAliased"]; !ok {
			t.Fatalf("aliased-import hex.DecodeString was NOT found — the B-2e alias hole is open again. Found: %v", sortedKeys(found))
		}
	})

	t.Run("foreign package named hex does not false-match", func(t *testing.T) {
		root := t.TempDir()
		writeSrc(t, filepath.Join(root, "enginex"), "a.go", `package enginex

import hex "example.com/nothex"

func notADecode(s string) ([]byte, error) { return hex.DecodeString(s) }
`)
		found, err := findHexDecodeSites(root)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 0 {
			t.Fatalf("a foreign package aliased AS hex was matched: %v", sortedKeys(found))
		}
	})

	t.Run("second decode in one function keys independently", func(t *testing.T) {
		root := t.TempDir()
		writeSrc(t, filepath.Join(root, "enginex"), "a.go", `package enginex

import "encoding/hex"

func decodeTwice(a, b string) ([]byte, []byte) {
	x, _ := hex.DecodeString(a)
	y, _ := hex.DecodeString(b)
	return x, y
}
`)
		found, err := findHexDecodeSites(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := found["enginex.decodeTwice#1"]; !ok {
			t.Fatalf("first call of a two-call function not keyed #1. Found: %v", sortedKeys(found))
		}
		if _, ok := found["enginex.decodeTwice#2"]; !ok {
			t.Fatalf("SECOND call of a two-call function has no key of its own — the B-2e dedup hole is open again. Found: %v", sortedKeys(found))
		}
		if _, ok := found["enginex.decodeTwice"]; ok {
			t.Fatalf("a two-call function still carries the un-suffixed key, which a stale single-verdict entry could bless. Found: %v", sortedKeys(found))
		}
		if found["enginex.decodeTwice#1"] == found["enginex.decodeTwice#2"] {
			t.Fatalf("both ordinals point at the same position %q", found["enginex.decodeTwice#1"])
		}
	})

	t.Run("single call keeps the un-suffixed key", func(t *testing.T) {
		root := t.TempDir()
		writeSrc(t, filepath.Join(root, "enginex"), "a.go", `package enginex

import "encoding/hex"

func decodeOnce(s string) ([]byte, error) { return hex.DecodeString(s) }
`)
		found, err := findHexDecodeSites(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := found["enginex.decodeOnce"]; !ok {
			t.Fatalf("single-call function no longer keys as pkg.func — every existing roster entry would go stale. Found: %v", sortedKeys(found))
		}
	})

	t.Run("dot-import fails loudly", func(t *testing.T) {
		root := t.TempDir()
		writeSrc(t, filepath.Join(root, "enginex"), "a.go", `package enginex

import . "encoding/hex"

func decodeDotted(s string) ([]byte, error) { return DecodeString(s) }
`)
		if _, err := findHexDecodeSites(root); err == nil {
			t.Fatal("a dot-import of encoding/hex walked clean — its bare Decode calls are invisible to this gate and it must refuse instead")
		}
	})

	t.Run("encode-side forms stay unmatched", func(t *testing.T) {
		root := t.TempDir()
		writeSrc(t, filepath.Join(root, "enginex"), "a.go", `package enginex

import "encoding/hex"

func encodeOnly(b []byte) (string, int) { return hex.EncodeToString(b), hex.DecodedLen(4) }
`)
		found, err := findHexDecodeSites(root)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 0 {
			t.Fatalf("an encode-side form was matched: %v", sortedKeys(found))
		}
	})
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func anyKeyInPackage(m map[string]string, pkg string) bool {
	for k := range m {
		if strings.HasPrefix(k, pkg+".") {
			return true
		}
	}
	return false
}
