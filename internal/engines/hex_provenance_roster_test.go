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

// findHexDecodeSites walks root for non-test Go sources and returns
// "<package dir>.<enclosing func>" → "file:line" for every call to
// hex.Decode / hex.DecodeString / decodeHexByteaText.
//
// Keyed by the DIRECTORY name rather than the package clause so the
// `-trigger` engines (whose package names differ from their dirs) key
// readably; every engine lives in its own directory.
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
		pkgDir := filepath.Base(filepath.Dir(path))
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			ast.Inspect(fn.Body, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				if !isHexDecodeCall(call.Fun) {
					return true
				}
				key := pkgDir + "." + fn.Name.Name
				if _, seen := out[key]; !seen {
					pos := fset.Position(call.Pos())
					out[key] = fmt.Sprintf("%s:%d", filepath.ToSlash(path), pos.Line)
				}
				return true
			})
			return true
		})
		return nil
	})
	return out, err
}

// isHexDecodeCall reports whether the call target is one of the three
// forms that turn hex text into bytes. hex.EncodeToString and
// hex.DecodedLen are deliberately NOT matched — encoding is never
// ambiguous, and DecodedLen only sizes a buffer for a Decode that is
// itself matched.
func isHexDecodeCall(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		pkg, ok := f.X.(*ast.Ident)
		if !ok || pkg.Name != "hex" {
			return false
		}
		return f.Sel.Name == "Decode" || f.Sel.Name == "DecodeString"
	case *ast.Ident:
		return f.Name == "decodeHexByteaText"
	}
	return false
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
