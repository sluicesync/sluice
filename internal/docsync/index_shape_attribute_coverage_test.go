// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every key-shape attribute must be handled by every DDL emitter
// (audit 2026-08-04).
//
// S8 was filed twice and fixed three times, and each fix enumerated the wrong
// axis:
//
//	first half   MySQL prefix length -> Postgres      4 PG emit sites listed,
//	                                                  SQLite never considered
//	second half  PG/SQLite predicate -> MySQL         3 MySQL sites listed,
//	                                                  Postgres's own inline
//	                                                  site never considered
//
// Both commits performed a careful sibling sweep OF SITES WITHIN ONE ENGINE.
// The class is cross-ENGINE, so both swept one level too low and the same
// defect was re-found by a later audit each time.
//
// This gate is the axis they were missing: for each attribute an ir.Index can
// carry that changes WHICH ROWS ARE LEGAL, every target engine must either
// emit it, refuse it, or warn — and say which, here, in one table. A new
// engine or a new attribute fails by default.
//
// It is a coverage matrix, not a behaviour test. The behaviour lives in each
// engine's own tests; what lives here is the ENUMERATION, in one place, so
// that "did we check the other engines?" has an answer that is not a promise.

package docsync

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// handling is what an engine does with a key-shape attribute.
type handling string

const (
	// emits: the target expresses the attribute faithfully.
	emits handling = "emits"
	// refuses: the target cannot express it AND dropping it would change
	// which rows are legal, so it fails loudly before any data moves.
	refuses handling = "refuses"
	// warnsAndDrops: the target cannot express it, but dropping it changes
	// only cost — never which rows are legal.
	warnsAndDrops handling = "warns-and-drops"
)

// indexShapeMatrix records, for every (attribute, engine) pair, what the
// engine does and the symbol that proves it. The proof symbol must exist in
// that engine's package, which is what stops a row from being aspirational.
var indexShapeMatrix = map[string]map[string]struct {
	Does  handling
	Proof string // a symbol that must appear in the engine's non-test sources
	Why   string
}{
	// A PREFIX LENGTH on a uniqueness-enforcing key is part of the constraint:
	// dropping it WIDENS what the target admits.
	"IndexColumn.Length": {
		"mysql":    {emits, "emitIndexColumnListWithPrefix", "native feature — `KEY (col(20))`"},
		"postgres": {refuses, "checkIndexPrefixLength", "no prefix indexes; a unique key would admit rows the source rejects"},
		"sqlite":   {refuses, "checkIndexPrefixLength", "no prefix indexes; same widening as Postgres"},
	},
	// A PARTIAL PREDICATE on a uniqueness-enforcing key is also part of the
	// constraint, but dropping it NARROWS: the target rejects rows the source
	// holds legally.
	"Index.Predicate": {
		"mysql":    {refuses, "checkIndexPredicate", "no partial indexes; an unconditional UNIQUE rejects rows the source permits"},
		"postgres": {emits, "translateIndexPredicate", "native feature — `CREATE INDEX … WHERE`; the source predicate is dialect-translated on the way out"},
		"sqlite":   {emits, "idx.Predicate", "native feature — `CREATE INDEX … WHERE`"},
	},
}

func TestEveryEngineHandlesEveryIndexShapeAttribute(t *testing.T) {
	root := repoRootFromDocsync(t)

	// Anti-vacuity on the matrix itself: a table that lost its rows would
	// pass every assertion below.
	if len(indexShapeMatrix) < 2 {
		t.Fatalf("indexShapeMatrix has %d attributes; it is supposed to enumerate every key-shape attribute "+
			"an ir.Index carries", len(indexShapeMatrix))
	}

	for attr, byEngine := range indexShapeMatrix {
		if len(byEngine) < 3 {
			t.Errorf("attribute %q covers %d engines; mysql, postgres and sqlite are all DDL targets and "+
				"each must be recorded. Omitting one is exactly how this class was missed twice.",
				attr, len(byEngine))
		}
		for engine, h := range byEngine {
			if strings.TrimSpace(h.Why) == "" {
				t.Errorf("%s/%s carries no reason", attr, engine)
			}
			switch h.Does {
			case emits, refuses, warnsAndDrops:
			default:
				t.Errorf("%s/%s has unknown handling %q", attr, engine, h.Does)
				continue
			}
			// The proof symbol must actually exist in that engine's package.
			// Without this the matrix is a wish list.
			if !symbolInEnginePackage(t, root, engine, h.Proof) {
				t.Errorf("%s/%s claims %q via symbol %q, which does not appear in "+
					"internal/engines/%s.\n\nEither the handling regressed or the row is stale — and a stale "+
					"row here is worse than no row, because it reads as coverage.",
					attr, engine, h.Does, h.Proof, engine)
			}
		}
	}
}

// symbolInEnginePackage reports whether sym appears in any non-test .go file
// of the engine's package.
func symbolInEnginePackage(t *testing.T, root, engine, sym string) bool {
	t.Helper()
	dir := filepath.Join(root, "internal", "engines", engine)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			continue
		}
		if strings.Contains(string(b), sym) {
			return true
		}
	}
	return false
}

// repoRootFromDocsync walks up from this package to the module root.
func repoRootFromDocsync(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for range 6 {
		if _, serr := os.Stat(filepath.Join(dir, "go.mod")); serr == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not locate the module root from internal/docsync")
	return ""
}

// TestIndexShapeMatrixNamesEveryConstraintBearingAttribute is the floor that
// keeps the matrix honest as ir.Index grows. It is deliberately a HARDCODED
// expected set rather than reflection over ir.Index: most of that struct's
// fields (Name, Columns, Type, Comment…) do not change which rows are legal,
// and a reflective version would either flag all of them or need an exclusion
// list as long as the struct. The judgement of "does this attribute constrain
// the data?" is human, so it is written down and reviewed.
func TestIndexShapeMatrixNamesEveryConstraintBearingAttribute(t *testing.T) {
	want := []string{"Index.Predicate", "IndexColumn.Length"}
	got := make([]string, 0, len(indexShapeMatrix))
	for k := range indexShapeMatrix {
		got = append(got, k)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("indexShapeMatrix covers %v, expected %v.\n\n"+
			"If ir.Index gained an attribute that changes which rows are legal, add it here AND to every "+
			"engine's row. If one was removed, drop it from both.", got, want)
	}
}
