// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// The premise the chain-restore rotation path rests on, made checkable.
//
// `Restore.restoreChunkGroup` refuses a DataOnly (rotation-segment) restore
// whose target row writer has no [ir.IdempotentRowWriter] form, because
// re-applying segment N's snapshot with a plain INSERT duplicates the rows
// segment N-1 already landed on any table with no key to collide on. Before
// the 2026-08-07 invariant sweep that branch silently fell back to plain
// WriteRows, under a comment arguing the fallback was safe.
//
// The refusal is a belt. The reason it never fires is a fact about the
// ENGINE REGISTRY: [backup.ChainRestore.Run] opens `OpenChangeApplier`
// BEFORE the segment walk, so only a CDC-apply target ever reaches a
// DataOnly segment — and every engine implementing that door also
// implements the idempotent writer. Per the premise-naming step, a safety
// argument citing a fact about the world gets that fact a check, and this
// is it. `internal/pipeline/backup` cannot import the registry; this
// package already links every engine, so the check lives here.
//
// # Scope, stated so the name is not read as broader than the truth
//
//   - It derives the CDC-APPLY set from the registry, two ways that share
//     no evidence (AST shape + calling the door), exactly as
//     [TestTargetEngineListMatchesTheCode] does for the writer doors. That
//     set is what fails the build when a new engine appears.
//   - The writer TYPE each engine opens is a rostered claim, not a
//     derivation — this test cannot call `OpenRowWriter` without a live
//     server. What is mechanical about it is that the roster holds the REAL
//     production type (a typed nil of `postgres.RowWriter`, not a stub) and
//     runs the SAME type assertion `restoreChunkGroup` runs, so an engine
//     losing `WriteRowsIdempotent` fails here. A DELEGATING engine's claim
//     is AST-checked (see delegatesRowWriterDoor) rather than trusted.
//   - It says nothing about whether a CDC-apply target can restore at all,
//     which is [TestTargetEngineListMatchesTheCode]'s question.
func TestChainRestoreTargetsAreIdempotentWriters(t *testing.T) {
	fromStructure := cdcApplyEnginesByStructure(t)
	fromBehaviour := cdcApplyEnginesByBehaviour(t)
	if !equalStringSets(fromStructure, fromBehaviour) {
		t.Fatalf("the two derivations of the CDC-apply engine set disagree — resolve that before trusting either.\n"+
			"  structure (AST: OpenChangeApplier is not a bare `return nil, <sentinel>`): %s\n"+
			"  behaviour (the door called with an empty DSN did not answer \"not implemented\"): %s",
			strings.Join(fromStructure, ", "), strings.Join(fromBehaviour, ", "))
	}
	fromCode := fromStructure

	if len(fromCode) == 0 {
		t.Fatalf("no registered engine implements OpenChangeApplier — the derivation broke; this gate cannot pass on "+
			"an empty set (%d engines registered)", len(engines.Names()))
	}
	if len(fromCode) == len(engines.Names()) {
		t.Fatalf("all %d registered engines classify as CDC-apply targets, so the derivation separates nothing. "+
			"sluice ships engines that refuse OpenChangeApplier (sqlite, d1, flatfile, mydumper); if that genuinely "+
			"changed, delete this gate — do not delete this check", len(engines.Names()))
	}

	for _, name := range fromCode {
		entry, rostered := chainRestoreWriterRoster[name]
		if !rostered {
			t.Errorf("engine %q implements OpenChangeApplier, so a `restore` of a ROTATED chain can reach its "+
				"DataOnly segment walk — but no entry records which row writer it opens. Add one naming the real "+
				"writer type. If that writer has no WriteRowsIdempotent, this engine cannot be a rotated-chain "+
				"restore target: `restoreChunkGroup` refuses it (errRestoreDataOnlyNotIdempotent) rather than "+
				"plain-INSERTing segment N's snapshot over segment N-1's rows", name)
			continue
		}
		if strings.TrimSpace(entry.why) == "" {
			t.Errorf("engine %q is rostered with an empty reason; the reason is what a future reader checks against", name)
		}
		if _, ok := entry.writer.(ir.IdempotentRowWriter); !ok {
			t.Errorf("engine %q is a CDC-apply target, so a rotated-chain restore reaches its DataOnly segments, but "+
				"its row writer (%T) does not implement ir.IdempotentRowWriter. This is the exact premise the refusal "+
				"in Restore.restoreChunkGroup is the belt for: either restore that method, or record here that this "+
				"engine is NOT a rotated-chain restore target and make the operator docs say so", name, entry.writer)
		}
		if entry.delegatesTo != "" && !delegatesRowWriterDoor(t, name, entry.delegatesTo) {
			t.Errorf("engine %q is rostered as delegating OpenRowWriter to the %s engine, but its OpenRowWriter is no "+
				"longer a one-line delegation — so the writer type pinned above may not be the one it opens. "+
				"Re-derive the entry", name, entry.delegatesTo)
		}
	}

	for name := range chainRestoreWriterRoster {
		if !containsString(fromCode, name) {
			t.Errorf("the roster lists %q, which no longer implements OpenChangeApplier. A stale entry pins a writer "+
				"type for a path that no longer exists — remove it", name)
		}
	}
}

// chainRestoreWriterEntry records, for one CDC-apply engine, the REAL row
// writer type it opens (as a typed nil — interface satisfaction is a
// property of the type, not the value) plus why that is the right type.
type chainRestoreWriterEntry struct {
	writer ir.RowWriter
	// delegatesTo names the engine package this one hands OpenRowWriter to,
	// "" when it constructs its own. Non-empty entries are AST-checked.
	delegatesTo string
	why         string
}

// chainRestoreWriterRoster is fail-by-default: an engine that grows an
// OpenChangeApplier and is not listed here fails the build.
//
// The values are typed nils of the production writer structs — the same
// concrete types the engines return — so this file stops COMPILING if one
// of them is renamed, and the assertion above fails if one loses
// WriteRowsIdempotent. A stub satisfying the interface by construction
// would prove nothing, which is the trap the pipeline's "pinned by the unit
// tests" claim fell into.
var chainRestoreWriterRoster = map[string]chainRestoreWriterEntry{
	"mysql": {
		writer: (*mysql.RowWriter)(nil),
		why:    "mysql.Engine.OpenRowWriter constructs *mysql.RowWriter; ON DUPLICATE KEY UPDATE is its idempotent form.",
	},
	"planetscale": {
		writer: (*mysql.RowWriter)(nil),
		why:    "a MySQL flavor — same mysql.Engine type and the same writer, different Capabilities.",
	},
	"vitess": {
		writer: (*mysql.RowWriter)(nil),
		why:    "a MySQL flavor — same mysql.Engine type and the same writer, different Capabilities.",
	},
	"mariadb": {
		writer: (*mysql.RowWriter)(nil),
		why:    "a MySQL flavor — same mysql.Engine type and the same writer, different Capabilities.",
	},
	"postgres": {
		writer: (*postgres.RowWriter)(nil),
		why:    "postgres.Engine.OpenRowWriter constructs *postgres.RowWriter; INSERT … ON CONFLICT is its idempotent form.",
	},
	"postgres-trigger": {
		writer:      (*postgres.RowWriter)(nil),
		delegatesTo: "postgres",
		why:         "the trigger engine composes postgres.Engine and its OpenRowWriter is a one-line delegation, so the writer IS postgres's — only the source-side capture differs.",
	},
}

// cdcApplyDoor is the one [ir.Engine] method that decides whether a
// rotated-chain restore can reach an engine at all: ChainRestore.Run opens
// it before the segment walk, so an engine refusing it never gets a
// DataOnly segment.
const cdcApplyDoor = "OpenChangeApplier"

// cdcApplyEnginesByStructure derives the set from the SOURCE — the same
// bare-sentinel shape [targetEnginesByStructure] keys on.
func cdcApplyEnginesByStructure(t *testing.T) []string {
	t.Helper()
	var out []string
	for key, names := range registeredEngineNamesByPackageType(t) {
		stub, found := refusalStubDoor(t, key, cdcApplyDoor)
		if !found {
			t.Fatalf("no %s method declared on %s, but it is registered as engine(s) %s — the AST scan cannot "+
				"classify it", cdcApplyDoor, key, strings.Join(names, ", "))
		}
		if !stub {
			out = append(out, names...)
		}
	}
	sort.Strings(out)
	return out
}

// cdcApplyEnginesByBehaviour derives the same set by CALLING the door with
// an empty DSN, sharing no evidence with the AST pass.
func cdcApplyEnginesByBehaviour(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var out []string
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}
		_, err := e.OpenChangeApplier(ctx, "")
		if err == nil {
			t.Fatalf("%s.%s accepted an EMPTY DSN — this probe opened something it should not have", name, cdcApplyDoor)
		}
		if !strings.Contains(err.Error(), "not implemented") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// delegatesRowWriterDoor reports whether the named engine's OpenRowWriter
// is a single `return <x>.OpenRowWriter(...)` — the delegation shape the
// roster's delegatesTo column claims. It checks the SHAPE, not the target
// package (the target is what the pinned writer type already asserts).
func delegatesRowWriterDoor(t *testing.T, engineName, _ string) bool {
	t.Helper()
	e, ok := engines.Get(engineName)
	if !ok {
		t.Fatalf("engines.Get(%q) missed", engineName)
	}
	pkgDir, typeName := engineTypeKey(e)

	dir := filepath.Join("..", "..", "internal", "engines", pkgDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		parsed, perr := parser.ParseFile(fset, filepath.Join(dir, f.Name()), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", filepath.Join(dir, f.Name()), perr)
		}
		for _, decl := range parsed.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Recv == nil || len(fn.Recv.List) != 1 || fn.Name.Name != "OpenRowWriter" {
				continue
			}
			if receiverTypeName(fn.Recv.List[0].Type) != typeName {
				continue
			}
			return isSingleDelegatingReturn(fn.Body, "OpenRowWriter")
		}
	}
	t.Fatalf("no OpenRowWriter declared on %s.%s", pkgDir, typeName)
	return false
}

// isSingleDelegatingReturn reports whether a body is exactly
// `return <expr>.<method>(...)`.
func isSingleDelegatingReturn(body *ast.BlockStmt, method string) bool {
	if body == nil || len(body.List) != 1 {
		return false
	}
	ret, ok := body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return false
	}
	call, ok := ret.Results[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == method
}

// engineTypeKey splits a registered engine value into the package
// directory its concrete type lives in and that type's name — the same
// join [registeredEngineNamesByPackageType] builds its keys from.
func engineTypeKey(e ir.Engine) (pkgDir, typeName string) {
	rt := reflect.TypeOf(e)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	return path.Base(rt.PkgPath()), rt.Name()
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
