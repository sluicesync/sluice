// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// manifestReadEntryPoints are the lineage calls that LOAD a manifest off a
// store. Every function in this package that calls one is a manifest
// consumer, and a manifest consumer either runs the shared integrity
// preflights or says in writing why it does not.
var manifestReadEntryPoints = map[string]bool{
	"lineage.ReadManifest":            true,
	"lineage.ReadManifestAt":          true,
	"lineage.ReadManifestIfPresent":   true,
	"lineage.ReadRootManifest":        true,
	"lineage.ListAllSegmentManifests": true,
	"lineage.ListAllManifestsViaWalk": true,
}

// preflightEntryPoint is the shared list itself.
const preflightEntryPoint = "restoreManifestIntegrityPreflights"

// manifestConsumerExempt maps a function that reads a manifest without
// running the preflights to the REASON that is correct. The distinction
// that matters is whether the function READS DATA OUT OF the manifest —
// creates target objects, streams chunks, reports the backup healthy — or
// only reads it as chain METADATA (positions, ids, encryption headers,
// resume state) to decide how to rewrite or extend the chain.
//
// A reason of "it's maintenance" is only good enough when the function
// genuinely applies nothing: prune and compact move bytes they never
// decode, and both re-verify the chain through [checkChainReadable],
// whose contract differs from restore's on purpose.
var manifestConsumerExempt = map[string]string{
	// --- backup WRITE path: reads the prior manifest as resume state ---
	"Backup.resolveResumeState": "backup RESUME reads the PRIOR manifest to decide which tables to skip. It is the " +
		"one legitimate reader of an in_progress manifest — the state refuseInterruptedManifests exists to refuse " +
		"— so running the list here would make every crashed backup unresumable, the exact opposite of the intent.",

	// --- chain maintenance (compact): metadata reads + verbatim byte moves ---
	"CompactChain": "compact applies nothing to a target: it reads each link's manifest for positions/ids and MOVES " +
		"chunk bytes it never decodes. Its read-back gate (checkChainReadable, Bug 214) re-walks the result the " +
		"way a restore would, before and after the destructive sweep, which is the check that matters for a " +
		"command whose output is another chain rather than a database.",
	"executeMergeGroup": "compact's staging/merge core — manifest metadata + verbatim chunk moves, under CompactChain's exemption.",
	"segmentByteTotal":  "compact size accounting: reads segment headers to total chunk bytes for the merge plan. Applies nothing, decodes nothing.",
	"applySmartCompactionToStagedGroup": "ADR-0064 smart compaction decodes CHANGE chunks to collapse events, but reads the manifest only to " +
		"ENUMERATE them; each chunk carries its own SHA and (encrypted chains) AEAD binding, and the compacted " +
		"chain goes through checkChainReadable before the sources are deleted.",

	// --- chain maintenance (prune): drops the OLDEST end ---
	"PruneChain": "prune reads manifests to decide which segments fall off the oldest end, and applies nothing. " +
		"Refusing a prune because some link is interrupted would strand precisely the debris prune removes — the " +
		"list's contract (refuse before READING DATA) is the wrong contract for a command that deletes.",
	"pruneWholeSegments":            "prune's segment sweep, under PruneChain's exemption — reads headers to enumerate and delete.",
	"pruneFloorLeadingIncrementals": "prune's within-floor incremental sweep, under PruneChain's exemption.",
	"pruneTrim.withinSegmentTrimRefusal": "roadmap item 100's purpose-built REFUSAL: it reads the survivor's manifest to explain why an " +
		"within-segment trim would leave an unrestorable chain. It is itself a refusal path — running restore's " +
		"list ahead of it would replace a precise message with a generic one.",
	"oldestRetainedBackupResumePosition": "post-prune position probe: reads the retained root's header so `sync start --position-from-manifest` " +
		"still has an anchor. Applies nothing.",
	"r0": "prune's zero-result helper (chain_prune.go): reads segment headers to report what a no-op prune would " +
		"have covered. Applies nothing, deletes nothing.",

	// --- the chain-maintenance readability gate ---
	"checkChainReadable": "the Bug 214 readability gate itself. It deliberately holds a DIFFERENT contract from " +
		"restore's preflights: it must be able to REPORT on a chain maintenance just rewrote — including shapes " +
		"restore would refuse — or it could never name what it broke. Routing it through the list would make it " +
		"refuse instead of report, on exactly the runs its finding is needed for.",
}

// funcKeyOf names a declaration the way the exemption map keys it:
// `Receiver.Method` for methods, the bare name for plain functions.
func funcKeyOf(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// TestManifestConsumersRouteThroughSharedPreflights is the ADR-0183 gate 3
// / Bug 218 consumer-coverage gate.
//
// Bug 218's lesson was that sharing the PREDICATE is not sharing the LIST:
// `backup verify` was given restore's schema-fingerprint check and its
// shape dispatch, and still diverged one preflight over. This gate is the
// generalization — it fails the build when a function in this package
// loads a manifest and neither runs the shared list nor records why.
// `export-as-parquet` is the instance it was written for: it ran the
// signature check and the interrupted-manifest check and NEITHER
// recompute, on any chain shape.
func TestManifestConsumersRouteThroughSharedPreflights(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	type fnInfo struct {
		file      string
		reads     bool // calls a lineage manifest-read entry point
		preflight bool // calls the shared preflight list directly
		calls     map[string]bool
	}
	fns := map[string]*fnInfo{}
	// byBase resolves a call site to a declaration. A call is only
	// followed when its name resolves to EXACTLY ONE declaration in the
	// package: an AST walker cannot type-resolve `x.Run()`, and three types
	// here have a `Run`, so an ambiguous name yields NO edge. That is the
	// conservative direction — an unfollowed edge can only make the gate
	// report MORE, never silently bless an unguarded consumer.
	byBase := map[string][]string{}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			c := &fnInfo{file: name, calls: map[string]bool{}}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if pkg, isIdent := fun.X.(*ast.Ident); isIdent && manifestReadEntryPoints[pkg.Name+"."+fun.Sel.Name] {
						c.reads = true
					}
					c.calls[fun.Sel.Name] = true
				case *ast.Ident:
					if fun.Name == preflightEntryPoint {
						c.preflight = true
					}
					c.calls[fun.Name] = true
				}
				return true
			})
			// Keyed by RECEIVER.Name, never the bare name: three different
			// `Run` methods read manifests in this package, and collapsing
			// them would let one command's exemption silently cover another's.
			key := funcKeyOf(fn)
			fns[key] = c
			byBase[fn.Name.Name] = append(byBase[fn.Name.Name], key)
		}
	}

	// resolve returns the single declaration a call name refers to, or ""
	// when the name is ambiguous or external.
	resolve := func(base string) string {
		if keys := byBase[base]; len(keys) == 1 {
			return keys[0]
		}
		return ""
	}

	// A function is GUARDED when it runs the shared list itself or calls
	// something that does (Restore.Run reaches it through
	// verifySingleFullBackupID). It is IN A GUARDED FLOW when a guarded
	// function calls it — a helper that reads the chain root's header on
	// behalf of a guarded command needs no preflight of its own. Both are
	// least-fixed-points over the resolvable call edges.
	guarded := map[string]bool{}
	for key, fn := range fns {
		if fn.preflight {
			guarded[key] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for key, fn := range fns {
			if guarded[key] {
				continue
			}
			for base := range fn.calls {
				if callee := resolve(base); callee != "" && guarded[callee] {
					guarded[key] = true
					changed = true
					break
				}
			}
		}
	}
	inGuardedFlow := map[string]bool{}
	for key := range guarded {
		inGuardedFlow[key] = true
	}
	for changed := true; changed; {
		changed = false
		for key, fn := range fns {
			if !inGuardedFlow[key] {
				continue
			}
			for base := range fn.calls {
				if callee := resolve(base); callee != "" && !inGuardedFlow[callee] {
					inGuardedFlow[callee] = true
					changed = true
				}
			}
		}
	}

	consumers := map[string]*fnInfo{}
	for key, fn := range fns {
		if fn.reads {
			consumers[key] = fn
		}
	}
	// Anti-vacuity floor: if the walk stops matching the real read entry
	// points (a lineage rename, a helper extraction), it would find zero
	// consumers and pass forever. The count only ever grows in practice.
	const minConsumers = 8
	if len(consumers) < minConsumers {
		t.Fatalf("found only %d manifest consumers (floor %d) — the walk has probably stopped matching the "+
			"lineage read entry points in manifestReadEntryPoints; fix the walk, do not lower the floor", len(consumers), minConsumers)
	}

	var offenders []string
	for name, c := range consumers {
		if inGuardedFlow[name] {
			if _, exempt := manifestConsumerExempt[name]; exempt {
				t.Errorf("%s (%s) runs the shared preflights AND carries an exemption — remove the exemption", name, c.file)
			}
			continue
		}
		reason, exempt := manifestConsumerExempt[name]
		if !exempt {
			offenders = append(offenders, name+" ("+c.file+")")
			continue
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s: exemption carries no reason", name)
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("these functions load a backup manifest without running %s:\n  %s\n"+
			"Route them through the shared list, or add a manifestConsumerExempt entry saying why the list is "+
			"wrong for them (Bug 218: a command holding its own subset of the list is the defect, not the fix).",
			preflightEntryPoint, strings.Join(offenders, "\n  "))
	}

	// Every exemption must name a real consumer, so a rename cannot leave a
	// decorative entry claiming to cover something that no longer reads
	// manifests.
	for name := range manifestConsumerExempt {
		if _, ok := consumers[name]; !ok {
			t.Errorf("manifestConsumerExempt has %q, which does not load a manifest in this package (renamed? removed?)", name)
		}
	}
}
