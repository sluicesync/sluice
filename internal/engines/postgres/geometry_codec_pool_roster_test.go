// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sibling-sweep gate for the PostGIS geometry codec (Bug 239).
//
// # Why a roster and not a promise
//
// [pgGeometryBinaryCodec] must be registered on every pool that BINDS a
// row value as a query parameter, because pgx has no built-in codec for
// PostGIS's dynamic `geometry` OID and falls back to TEXT, which
// `geometry_in` rejects. Task #20 registered it on the CDC applier's three
// pools and stopped there — its own doc comment says "the CDC applier
// installs on BOTH its pools", which was true and was never the whole
// universe. The [RowWriter]'s two PARAMETER-binding cores
// ([writeViaBatch], [writeViaBatchIdempotent]) were left out, and a
// VStream cold start — which always takes the idempotent core — could not
// copy a spatial column at all (Bug 239).
//
// So the enumeration lives here, derived from the package's own AST rather
// than from a list someone remembers to update: every call to a pool
// constructor is discovered, and each ENCLOSING function must appear in
// [pgPoolGeometryRoster] declaring whether it registers the codec and WHY.
// A new pool that nobody classified fails the build.
//
// # What these gates reach, precisely
//
// Two halves, because neither alone is the universe:
//
//  1. [TestPGPoolRoster_GeometryCodecRegistration] walks calls to the
//     ROLE-AWARE pool constructors — openDBAs, openPgxDBAs,
//     openPgxDBDescribeExec — which are the ones every engine entry point
//     (Open*) uses. It deliberately does NOT walk the [openDB] wrapper:
//     that is the roleControl helper behind ~20 read-only / SQL-text call
//     sites (snapshot export/import, keyset store, preflights, row-count
//     estimators, backfill's operator-supplied `--set` text), none of which
//     binds an [ir.Row] VALUE. Which is a claim, so half (2) checks it.
//  2. [TestPGPrepareValueCallersAreEnumerated] pins the EXACT universe of
//     the defect: every caller of [prepareValue], the one function that
//     turns an ir.Row value into the wire form a geometry column needs. A
//     new value-binding path anywhere in this package has to appear there
//     and declare which pool executes it.
//
// Neither proves a statement encodes correctly — that is
// [TestRowWriter_PostGIS_GeometryFamiliesAcrossWriteCores] (the postgis-
// tagged family matrix over all three write cores, geometry AND geography
// arms) and the pipelined / concurrent applier PostGIS pins. The
// `geography` type rides the same registration: [afterConnectRegister-
// Geometry] registers [pgGeometryBinaryCodec] for both spatial OIDs
// (audit GAP 1 — before that, geography parameter-binds died in
// `geography_in` with the same error text Bug 239 fixed for geometry).

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pgPoolConstructors are the functions in this package that mint a
// *sql.DB. Any call to one of these is a pool site the roster must
// classify.
var pgPoolConstructors = map[string]bool{
	"openDBAs":              true,
	"openPgxDBAs":           true,
	"openPgxDBDescribeExec": true,
}

// poolSiteExpectation is one roster entry: does the enclosing function's
// pool register the PostGIS geometry codec, and on what grounds.
type poolSiteExpectation struct {
	registersGeometry bool
	reason            string
}

// pgPoolGeometryRoster is FAIL-BY-DEFAULT: an unlisted pool site fails the
// test with instructions rather than being assumed safe. Keyed by the
// enclosing function name.
var pgPoolGeometryRoster = map[string]poolSiteExpectation{
	// ---- pools that bind ir.Row values as query parameters ----
	"OpenRowWriter": {
		registersGeometry: true,
		reason: "Bug 239: writeViaBatch and writeViaBatchIdempotent bind geometry as " +
			"a query PARAMETER (only the COPY core ships COPY-BINARY, which needs no codec)",
	},
	"OpenChangeApplier": {
		registersGeometry: true,
		reason:            "task #20: the serial applier binds geometry as a query parameter",
	},
	"pipelinePool": {
		registersGeometry: true,
		reason:            "task #20: the ADR-0092 pipelined DescribeExec pool binds geometry as a parameter",
	},
	"applyBatchConcurrent": {
		registersGeometry: true,
		reason:            "task #20: the concurrent lane pool binds geometry as a parameter",
	},

	// ---- pools that never bind a geometry value ----
	"openDBAs": {
		reason: "plumbing: the shared constructor itself; its CALLERS pass the hook, " +
			"and adding it here would silently register on every role",
	},
	"openDB": {
		reason: "plumbing: the roleControl wrapper over openDBAs. Its ~20 callers " +
			"(snapshot export/import, keyset store, chain/where preflights, row-count " +
			"estimators, the backfill executor's operator-supplied `--set` SQL text) read " +
			"or emit SQL text; none binds an ir.Row value — the claim TestPGPrepareValue" +
			"CallersAreEnumerated makes mechanical",
	},
	"OpenPgxDB": {
		reason: "roleControl pool for sluice's own control/state tables — no geometry columns",
	},
	"ProbeTargetConnectionBudget": {
		reason: "roleControl: reads pg_settings / pg_stat_activity only",
	},
	"DetectStaleBackends": {
		reason: "roleControl: reads and terminates backends; binds no row values",
	},
	"OpenSchemaReader": {
		reason: "roleSchema: catalog reads only — DDL and metadata, never an ir.Row value",
	},
	"OpenSchemaWriter": {
		reason: "roleSchema: emits DDL text; geometry columns are refused here when " +
			"PostGIS is absent (hasPostGIS), never bound as a value",
	},
	"OpenRowReader": {
		reason: "roleSnapshot READ side: geometry arrives as PostGIS's hex-EWKB text " +
			"spelling and is decoded by decodePGGeometry; registering a binary codec " +
			"would change the scanned Go shape, so this pool is deliberately left bare",
	},
	"OpenCDCReaderWithSlot": {
		reason: "roleCDCReader: pgoutput decoding happens on the replication connection; " +
			"this pool runs preconditions and DDL only",
	},
}

// TestPGPoolRoster_GeometryCodecRegistration walks the package's own AST,
// finds every pool-constructor call site, and holds each enclosing
// function to its roster entry.
func TestPGPoolRoster_GeometryCodecRegistration(t *testing.T) {
	// funcName -> (total pool sites, sites passing the geometry hook)
	type tally struct{ total, registering int }
	found := map[string]*tally{}

	forEachPackageFunc(t, func(fnName string, body *ast.BlockStmt) {
		ast.Inspect(body, func(n ast.Node) bool {
			call, isCall := n.(*ast.CallExpr)
			if !isCall {
				return true
			}
			ident, isIdent := call.Fun.(*ast.Ident)
			if !isIdent || !pgPoolConstructors[ident.Name] {
				return true
			}
			tl := found[fnName]
			if tl == nil {
				tl = &tally{}
				found[fnName] = tl
			}
			tl.total++
			if callMentionsIdent(call, "afterConnectRegisterGeometry") {
				tl.registering++
			}
			return true
		})
	})

	// Anti-vacuity floor: this package has always had many pools and at
	// least the applier's three registrations. An empty or shrunken
	// discovery means the walker broke, not that the code got simpler.
	if len(found) < 12 {
		t.Fatalf("discovered %d pool-site functions; want >= 12 — the AST walk is broken, "+
			"refusing to pass vacuously", len(found))
	}
	registering := 0
	for _, tl := range found {
		if tl.registering > 0 {
			registering++
		}
	}
	if registering < 4 {
		t.Fatalf("only %d pool-site functions register the geometry codec; want >= 4 "+
			"(RowWriter + the applier's three pools) — the walk is not seeing the hook", registering)
	}

	for name, tl := range found {
		want, listed := pgPoolGeometryRoster[name]
		if !listed {
			t.Errorf("pool site %q is not in pgPoolGeometryRoster.\n"+
				"  Add an entry saying whether it must register afterConnectRegisterGeometry.\n"+
				"  It MUST if the pool ever binds an ir.Row value as a query parameter "+
				"(Bug 239); say why not otherwise.", name)
			continue
		}
		switch {
		case want.registersGeometry && tl.registering != tl.total:
			t.Errorf("pool site %q: %d/%d constructions register the geometry codec; want all.\n"+
				"  Roster reason: %s\n"+
				"  Without it, a geometry parameter is TEXT-encoded and PostGIS refuses it "+
				"with \"parse error - invalid geometry\" (SQLSTATE XX000).",
				name, tl.registering, tl.total, want.reason)
		case !want.registersGeometry && tl.registering != 0:
			t.Errorf("pool site %q registers the geometry codec but the roster says it must not.\n"+
				"  Roster reason: %s\n"+
				"  If this pool now binds geometry values, flip the roster entry deliberately.",
				name, want.reason)
		}
	}

	// The roster must not accumulate entries for sites that no longer
	// exist — a stale entry is an enumeration nobody is checking.
	for name := range pgPoolGeometryRoster {
		if found[name] == nil {
			t.Errorf("pgPoolGeometryRoster lists %q, which no longer opens a pool — drop the entry", name)
		}
	}
}

// prepareValueCallers is FAIL-BY-DEFAULT: every function that turns an
// ir.Row value into its Postgres wire form must say which pool executes
// the result, because that is what decides whether a geometry value needs
// the codec. Keyed by enclosing function name.
var prepareValueCallers = map[string]string{
	"flattenArgs": "RowWriter's plain + idempotent batch cores — query PARAMETERS on the " +
		"OpenRowWriter pool, which registers the geometry codec (Bug 239)",
	"prepareApplierValue": "ChangeApplier's value shaping — query PARAMETERS on the applier's " +
		"three pools, all of which register the geometry codec",
	"Next": "the COPY sources (chanCopySource / sliceCopySource) — COPY-BINARY, which reaches " +
		"geometry_recv directly and needs no codec",
	"prepareValue": "its own recursive call for a DOMAIN's base type — same call site as the caller",
}

// TestPGPrepareValueCallersAreEnumerated is the exact half of the
// sibling sweep: [prepareValue] is the single funnel every ir.Row value
// crosses on its way to Postgres, so its caller set IS the universe of
// paths a geometry value can take. A new one must be classified.
func TestPGPrepareValueCallersAreEnumerated(t *testing.T) {
	callers := map[string]int{}
	forEachPackageFunc(t, func(fnName string, body *ast.BlockStmt) {
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, isIdent := call.Fun.(*ast.Ident); isIdent && id.Name == "prepareValue" {
				callers[fnName]++
			}
			return true
		})
	})

	// Anti-vacuity floor: the writer, the applier and both COPY sources
	// have always been in here. An empty result means the walk broke.
	if len(callers) < 4 {
		t.Fatalf("found %d prepareValue callers; want >= 4 — the AST walk is broken, refusing to pass vacuously", len(callers))
	}
	for name := range callers {
		if _, listed := prepareValueCallers[name]; !listed {
			t.Errorf("%q calls prepareValue but is not in prepareValueCallers.\n"+
				"  Add it, naming the pool that executes the result. If that pool binds the value as a\n"+
				"  query PARAMETER it MUST register afterConnectRegisterGeometry (Bug 239); COPY-BINARY does not.", name)
		}
	}
	for name := range prepareValueCallers {
		if callers[name] == 0 {
			t.Errorf("prepareValueCallers lists %q, which no longer calls prepareValue — drop the entry", name)
		}
	}
}

// forEachPackageFunc parses this package's non-test sources and calls fn
// for every function declaration with a body.
func forEachPackageFunc(t *testing.T, fn func(name string, body *ast.BlockStmt)) {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn(fd.Name.Name, fd.Body)
		}
	}
}

// callMentionsIdent reports whether name appears anywhere inside call's
// argument list. The registration is always passed as
// stdlib.OptionAfterConnect(composeAfterConnect(..., afterConnectRegisterGeometry)),
// so an identifier scan over the args is exact enough and does not care
// how the hooks are composed.
func callMentionsIdent(call *ast.CallExpr, name string) bool {
	seen := false
	for _, arg := range call.Args {
		ast.Inspect(arg, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == name {
				seen = true
			}
			return !seen
		})
	}
	return seen
}
