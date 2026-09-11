//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// nekiMetaFuncRef matches a `__neki.<name>(` reference in this package's
// source. The trailing paren is what separates a FUNCTION reference from the
// `__neki.shard` session parameter, which is a setting and not callable.
var nekiMetaFuncRef = regexp.MustCompile(`__neki\.([a-z0-9_]+)\(`)

// A remedy an operator cannot run is a Tier-2 harm in its own right — the
// standing work loop ranks "a recovery hint that cannot run" immediately
// below silent loss, and for good reason: it is read mid-incident, by someone
// whose stream has already stopped.
//
// sluice's Neki refusals tell operators to run specific `__neki` functions.
// `SLUICE-E-TARGET-TABLE-BLOCKED-BY-WORKFLOW` names three
// (list_blocked_tables, move_tables_status, move_tables_reverse_traffic);
// `SLUICE-E-TARGET-SHARD-PLACEMENT-STALE` names two more. Every one of them
// is a PREMISE about a platform in preview, and the `__neki` surface is
// exactly the kind of thing that gets renamed between previews. A rename
// costs nothing to sluice's own execution and everything to the operator
// following the hint.
//
// # Why the universe is derived, not listed
//
// A hand-maintained list of function names would drift the moment a refusal's
// wording changed — and it would drift SILENTLY, which is the shape this repo
// keeps paying for. So the list comes from the SOURCE: every `__neki.<name>(`
// written anywhere in the three packages that mention the surface is a name
// sluice has put in front of somebody, and each must exist on the router.
//
// Those three packages are `engines/postgres` (this one), `pipeline/migcore`
// and `sluicecode` — enumerated by `grep -rl '__neki\.' internal/` on
// 2026-09-11 and re-derived by the scan itself, which fails if a directory it
// names has moved. Splitting the scan across them matters: the
// SHARD-PLACEMENT remedy's `reshard_create` and `workflow_switch_traffic`
// live in the other two, so a package-local scan would have quietly covered
// only the MoveTables half while reading as if it covered the surface.
//
// Anti-vacuity floor below, because a regex that stops matching would
// otherwise make this pass by checking nothing.
//
// # What it does NOT check
//
// That the functions still DO what the remedy says. Calling
// move_tables_reverse_traffic to find out would need a real MoveTables
// cutover, which is Tier-2 coverage item #3 and costs a second database plus
// a workflow — see docs/dev/neki-integration-coverage.md. Existence is the
// cheap half and catches the likeliest failure (a rename), and the name of
// this function says so rather than implying more.
func nekiRemedyFunctionsExist(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	named := nekiFunctionsNamedInSluice(t)
	// Floor of 8, the count derived on 2026-09-11. Set at the measured number
	// rather than at a token 1: a refusal losing its remedy is exactly the
	// regression worth catching, and a floor that only rejects zero would not
	// catch it. Raise it deliberately when a new one lands.
	if len(named) < 8 {
		t.Fatalf("derived only %d __neki function name(s) from sluice's source (%v), expected at least 8 — "+
			"either a remedy lost its function reference, or the scan broke. A green run here would be "+
			"checking less than it did yesterday", len(named), named)
	}
	t.Logf("checking %d __neki function(s) sluice names in operator-facing text: %v", len(named), named)

	// The router serves the standard catalogs (pg_sequences is already relied
	// on by readSequencePositionFromCatalog), so pg_proc is the portable way
	// to ask. If this query itself fails, say so plainly rather than letting
	// the existence check degrade into "we could not look".
	const q = `SELECT EXISTS (
	             SELECT 1 FROM pg_catalog.pg_proc p
	             JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
	             WHERE n.nspname = '__neki' AND p.proname = $1)`

	// Anti-vacuity in the other direction: a query that returned true for
	// everything would pass every name. A deliberately absent name must come
	// back false before any result above is believed.
	var control bool
	if err := db.QueryRowContext(ctx, q, "sluice_no_such_metafunction").Scan(&control); err != nil {
		t.Fatalf("the pg_proc existence probe itself failed, so nothing below was actually checked: %v", err)
	}
	if control {
		t.Fatalf("the existence probe reported a fabricated function as present — it answers true for " +
			"anything, so every name it approves proves nothing")
	}

	for _, fn := range named {
		var exists bool
		if err := db.QueryRowContext(ctx, q, fn).Scan(&exists); err != nil {
			t.Errorf("existence probe for __neki.%s failed: %v", fn, err)
			continue
		}
		if !exists {
			t.Errorf("sluice's operator-facing text tells people to run `__neki.%s(...)`, and no such "+
				"function exists on this cluster. The refusal that names it is still correct about WHAT "+
				"went wrong and now wrong about how to recover — which is read mid-incident. Re-derive the "+
				"remedy against the current __neki surface", fn)
		}
	}
}

// nekiFunctionsNamedInSluice scans every source file in the packages that
// mention the `__neki` surface for `__neki.<name>(` and returns the distinct
// names, sorted.
//
// Reading source at test time rather than maintaining a list is what makes
// this gate self-deriving: a new refusal that names a new function is covered
// the day it lands, with nobody remembering to add it here.
func nekiFunctionsNamedInSluice(t *testing.T) []string {
	t.Helper()

	// `go test` runs with the package directory as the working directory, so
	// these are relative to internal/engines/postgres. A directory that moves
	// is a FAILURE rather than a silent narrowing of the scan — that is the
	// difference between this and a list that rots.
	dirs := []string{".", "../../pipeline/migcore", "../../sluicecode"}

	set := map[string]struct{}{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v — this scan's universe is three packages; one of them moved, so the "+
				"remedy names living there would go unchecked", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
				continue
			}
			// This file writes the pattern in its own regex and prose;
			// excluding it by name keeps the gate from grading itself.
			if e.Name() == "nekiverify_remedy_test.go" {
				continue
			}
			b, err := os.ReadFile(filepath.Clean(filepath.Join(dir, e.Name())))
			if err != nil {
				t.Fatalf("read %s: %v", filepath.Join(dir, e.Name()), err)
			}
			for _, m := range nekiMetaFuncRef.FindAllSubmatch(b, -1) {
				set[string(m[1])] = struct{}{}
			}
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
