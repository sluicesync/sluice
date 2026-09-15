// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// shardedFixtureTopology is the document the nekiverify fixture declares
// (nekiverify_fixture_test.go, declareTopology) as the live router returned
// it on 2026-09-14, with the shard uids kept: an authoritative group with no
// shard index over one shard, a sharded default group over two, and the
// fixture's three tables placed in the sharded group. `public` is the
// database default, so a table not listed under `tables` is routed by
// `xxhash_tenant_id` — the exact state in which sluice's control tables were
// refused with NK306.
const shardedFixtureTopology = `{
  "shard_indexes": {"xxhash_tenant_id": {"type": "xxhash", "columns": ["tenant_id"]}},
  "shard_groups": [
    {"uid": "sh7xeak9f22vu6", "key_ranges": [{"shard_uid": "sh7xeak9f22vu6"}]},
    {"uid": "nv_group", "default_shard_index": "xxhash_tenant_id",
     "key_ranges": [{"shard_uid": "sh7xeak9f22vu6", "end": "80"}, {"shard_uid": "shqm0dlbf83wx0", "start": "80"}]}
  ],
  "authoritative_shard_group": "sh7xeak9f22vu6",
  "databases": {"postgres": {"default_shard_group": "nv_group", "schemas": {"public": {"tables": {
    "sk_good": {"shard_group": "nv_group"},
    "sk_bad": {"shard_group": "nv_group"},
    "uq_email": {"shard_group": "nv_group"}
  }}}}},
  "future_field": {"n": 12345678901234567890, "f": 1.50, "e": 1e5, "z": -0.0,
                   "s": "<a>&b c", "nested": [{"a": [null, {}, [], true]}]}
}`

func decodeTopologyDoc(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return m
}

func TestPlanNekiControlPlacement_PlacesRoutedTablesInTheAuthoritativeGroup(t *testing.T) {
	t.Parallel()
	tables := []string{"sluice_cdc_state", "sluice_cdc_skipped_tables"}
	plan, err := planNekiControlPlacement([]byte(shardedFixtureTopology), "postgres", "public", tables)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if !plan.changed {
		t.Fatal("a control table routed by the sharded default group must be placed; plan reports nothing to do")
	}
	if plan.group != "sh7xeak9f22vu6" {
		t.Fatalf("group = %q, want the authoritative group", plan.group)
	}
	if want := []string{"sluice_cdc_skipped_tables", "sluice_cdc_state"}; !reflect.DeepEqual(plan.placed, want) {
		t.Fatalf("placed = %v, want %v", plan.placed, want)
	}

	// The written document must (a) place both tables and (b) leave every
	// other byte of meaning intact — including a field this code has never
	// heard of, and a number that float64 would have mangled.
	got := decodeTopologyDoc(t, plan.doc)
	want := decodeTopologyDoc(t, []byte(shardedFixtureTopology))
	wantTables := want["databases"].(map[string]any)["postgres"].(map[string]any)["schemas"].(map[string]any)["public"].(map[string]any)["tables"].(map[string]any)
	for _, tb := range tables {
		wantTables[tb] = map[string]any{"shard_group": "sh7xeak9f22vu6"}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("written document differs from the input plus the two placements:\n got: %s\nwant: %v", plan.doc, want)
	}
	for _, want := range []string{"12345678901234567890", "1.50", "1e5", "-0.0", `"<a>&b\u2028c"`} {
		if !strings.Contains(string(plan.doc), want) {
			t.Fatalf("value %s in an unknown field did not survive the round trip verbatim: %s", want, plan.doc)
		}
	}
	if strings.Contains(string(plan.doc), `\u003c`) || strings.Contains(string(plan.doc), `\u0026`) {
		t.Fatalf("HTML escaping rewrote the operator's strings (a needless diff in the topology change log): %s", plan.doc)
	}

	// And the placement is effective by sluice's own predicate: after the
	// write, no shard index routes either table.
	var after nekiTopology
	if err := json.Unmarshal(plan.doc, &after); err != nil {
		t.Fatal(err)
	}
	for _, tb := range tables {
		if cols, _ := after.shardKeyFor("postgres", "public", tb); len(cols) != 0 {
			t.Fatalf("after placement %q is still routed by %v", tb, cols)
		}
	}
}

func TestPlanNekiControlPlacement_IsANoOpWhenNothingRoutesTheTables(t *testing.T) {
	t.Parallel()

	t.Run("already placed", func(t *testing.T) {
		t.Parallel()
		first, err := planNekiControlPlacement([]byte(shardedFixtureTopology), "postgres", "public",
			[]string{"sluice_cdc_state"})
		if err != nil || !first.changed {
			t.Fatalf("first plan: changed=%v err=%v", first.changed, err)
		}
		// A second start reads the document the first one wrote.
		second, err := planNekiControlPlacement(first.doc, "postgres", "public", []string{"sluice_cdc_state"})
		if err != nil {
			t.Fatal(err)
		}
		if second.changed {
			t.Fatalf("the placement already holds, yet the plan would write again — every start would mint a "+
				"topology revision: %s", second.doc)
		}
	})

	t.Run("unsharded database: no shard index anywhere", func(t *testing.T) {
		t.Parallel()
		const unsharded = `{
		  "shard_groups": [{"uid": "sh1", "key_ranges": [{"shard_uid": "sh1"}]}],
		  "authoritative_shard_group": "sh1",
		  "default_shard_group": "sh1",
		  "databases": {"postgres": {"default_shard_group": "sh1", "schemas": {}}}
		}`
		plan, err := planNekiControlPlacement([]byte(unsharded), "postgres", "public", []string{"sluice_cdc_state"})
		if err != nil {
			t.Fatal(err)
		}
		if plan.changed {
			t.Fatal("an unsharded database must never have its topology written by sluice")
		}
	})

	t.Run("no tables", func(t *testing.T) {
		t.Parallel()
		plan, err := planNekiControlPlacement([]byte(shardedFixtureTopology), "postgres", "public", nil)
		if err != nil || plan.changed {
			t.Fatalf("changed=%v err=%v", plan.changed, err)
		}
	})
}

func TestPlanNekiControlPlacement_CreatesTheDatabaseEntryWhenAbsent(t *testing.T) {
	t.Parallel()
	// The document keys `databases` by real name; a cluster whose document
	// does not mention this database at all resolves through the cluster
	// default and still needs the entry created on the way down.
	const noDB = `{
	  "shard_indexes": {"xx": {"type": "xxhash", "columns": ["k"]}},
	  "shard_groups": [
	    {"uid": "auth", "key_ranges": [{"shard_uid": "auth"}]},
	    {"uid": "big", "default_shard_index": "xx", "key_ranges": [{"shard_uid": "a", "end": "80"}, {"shard_uid": "b", "start": "80"}]}
	  ],
	  "authoritative_shard_group": "auth",
	  "default_shard_group": "big"
	}`
	plan, err := planNekiControlPlacement([]byte(noDB), "appdb", "ctl", []string{"sluice_migrate_state"})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.changed {
		t.Fatal("routed through the cluster default, so the table needs placing")
	}
	got := decodeTopologyDoc(t, plan.doc)
	entry := got["databases"].(map[string]any)["appdb"].(map[string]any)["schemas"].(map[string]any)["ctl"].(map[string]any)["tables"].(map[string]any)["sluice_migrate_state"]
	if !reflect.DeepEqual(entry, map[string]any{"shard_group": "auth"}) {
		t.Fatalf("entry = %v", entry)
	}
}

func TestPlanNekiControlPlacement_RefusesWhatItCannotPlace(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		doc  string
		want string // substring of the refusal
	}{
		{
			name: "no authoritative group",
			doc: `{
			  "shard_indexes": {"xx": {"type": "xxhash", "columns": ["k"]}},
			  "shard_groups": [{"uid": "big", "default_shard_index": "xx",
			    "key_ranges": [{"shard_uid": "a", "end": "80"}, {"shard_uid": "b", "start": "80"}]}],
			  "default_shard_group": "big"
			}`,
			want: "names no authoritative_shard_group",
		},
		{
			name: "authoritative group declares a shard index",
			doc: `{
			  "shard_indexes": {"xx": {"type": "xxhash", "columns": ["k"]}},
			  "shard_groups": [
			    {"uid": "auth", "default_shard_index": "xx", "key_ranges": [{"shard_uid": "auth"}]},
			    {"uid": "big", "default_shard_index": "xx", "key_ranges": [{"shard_uid": "a", "end": "80"}, {"shard_uid": "b", "start": "80"}]}
			  ],
			  "authoritative_shard_group": "auth",
			  "default_shard_group": "big"
			}`,
			want: `authoritative shard group "auth" declares a default shard index on (k)`,
		},
		{
			name: "operator pinned the control table to a shard index",
			doc: `{
			  "shard_indexes": {"xx": {"type": "xxhash", "columns": ["k"]}},
			  "shard_groups": [
			    {"uid": "auth", "key_ranges": [{"shard_uid": "auth"}]},
			    {"uid": "big", "default_shard_index": "xx", "key_ranges": [{"shard_uid": "a", "end": "80"}, {"shard_uid": "b", "start": "80"}]}
			  ],
			  "authoritative_shard_group": "auth",
			  "default_shard_group": "big",
			  "databases": {"postgres": {"schemas": {"public": {"tables": {
			    "sluice_cdc_state": {"shard_group": "big", "shard_index": "xx"}
			  }}}}}
			}`,
			want: `pins sluice's control table "sluice_cdc_state" to shard index "xx"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := planNekiControlPlacement([]byte(tc.doc), "postgres", "public", []string{"sluice_cdc_state"})
			if err == nil {
				t.Fatal("expected a refusal")
			}
			var coded *sluicecode.CodedError
			if !errors.As(err, &coded) || coded.Code != sluicecode.CodeTargetControlTablePlacement {
				t.Fatalf("refusal is not coded %s: %v", sluicecode.CodeTargetControlTablePlacement, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refusal does not say why:\n got: %v\nwant substring: %s", err, tc.want)
			}
			if !strings.Contains(err.Error(), "sluice_cdc_state") {
				t.Fatalf("refusal must name the table so the operator can place it: %v", err)
			}
		})
	}
}

func TestRefuseNekiControlPlacement_NamesThePermissionCase(t *testing.T) {
	t.Parallel()
	plan := nekiControlPlacementPlan{placed: []string{"sluice_cdc_state"}, group: "auth"}
	err := refuseNekiControlPlacement("postgres", "public", plan,
		&pgconn.PgError{Code: nekiSQLStateInsufficientPrivilege, Message: "permission denied for function set_data_topology"})
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) || coded.Code != sluicecode.CodeTargetControlTablePlacement {
		t.Fatalf("not the placement refusal: %v", err)
	}
	for _, want := range []string{"may not call __neki.set_data_topology", "sluice_cdc_state", `"public"`, `"postgres"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q:\n%v", want, err)
		}
	}
	if !strings.Contains(coded.Hint, `"shard_group": "auth"`) {
		t.Errorf("hint does not carry the exact entry to add: %s", coded.Hint)
	}
}

// NK306 must come out of the applier's classifier CODED and TERMINAL. Before
// 2026-09-14 it fell through as an unclassified terminal — the class that
// cost v0.152.1 a full-volume copy at NK205.
func TestClassifyApplierError_NK306IsCodedAndTerminal(t *testing.T) {
	t.Parallel()
	raw := &pgconn.PgError{
		Code:    nekiShardKeyMissingCode,
		Message: `shard-key column "tenant_id" of primary index 0 is required but missing from INSERT`,
	}
	err := classifyApplierError(raw)
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) || coded.Code != sluicecode.CodeTargetShardKeyMissing {
		t.Fatalf("NK306 is not classified as %s: %v", sluicecode.CodeTargetShardKeyMissing, err)
	}
	var retriable *retriablePGError
	if errors.As(err, &retriable) {
		t.Fatal("NK306 is refused on the statement's shape and must not be retriable")
	}
	if !errors.Is(err, raw) {
		t.Fatal("the underlying pgconn error must stay reachable through the wrap")
	}

	// Idempotent: the position-write sites annotate at the wrap, and the
	// same error may then pass through the classifier — one code, once.
	twice := annotateNekiShardKeyMissing(annotateNekiShardKeyMissing(raw))
	const wrapPrefix = "a PlanetScale Neki target refused an INSERT"
	if n := strings.Count(twice.Error(), wrapPrefix); n != 1 {
		t.Fatalf("annotated %d times, want exactly once: %v", n, twice)
	}

	// And every other shape passes through untouched.
	other := &pgconn.PgError{Code: "23505"}
	got := annotateNekiShardKeyMissing(other)
	var otherCoded *sluicecode.CodedError
	if !errors.Is(got, other) || errors.As(got, &otherCoded) {
		t.Fatalf("a non-NK306 error was rewritten: %v", got)
	}
}

// Every step of the placement path can hold something that is not an object
// — null, a string, an array — and each must be refused rather than
// overwritten, because the alternative is sluice silently replacing part of
// the operator's routing document.
func TestAssignTablesToShardGroup_RefusesToOverwriteANonObject(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, doc, want string }{
		{"string at schemas", `{"databases": {"postgres": {"schemas": "oops"}}}`, "databases.postgres.schemas is not an object"},
		{"null at databases", `{"databases": null}`, "databases is not an object"},
		{"array at tables", `{"databases": {"postgres": {"schemas": {"public": {"tables": []}}}}}`, "databases.postgres.schemas.public.tables is not an object"},
		{"null at the table entry", `{"databases": {"postgres": {"schemas": {"public": {"tables": {"t": null}}}}}}`, "tables.t is not an object"},
		{"string at the table entry", `{"databases": {"postgres": {"schemas": {"public": {"tables": {"t": "x"}}}}}}`, "tables.t is not an object"},
		{"array at the table entry", `{"databases": {"postgres": {"schemas": {"public": {"tables": {"t": [1]}}}}}}`, "tables.t is not an object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := assignTablesToShardGroup([]byte(tc.doc), "postgres", "public", []string{"t"}, "auth")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("must be refused with %q, not overwritten: %v", tc.want, err)
			}
		})
	}

	// And the one shape that IS allowed to be absent: a missing step is
	// created, an existing entry keeps its other fields.
	doc, err := assignTablesToShardGroup([]byte(`{"databases": {"postgres": {"schemas": {"public": {"tables": {"t": {"extra": 1}}}}}}}`),
		"postgres", "public", []string{"t", "u"}, "auth")
	if err != nil {
		t.Fatal(err)
	}
	got := decodeTopologyDoc(t, doc)
	tables := got["databases"].(map[string]any)["postgres"].(map[string]any)["schemas"].(map[string]any)["public"].(map[string]any)["tables"].(map[string]any)
	if !reflect.DeepEqual(tables["t"], map[string]any{"extra": json.Number("1"), "shard_group": "auth"}) ||
		!reflect.DeepEqual(tables["u"], map[string]any{"shard_group": "auth"}) {
		t.Fatalf("tables = %v", tables)
	}
}

// TestEveryPostgresControlTableIsPlacedOnNeki is the sibling sweep as a
// gate: every control-table NAME constant this package declares must be
// handed to ensureNekiControlTablePlacement somewhere in the package's
// non-test sources, and everything handed to it must be such a constant.
//
// The universe is the package's own `*TableName` constants (the roster
// convention appliershared.TestControlTableRoster_SourceSync enforces), read
// off the AST, so a control table added tomorrow is covered the day its
// constant is declared. A table whose placement is deliberately NOT done here
// goes in the exemption map WITH its reason, which is the only way it gets
// out of the sweep.
//
// Anti-vacuity: the package is known to declare at least eight such
// constants across at least four placement call sites; fewer means the scan
// broke, not that the world shrank.
func TestEveryPostgresControlTableIsPlacedOnNeki(t *testing.T) {
	t.Parallel()

	// Exemptions — control tables this package names but deliberately does
	// not place, each with the reason a reader needs to re-check it.
	exempt := map[string]string{
		// (none today; the heartbeat table's name arrives as a parameter
		// from the pipeline rather than as a constant here, and is exempt
		// on its own terms: it is written on the SOURCE, and a Neki cannot
		// be a continuous-sync source — ADR-0186 probe R-1.)
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	declared := map[string]string{} // const name -> file
	placed := map[string][]string{} // const name -> files where it is placed
	callSites := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.GenDecl:
				if x.Tok != token.CONST {
					return true
				}
				for _, spec := range x.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, id := range vs.Names {
						if strings.HasSuffix(id.Name, "TableName") {
							declared[id.Name] = name
						}
					}
				}
			case *ast.CallExpr:
				id, ok := x.Fun.(*ast.Ident)
				if !ok || id.Name != "ensureNekiControlTablePlacement" || len(x.Args) == 0 {
					return true
				}
				callSites++
				lit, ok := x.Args[len(x.Args)-1].(*ast.CompositeLit)
				if !ok {
					t.Errorf("%s: ensureNekiControlTablePlacement's table list must be a []string literal of "+
						"table-name constants so this gate can read it; got %T", name, x.Args[len(x.Args)-1])
					return true
				}
				for _, el := range lit.Elts {
					eid, ok := el.(*ast.Ident)
					if !ok {
						t.Errorf("%s: placement list element %T is not a table-name constant", name, el)
						continue
					}
					placed[eid.Name] = append(placed[eid.Name], name)
				}
			}
			return true
		})
	}

	if len(declared) < 8 {
		t.Fatalf("found only %d *TableName constants (%v); the scan is broken", len(declared), declared)
	}
	if callSites < 4 {
		t.Fatalf("found only %d ensureNekiControlTablePlacement call sites; the applier bundle, the "+
			"target-metrics door, the migrate-state store and the keyset store each have one", callSites)
	}

	names := make([]string, 0, len(declared))
	for n := range declared {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if _, ok := placed[n]; ok {
			continue
		}
		if reason, ok := exempt[n]; ok {
			t.Logf("%s (declared in %s) is exempt from placement: %s", n, declared[n], reason)
			continue
		}
		t.Errorf("control table constant %s (declared in %s) is never handed to "+
			"ensureNekiControlTablePlacement: on a sharded PlanetScale Neki every write to it will be refused "+
			"with NK306. Place it where it is created, or add it to the exemption map with a reason.",
			n, declared[n])
	}
	for n, files := range placed {
		if _, ok := declared[n]; !ok {
			t.Errorf("%s is placed in %v but is not a *TableName constant of this package", n, files)
		}
		if _, ok := exempt[n]; ok {
			t.Errorf("%s is both placed (%v) and listed as exempt; pick one", n, files)
		}
	}

	// And every name this package places must be on the shared roster, so
	// the readers exclude it from user-table enumeration. The roster test
	// enforces the reverse direction for the whole tree.
	for n := range placed {
		val := constStringValue(t, fset, n)
		if val != "" && !appliershared.IsControlTable(val) {
			t.Errorf("%s = %q is placed as a control table but is not on appliershared.ControlTableNames", n, val)
		}
	}
}

// constStringValue resolves a package-level string constant's literal value
// when it is declared as a plain literal in this package; "" when it is an
// alias of another package's constant (those are pinned by the roster test).
func constStringValue(t *testing.T, fset *token.FileSet, name string) string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		fn := e.Name()
		if e.IsDir() || !strings.HasSuffix(fn, ".go") || strings.HasSuffix(fn, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", fn), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, id := range vs.Names {
					if id.Name != name || i >= len(vs.Values) {
						continue
					}
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						return strings.Trim(lit.Value, "`\"")
					}
					return ""
				}
			}
		}
	}
	return ""
}
