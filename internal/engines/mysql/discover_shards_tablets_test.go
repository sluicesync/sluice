// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"
)

// fourShards is the canonical 4-shard split PlanetScale/Vitess produces; the
// live v0.99.195 repro had two of these (sks, sks2).
var fourShards = []string{"-40", "40-80", "80-c0", "c0-"}

// findDiscrepancy returns the discrepancy for keyspace ks, or nil if none.
func findDiscrepancy(ds []shardDiscrepancy, ks string) *shardDiscrepancy {
	for i := range ds {
		if ds[i].keyspace == ks {
			return &ds[i]
		}
	}
	return nil
}

// (a) THE OBSERVED BUG: SHOW VITESS_SHARDS dropped an entire sharded keyspace
// (sks2) that SHOW VITESS_TABLETS reports fully SERVING. The union must recover
// sks2's 4 shards and flag the discrepancy naming them.
func TestReconcileShardSources_TabletsRecoversDroppedKeyspace(t *testing.T) {
	fromShards := map[string][]string{
		"sks":     fourShards,
		"default": {"-"},
		// sks2 is ENTIRELY absent — the live bug.
	}
	fromTablets := map[string][]string{
		"sks":     fourShards,
		"sks2":    fourShards,
		"default": {"-"},
	}
	out, ds := reconcileShardSources(fromShards, fromTablets)

	if !reflect.DeepEqual(out["sks2"], fourShards) {
		t.Fatalf("union sks2 = %v; want the recovered 4 shards %v", out["sks2"], fourShards)
	}
	if !reflect.DeepEqual(out["sks"], fourShards) {
		t.Errorf("union sks = %v; want %v (unchanged)", out["sks"], fourShards)
	}
	if !reflect.DeepEqual(out["default"], []string{"-"}) {
		t.Errorf("union default = %v; want [-] (unchanged)", out["default"])
	}

	d := findDiscrepancy(ds, "sks2")
	if d == nil {
		t.Fatalf("no discrepancy flagged for sks2; want the recovered shards named")
	}
	if !reflect.DeepEqual(d.recoveredFromTablets, fourShards) {
		t.Errorf("sks2 recoveredFromTablets = %v; want %v", d.recoveredFromTablets, fourShards)
	}
	if len(d.shardsWithoutServingTablet) != 0 {
		t.Errorf("sks2 shardsWithoutServingTablet = %v; want none", d.shardsWithoutServingTablet)
	}
	// Keyspaces that fully agree must NOT be flagged.
	if findDiscrepancy(ds, "sks") != nil || findDiscrepancy(ds, "default") != nil {
		t.Errorf("agreeing keyspaces flagged: %+v", ds)
	}
}

// (b) FULL AGREEMENT: identical sources → identical output, no discrepancy.
func TestReconcileShardSources_FullAgreementNoDiscrepancy(t *testing.T) {
	src := map[string][]string{
		"sks":     fourShards,
		"default": {"-"},
	}
	// Distinct map instances with equal contents.
	shards := map[string][]string{"sks": append([]string(nil), fourShards...), "default": {"-"}}
	tablets := map[string][]string{"sks": append([]string(nil), fourShards...), "default": {"-"}}

	out, ds := reconcileShardSources(shards, tablets)
	if !reflect.DeepEqual(out, src) {
		t.Errorf("union = %v; want identical %v", out, src)
	}
	if len(ds) != 0 {
		t.Errorf("discrepancies = %+v; want none on full agreement", ds)
	}
}

// (c) SHARDS-ONLY SHARD: a shard SHOW VITESS_SHARDS lists that has NO serving
// tablet is KEPT in the union (never drop a shard a source reports) but FLAGGED
// as possibly-unstreamable.
func TestReconcileShardSources_ShardWithoutServingTabletKeptAndFlagged(t *testing.T) {
	fromShards := map[string][]string{"ks": {"-80", "80-"}}
	fromTablets := map[string][]string{"ks": {"-80"}} // 80- has no serving tablet

	out, ds := reconcileShardSources(fromShards, fromTablets)
	if want := []string{"-80", "80-"}; !reflect.DeepEqual(out["ks"], want) {
		t.Fatalf("union ks = %v; want %v (80- kept)", out["ks"], want)
	}
	d := findDiscrepancy(ds, "ks")
	if d == nil {
		t.Fatalf("no discrepancy flagged for ks; want 80- flagged as possibly-unstreamable")
	}
	if want := []string{"80-"}; !reflect.DeepEqual(d.shardsWithoutServingTablet, want) {
		t.Errorf("shardsWithoutServingTablet = %v; want %v", d.shardsWithoutServingTablet, want)
	}
	if len(d.recoveredFromTablets) != 0 {
		t.Errorf("recoveredFromTablets = %v; want none", d.recoveredFromTablets)
	}
}

// (c') A keyspace tablets NEVER enumerates (e.g. a system keyspace the tablets
// query doesn't surface) must NOT be flagged shard-by-shard — that would
// false-fire the possibly-unstreamable WARN on keyspaces the cross-check has no
// opinion on. The shards are still kept in the union.
func TestReconcileShardSources_KeyspaceAbsentFromTabletsNotFlagged(t *testing.T) {
	fromShards := map[string][]string{"_vt": {"-"}, "app": {"-"}}
	fromTablets := map[string][]string{"app": {"-"}} // _vt absent from tablets

	out, ds := reconcileShardSources(fromShards, fromTablets)
	if want := []string{"-"}; !reflect.DeepEqual(out["_vt"], want) {
		t.Errorf("union _vt = %v; want %v (kept)", out["_vt"], want)
	}
	if findDiscrepancy(ds, "_vt") != nil {
		t.Errorf("_vt flagged despite tablets having no opinion on it: %+v", ds)
	}
}

// (d) An UNSHARDED keyspace ("-") stays "-": the control-keyspace classifier
// (which keys on exactly []string{"-"}) must be unaffected by the cross-check.
func TestReconcileShardSources_UnshardedStaysUnsharded(t *testing.T) {
	fromShards := map[string][]string{"app": {"-"}}
	fromTablets := map[string][]string{"app": {"-"}}
	out, ds := reconcileShardSources(fromShards, fromTablets)
	if want := []string{"-"}; !reflect.DeepEqual(out["app"], want) {
		t.Fatalf("union app = %v; want [-] (unsharded classifier must see exactly [-])", out["app"])
	}
	if len(ds) != 0 {
		t.Errorf("discrepancies = %+v; want none", ds)
	}
}

// (e) DISAGREEMENT ON SHARDEDNESS: tablets reports a keyspace sharded that
// shards reported unsharded. The union carries the sharded shards (never drop a
// shard a source reports); it fails loud downstream rather than silently copying
// the wrong layout.
func TestReconcileShardSources_TabletsShardedShardsUnsharded(t *testing.T) {
	fromShards := map[string][]string{"ks": {"-"}}
	fromTablets := map[string][]string{"ks": {"-80", "80-"}}

	out, ds := reconcileShardSources(fromShards, fromTablets)
	// The sharded shards must be present (that's "union sharded").
	for _, want := range []string{"-80", "80-"} {
		found := false
		for _, s := range out["ks"] {
			if s == want {
				found = true
			}
		}
		if !found {
			t.Errorf("union ks = %v; missing recovered sharded shard %q", out["ks"], want)
		}
	}
	d := findDiscrepancy(ds, "ks")
	if d == nil {
		t.Fatalf("no discrepancy flagged for ks; want the sharded shards recovered")
	}
	if want := []string{"-80", "80-"}; !reflect.DeepEqual(d.recoveredFromTablets, want) {
		t.Errorf("recoveredFromTablets = %v; want %v", d.recoveredFromTablets, want)
	}
	// "-" is present-only-in-shards and has no serving tablet → flagged too.
	if want := []string{"-"}; !reflect.DeepEqual(d.shardsWithoutServingTablet, want) {
		t.Errorf("shardsWithoutServingTablet = %v; want %v", d.shardsWithoutServingTablet, want)
	}
}

// The full live column shape, in the confirmed order.
var vitessTabletCols = []string{
	"Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime",
}

func tabletRow(cell, ks, shard, ttype, state string) []string {
	return []string{cell, ks, shard, ttype, state, "alias-" + shard, "host-" + shard, "2026-01-01T00:00:00Z"}
}

// The parser reduces the ~3 tablet rows/shard to a distinct SERVING shard set,
// accepting ANY serving tablet type and dropping non-SERVING rows. A shard with
// at least one SERVING tablet survives even if a sibling tablet is NOT_SERVING
// (the momentary-reparent case); a shard with ALL tablets non-serving is dropped.
func TestServingShardsFromTablets_DedupeFilterAnyType(t *testing.T) {
	rows := [][]string{
		// sks/-80: full healthy triple.
		tabletRow("z1", "sks", "-80", "PRIMARY", "SERVING"),
		tabletRow("z1", "sks", "-80", "REPLICA", "SERVING"),
		tabletRow("z1", "sks", "-80", "REPLICA", "SERVING"),
		// sks/80-: primary mid-reparent (NOT_SERVING) but a replica still SERVING
		// → shard must survive on the replica (we accept any serving type).
		tabletRow("z1", "sks", "80-", "PRIMARY", "NOT_SERVING"),
		tabletRow("z1", "sks", "80-", "REPLICA", "SERVING"),
		// unsharded default keyspace: single serving shard "-".
		tabletRow("z1", "default", "-", "PRIMARY", "SERVING"),
		// dead/deploying shard: ALL tablets non-serving → dropped entirely.
		tabletRow("z1", "dead", "-", "PRIMARY", "NOT_SERVING"),
		tabletRow("z1", "dead", "-", "REPLICA", "NOT_SERVING"),
	}
	got, err := servingShardsFromTablets(vitessTabletCols, rows)
	if err != nil {
		t.Fatalf("servingShardsFromTablets: %v", err)
	}
	want := map[string][]string{
		"sks":     {"-80", "80-"},
		"default": {"-"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("serving shards = %v; want %v", got, want)
	}
	if _, ok := got["dead"]; ok {
		t.Errorf("keyspace with no serving tablet appeared in result: %v", got["dead"])
	}
}

// Columns are matched BY NAME, not position: a reordered header (and extra
// columns) must still parse correctly.
func TestServingShardsFromTablets_ColumnByName(t *testing.T) {
	// Deliberately reorder + add an unknown column vtgate might introduce.
	cols := []string{"State", "NewFutureCol", "Shard", "Keyspace"}
	rows := [][]string{
		{"SERVING", "junk", "-80", "sks"},
		{"NOT_SERVING", "junk", "80-", "sks"},
		{"SERVING", "junk", "80-", "sks"},
	}
	got, err := servingShardsFromTablets(cols, rows)
	if err != nil {
		t.Fatalf("servingShardsFromTablets (reordered): %v", err)
	}
	if want := map[string][]string{"sks": {"-80", "80-"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("serving shards = %v; want %v", got, want)
	}
}

// A missing expected column (here: State) is a LOUD error, not a silent
// mis-parse — the cross-check refuses to guess which column carries the state.
func TestServingShardsFromTablets_MissingColumnErrors(t *testing.T) {
	cols := []string{"Cell", "Keyspace", "Shard", "TabletType"} // no State
	_, err := servingShardsFromTablets(cols, [][]string{{"z1", "sks", "-80", "PRIMARY"}})
	if err == nil {
		t.Fatal("servingShardsFromTablets with no State column = nil error; want a loud missing-column error")
	}
}

// vitessTabletColumns resolves indexes by name and errors on any missing one.
func TestVitessTabletColumns(t *testing.T) {
	ks, shard, state, err := vitessTabletColumns(vitessTabletCols)
	if err != nil {
		t.Fatalf("vitessTabletColumns: %v", err)
	}
	if ks != 1 || shard != 2 || state != 4 {
		t.Errorf("indexes = (ks %d, shard %d, state %d); want (1, 2, 4)", ks, shard, state)
	}
	for _, missing := range []string{"Keyspace", "Shard", "State"} {
		cols := make([]string, 0, len(vitessTabletCols))
		for _, c := range vitessTabletCols {
			if c != missing {
				cols = append(cols, c)
			}
		}
		if _, _, _, err := vitessTabletColumns(cols); err == nil {
			t.Errorf("vitessTabletColumns without %q = nil error; want missing-column error", missing)
		}
	}
}
