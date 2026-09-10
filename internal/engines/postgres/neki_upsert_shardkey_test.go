// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The bug these cells exist for, in one sentence: a table in a shard group
// with a SINGLE key range still refuses `SET <shard key> = …` with NK013, and
// the first cut of this code decided both of its questions from one flag.
//
// Measured on the live cluster, on a table in a one-range group:
//
//	INSERT … ON CONFLICT (tenant_id,id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, …
//	  -> ERROR: not implemented: updating index column "tenant_id" is not supported
//	UPDATE rs_live SET tenant_id=6 WHERE tenant_id=5 AND id=90002
//	  -> same error
//
// while the same statement WITHOUT the shard key in the SET list succeeded.
// So "may the shard key be named in a SET list" is answered by whether an
// index routes the table; only "can ON CONFLICT duplicate the key" depends on
// the shard count.

const oneRangeTopology = `{
  "shard_indexes": {"xxhash_tenant_id": {"type": "xxhash", "columns": ["tenant_id"]}},
  "shard_groups": [
    {"uid": "one", "default_shard_index": "xxhash_tenant_id",
     "key_ranges": [{"shard_uid": "s1"}]},
    {"uid": "many", "default_shard_index": "xxhash_tenant_id",
     "key_ranges": [{"shard_uid": "s1", "end": "80"}, {"shard_uid": "s2", "start": "80"}]}
  ],
  "databases": {"postgres": {"default_shard_group": "many", "schemas": {"public": {"tables": {
    "single": {"shard_group": "one"},
    "split":  {"shard_group": "many"}
  }}}}},
  "default_shard_group": "one"
}`

// The regression this file is named for: a one-range group must still report
// its routing COLUMNS, while reporting that it is not multi-shard.
func TestSingleShardGroupStillReportsItsShardKeyColumns(t *testing.T) {
	t.Parallel()
	var topo nekiTopology
	if err := json.Unmarshal([]byte(oneRangeTopology), &topo); err != nil {
		t.Fatalf("parse: %v", err)
	}

	cols, multiShard := topo.shardKeyFor("postgres", "public", "single")
	if multiShard {
		t.Error("a one-key-range group reported multiShard; the duplication refusal would fire where the " +
			"primary key IS globally enforced")
	}
	if len(cols) != 1 || cols[0] != "tenant_id" {
		t.Errorf("columns=%v, want [tenant_id] — the target refuses the shard key in a SET list on a "+
			"single-shard group exactly as on a split one, so the columns must come back either way", cols)
	}

	cols, multiShard = topo.shardKeyFor("postgres", "public", "split")
	if !multiShard {
		t.Error("a two-key-range group reported single-shard")
	}
	if len(cols) != 1 || cols[0] != "tenant_id" {
		t.Errorf("columns=%v, want [tenant_id]", cols)
	}
}

func shardKeyTable() *ir.Table {
	return &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id"}, {Name: "tenant_id"}, {Name: "v"},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// The rendered statement is the contract with the server, so pin it.
func TestBuildBatchUpsertOmitsTheShardKeyAndGuards(t *testing.T) {
	t.Parallel()

	t.Run("zero plan renders exactly as before", func(t *testing.T) {
		t.Parallel()
		got, _ := buildBatchUpsert("public", shardKeyTable(), 1, []string{"id"}, upsertShardKeyPlan{})
		if !strings.Contains(got, `"tenant_id" = EXCLUDED."tenant_id"`) {
			t.Errorf("an ordinary PostgreSQL target lost a column from its SET list: %s", got)
		}
		if strings.Contains(got, "IS NOT DISTINCT FROM") {
			t.Errorf("an ordinary PostgreSQL target grew a guard predicate: %s", got)
		}
	})

	t.Run("shard key outside the conflict key: omitted AND guarded", func(t *testing.T) {
		t.Parallel()
		plan := upsertShardKeyPlan{omitFromSet: []string{"tenant_id"}, guard: true}
		got, _ := buildBatchUpsert("public", shardKeyTable(), 2, []string{"id"}, plan)
		if strings.Contains(got, `"tenant_id" = EXCLUDED."tenant_id"`) {
			t.Errorf("the SET list still names the shard key, which the target refuses on shape: %s", got)
		}
		if !strings.Contains(got, `"public"."orders"."tenant_id" IS NOT DISTINCT FROM EXCLUDED."tenant_id"`) {
			t.Errorf("the guard predicate is missing, so a row whose shard key differs would have every "+
				"OTHER column updated and the routing column silently left behind: %s", got)
		}
		// The conflict target must be untouched: the guard belongs on the
		// DO UPDATE, not on the inference clause.
		if !strings.Contains(got, `ON CONFLICT ("id")`) {
			t.Errorf("the conflict target changed: %s", got)
		}
	})

	t.Run("shard key inside the conflict key: no guard needed", func(t *testing.T) {
		t.Parallel()
		// It is already excluded from the SET list because key columns are,
		// and its value cannot differ for a given conflict key — so the plan
		// carries nothing and the statement is unchanged.
		got, _ := buildBatchUpsert("public", shardKeyTable(), 1, []string{"id", "tenant_id"}, upsertShardKeyPlan{})
		if strings.Contains(got, "IS NOT DISTINCT FROM") {
			t.Errorf("a guard was added where the conflict key already fixes the shard key: %s", got)
		}
		if strings.Contains(got, `"tenant_id" = EXCLUDED."tenant_id"`) {
			t.Errorf("a key column reached the SET list: %s", got)
		}
	})
}

// A table whose only non-key column IS the shard key renders `DO NOTHING`,
// which never reaches the guard — and a `DO NOTHING` statement under-counts
// by design, so an affected-row check armed for it fires on every re-copy and
// blames the shard key for rows whose shard key is identical.
//
// Caught by the pre-tag value-fidelity pass, which also named why the existing
// pins missed it: shardKeyTable() carries a third column, so `nonKey` was
// never empty in any cell above. This one has exactly two.
func TestBuildBatchUpsertDoesNotArmTheGuardForADoNothingStatement(t *testing.T) {
	t.Parallel()
	table := &ir.Table{
		Name:       "orders",
		Columns:    []*ir.Column{{Name: "id"}, {Name: "tenant_id"}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	plan := upsertShardKeyPlan{omitFromSet: []string{"tenant_id"}, guard: true}

	got, guarded := buildBatchUpsert("public", table, 2, []string{"id"}, plan)
	if !strings.Contains(got, "DO NOTHING") {
		t.Fatalf("expected DO NOTHING when every non-key column is the shard key: %s", got)
	}
	if guarded {
		t.Error("the builder reported the statement as guarded, but it carries no predicate — the caller " +
			"would then treat DO NOTHING's by-design under-count as a shard-key skip and refuse a healthy re-copy")
	}
	// And the caller must therefore not refuse on the shortfall.
	if err := refuseShardKeySkip("public", "orders", plan, guarded, 2, 0); err != nil {
		t.Errorf("a DO NOTHING statement's under-count was reported as a shard-key skip: %v", err)
	}
}

// The plan builder must REFUSE the combination whose guard would be inert,
// rather than trusting a preflight to have refused it earlier.
//
// Caught by the pre-tag perf-parity pass, and it was a regression introduced
// by the fix it sits next to: PreflightShardKeyUpsert is wired into
// phasePreflightTarget and nothing else, while this writer is also reached by
// the sync cold start (any source that forces idempotent writes), `schema
// add-table` and restore. On those paths, making the statement LEGAL by
// omitting the shard key had converted a loud NK013 into SILENT primary-key
// duplication — on a multi-shard group the conflict is never found, so the
// predicate never runs, the row INSERTs, and the affected count matches.
//
// Graded through the pure decision so every combination is reachable without
// a live cluster.
func TestPlanShardKeyUpsert(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		shardCols  []string
		multiShard bool
		keyCols    []string
		wantErr    bool
		wantOmit   bool
		wantGuard  bool
		why        string
	}{
		{
			name:      "multi-shard, shard key OUTSIDE the key: REFUSED",
			shardCols: []string{"tenant_id"}, multiShard: true, keyCols: []string{"id"},
			wantErr: true,
			why: "the guard is inert here — ON CONFLICT is evaluated only on the shard the incoming row " +
				"routes to, so nothing is skipped and nothing is counted; the row is duplicated silently",
		},
		{
			name:      "single-shard, shard key OUTSIDE the key: omit + guard",
			shardCols: []string{"tenant_id"}, multiShard: false, keyCols: []string{"id"},
			wantOmit: true, wantGuard: true,
			why: "the conflict IS found on one shard, so the predicate runs and a skip shows as a shortfall",
		},
		{
			name:      "shard key INSIDE the key: nothing to do",
			shardCols: []string{"tenant_id"}, multiShard: true, keyCols: []string{"tenant_id", "id"},
			why: "a key column is already excluded from the SET list, and its value cannot differ for a " +
				"given conflict key",
		},
		{
			name:      "composite shard key, one column outside: REFUSED on the multi-shard group",
			shardCols: []string{"org_id", "region"}, multiShard: true, keyCols: []string{"org_id", "id"},
			wantErr: true,
			why:     "a partially-contained shard key routes on a value the conflict key does not fix",
		},
		{
			name:      "no shard key at all: zero plan",
			shardCols: nil, multiShard: true, keyCols: []string{"id"},
			why: "every non-Neki target, and any table no shard index routes, must render byte-identical SQL",
		},
		{
			name:      "case difference between topology and key spelling is not a miss",
			shardCols: []string{"Tenant_ID"}, multiShard: true, keyCols: []string{"tenant_id", "id"},
			why: "PostgreSQL folds unquoted identifiers; treating these as different columns would refuse a " +
				"safe table, or worse leave the column in a SET list the plan believed it had removed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan, err := planShardKeyUpsert("orders", tc.shardCols, tc.multiShard, tc.keyCols)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("accepted a combination it cannot make safe — %s", tc.why)
				}
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeTargetShardKeyNotInUpsertKey {
					t.Errorf("refusal carried code %v (coded=%v)", ce, ok)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a safe combination (%s): %v", tc.why, err)
			}
			if got := len(plan.omitFromSet) > 0; got != tc.wantOmit {
				t.Errorf("omitFromSet=%v, want-non-empty=%v — %s", plan.omitFromSet, tc.wantOmit, tc.why)
			}
			if plan.guard != tc.wantGuard {
				t.Errorf("guard=%v, want %v — %s", plan.guard, tc.wantGuard, tc.why)
			}
		})
	}
}

// The applier lane refuses where the writer lane guards, and that asymmetry
// is deliberate: the pipelined dispatch has no per-statement affected-row
// count to check, so it cannot verify a guard it emitted.
func TestRefuseShardKeyOutsideConflictKey(t *testing.T) {
	t.Parallel()

	t.Run("outside the key: refused, both shard counts", func(t *testing.T) {
		t.Parallel()
		err := refuseShardKeyOutsideConflictKey("public", "orders", []string{"id"}, []string{"tenant_id"})
		if err == nil {
			t.Fatal("an ON CONFLICT upsert was allowed to key on a set that does not fix the routing " +
				"column; it has no before-image and cannot establish the column is unchanged")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeTargetShardKeyNotInUpsertKey {
			t.Errorf("refusal carried code %v (coded=%v)", ce, ok)
		}
		for _, want := range []string{"orders", "tenant_id", "id"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not name %q: %v", want, err)
			}
		}
	})

	t.Run("inside the key: allowed", func(t *testing.T) {
		t.Parallel()
		if err := refuseShardKeyOutsideConflictKey("public", "orders",
			[]string{"tenant_id", "id"}, []string{"tenant_id"}); err != nil {
			t.Fatalf("the supported shape was refused: %v", err)
		}
	})

	t.Run("no shard key: allowed, and costs nothing", func(t *testing.T) {
		t.Parallel()
		if err := refuseShardKeyOutsideConflictKey("public", "orders", []string{"id"}, nil); err != nil {
			t.Fatalf("an ordinary PostgreSQL target was refused: %v", err)
		}
	})

	t.Run("case-insensitive, like the writer lane", func(t *testing.T) {
		t.Parallel()
		if err := refuseShardKeyOutsideConflictKey("public", "orders",
			[]string{"TENANT_ID", "id"}, []string{"tenant_id"}); err != nil {
			t.Fatalf("a spelling difference was treated as a missing column: %v", err)
		}
	})
}

func TestRefuseShardKeySkip(t *testing.T) {
	t.Parallel()
	plan := upsertShardKeyPlan{omitFromSet: []string{"tenant_id"}, guard: true}

	t.Run("a shortfall is refused, and names the numbers", func(t *testing.T) {
		t.Parallel()
		err := refuseShardKeySkip("public", "orders", plan, true, 10, 9)
		if err == nil {
			t.Fatal("a skipped row was swallowed; the guard turned a partial update into a no-op and " +
				"nothing told the operator the change never landed")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeTargetShardKeyUpdateUnsupported {
			t.Errorf("refusal carried code %v (coded=%v)", ce, ok)
		}
		for _, want := range []string{"orders", "tenant_id", "9 of 10"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not carry %q: %v", want, err)
			}
		}
	})

	t.Run("a full batch passes", func(t *testing.T) {
		t.Parallel()
		if err := refuseShardKeySkip("public", "orders", plan, true, 10, 10); err != nil {
			t.Fatalf("a fully-applied batch was refused: %v", err)
		}
	})

	t.Run("an UNGUARDED statement is never checked", func(t *testing.T) {
		t.Parallel()
		// Without the predicate in the statement a shortfall has other
		// explanations, and this refusal would be a false accusation.
		if err := refuseShardKeySkip("public", "orders", plan, false, 10, 3); err != nil {
			t.Fatalf("a shortfall was refused on an unguarded statement: %v", err)
		}
	})

	t.Run("an unreported count is not a verdict", func(t *testing.T) {
		t.Parallel()
		// A driver that will not report RowsAffected tells us nothing. Failing
		// every such run would punish drivers unrelated to this hazard.
		if err := refuseShardKeySkip("public", "orders", plan, true, 10, -1); err != nil {
			t.Fatalf("an unreported affected-row count was treated as a shortfall: %v", err)
		}
	})
}
