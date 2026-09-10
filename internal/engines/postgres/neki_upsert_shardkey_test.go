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
		got := buildBatchUpsert("public", shardKeyTable(), 1, []string{"id"}, upsertShardKeyPlan{})
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
		got := buildBatchUpsert("public", shardKeyTable(), 2, []string{"id"}, plan)
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
		got := buildBatchUpsert("public", shardKeyTable(), 1, []string{"id", "tenant_id"}, upsertShardKeyPlan{})
		if strings.Contains(got, "IS NOT DISTINCT FROM") {
			t.Errorf("a guard was added where the conflict key already fixes the shard key: %s", got)
		}
		if strings.Contains(got, `"tenant_id" = EXCLUDED."tenant_id"`) {
			t.Errorf("a key column reached the SET list: %s", got)
		}
	})
}

func TestRefuseShardKeySkip(t *testing.T) {
	t.Parallel()
	guarded := upsertShardKeyPlan{omitFromSet: []string{"tenant_id"}, guard: true}

	t.Run("a shortfall is refused, and names the numbers", func(t *testing.T) {
		t.Parallel()
		err := refuseShardKeySkip("public", "orders", guarded, 10, 9)
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
		if err := refuseShardKeySkip("public", "orders", guarded, 10, 10); err != nil {
			t.Fatalf("a fully-applied batch was refused: %v", err)
		}
	})

	t.Run("no guard means no check", func(t *testing.T) {
		t.Parallel()
		// Without the guard predicate a shortfall has other explanations and
		// this refusal would be a false accusation.
		if err := refuseShardKeySkip("public", "orders", upsertShardKeyPlan{}, 10, 3); err != nil {
			t.Fatalf("a shortfall was refused on an unguarded statement: %v", err)
		}
	})

	t.Run("an unreported count is not a verdict", func(t *testing.T) {
		t.Parallel()
		// A driver that will not report RowsAffected tells us nothing. Failing
		// every such run would punish drivers unrelated to this hazard.
		if err := refuseShardKeySkip("public", "orders", guarded, 10, -1); err != nil {
			t.Fatalf("an unreported affected-row count was treated as a shortfall: %v", err)
		}
	})
}
