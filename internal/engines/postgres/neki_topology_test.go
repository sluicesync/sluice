// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The writer must actually satisfy the surface the pipeline preflight looks
// for. A prober that does not is skipped SILENTLY — the check would report
// "safe" for every target forever, which is the vacuous-gate shape.
var _ migcore.ShardKeyUpsertProber = (*RowWriter)(nil)

// liveTopology is the exact JSON `SELECT __neki.get_data_topology()` returned
// from the live 3-shard `neki-torture` cluster on 2026-09-10, trimmed only of
// tables irrelevant to these cases. Pinning the MEASURED document rather than
// a hand-written one is the point: this parser's whole job is to agree with
// what the platform actually emits, and a fixture invented from the struct
// definition would agree with itself and prove nothing.
//
// Its shape, for a reader: shard group `events_v4` spans two shards (two key
// ranges) and is the database default; `shz7wrfow3nkri` has one key range and
// is therefore a single shard; `sharded_events` overrides its group.
const liveTopology = `{
  "shard_indexes": {
    "xxhash_tenant_id": {"type": "xxhash", "columns": ["tenant_id"]},
    "xxhash_org": {"type": "xxhash", "columns": ["org_id", "region"]}
  },
  "shard_groups": [
    {"uid": "shz7wrfow3nkri", "default_shard_index": "xxhash_tenant_id",
     "key_ranges": [{"shard_uid": "shz7wrfow3nkri"}]},
    {"uid": "events_by_tenant", "default_shard_index": "xxhash_tenant_id",
     "key_ranges": [{"shard_uid": "shz7wrfow3nkri", "end": "80"},
                    {"shard_uid": "shk3owth68zsjl", "start": "80"}]},
    {"uid": "events_v4", "default_shard_index": "xxhash_tenant_id",
     "key_ranges": [{"shard_uid": "shk3owth68zsjl", "end": "80"},
                    {"shard_uid": "shoizkgheesrb2", "start": "80"}]}
  ],
  "databases": {
    "postgres": {
      "default_shard_group": "events_v4",
      "schemas": {
        "public": {
          "tables": {
            "nk_a": {"shard_group": "events_v4"},
            "sharded_events": {"shard_group": "events_by_tenant"},
            "pinned_one": {"shard_group": "shz7wrfow3nkri"},
            "other_key": {"shard_group": "events_v4", "shard_index": "xxhash_org"}
          }
        }
      }
    }
  },
  "authoritative_shard_group": "shz7wrfow3nkri",
  "default_shard_group": "shz7wrfow3nkri"
}`

func parseLive(t *testing.T) *nekiTopology {
	t.Helper()
	var topo nekiTopology
	if err := json.Unmarshal([]byte(liveTopology), &topo); err != nil {
		t.Fatalf("the measured topology document does not parse: %v", err)
	}
	return &topo
}

func TestNekiTopologyShardKeyResolution(t *testing.T) {
	t.Parallel()
	topo := parseLive(t)

	cases := []struct {
		name        string
		table       string
		wantCols    []string
		wantSharded bool
		why         string
	}{
		{
			name: "declared table in the multi-shard default group", table: "nk_a",
			wantCols: []string{"tenant_id"}, wantSharded: true,
			why: "the ordinary case this refusal exists for",
		},
		{
			name: "table overriding its shard group", table: "sharded_events",
			wantCols: []string{"tenant_id"}, wantSharded: true,
			why: "a table's own shard_group must beat the database default",
		},
		{
			name: "UNDECLARED table inherits the database default", table: "not_in_the_topology",
			wantCols: []string{"tenant_id"}, wantSharded: true,
			why: "PlanetScale documents that an unlisted table inherits schema, then database, then cluster — " +
				"which is exactly how sluice's own control tables ended up needing the shard key (NEKI-009). " +
				"Resolving an unlisted table to 'not sharded' would silently exempt the majority of tables",
		},
		{
			name: "SINGLE-shard group is not sharded", table: "pinned_one",
			wantCols: []string{"tenant_id"}, wantSharded: false,
			why: "one key range is one shard: the primary key IS globally enforced there and neither failure " +
				"mode can occur, so refusing would be a false positive",
		},
		{
			name: "table naming its own shard index", table: "other_key",
			wantCols: []string{"org_id", "region"}, wantSharded: true,
			why: "a composite shard key must come back whole; dropping a column would under-report the mismatch",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cols, sharded := topo.shardKeyFor("postgres", "public", tc.table)
			if sharded != tc.wantSharded {
				t.Errorf("sharded=%v, want %v — %s", sharded, tc.wantSharded, tc.why)
			}
			if len(cols) != len(tc.wantCols) {
				t.Fatalf("columns=%v, want %v — %s", cols, tc.wantCols, tc.why)
			}
			for i := range cols {
				if cols[i] != tc.wantCols[i] {
					t.Errorf("columns=%v, want %v — %s", cols, tc.wantCols, tc.why)
				}
			}
		})
	}
}

// An unknown database name must not resolve to "sharded on nothing" by
// accident: falling through to the cluster default is a deliberate choice, and
// a silent empty answer here would make the whole refusal inert.
func TestNekiTopologyUnknownDatabaseFallsBackToTheClusterDefault(t *testing.T) {
	t.Parallel()
	topo := parseLive(t)
	cols, sharded := topo.shardKeyFor("some_other_db", "public", "nk_a")
	// The cluster default here is the SINGLE-shard group, so the honest
	// answer is "not sharded" — and crucially it is reached by resolving a
	// real group, not by giving up.
	if sharded {
		t.Errorf("resolved to sharded=%v via the cluster default group, which has one key range", sharded)
	}
	if len(cols) != 1 || cols[0] != "tenant_id" {
		t.Errorf("columns=%v, want [tenant_id] — the fallback must still resolve the group's shard index", cols)
	}
}

// A topology with a group naming an index that does not exist must not
// pretend the table is unsharded. Reporting no columns while reporting
// sharded=true is the honest answer, and the caller skips (it has nothing to
// compare) rather than refusing on a guess.
func TestNekiTopologyMissingShardIndexDoesNotClaimUnsharded(t *testing.T) {
	t.Parallel()
	var topo nekiTopology
	const doc = `{"shard_indexes":{},"shard_groups":[{"uid":"g","default_shard_index":"gone",
	 "key_ranges":[{"shard_uid":"a","end":"80"},{"shard_uid":"b","start":"80"}]}],
	 "databases":{"postgres":{"default_shard_group":"g","schemas":{"public":{"tables":{}}}}}}`
	if err := json.Unmarshal([]byte(doc), &topo); err != nil {
		t.Fatalf("parse: %v", err)
	}
	cols, sharded := topo.shardKeyFor("postgres", "public", "t")
	if !sharded {
		t.Error("a two-key-range group reported unsharded because its index was unresolvable; " +
			"the shardedness of a group does not depend on naming its columns")
	}
	if len(cols) != 0 {
		t.Errorf("columns=%v, want none — an unresolvable index must not invent one", cols)
	}
}

// The refusal must be scoped to Neki. A non-Neki writer must not even reach
// the topology query (which would fail with 42P01 on ordinary PostgreSQL).
func TestShardKeyUpsertMismatchIsANoOpOffNeki(t *testing.T) {
	t.Parallel()
	w := &RowWriter{isNeki: false}
	detail, err := w.ShardKeyUpsertMismatch(t.Context(), []*ir.Table{{Name: "orders"}})
	if err != nil {
		t.Fatalf("a non-Neki writer errored: %v — it must not query __neki at all", err)
	}
	if detail != "" {
		t.Errorf("a non-Neki writer reported a mismatch: %q", detail)
	}
}
