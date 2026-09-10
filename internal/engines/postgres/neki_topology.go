// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// Reading a PlanetScale Neki cluster's data topology, and the one refusal it
// buys.
//
// # The state this exists to refuse, and why it is a refusal
//
// sluice's idempotent write is `INSERT … ON CONFLICT (k) DO UPDATE SET <every
// other column>`. Both the CDC applier and the bulk-copy resume path use it.
// On a SHARDED Neki table where the shard key is not one of `k`'s columns,
// that statement has two failure modes and neither is survivable:
//
//   - Naming the shard key in the SET list is REFUSED outright — `ERROR: not
//     implemented: updating index column "tenant_id" is not supported`
//     (SQLSTATE NK013). It is refused on the statement SHAPE, not the data:
//     measured 2026-09-10 with the stored and incoming values both equal to
//     5. So the generic upsert cannot even be issued.
//
//   - Leaving the shard key OUT of the SET list — the obvious workaround, and
//     the one this fix originally set out to implement — is worse. `ON
//     CONFLICT (k)` evaluates its conflict on the shard the INCOMING row
//     routes to. Routing is by shard key. So an incoming row whose shard key
//     differs from the stored row's routes elsewhere, finds no conflict, and
//     INSERTS. Measured on a live 3-shard cluster: after one such statement,
//     `SELECT count(*) … WHERE id = 3001` returned 2, with the two rows
//     physically resident on different shards, at exit 0, no warning. A
//     PRIMARY KEY on a sharded table is enforced WITHIN a shard.
//
// An equality-routed read hides the duplicate and a scattering read reveals
// it, so the corruption is invisible to exactly the queries a sharded
// application is written to use. See neki-issues/NEKI-011.
//
// The condition that makes the write safe is checkable before anything is
// written, and it is simple:
//
//	every shard-key column is contained in the upsert conflict key
//
// When it holds, the shard key cannot change for a given conflict key (it IS
// part of it), so routing is stable and the conflict is always evaluated on
// the shard that holds the row — and the shard key is already excluded from
// the SET list because key columns are. Both failure modes vanish. When it
// does not hold, there is no statement sluice can emit that is both accepted
// and correct, so it refuses and says which column to add to the key.
//
// # Why the topology rather than a behavioural probe
//
// [RowWriter.ShardPlacementMismatch] deliberately probes behaviourally, because
// the routing-vs-placement question it answers is about DATA. This question is
// about SCHEMA — which columns route — and the topology states that directly.
// Asking behaviourally would mean writing a probe row to find out whether
// writing is safe.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
//   - NEKI ONLY. Every other PostgreSQL endpoint answers "" and pays nothing.
//   - SHARDED SHARD GROUPS ONLY. A group with a single key range is one shard;
//     its primary key is globally enforced and neither failure mode exists.
//   - Tables with a usable upsert key only. Without one the writer does not
//     emit `ON CONFLICT` at all, so there is no conflict to mis-evaluate.
//     (Such a table has its own, separate problem — a keyless copy is not
//     idempotent on any engine — and that is not this refusal's business.)

// nekiTopology is the subset of `__neki.get_data_topology()` this file reads.
// Fields Neki returns that nothing here needs are deliberately absent; an
// unknown field is ignored by encoding/json rather than being an error,
// because the platform is in preview and a new field must not turn into a
// refusal.
type nekiTopology struct {
	ShardIndexes map[string]struct {
		Type    string   `json:"type"`
		Columns []string `json:"columns"`
	} `json:"shard_indexes"`

	ShardGroups []struct {
		UID               string `json:"uid"`
		DefaultShardIndex string `json:"default_shard_index"`
		KeyRanges         []struct {
			ShardUID string `json:"shard_uid"`
			Start    string `json:"start"`
			End      string `json:"end"`
		} `json:"key_ranges"`
	} `json:"shard_groups"`

	Databases map[string]struct {
		DefaultShardGroup string `json:"default_shard_group"`
		Schemas           map[string]struct {
			DefaultShardGroup string `json:"default_shard_group"`
			Tables            map[string]struct {
				ShardGroup string `json:"shard_group"`
				ShardIndex string `json:"shard_index"`
			} `json:"tables"`
		} `json:"schemas"`
	} `json:"databases"`

	DefaultShardGroup string `json:"default_shard_group"`
}

// shardKeyFor returns the shard-key columns that route `table` in `schema` of
// `database`, and whether that table's shard group is actually SHARDED (more
// than one key range).
//
// The resolution chain is the one PlanetScale documents for undeclared
// tables — table, then schema, then database — with the cluster default last.
// A table that names its own shard index overrides the group's default.
func (t *nekiTopology) shardKeyFor(database, schema, table string) (cols []string, sharded bool) {
	db, ok := t.Databases[database]
	if !ok {
		// A single-database cluster that does not name it by the value we
		// asked with: fall back to the cluster default rather than guessing
		// which entry is meant.
		return t.columnsForGroup(t.DefaultShardGroup, "")
	}
	group := db.DefaultShardGroup
	index := ""
	if sc, ok := db.Schemas[schema]; ok {
		if sc.DefaultShardGroup != "" {
			group = sc.DefaultShardGroup
		}
		if tb, ok := sc.Tables[table]; ok {
			if tb.ShardGroup != "" {
				group = tb.ShardGroup
			}
			index = tb.ShardIndex
		}
	}
	if group == "" {
		group = t.DefaultShardGroup
	}
	return t.columnsForGroup(group, index)
}

// columnsForGroup resolves a shard group to its routing columns. indexOverride,
// when non-empty, replaces the group's default shard index.
func (t *nekiTopology) columnsForGroup(group, indexOverride string) (cols []string, sharded bool) {
	for _, g := range t.ShardGroups {
		if g.UID != group {
			continue
		}
		// One key range is one shard: the primary key is globally enforced
		// and neither failure mode in this file's header can occur.
		sharded = len(g.KeyRanges) > 1
		idx := g.DefaultShardIndex
		if indexOverride != "" {
			idx = indexOverride
		}
		if si, ok := t.ShardIndexes[idx]; ok {
			return si.Columns, sharded
		}
		return nil, sharded
	}
	return nil, false
}

// A per-server memo for the parsed topology. Same two rules as [nekiMemo]:
// key on the server's network identity, store no credentials.
//
// The topology CAN change under a running process — that is what a reshard
// does — so this is not a "cannot change" memo like the version probe. It is
// bounded to the preflight's lifetime by construction: the check runs once per
// run, before any data moves, and a reshard that starts afterwards is
// [RowWriter.ShardPlacementMismatch]'s and the CDC lane's problem, not this
// one's. Memoising still matters because the preflight asks per table and a
// 10k-table schema would otherwise fetch the same blob 10k times.
var nekiTopoMemo = struct {
	mu       sync.Mutex
	byServer map[string]*nekiTopologySnapshot
}{}

// nekiTopologySnapshot pairs the parsed topology with the database name it
// must be indexed by. The topology keys its `databases` map by the real
// database name, which the DSN does not always spell the same way, so it is
// asked for rather than derived.
type nekiTopologySnapshot struct {
	topo     *nekiTopology
	database string
}

// loadNekiTopology fetches and parses `__neki.get_data_topology()` plus the
// current database name, memoised per server.
func loadNekiTopology(ctx context.Context, serverKey string, q querier) (*nekiTopologySnapshot, error) {
	nekiTopoMemo.mu.Lock()
	if t, ok := nekiTopoMemo.byServer[serverKey]; ok {
		nekiTopoMemo.mu.Unlock()
		return t, nil
	}
	nekiTopoMemo.mu.Unlock()

	rows, err := q.QueryContext(ctx, "SELECT __neki.get_data_topology()::text, pg_catalog.current_database()")
	if err != nil {
		return nil, fmt.Errorf("postgres: read Neki data topology: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var raw, database string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("postgres: read Neki data topology: %w", err)
		}
		return nil, errors.New("postgres: read Neki data topology: the function returned no rows")
	}
	if err := rows.Scan(&raw, &database); err != nil {
		return nil, fmt.Errorf("postgres: read Neki data topology: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read Neki data topology: %w", err)
	}

	var t nekiTopology
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return nil, fmt.Errorf("postgres: parse Neki data topology: %w", err)
	}
	snap := &nekiTopologySnapshot{topo: &t, database: database}

	nekiTopoMemo.mu.Lock()
	if nekiTopoMemo.byServer == nil {
		nekiTopoMemo.byServer = map[string]*nekiTopologySnapshot{}
	}
	nekiTopoMemo.byServer[serverKey] = snap
	nekiTopoMemo.mu.Unlock()
	return snap, nil
}

// ShardKeyUpsertMismatch implements the target-side probe the pipeline's
// preflight calls. It reports the first table whose SHARD KEY is not contained
// in the key sluice's idempotent upsert would use as its conflict target,
// together with the columns involved, or "" when every table is safe.
//
// A probe that cannot RUN is not a verdict: an error reading or parsing the
// topology is returned as an error, never as a mismatch.
func (w *RowWriter) ShardKeyUpsertMismatch(ctx context.Context, tables []*ir.Table) (detail string, err error) {
	if !w.isNeki {
		return "", nil
	}
	snap, err := loadNekiTopology(ctx, w.serverKey, w.db)
	if err != nil {
		return "", err
	}
	for _, t := range tables {
		if t == nil {
			continue
		}
		// The writer emits ON CONFLICT only when it can resolve a key. No
		// key, no conflict clause, no conflict to mis-evaluate — a different
		// problem, and not this refusal's.
		keyCols, ok := effectiveUpsertKeyColumns(t)
		if !ok || len(keyCols) == 0 {
			continue
		}
		shardCols, sharded := snap.topo.shardKeyFor(snap.database, w.schemaOrPublic(), t.Name)
		if !sharded || len(shardCols) == 0 {
			continue
		}
		inKey := make(map[string]struct{}, len(keyCols))
		for _, c := range keyCols {
			inKey[strings.ToLower(c)] = struct{}{}
		}
		var missing []string
		for _, sc := range shardCols {
			if _, ok := inKey[strings.ToLower(sc)]; !ok {
				missing = append(missing, sc)
			}
		}
		if len(missing) > 0 {
			return fmt.Sprintf("table %q is sharded on (%s) but sluice's idempotent upsert keys on (%s); "+
				"%s is not in that key",
				t.Name, strings.Join(shardCols, ", "), strings.Join(keyCols, ", "),
				strings.Join(missing, ", ")), nil
		}
	}
	return "", nil
}

// schemaOrPublic is the schema this writer targets, defaulted the way
// PostgreSQL itself defaults it — the topology keys schemas by real name, and
// an empty writer schema means the search path's default.
func (w *RowWriter) schemaOrPublic() string {
	if w.schema == "" {
		return "public"
	}
	return w.schema
}
