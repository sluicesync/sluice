// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The CDC applier's UPDATE path on a sharded PlanetScale Neki target.
//
// # Why this exists separately from the upsert refusal
//
// [PreflightShardKeyUpsert] refuses a sharded target whose shard key is not
// contained in the upsert conflict key, because there `ON CONFLICT` cannot see
// the conflict and silently duplicates the key. Requiring the shard key to be
// IN the key makes the INSERT path safe.
//
// It does NOT make the UPDATE path safe, and the live test is what said so.
// After the preflight landed, a `sync` into a table keyed `(tenant_id, id)`
// and sharded on `tenant_id` copied cleanly, entered CDC, applied a DELETE —
// and died on the first UPDATE:
//
//	applier: update public.sk_good: ERROR: not implemented:
//	updating index column "tenant_id" is not supported (SQLSTATE NK013)
//
// [buildUpdateSQL] sets every column in the After image. The shard key is one
// of them, so the statement names it, and Neki refuses on the SHAPE — measured
// with the before and after values equal (neki-issues/NEKI-011). Being in the
// primary key does not exempt a column from the SET list, because the SET list
// is built from the row, not from the key.
//
// # Why omitting it here is safe, where omitting it from the UPSERT was not
//
// The two look like the same edit and are not, and the difference is the
// before-image.
//
// An `ON CONFLICT` upsert has no before-image: the statement cannot tell
// whether the stored row's shard key differs from the incoming one, so
// dropping the column from the SET list converts a loud NK013 into a silent
// duplicate primary key.
//
// A CDC UPDATE carries both images. So the question "did the shard key
// actually change?" is answerable, and the answer decides:
//
//   - UNCHANGED — drop the column from the SET list. Assigning a column its
//     own current value is a no-op, so the statement is semantically identical
//     and Neki accepts it. The WHERE clause is still built from the whole
//     before-image, shard key included, so the statement routes to the shard
//     that holds the row.
//   - CHANGED — refuse loudly. The row would have to move between shards,
//     which Neki cannot express at all: the same NK013 refuses a plain
//     `UPDATE … SET tenant_id = …`. There is nothing to degrade to, and
//     applying the other columns while silently leaving the routing column
//     behind would be a partial update — divergence with no signal, which is
//     the outcome this whole area exists to avoid.
//
// # Direction of the comparison's error
//
// Values arrive as `any` off the wire and two encodings of the same logical
// value can compare unequal. That is deliberately the tolerable direction: a
// false CHANGED verdict costs a loud, explained refusal, while a false
// UNCHANGED verdict would drop a real shard-key change silently. reflect.
// DeepEqual is used for exactly that bias — it is conservative, and every way
// it is wrong lands on the loud side.

// nekiShardKeyCache memoises the per-table shard-key columns an applier
// resolves, so a busy CDC stream does not re-walk the topology per change.
// Keyed by "schema.table" within one applier.
type nekiShardKeyCache struct {
	mu   sync.Mutex
	cols map[string][]string
}

// shardKeyColumnsFor returns the shard-key columns routing schema.table on
// this applier's target, or nil when the target is not a sharded Neki (which
// is every ordinary PostgreSQL endpoint, and every single-shard group).
//
// An error means the topology could not be read. It is returned rather than
// swallowed: on a Neki target this function's answer decides whether a
// statement is legal, so guessing "no shard key" would put us straight back
// on the NK013 failure with no explanation.
func (a *ChangeApplier) shardKeyColumnsFor(ctx context.Context, schema, table string) ([]string, error) {
	if !a.isNeki {
		return nil, nil
	}
	key := schema + "." + table
	a.shardKeys.mu.Lock()
	if a.shardKeys.cols != nil {
		if c, ok := a.shardKeys.cols[key]; ok {
			a.shardKeys.mu.Unlock()
			return c, nil
		}
	}
	a.shardKeys.mu.Unlock()

	snap, err := loadNekiTopology(ctx, a.serverKey, a.db)
	if err != nil {
		return nil, err
	}
	cols, sharded := snap.topo.shardKeyFor(snap.database, schema, table)
	if !sharded {
		cols = nil
	}

	a.shardKeys.mu.Lock()
	if a.shardKeys.cols == nil {
		a.shardKeys.cols = map[string][]string{}
	}
	a.shardKeys.cols[key] = cols
	a.shardKeys.mu.Unlock()
	return cols, nil
}

// dropUnchangedShardKeys returns `after` with every shard-key column whose
// value is unchanged removed, so the rendered SET list does not name it.
//
// A shard-key column whose value CHANGED is refused: see this file's header.
// A shard-key column absent from either image is left alone — there is nothing
// to compare and nothing to drop.
func dropUnchangedShardKeys(schema, table string, before, after ir.Row, shardKeys []string) (ir.Row, error) {
	if len(shardKeys) == 0 || len(after) == 0 {
		return after, nil
	}
	var drop []string
	var changed []string
	for _, k := range shardKeys {
		av, inAfter := after[k]
		if !inAfter {
			continue
		}
		bv, inBefore := before[k]
		if !inBefore {
			// No before-image for the routing column: we cannot establish
			// that it is unchanged, and the loud side is the safe side.
			changed = append(changed, k)
			continue
		}
		if reflect.DeepEqual(bv, av) {
			drop = append(drop, k)
			continue
		}
		changed = append(changed, k)
	}
	if len(changed) > 0 {
		return nil, &sluicecode.CodedError{
			Code: sluicecode.CodeTargetShardKeyUpdateUnsupported,
			Hint: "a row cannot move between shards on this target; stop routing on a column the source " +
				"updates, or exclude the table with --exclude-table and reconcile it separately",
			Err: fmt.Errorf(
				"postgres: applier: update %s.%s changes the target's shard-key column(s) (%s), which a "+
					"sharded PlanetScale Neki target cannot express — the row would have to move to a "+
					"different shard"+
					"\nsluice refuses rather than applying the other columns and leaving the routing column "+
					"behind, which would diverge silently",
				schema, table, strings.Join(changed, ", "),
			),
		}
	}
	if len(drop) == 0 {
		return after, nil
	}
	trimmed := make(ir.Row, len(after))
	for k, v := range after {
		trimmed[k] = v
	}
	for _, k := range drop {
		delete(trimmed, k)
	}
	if len(trimmed) == 0 {
		// Every column in the update was an unchanged shard key, so there is
		// nothing left to SET. Rendering `UPDATE t SET  WHERE …` would be a
		// syntax error; the caller treats a nil row as "no work".
		return nil, nil
	}
	return trimmed, nil
}
