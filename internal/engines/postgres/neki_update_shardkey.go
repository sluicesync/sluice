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
//
// Both images come off ONE decoder against the same relation column type, so
// encoding drift between them is not a live risk on this lane; the surviving
// asymmetries (`-0.0` versus `0.0`, a `time.Time` differing only by location,
// whitespace in a json byte slice) all resolve to CHANGED, which refuses.
//
// # The load-bearing premise is the BEFORE-IMAGE, not the comparison
//
// The sentence above grades the comparison and is true. It was also, on its
// own, a misleading safety argument: what actually decides correctness is
// whether the routing column is IN the before-image at all. On a PostgreSQL
// source it usually is not — the CDC reader narrows every before-image to the
// relation's identity columns — so a routing column outside the primary key
// never arrives and the "did it change?" question is unanswerable rather than
// answered conservatively. That case is refused as a SCHEMA mismatch, with
// its own code and its own message, in the `!inBefore` arm below.

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
	// Deliberately NOT gated on multiShard. The first cut was, and it was
	// wrong: measured on the live cluster, a table in a group with a SINGLE
	// key range still refuses `UPDATE … SET tenant_id = …` with NK013. The
	// refusal keys on a shard index applying to the table, not on how many
	// shards the group spans — so a single-shard group needs this handling
	// exactly as much as a split one. The multiShard flag governs the
	// DUPLICATION question instead, over in [RowWriter.ShardKeyUpsertMismatch].
	cols, _ := snap.topo.shardKeyFor(snap.database, schema, table)

	a.shardKeys.mu.Lock()
	if a.shardKeys.cols == nil {
		a.shardKeys.cols = map[string][]string{}
	}
	a.shardKeys.cols[key] = cols
	a.shardKeys.mu.Unlock()
	return cols, nil
}

// refuseShardKeyOutsideConflictKey refuses a CDC upsert whose conflict key
// does not contain the target's routing columns.
//
// This is the applier's counterpart to [RowWriter.upsertShardKeyPlanFor]'s
// refusal, and it is deliberately STRICTER: the writer permits the
// single-shard case because it can arm a guard predicate and check the
// server's affected-row count, while this lane's pipelined dispatch queues
// statements into a batch and has no per-statement count to check. Rather
// than emit a statement whose safety it cannot verify, it refuses the schema.
//
// The practical effect is one rule, easy to state and the same one the
// preflight gives: on a sharded Neki target, put the shard key in the primary
// key. Then nothing here fires, because a key column is already excluded from
// every SET list this package builds.
//
// nil shardKeys (every non-Neki target, and any table no shard index routes)
// returns nil immediately, so ordinary PostgreSQL pays nothing.
func refuseShardKeyOutsideConflictKey(schema, table string, conflictKey, shardKeys []string) error {
	if len(shardKeys) == 0 {
		return nil
	}
	inKey := make(map[string]struct{}, len(conflictKey))
	for _, c := range conflictKey {
		inKey[strings.ToLower(c)] = struct{}{}
	}
	var outside []string
	for _, sc := range shardKeys {
		if _, ok := inKey[strings.ToLower(sc)]; !ok {
			outside = append(outside, sc)
		}
	}
	if len(outside) == 0 {
		return nil
	}
	return &sluicecode.CodedError{
		Code: sluicecode.CodeTargetShardKeyNotInUpsertKey,
		Hint: "add the shard-key column(s) to the table's PRIMARY KEY on the target (or to a NOT NULL UNIQUE " +
			"index sluice can key on), then restart the stream; alternatively take the table out of scope " +
			"with --exclude-table",
		Err: fmt.Errorf(
			"postgres: applier: %s.%s is routed on (%s) but the change apply keys on (%s); %s is not in that key"+
				"\nan ON CONFLICT upsert carries no before-image, so it cannot establish that the routing "+
				"column is unchanged — writing it is refused by the target, and omitting it would either "+
				"leave the target's routing column disagreeing with the source or, across shards, insert a "+
				"second row under the same key"+
				"\nrefused rather than applied",
			schema, table, strings.Join(shardKeys, ", "), strings.Join(conflictKey, ", "),
			strings.Join(outside, ", "),
		),
	}
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
			// The routing column is not in the before-image, so nothing here
			// can establish whether it changed. Refusing is right; blaming the
			// ROW for it is not, and the first cut did.
			//
			// On a PostgreSQL source this is a SCHEMA fact, not a data one:
			// the CDC reader narrows every before-image to the relation's
			// identity key columns, so a shard key outside the primary key
			// (or the REPLICA IDENTITY set) is never present — and then EVERY
			// update to the table refuses, reporting a shard-key change that
			// did not happen, with a hint the operator cannot act on. Caught
			// by the pre-tag value-fidelity pass, which also spotted the tell:
			// the same table works when --where-filtered, because that path
			// emits a full before-image.
			//
			// So this arm reports the real, actionable problem — the target's
			// routing column is not covered by the key the stream identifies
			// rows with — and carries the schema code, not the data one.
			return nil, &sluicecode.CodedError{
				Code: sluicecode.CodeTargetShardKeyNotInUpsertKey,
				Hint: "add the shard-key column(s) to the table's PRIMARY KEY (or, on a PostgreSQL source, " +
					"to its REPLICA IDENTITY) so every change carries them, then restart the stream; " +
					"alternatively take the table out of scope with --exclude-table",
				Err: fmt.Errorf(
					"postgres: applier: update %s.%s cannot be applied: the target routes on %q, and that "+
						"column is absent from the change's before-image, so nothing establishes whether it "+
						"changed"+
						"\non a PostgreSQL source the before-image carries only the relation's identity "+
						"columns, so a routing column outside the primary key / REPLICA IDENTITY is never "+
						"present and every update to this table would refuse"+
						"\nthis is a schema mismatch between source and target, not a property of this row",
					schema, table, k,
				),
			}
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
