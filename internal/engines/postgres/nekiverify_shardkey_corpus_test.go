//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"
)

// # An adversarial corpus for the one thing only a real router can answer
//
// The general adversarial-corpus work in this project asks "does any value
// survive the round trip?" and belongs on a diff trigger, not a schedule: its
// answer moves when sluice's codecs change, not when time passes. This is the
// slice that does NOT have that property, because it is about the PLATFORM's
// routing and nothing outside a live sharded cluster can exercise it.
//
// # The class it exists for
//
// The router hashes the shard key to choose a shard, and this suite separately
// proves that UNIQUE is enforced only WITHIN a shard. Put those together and a
// routing discrepancy is a silent-duplication engine: the same logical row
// landing on two shards is two rows that no constraint is watching, at exit 0,
// with every liveness signal green. That outranks everything else in this
// codebase's priority order, and it is invisible to any test that reads the
// table without pinning a shard.
//
// # What this arm covers, stated narrowly
//
// DETERMINISM and REACH of the routing function over hostile shard-key values:
// every value routes somewhere, the same value always routes to the same
// place, and the corpus as a whole reaches more than one shard. Boundary
// values are the interesting half — xxhash over a SIGNED integer is exactly
// where hash implementations differ, and negatives, zero and the type's
// extremes are the values a tenant-id column acquires by accident.
//
// # What it does NOT cover, and why not yet
//
// The sharper question is whether SLUICE's rendering of a shard-key value
// routes the same way a direct write does — if sluice normalises a value in
// any way (encoding, trailing space, unicode form), its row lands on a
// different shard than the source's would, which is the silent-duplication
// case above.
//
// That differential is NOT BUILDABLE TODAY: it needs sluice's applier to write
// into this fixture, and both apply lanes are currently refused — the serial
// one on sluice's control tables and the pipelined one on the data table (see
// the NK306 bisect arm). This arm therefore establishes the per-shard census
// harness the differential will use, and the differential itself is filed
// rather than faked. A version that wrote only through `db` and called it a
// sluice test would be the evidence-sharing failure this repo keeps finding:
// the check and the thing checked taking the same path.
//
// The TEXT shard key is the other uncovered half and is deliberately out of
// scope here: this fixture declares `xxhash_tenant_id` over an `int`, so a text
// shard key means a second shard index in the topology document — which the
// MoveTables arm also reads and reduces. Changing a shared fixture document to
// add a value family is how one arm starts failing for another arm's reasons.
func nekiShardKeyRoutingCorpus(ctx context.Context, t *testing.T, db *sql.DB, shards []string) {
	t.Helper()

	t.Run("CORPUS: hostile shard-key values route deterministically and reach >1 shard", func(t *testing.T) {
		// The column is `int` (int4), so the extremes are int32's. Each value
		// is named so a failure says WHICH family broke rather than printing a
		// number and leaving the reader to work out why it was chosen.
		corpus := []struct {
			name  string
			value int32
		}{
			{"zero", 0},
			{"one", 1},
			{"minus-one", -1},
			{"int32-min", -2147483648},
			{"int32-max", 2147483647},
			{"small-negative", -42},
			{"power-of-two", 65536},
			{"power-of-two-minus-one", 65535},
			{"large-negative", -1000000},
			{"large-positive", 1000000},
		}

		const idBase = 950000

		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, _ = db.ExecContext(cctx, `DELETE FROM sk_good WHERE id >= $1`, idBase)
		})

		// Write each value twice, under two different ids. Same shard key, so
		// both rows MUST route identically — that is the determinism half, and
		// it is checked against the router's own behaviour rather than against
		// any expectation of ours.
		for i, c := range corpus {
			for _, dup := range []int{0, 1} {
				if _, err := db.ExecContext(ctx,
					`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, $2, $3)
					 ON CONFLICT (tenant_id, id) DO NOTHING`,
					c.value, idBase+i*2+dup, c.name); err != nil {
					t.Fatalf("the corpus value %s (%d) could not be written at all: %v\n\n"+
						"A shard key the router will not accept is itself a finding — sluice's preflight "+
						"does not screen tenant-id VALUES, so a source carrying this one would fail "+
						"mid-copy rather than at the door", c.name, c.value, err)
				}
			}
		}

		// Census: which shard is each row actually on? Read with the shard pin
		// the refusal suite already relies on, because an unpinned read merges
		// the shards and would make a routing split invisible — which is the
		// entire failure mode under test.
		placement := map[string][]string{} // value name -> shards it appears on
		reached := map[string]bool{}       // shard uid -> saw at least one corpus row

		for _, uid := range shards {
			if _, err := db.ExecContext(ctx, `SET __neki.shard = '`+uid+`'`); err != nil {
				t.Fatalf("pin to shard %s: %v", uid, err)
			}
			rows, err := db.QueryContext(ctx,
				`SELECT DISTINCT v FROM sk_good WHERE id >= $1`, idBase)
			if err != nil {
				t.Fatalf("census on shard %s: %v", uid, err)
			}
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatalf("scan census row on %s: %v", uid, err)
				}
				placement[name] = append(placement[name], uid)
				reached[uid] = true
			}
			_ = rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate census on %s: %v", uid, err)
			}
		}
		if _, err := db.ExecContext(ctx, `RESET __neki.shard`); err != nil {
			t.Logf("reset shard pin: %v", err)
		}

		// ANTI-VACUITY, first and hardest. A corpus that all landed on one
		// shard would satisfy every assertion below while testing nothing
		// about routing — the same trap tenantsOnDistinctShards exists to
		// avoid for the rest of this file.
		if len(reached) < 2 {
			t.Fatalf("every corpus value landed on %d shard(s) (%v) out of %d. The determinism check "+
				"below would then be trivially true, so this is a hard stop: the corpus needs values "+
				"that genuinely spread across the key ranges before it can say anything about routing",
				len(reached), reached, len(shards))
		}

		// REACH: every value must have landed somewhere. A value present in
		// the corpus and absent from every shard is a row that was accepted
		// and cannot be read back, which is the worst outcome available here.
		var missing []string
		for _, c := range corpus {
			if len(placement[c.name]) == 0 {
				missing = append(missing, fmt.Sprintf("%s(%d)", c.name, c.value))
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("these shard-key values were INSERTed without error and appear on NO shard: %v\n\n"+
				"The write was accepted and the row is not readable from any shard — rows that exist "+
				"nowhere are the silent-loss shape, and no constraint or count would reveal them",
				missing)
		}

		// DETERMINISM: one shard key, one shard. Two rows sharing a tenant_id
		// that landed on different shards means the routing function is not a
		// function of the value alone — and since UNIQUE is enforced only
		// within a shard, that is the silent-duplication engine this arm
		// exists for.
		var split []string
		for _, c := range corpus {
			if len(placement[c.name]) > 1 {
				split = append(split, fmt.Sprintf("%s(%d) -> %v", c.name, c.value, placement[c.name]))
			}
		}
		sort.Strings(split)
		if len(split) > 0 {
			t.Errorf("these shard-key values routed to MORE THAN ONE shard: %v\n\n"+
				"Two rows with the same shard key must land on the same shard — routing is supposed to "+
				"be a function of the value. It is not, for these values. Combined with UNIQUE being "+
				"enforced only WITHIN a shard (proven elsewhere in this suite), a key that routes two "+
				"ways is a duplicate that nothing on either side is watching.", split)
		}

		t.Logf("corpus routed %d values across %d shard(s), deterministically", len(corpus), len(reached))
	})
}
