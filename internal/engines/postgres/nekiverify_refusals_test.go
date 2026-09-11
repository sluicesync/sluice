//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// # What this suite actually asserts, and why it is not a sluice test
//
// sluice carries three refusals that exist because of how a sharded Neki
// behaves: `SLUICE-E-TARGET-SHARD-KEY-NOT-IN-UPSERT-KEY`,
// `SLUICE-E-TARGET-SHARD-KEY-UPDATE-UNSUPPORTED`, and the advice that a
// `UNIQUE` constraint stops being global. Each rests on a PREMISE about the
// platform, measured once by hand on 2026-09-10.
//
// Neki is a platform preview. Those premises can change, and the dangerous
// direction is not the one people expect:
//
//   - If a refusal becomes UNNECESSARY (Neki starts enforcing UNIQUE
//     globally, say), sluice is merely over-cautious. Annoying, safe.
//   - If a LOUD refusal becomes SILENT ACCEPTANCE — the router stops
//     raising NK013 and quietly does something else with the row — then
//     sluice's protection is resting on a premise that no longer holds,
//     and nothing in sluice would say so.
//
// This file exists for the second case. It is a premise check, not a
// product test: it asserts that the PLATFORM still behaves the way sluice's
// refusals assume. That is the whole argument for spending money on a live
// cluster weekly rather than pinning this to a unit test, which could only
// ever confirm we still believe what we already believed.
//
// Every case carries an anti-vacuity arm, because each of these can pass
// for the wrong reason — a statement that errors for an unrelated reason
// looks identical to one refused on the shape, and two rows that fail to
// collide prove nothing if they landed on the same shard.

// tenantsOnDistinctShards finds two tenant_id values that route to
// DIFFERENT shards.
//
// It is discovered rather than hard-coded: routing is xxhash(tenant_id)
// against the key ranges, so which ids land where is not predictable from
// the outside and would silently rot if guessed. Every per-shard assertion
// below is meaningless unless the two ids genuinely differ in placement,
// which makes this the anti-vacuity foundation for the whole file.
func tenantsOnDistinctShards(ctx context.Context, t *testing.T, db *sql.DB, shards []string) (int, int) {
	t.Helper()

	// A spread of candidate tenants, written to the supported-shape table.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sk_good (tenant_id, id, v)
		 SELECT g, 900000 + g, 'probe' FROM generate_series(1, 40) g
		 ON CONFLICT (tenant_id, id) DO NOTHING`); err != nil {
		t.Fatalf("seed routing probe: %v", err)
	}

	perShard := map[string][]int{}
	for _, uid := range shards {
		if _, err := db.ExecContext(ctx, `SET __neki.shard = '`+uid+`'`); err != nil {
			t.Fatalf("pin to shard %s: %v", uid, err)
		}
		rows, err := db.QueryContext(ctx,
			`SELECT tenant_id FROM sk_good WHERE id >= 900000 ORDER BY tenant_id`)
		if err != nil {
			t.Fatalf("read tenants on shard %s: %v", uid, err)
		}
		for rows.Next() {
			var tid int
			if err := rows.Scan(&tid); err != nil {
				t.Fatalf("scan tenant: %v", err)
			}
			perShard[uid] = append(perShard[uid], tid)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate tenants on %s: %v", uid, err)
		}
	}
	if _, err := db.ExecContext(ctx, `RESET __neki.shard`); err != nil {
		t.Logf("reset shard pin: %v", err)
	}

	var a, b int
	var firstShard string
	for uid, tids := range perShard {
		if len(tids) == 0 {
			continue
		}
		if firstShard == "" {
			firstShard, a = uid, tids[0]
			continue
		}
		b = tids[0]
		t.Logf("routing: tenant_id %d → %s, tenant_id %d → %s", a, firstShard, b, uid)
		return a, b
	}
	t.Fatalf("could not find two tenant_ids on different shards (census: %v) — every per-shard assertion in this "+
		"file would be vacuous, so this is a hard stop rather than a skip", perShard)
	return 0, 0
}

func isPGCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// TestNekiverify_ShardedRefusalPremises checks the three platform
// behaviours sluice's sharded-target refusals are built on.
func TestNekiverify_ShardedRefusalPremises(t *testing.T) {
	c := nekiverifyCreds(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	fx := provisionShardedNeki(ctx, t, c)
	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	tenantA, tenantB := tenantsOnDistinctShards(ctx, t, db, fx.shards)

	// Tier-2 coverage item #4 rides this fixture rather than provisioning a
	// second database: none of its premises need sharding, and a database is
	// minutes of wall clock and a real bill. See nekiverify_ddl_sequence_test.go.
	nekiDDLAndSequencePremises(ctx, t, db)

	t.Run("PREMISE: the shard key may not be named in an UPDATE SET list", func(t *testing.T) {
		// sluice's SLUICE-E-TARGET-SHARD-KEY-UPDATE-UNSUPPORTED refusal, and
		// the dropUnchangedShardKeys machinery that avoids tripping it,
		// both exist because of this. The refusal is on the statement
		// SHAPE, not the data — it fires even assigning the column its own
		// current value.
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, 1, 'a')
			 ON CONFLICT (tenant_id, id) DO NOTHING`, tenantA); err != nil {
			t.Fatalf("seed: %v", err)
		}

		_, err := db.ExecContext(ctx,
			`UPDATE sk_good SET tenant_id = tenant_id WHERE tenant_id = $1 AND id = 1`, tenantA)
		if err == nil {
			t.Error("PREMISE BROKEN: assigning the shard key its OWN CURRENT VALUE was accepted. sluice drops " +
				"unchanged shard keys from its UPDATE SET list specifically because this is refused; if the " +
				"platform now accepts it, that machinery is unnecessary — and more importantly, a shard-key " +
				"UPDATE that CHANGES the value may now also be accepted, which would move a row between shards " +
				"in a way sluice's refusal assumes is impossible")
			return
		}
		if !isPGCode(err, "NK013") {
			t.Errorf("PREMISE SHIFTED: the shard-key SET refusal is no longer NK013, it is %v. sluice keys its "+
				"classification on that code", err)
			return
		}
		t.Logf("premise holds: %v", err)
	})

	t.Run("anti-vacuity: a NON-shard-key column updates fine", func(t *testing.T) {
		// Without this, the case above passes on any target where UPDATE is
		// broken for an unrelated reason.
		if _, err := db.ExecContext(ctx,
			`UPDATE sk_good SET v = 'updated' WHERE tenant_id = $1 AND id = 1`, tenantA); err != nil {
			t.Fatalf("updating a non-shard-key column failed (%v) — so the refusal above may be nothing to do "+
				"with the shard key, and that case proves nothing", err)
		}
	})

	t.Run("PREMISE: ON CONFLICT only sees the shard the incoming row routes to", func(t *testing.T) {
		// The measured cause of SLUICE-E-TARGET-SHARD-KEY-NOT-IN-UPSERT-KEY.
		// sk_bad has its PRIMARY KEY on id alone, with tenant_id as the
		// shard key — so two rows sharing an id but routing differently do
		// not see each other's conflict.
		const sharedID = 3001
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_bad (tenant_id, id, v) VALUES ($1, $2, 'first')`, tenantA, sharedID); err != nil {
			t.Fatalf("seed sk_bad: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_bad (tenant_id, id, v) VALUES ($1, $2, 'second')
			 ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v`, tenantB, sharedID); err != nil {
			// A refusal here would be the platform CLOSING the hole, which
			// is good news and still a premise change sluice should know
			// about.
			t.Logf("PREMISE CHANGED (in the SAFE direction): the cross-shard upsert was refused: %v. sluice's "+
				"SLUICE-E-TARGET-SHARD-KEY-NOT-IN-UPSERT-KEY preflight may no longer be load-bearing", err)
			return
		}

		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM sk_bad WHERE id = $1`, sharedID).Scan(&n); err != nil {
			t.Fatalf("count sk_bad: %v", err)
		}
		if n == 1 {
			t.Logf("PREMISE CHANGED (in the SAFE direction): the upsert UPDATED across shards rather than "+
				"duplicating — %d row for id=%d. The duplication sluice refuses to risk may no longer occur",
				n, sharedID)
			return
		}
		if n != 2 {
			t.Errorf("unexpected row count %d for id=%d; expected 2 (the duplication) or 1 (the hole closed)", n, sharedID)
			return
		}
		t.Logf("premise holds: %d rows share id=%d across shards — a PRIMARY KEY is not globally enforced, "+
			"which is exactly what sluice's upsert-key preflight refuses to walk into", n, sharedID)
	})

	t.Run("anti-vacuity: the same upsert UPDATES on the supported shape", func(t *testing.T) {
		// On sk_good the shard key is inside the primary key, so the
		// conflict is visible and the upsert must update rather than
		// duplicate. If this duplicated too, the case above would be
		// measuring something other than shard routing.
		const id = 4002
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, $2, 'first')
			 ON CONFLICT (tenant_id, id) DO UPDATE SET v = EXCLUDED.v`, tenantA, id); err != nil {
			t.Fatalf("seed sk_good: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, $2, 'second')
			 ON CONFLICT (tenant_id, id) DO UPDATE SET v = EXCLUDED.v`, tenantA, id); err != nil {
			t.Fatalf("upsert sk_good: %v", err)
		}
		var n int
		var v string
		if err := db.QueryRowContext(ctx,
			`SELECT count(*), max(v) FROM sk_good WHERE tenant_id = $1 AND id = $2`, tenantA, id).Scan(&n, &v); err != nil {
			t.Fatalf("count sk_good: %v", err)
		}
		if n != 1 || v != "second" {
			t.Errorf("the supported shape did not upsert cleanly: %d row(s), v=%q — the duplication case above "+
				"cannot be attributed to shard routing if the same statement misbehaves here", n, v)
		}
	})

	t.Run("PREMISE: UNIQUE is enforced only within a shard", func(t *testing.T) {
		// The basis for the operator guidance that a UNIQUE constraint
		// stops being global once the table is sharded.
		const email = "collide@example.com"
		if _, err := db.ExecContext(ctx,
			`INSERT INTO uq_email (tenant_id, id, email) VALUES ($1, 1, $2)`, tenantA, email); err != nil {
			t.Fatalf("seed uq_email: %v", err)
		}
		_, err := db.ExecContext(ctx,
			`INSERT INTO uq_email (tenant_id, id, email) VALUES ($1, 2, $2)`, tenantB, email)
		if err != nil {
			if isPGCode(err, "23505") {
				t.Logf("PREMISE CHANGED (in the SAFE direction): the cross-shard duplicate was rejected 23505 — "+
					"UNIQUE now appears to be enforced globally. The operator guidance that it is per-shard "+
					"would be wrong: %v", err)
				return
			}
			t.Fatalf("cross-shard insert failed for an unexpected reason: %v", err)
		}

		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM uq_email WHERE email = $1`, email).Scan(&n); err != nil {
			t.Fatalf("count uq_email: %v", err)
		}
		if n != 2 {
			t.Errorf("expected 2 rows sharing a UNIQUE email across shards, found %d", n)
			return
		}
		t.Logf("premise holds: %d rows share a UNIQUE email across shards — the constraint is enforced per shard", n)
	})

	t.Run("anti-vacuity: UNIQUE still fires WITHIN one shard", func(t *testing.T) {
		// If the constraint were simply not enforced at all, the case above
		// would pass for the wrong reason.
		const email = "within@example.com"
		if _, err := db.ExecContext(ctx,
			`INSERT INTO uq_email (tenant_id, id, email) VALUES ($1, 10, $2)`, tenantA, email); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, err := db.ExecContext(ctx,
			`INSERT INTO uq_email (tenant_id, id, email) VALUES ($1, 11, $2)`, tenantA, email)
		if err == nil {
			t.Error("the UNIQUE constraint did not fire even WITHIN a single shard — so the cross-shard case " +
				"above proves nothing about sharding; the constraint is simply not enforced")
			return
		}
		if !isPGCode(err, "23505") {
			t.Errorf("within-shard duplicate failed with %v, want SQLSTATE 23505", err)
		}
	})
}
