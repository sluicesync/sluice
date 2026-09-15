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

// # BUDGETS AND ORDER — where the suite's wall clock is spent, and why
//
// ## Every arm gets a budget of its own
//
// Until run 34932058458 (2026-09-15) this function ran every arm on ONE
// 30-minute context. That run is what the shared budget costs: the NK306 arm
// — two single-row INSERTs, 0.33 s and 0.36 s in the runs either side of it —
// took 1008.76 s, and the backup arm then died mid-read when the shared
// context expired at 1800.81 s. Eight arms never ran, including the
// shard-targeting probe the dispatch had been paid for, and the run's only red
// pointed at the arm that happened to be holding the context when it expired
// rather than the one that spent it.
//
// So each arm now runs under [nekiArm] with a ceiling of its own. The
// ceilings are sized from the MEASURED cost in run 34928571469 (the healthy
// run of the same code), rounded up hard — roughly 4× measured with a 90 s
// floor — because the purpose is to bound a stall, not to police a slow-ish
// arm into a flake. They are deliberately NOT additive: the suite ceiling
// below is the wall, and the per-arm ceilings exist so that no single arm can
// reach it. Normal total work is ~5.5 minutes, so several arms can burn their
// whole budget and the rest of the suite still runs.
//
// The suite ceiling is 42 minutes against the workflow's `-timeout=50m`,
// leaving room for [TestNekiverify_FixtureProvisions] (measured 204 s) and for
// the fixture teardown that deletes the database. Provisioning takes a budget
// of its own — measured 109–789 s across eight runs — so a slow control plane
// cannot present as every arm being starved.
//
// ## Order
//
// Cheap and decisive first, expensive and destructive last. Two orderings are
// load-bearing rather than aesthetic:
//
//   - The SHARD-TARGETING probe now runs BEFORE restore/backup. It was last
//     but one, on the argument that it writes transient rows into sk_good and
//     the backup arm reads the whole schema. That argument is satisfied by
//     the probe cleaning up WITHIN its own arm — every row it inserts is
//     removed in a `t.Cleanup` on its own subtest, so the rows are gone before
//     this function calls the next arm, and the backup arm takes its
//     independent expected value afterwards. What the old order did NOT
//     survive is a stall upstream of it: it is the cheapest, most decisive arm
//     in the suite and the one this dispatch existed to run, and in run
//     34932058458 it never executed. A per-arm budget alone would have saved
//     it; ordering it ahead of the two multi-minute arms is the belt to that
//     braces.
//   - MoveTables stays LAST. It creates a workflow that blocks a table and a
//     second logical database, and although it reverses both, a failure
//     part-way through leaves the cluster in a state no later arm should have
//     to reason about. Running it last means the only thing downstream of a
//     bad outcome is teardown.

// TestNekiverify_ShardedRefusalPremises checks the three platform
// behaviours sluice's sharded-target refusals are built on.
func TestNekiverify_ShardedRefusalPremises(t *testing.T) {
	c := nekiverifyCreds(t)

	ctx, cancel := context.WithTimeout(context.Background(), 42*time.Minute)
	defer cancel()

	var fx *nekiFixture
	nekiArm(ctx, t, "provision the sharded fixture", 20*time.Minute, func(ctx context.Context) {
		fx = provisionShardedNeki(ctx, t, c)
	})
	if fx == nil {
		// Reachable only if provisioning returns without having called
		// t.Fatalf — which today it cannot, but a nil dereference two lines
		// down would report a panic where the truth is "the fixture was never
		// provisioned", and that is a bad first line for a weekly's log.
		t.Fatal("nekiverify: the fixture was never provisioned, so no arm below has anything to run against")
	}
	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	var tenantA, tenantB int
	nekiArm(ctx, t, "discover two tenants on distinct shards", 3*time.Minute, func(ctx context.Context) {
		tenantA, tenantB = tenantsOnDistinctShards(ctx, t, db, fx.shards)
	})

	// Tier-2 coverage item #4 rides this fixture rather than provisioning a
	// second database: none of its premises need sharding, and a database is
	// minutes of wall clock and a real bill. See nekiverify_ddl_sequence_test.go.
	nekiArm(ctx, t, "DDL and sequence premises", 3*time.Minute, func(ctx context.Context) {
		nekiDDLAndSequencePremises(ctx, t, db)
	})

	// And the remedies sluice's Neki refusals tell operators to run must
	// still exist on the router — a hint that cannot run is read mid-incident.
	nekiArm(ctx, t, "remedy functions exist", 2*time.Minute, func(ctx context.Context) {
		t.Run("PREMISE: every __neki function sluice's remedies name still exists", func(t *testing.T) {
			nekiRemedyFunctionsExist(ctx, t, db)
		})
	})

	// The platform facts sluice COMPILES IN, measured rather than trusted.
	// Both ride this fixture — neither needs a second database, and a database
	// is minutes of wall clock and a real bill.
	//
	// These are the premise-naming rule applied where it can actually be
	// applied: nekiConcurrentCopyLimit and the mandatory shard key are claims
	// about somebody else's platform baked into shipped behaviour, and a unit
	// test can only ever prove sluice agrees with itself about them.
	nekiArm(ctx, t, "concurrent-COPY limit", 4*time.Minute, func(ctx context.Context) {
		nekiConcurrentCopyLimitHoldsOnTheCluster(ctx, t, fx, []int{tenantA, tenantB})
	})
	nekiArm(ctx, t, "shard key required on INSERT (NK306)", 90*time.Second, func(ctx context.Context) {
		nekiShardKeyRequiredOnInsert(ctx, t, db, tenantA)
	})
	nekiArm(ctx, t, "hostile shard-key routing corpus", 2*time.Minute, func(ctx context.Context) {
		nekiShardKeyRoutingCorpus(ctx, t, db, fx.shards)
	})

	// Bisecting the open NK306 finding. These run BEFORE the CDC arm on
	// purpose: they answer "whose fault is it" independently of whether that
	// arm passes, so a week where CDC fails still yields the diagnosis rather
	// than only the symptom. sk_good is the fixture's own sharded table, so
	// none depends on the CDC arm having created anything.
	nekiArm(ctx, t, "shard key is not reported GENERATED", 90*time.Second, func(ctx context.Context) {
		nekiShardKeyIsNotReportedGenerated(ctx, t, db, "sk_good", "tenant_id")
	})
	nekiArm(ctx, t, "control tables in a separate schema", 2*time.Minute, func(ctx context.Context) {
		nekiControlTableSchemaProbe(ctx, t, db)
	})
	nekiArm(ctx, t, "control table with a constant shard key", 2*time.Minute, func(ctx context.Context) {
		nekiControlTableShardKeyProbe(ctx, t, db, "tenant_id")
	})
	nekiArm(ctx, t, "control table in the authoritative shard group", 2*time.Minute, func(ctx context.Context) {
		nekiControlTableAuthoritativeGroupProbe(ctx, t, db, readAuthoritativeShardGroup(ctx, t, fx))
	})
	nekiArm(ctx, t, "set_data_topology write semantics", 2*time.Minute, func(ctx context.Context) {
		nekiTopologyWriteSemanticsProbe(ctx, t, db)
	})

	// What a SHARD-TARGETED session can do — the probe that decides whether
	// per-shard consistent reads and composite-position incrementals are
	// mechanisms sluice could have, or neither. Cheap and decisive, so it runs
	// ahead of the multi-minute arms; see this function's ordering note.
	// See nekiverify_shard_targeting_probe_test.go.
	nekiArm(ctx, t, "shard-targeted session probe", 4*time.Minute, func(ctx context.Context) {
		nekiShardTargetedSessionProbe(ctx, t, db, fx, tenantA, tenantB, readAuthoritativeShardGroup(ctx, t, fx))
	})

	nekiArm(ctx, t, "CDC serial vs batched applier bisect", 4*time.Minute, func(ctx context.Context) {
		nekiCDCSerialVsBatchedIntoSharded(ctx, t, db, fx, tenantA, tenantB)
	})

	// Coverage item #2: CDC into this sharded target, graded on ordered
	// CONTENT rather than a row count.
	nekiArm(ctx, t, "CDC into a sharded target", 4*time.Minute, func(ctx context.Context) {
		nekiCDCIntoShardedTarget(ctx, t, db, fx, tenantA, tenantB)
	})

	// backup/restore against a sharded Neki — filed 2026-09-14, blocked on
	// the NK306 control-table fix and unblocked by ADR-0187.
	//
	// RESTORE FIRST, then BACKUP, so the backup arm can include the tables the
	// restore created: two more sharded tables to scatter-read across, at no
	// extra cost. Both run AFTER the CDC arm, which is what has already
	// created and placed the applier's control tables — the restore arm's
	// placement check is worded as a statement about the target's STATE for
	// exactly that reason, and measures separately which control tables the
	// restore itself created. See nekiverify_backup_restore_test.go.
	nekiArm(ctx, t, "restore INTO a sharded Neki", 5*time.Minute, func(ctx context.Context) {
		nekiRestoreIntoShardedTarget(ctx, t, db, fx, tenantA, tenantB)
	})
	nekiArm(ctx, t, "backup FROM a sharded Neki", 5*time.Minute, func(ctx context.Context) {
		nekiBackupFromShardedSource(ctx, t, db, fx, tenantA, tenantB)
	})

	nekiArm(ctx, t, "statement-shape premises", 3*time.Minute, func(ctx context.Context) {
		nekiStatementShapePremises(ctx, t, db, tenantA, tenantB)
	})

	// Coverage item #3: the NK213 block from a real MoveTables cutover. LAST,
	// deliberately — see this function's ordering note.
	nekiArm(ctx, t, "MoveTables blocks with NK213", 5*time.Minute, func(ctx context.Context) {
		nekiMoveTablesBlocksWithNK213(ctx, t, db)
	})
}

// nekiStatementShapePremises checks the refusals sluice's sharded-target
// preflights are built on: the shard key in an UPDATE SET list, ON CONFLICT
// across shards, and UNIQUE's scope.
//
// Extracted from the test function so it can take a budget of its own like
// every other arm — see the budget note on
// [TestNekiverify_ShardedRefusalPremises].
func nekiStatementShapePremises(ctx context.Context, t *testing.T, db *sql.DB, tenantA, tenantB int) {
	t.Helper()

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
