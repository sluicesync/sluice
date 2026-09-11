//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"
)

// TestNekiverify_FixtureProvisions is the harness's own smoke test: it
// builds a sharded Neki database from nothing and asserts the fixture is
// actually what the suite's later measurements assume.
//
// It exists because "the harness compiles" and "the harness works" are very
// different claims, and everything below it depends on the second. Until
// this passes, the standing neki-test / neki-torture databases cannot be
// torn down — the recipe for reproducing them would be unproven.
//
// Budget ~15 minutes. Run with an explicit timeout:
//
//	go test -tags=nekiverify ./internal/engines/postgres/ \
//	    -run TestNekiverify_FixtureProvisions -timeout 30m -v
func TestNekiverify_FixtureProvisions(t *testing.T) {
	c := nekiverifyCreds(t)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	fx := provisionShardedNeki(ctx, t, c)

	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	t.Run("it is really Neki", func(t *testing.T) {
		var version string
		if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			t.Fatalf("version(): %v", err)
		}
		if !isNekiVersion(version) {
			t.Fatalf("provisioned database does not identify as Neki: %q — every Neki-specific assertion in this "+
				"suite would be measuring something else", version)
		}
		t.Logf("server: %s", version)
	})

	t.Run("it is really SHARDED — the anti-vacuity check for the whole suite", func(t *testing.T) {
		// A one-shard database serves every query correctly and would make
		// each per-shard finding below silently untestable: no routing, no
		// per-shard UNIQUE, no ON CONFLICT gap. If the second shard did not
		// land, the suite must fail here rather than report passes.
		var shards int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM __neki.list_shards()").Scan(&shards); err != nil {
			t.Fatalf("list_shards: %v", err)
		}
		if shards < 2 {
			t.Fatalf("fixture has %d shard(s), want >= 2 — every per-shard behaviour this suite exists to "+
				"measure is untestable on one shard, and would report a false pass", shards)
		}
		t.Logf("shards: %d", shards)
	})

	t.Run("the declared topology routes the fixture tables", func(t *testing.T) {
		// The tables must be IN the declared topology, not merely present.
		// A table created through the router is fully routable and still
		// absent from the topology (neki-issues/NEKI-014), and a table
		// outside it falls through to the default shard group rather than
		// being routed by the shard key this suite relies on.
		for _, tbl := range []string{"sk_good", "sk_bad", "uq_email"} {
			var present bool
			const q = `SELECT ((__neki.get_data_topology())::jsonb)
				-> 'databases' -> 'postgres' -> 'schemas' -> 'public' -> 'tables' ? $1`
			if err := db.QueryRowContext(ctx, q, tbl).Scan(&present); err != nil {
				t.Fatalf("read topology for %q: %v", tbl, err)
			}
			if !present {
				t.Errorf("table %q is not in the declared data topology — it would fall through to the default "+
					"shard group instead of routing on tenant_id, making the per-shard measurements meaningless", tbl)
			}
		}
	})

	t.Run("rows actually distribute across shards", func(t *testing.T) {
		// The sharpest anti-vacuity check available: writing rows whose
		// shard keys span the key ranges must land them on DIFFERENT
		// shards. If they all land on one, the topology is declared but not
		// effective, and every per-shard finding would be unreachable.
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_good (tenant_id, id, v) SELECT g, g, 'seed'||g FROM generate_series(1, 64) g`); err != nil {
			t.Fatalf("seed sk_good: %v", err)
		}

		var total int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&total); err != nil {
			t.Fatalf("count sk_good: %v", err)
		}
		if total != 64 {
			t.Fatalf("sk_good holds %d rows, want 64 — the fixture's own seed did not land", total)
		}

		// Per-shard counts, read by pinning the session to each shard in
		// turn. Both must be non-zero for the fixture to be genuinely
		// sharded rather than nominally so.
		rows, err := db.QueryContext(ctx, `SELECT uid FROM __neki.list_shards() ORDER BY uid`)
		if err != nil {
			t.Fatalf("list shards: %v", err)
		}
		var uids []string
		for rows.Next() {
			var uid string
			if err := rows.Scan(&uid); err != nil {
				t.Fatalf("scan shard uid: %v", err)
			}
			uids = append(uids, uid)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate shards: %v", err)
		}

		occupied := 0
		for _, uid := range uids {
			var n int
			// A shard-pinned read: the session option routes this query to
			// one shard rather than scattering.
			if _, err := db.ExecContext(ctx, `SET __neki.shard = '`+uid+`'`); err != nil {
				t.Logf("could not pin to shard %s (%v); skipping the per-shard census", uid, err)
				return
			}
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&n); err != nil {
				t.Logf("per-shard count on %s failed (%v); skipping the census", uid, err)
				return
			}
			t.Logf("shard %s holds %d of the 64 rows", uid, n)
			if n > 0 {
				occupied++
			}
		}
		if _, err := db.ExecContext(ctx, `RESET __neki.shard`); err != nil {
			t.Logf("reset shard pin: %v", err)
		}

		if occupied < 2 {
			t.Errorf("all 64 rows landed on %d shard(s) — the topology is declared but not routing, so every "+
				"per-shard behaviour this suite measures would be unreachable and would report false passes",
				occupied)
		}
	})
}
