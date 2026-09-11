//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// # The sharded Neki fixture, built from nothing
//
// This is the harness the standing `neki-torture` database has been
// standing in for. Everything it does was previously done by hand — which
// is why the four cloud databases could not be torn down without losing the
// ability to reproduce them.
//
// The expensive part is NOT the tables. It is the TOPOLOGY: a shard index,
// a shard group with two key ranges, and a declared data-topology document
// naming every table. A fresh Neki database has exactly one shard and an
// empty topology, and `validate_data_topology` refuses a group naming a
// shard the cluster has not created — so the order below is forced:
//
//	create database  →  add a shard  →  declare the topology  →  create tables
//
// # The three table shapes, and why each exists
//
//	sk_good   shard key INSIDE the primary key. The supported shape; every
//	          refusal must stay silent on it. This is the anti-vacuity
//	          table — if a refusal fires here, the refusal is wrong.
//	sk_bad    shard key OUTSIDE the primary key. The shape where ON CONFLICT
//	          inserts a duplicate instead of updating, because the conflict
//	          is only evaluated on the shard the incoming row routes to.
//	uq_email  a UNIQUE constraint on a non-shard-key column. Accepted in
//	          full by the platform and enforced only WITHIN a shard.
//
// Two rows that route differently but collide on the constrained column are
// the measurement; the constraint's existence is not.

// nekiFixture is a provisioned, sharded Neki database with its own teardown.
type nekiFixture struct {
	name   string
	dsn    string
	shards []string
}

// provisionShardedNeki creates a Neki database, adds a second shard,
// declares a two-range topology over both, and creates the three table
// shapes. It registers its own teardown.
//
// Budget ~10 minutes: the database create is the long pole (418 s measured
// at --replicas 0; the HA shape used here is untimed and may be slower).
func provisionShardedNeki(ctx context.Context, t *testing.T, c psCreds) *nekiFixture {
	t.Helper()

	// The sweep runs FIRST, before anything is created — see the file
	// comment in nekiverify_provision_test.go for why that order is what
	// bounds a leak.
	if n := sweepOrphans(ctx, t, c); n > 0 {
		t.Logf("nekiverify: swept %d orphaned database(s) before provisioning", n)
	}

	name := fmt.Sprintf("%s%d", nekiPrefix, time.Now().UnixNano())
	t.Logf("nekiverify: creating %s (this takes several minutes)", name)

	if _, _, err := c.api(ctx, http.MethodPost, "/organizations/"+c.org+"/databases", map[string]any{
		"name":         name,
		"kind":         "neki",
		"region":       "us-east",
		"cluster_size": "PS-10-AWS-ARM-NEKI",
		// The HA shape, which is what the console offers and therefore what
		// customers run. Single-node Neki is creatable through this API and
		// refused by the console (neki-issues/NEKI-015) — a shape that works
		// today and may not be meant to exist is a bad foundation for a
		// weekly suite.
		"replicas": 2,
	}); err != nil {
		t.Fatalf("nekiverify: create database: %v", err)
	}

	fx := &nekiFixture{name: name}
	t.Cleanup(func() {
		// Unconditional: a failed assertion must not leak a billable
		// database. The sweep is the backstop for the case this cannot
		// cover — a runner that vanishes before Cleanup runs at all.
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if _, _, err := c.api(cctx, http.MethodDelete, "/organizations/"+c.org+"/databases/"+name, nil); err != nil {
			t.Errorf("nekiverify: LEAKED database %q — teardown failed: %v. The next run's orphan sweep will "+
				"delete it, but a recurring failure here means the sweep is carrying the whole burden", name, err)
			return
		}
		t.Logf("nekiverify: deleted %s", name)
	})

	waitBranchReady(ctx, t, c, name)
	fx.shards = addShard(ctx, t, c, name)
	fx.dsn = mintDSN(ctx, t, c, name)
	declareTopology(ctx, t, fx)
	createFixtureTables(ctx, t, fx)
	return fx
}

// waitBranchReady blocks until the branch reports ready.
func waitBranchReady(ctx context.Context, t *testing.T, c psCreds, db string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Minute)
	for time.Now().Before(deadline) {
		out, _, err := c.api(ctx, http.MethodGet, "/organizations/"+c.org+"/databases/"+db+"/branches/main", nil)
		if err == nil {
			if ready, _ := out["ready"].(bool); ready {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("nekiverify: context cancelled waiting for %s to become ready", db)
		case <-time.After(20 * time.Second):
		}
	}
	t.Fatalf("nekiverify: %s did not become ready within 20 minutes", db)
}

// addShard creates a second shard and waits for it, returning every shard
// uid on the branch.
//
// The endpoint is NOT the obvious one. `POST …/branches/{branch}/shards`
// answers 308 pointing at the configuration-profiles form; [psCreds.api]
// re-issues with the body intact. Neither `pscale` nor the 89-function
// __neki SQL surface exposes shard creation at all, which is why this is
// written down rather than left to be rediscovered.
func addShard(ctx context.Context, t *testing.T, c psCreds, db string) []string {
	t.Helper()
	base := "/organizations/" + c.org + "/databases/" + db + "/branches/main"

	if _, _, err := c.api(ctx, http.MethodPost, base+"/shards", map[string]any{}); err != nil {
		t.Fatalf("nekiverify: add shard: %v", err)
	}

	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		out, _, err := c.api(ctx, http.MethodGet, base+"/shards", nil)
		if err == nil {
			list, _ := out["data"].([]any)
			uids := make([]string, 0, len(list))
			allReady := len(list) >= 2
			for _, item := range list {
				s, _ := item.(map[string]any)
				uid, _ := s["name"].(string)
				uids = append(uids, uid)
				if ready, _ := s["ready"].(bool); !ready {
					allReady = false
				}
			}
			if allReady {
				t.Logf("nekiverify: %d shards ready: %v", len(uids), uids)
				return uids
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("nekiverify: context cancelled waiting for the second shard")
		case <-time.After(10 * time.Second):
		}
	}
	t.Fatal("nekiverify: the second shard did not become ready within 10 minutes")
	return nil
}

// mintDSN resets the default role and returns a usable connection string.
func mintDSN(ctx context.Context, t *testing.T, c psCreds, db string) string {
	t.Helper()
	out, _, err := c.api(ctx, http.MethodPost,
		"/organizations/"+c.org+"/databases/"+db+"/branches/main/roles", map[string]any{})
	if err != nil {
		t.Fatalf("nekiverify: mint role: %v", err)
	}
	dsn, _ := out["connection_url"].(string)
	if dsn == "" {
		dsn, _ = out["database_url"].(string)
	}
	if dsn == "" {
		t.Fatalf("nekiverify: role response carried no connection string (keys: %v)", mapKeys(out))
	}
	// verify-full needs a CA bundle the runner may not have; require still
	// encrypts and is what the operator docs prescribe for this endpoint.
	return strings.ReplaceAll(dsn, "sslmode=verify-full", "sslmode=require")
}

// declareTopology installs a shard index and a two-range shard group over
// the branch's shards, then waits for every router to apply it.
func declareTopology(ctx context.Context, t *testing.T, fx *nekiFixture) {
	t.Helper()
	if len(fx.shards) < 2 {
		t.Fatalf("nekiverify: need 2 shards to declare a sharded topology, have %d", len(fx.shards))
	}

	topo := map[string]any{
		"shard_indexes": map[string]any{
			"xxhash_tenant_id": map[string]any{"type": "xxhash", "columns": []string{"tenant_id"}},
		},
		"shard_groups": []any{map[string]any{
			"uid":                 "nv_group",
			"default_shard_index": "xxhash_tenant_id",
			"key_ranges": []any{
				map[string]any{"shard_uid": fx.shards[0], "end": "80"},
				map[string]any{"shard_uid": fx.shards[1], "start": "80"},
			},
		}},
		"authoritative_shard_group": "nv_group",
		"databases": map[string]any{
			"postgres": map[string]any{
				"default_shard_group": "nv_group",
				"schemas": map[string]any{"public": map[string]any{"tables": map[string]any{
					"sk_good":  map[string]any{"shard_group": "nv_group"},
					"sk_bad":   map[string]any{"shard_group": "nv_group"},
					"uq_email": map[string]any{"shard_group": "nv_group"},
				}}},
			},
		},
	}
	doc, err := json.Marshal(topo)
	if err != nil {
		t.Fatalf("nekiverify: marshal topology: %v", err)
	}

	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	var rev any
	if err := db.QueryRowContext(ctx,
		`SELECT __neki.set_data_topology($1, true, '{"comment":"nekiverify fixture"}')`, string(doc)).Scan(&rev); err != nil {
		t.Fatalf("nekiverify: set_data_topology: %v", err)
	}
	// Wait for every router, so a later CREATE TABLE cannot land on a
	// router that has not seen the topology yet.
	if _, err := db.ExecContext(ctx, `SELECT __neki.wait_for_data_topology($1, '120 seconds')`, rev); err != nil {
		t.Logf("nekiverify: wait_for_data_topology: %v (continuing; the tables below will fail loudly if the "+
			"topology has not landed)", err)
	}
}

// createFixtureTables creates the three shapes the suite measures against.
func createFixtureTables(ctx context.Context, t *testing.T, fx *nekiFixture) {
	t.Helper()
	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	// Each statement runs on its own — DDL inside an explicit transaction is
	// invisible to that transaction on Neki and does not survive the commit
	// (neki-issues/NEKI-013), so batching these would silently create
	// nothing.
	stmts := []string{
		// The supported shape: shard key inside the primary key. Anti-vacuity
		// for every refusal — a refusal that fires here is wrong.
		`CREATE TABLE IF NOT EXISTS sk_good (
			tenant_id int NOT NULL, id int NOT NULL, v text, PRIMARY KEY (tenant_id, id))`,
		// The duplicating shape: shard key OUTSIDE the primary key.
		`CREATE TABLE IF NOT EXISTS sk_bad (
			tenant_id int NOT NULL, id int NOT NULL PRIMARY KEY, v text)`,
		// UNIQUE on a non-shard-key column: accepted in full, enforced per shard.
		`CREATE TABLE IF NOT EXISTS uq_email (
			tenant_id int NOT NULL, id int NOT NULL, email text NOT NULL UNIQUE, PRIMARY KEY (tenant_id, id))`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("nekiverify: create fixture table: %v\nstatement: %s", err, s)
		}
	}
}

func openFixtureDB(t *testing.T, fx *nekiFixture) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", fx.dsn)
	if err != nil {
		t.Fatalf("nekiverify: open %s: %v", fx.name, err)
	}
	return db
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
