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
	"net/url"
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
// Budget ~10 minutes, with headroom. Measured end to end 2026-09-11: create
// through BOTH shards ready took 169 s at --replicas 2 — FASTER than the
// 418 s measured at --replicas 0 the day before, which is the opposite of
// what the earlier note here predicted. Two runs of one database each is
// not enough to call HA genuinely quicker; the honest reading is that
// provisioning time VARIES by several minutes and a timeout tuned to one
// observation would be flaky.
func provisionShardedNeki(ctx context.Context, t *testing.T, c psCreds) *nekiFixture {
	t.Helper()

	// The sweep runs FIRST, before anything is created — see the file
	// comment in nekiverify_provision_test.go for why that order is what
	// bounds a leak.
	if n := sweepOrphans(ctx, t, c); n > 0 {
		t.Logf("nekiverify: swept %d orphaned database(s) before provisioning", n)
	}

	name := fmt.Sprintf("%s%d", nekiPrefix, time.Now().UnixNano())
	t.Logf("nekiverify: creating %s at %s UTC (this takes several minutes)", name, time.Now().UTC().Format(time.RFC3339))

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
	// The DSN is minted BEFORE the shard is added, because the shard census
	// below reads the ROUTER's view over SQL rather than the control
	// plane's — see [waitShardsVisibleToRouter].
	fx.dsn = mintDSN(ctx, t, c, name)
	addShard(ctx, t, c, name)
	fx.shards = waitShardsVisibleToRouter(ctx, t, fx, 2)
	declareTopology(ctx, t, fx)
	createFixtureTables(ctx, t, fx)
	return fx
}

// waitBranchReady blocks until the branch reports ready, and logs the
// BRANCH ID and a UTC timestamp when it does.
//
// Both are logged deliberately rather than incidentally. Anything surprising
// this suite finds becomes a report shared with PlanetScale, and those are
// the two fields that make one cross-referenceable against THEIR logs: the
// branch id is the handle their systems index on (a database name is ours
// and means little to them), and UTC timing is what lines up against a
// server-side trace. Operator requirement, 2026-09-11 — see
// neki-issues/README.md.
//
// Capturing it here rather than relying on whoever writes the report to
// remember is the point: these databases are deleted at the end of every
// run, so a branch id not captured while it existed cannot be recovered.
func waitBranchReady(ctx context.Context, t *testing.T, c psCreds, db string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Minute)
	for time.Now().Before(deadline) {
		out, _, err := c.api(ctx, http.MethodGet, "/organizations/"+c.org+"/databases/"+db+"/branches/main", nil)
		if err == nil {
			if ready, _ := out["ready"].(bool); ready {
				branchID, _ := out["id"].(string)
				t.Logf("nekiverify: branch READY at %s — database=%q branch_id=%q (quote both in any Neki finding: "+
					"the branch id is what PlanetScale can cross-reference, and this database is deleted at the "+
					"end of the run)",
					time.Now().UTC().Format(time.RFC3339), db, branchID)
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
func addShard(ctx context.Context, t *testing.T, c psCreds, db string) {
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
				t.Logf("nekiverify: control plane reports %d shards ready: %v", len(uids), uids)
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("nekiverify: context cancelled waiting for the second shard")
		case <-time.After(10 * time.Second):
		}
	}
	t.Fatal("nekiverify: the second shard did not become ready within 10 minutes")
}

// mintDSN resets the default role and returns a usable connection string.
func mintDSN(ctx context.Context, t *testing.T, c psCreds, db string) string {
	t.Helper()
	// inherited_roles is REQUIRED, and an empty body is not a sensible
	// default — measured 2026-09-11: a role minted with `{}` gets neither
	// `neki_operator` nor REPLICATION, so `__neki.set_data_topology` fails
	// with "permission denied for function set_data_topology" (42501) and
	// the fixture cannot declare its own topology.
	//
	// `__neki.list_metafuncs()` publishes the requirement per function in
	// its `required_role` column: the read side (get_data_topology,
	// list_shards, wait_for_data_topology) needs `neki_viewer`, while
	// set_data_topology needs `neki_operator`. Inheriting `postgres` is what
	// carries both — the working operator DSN connects as `postgres` with
	// memberships neki_viewer, neki_operator, pscale_superuser.
	//
	// with_replication is requested here rather than later because the CDC
	// arms of this suite need a replication connection, and the privilege
	// cannot be added to an existing role without minting a new one.
	out, _, err := c.api(ctx, http.MethodPost,
		"/organizations/"+c.org+"/databases/"+db+"/branches/main/roles", map[string]any{
			"inherited_roles":  []string{"postgres"},
			"with_replication": true,
		})
	if err != nil {
		t.Fatalf("nekiverify: mint role: %v", err)
	}
	// The role endpoint does NOT return a ready-made connection string —
	// measured 2026-09-11, the response carries components instead
	// (username, password, access_host_url, database_name) and no
	// connection_url/database_url of any kind. `pscale role reset-default
	// --format json` does synthesise one, which is what made the assumption
	// look safe; the raw API does not.
	if dsn, _ := out["connection_url"].(string); dsn != "" {
		return strings.ReplaceAll(dsn, "sslmode=verify-full", "sslmode=require")
	}

	user, _ := out["username"].(string)
	pass, _ := out["password"].(string)
	host, _ := out["access_host_url"].(string)
	name, _ := out["database_name"].(string)
	if user == "" || pass == "" || host == "" {
		t.Fatalf("nekiverify: role response carried neither a connection string nor the components to build one "+
			"(keys: %v)", mapKeys(out))
	}
	if name == "" {
		name = "postgres"
	}
	// sslmode=require rather than verify-full: verify-full needs a CA bundle
	// the runner may not have, and require still encrypts.
	return fmt.Sprintf("postgresql://%s:%s@%s:5432/%s?sslmode=require",
		url.QueryEscape(user), url.QueryEscape(pass), host, name)
}

// declareTopology installs a shard index and a two-range shard group over
// the branch's shards, then waits for every router to apply it.
func declareTopology(ctx context.Context, t *testing.T, fx *nekiFixture) {
	t.Helper()
	if len(fx.shards) < 2 {
		t.Fatalf("nekiverify: need 2 shards to declare a sharded topology, have %d", len(fx.shards))
	}

	// TWO shard groups, and the split is mandatory rather than stylistic.
	//
	// The AUTHORITATIVE group "must have exactly one key_range" (measured
	// 2026-09-11 — set_data_topology rejects a two-range group in that
	// role), because it is the group that owns sequences and schema
	// publishing; the validator's own remediation text calls it "the
	// single-shard group that owns sequences and schema publishing". So it
	// cannot double as the group the fixture tables shard across.
	//
	// This is why a live neki-torture carried a single-range group named
	// after a shard uid alongside its multi-range ones — a structure that
	// looked like leftovers from earlier experiments and is in fact
	// required.
	// WHICH shard is authoritative is not ours to choose. The database is
	// created with one shard, that shard is already the authoritative one,
	// and set_data_topology refuses to move the role:
	//
	//	authoritative shard cannot change from shard "shy49apio09k0p"
	//	to shard "shqm0dlbf83wx0"
	//
	// Picking fx.shards[0] produced exactly that, because readShardUIDs
	// orders by uid — ALPHABETICALLY, not by creation order. The two logs
	// from that run show the trap plainly: the control plane listed
	// [shy49…, shqm0…] (creation order) while the router listed
	// [shqm0…, shy49…] (sorted), so shards[0] was the shard added a minute
	// earlier, not the original.
	//
	// So the existing topology is asked instead of inferred from position.
	authGroup := readAuthoritativeShardGroup(ctx, t, fx)
	topo := map[string]any{
		"shard_indexes": map[string]any{
			"xxhash_tenant_id": map[string]any{"type": "xxhash", "columns": []string{"tenant_id"}},
		},
		"shard_groups": []any{
			// Authoritative: exactly one range, over the authoritative SHARD.
			//
			// The range must name authGroup itself, not fx.shards[0]. The
			// auto-created group's uid IS its shard's uid, and the previous
			// cut used the positional shards[0] here — which is alphabetical
			// (see readShardUIDs), so on a run where the NEW shard sorted
			// first the authoritative group was declared over the wrong
			// shard and the platform refused:
			//
			//	authoritative shard cannot change from shard "sh7apl6sab8po2"
			//	to shard "sh32bvcq6b93tc"
			//
			// The group name had already been fixed to be read rather than
			// guessed; this line was left behind, so the fix was half
			// applied and failed identically.
			map[string]any{
				"uid":        authGroup,
				"key_ranges": []any{map[string]any{"shard_uid": authGroup}},
			},
			// The sharded group the fixture tables actually live in.
			map[string]any{
				"uid":                 "nv_group",
				"default_shard_index": "xxhash_tenant_id",
				"key_ranges": []any{
					// Positional indexing is fine HERE, unlike above: for a
					// sharded group it does not matter which shard takes
					// which half of the key space, only that both are
					// covered. No "original shard" semantics attach.
					map[string]any{"shard_uid": fx.shards[0], "end": "80"},
					map[string]any{"shard_uid": fx.shards[1], "start": "80"},
				},
			},
		},
		"authoritative_shard_group": authGroup,
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

	// set_data_topology returns `record(success boolean, revision bigint)`,
	// not a bare revision. Scanning it into one value yields the composite
	// literal "(t,35150)", which wait_for_data_topology then rejects with
	// `invalid input syntax for type bigint` (22P02) — a confusing error,
	// because the topology write itself had already SUCCEEDED. Select the
	// fields instead.
	var (
		ok  bool
		rev int64
	)
	if err := db.QueryRowContext(ctx,
		`SELECT success, revision FROM __neki.set_data_topology($1, true, '{"comment":"nekiverify fixture"}')`,
		string(doc)).Scan(&ok, &rev); err != nil {
		t.Fatalf("nekiverify: set_data_topology: %v", err)
	}
	if !ok {
		t.Fatalf("nekiverify: set_data_topology reported success=false at revision %d", rev)
	}
	t.Logf("nekiverify: topology stored at revision %d, %s UTC", rev, time.Now().UTC().Format(time.RFC3339))

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

// waitShardsVisibleToRouter polls the ROUTER's own shard census over SQL
// until it reports at least want shards, and returns their uids.
//
// # Why not just use the control plane's list
//
// Because the two views disagree, and the topology validator trusts only
// one of them. Measured 2026-09-11: `GET …/shards` reported both shards
// `ready: true`, and `__neki.set_data_topology` immediately rejected the
// second one —
//
//	data topology shard group "nv_group" key_ranges[1].shard_uid
//	"shwd5krps1pnhp" is not a shard the cluster has created (SQLSTATE 42704)
//
// — for a shard the API had just declared ready. The control plane knows
// about a shard before the router does, and `set_data_topology` is
// validated against the router's view.
//
// So the census is read from `__neki.list_shards()` on the same connection
// that will declare the topology. That makes the check and the write share
// a view, which is the only way the check means anything.
//
// The uid mapping is worth stating because the names invert: the API's
// `name` is the SQL `uid` (shx43lsrgp4q92), and the API's `display_name`
// is the SQL `name` (sh1). Reading uids from SQL sidesteps that trap too.
func waitShardsVisibleToRouter(ctx context.Context, t *testing.T, fx *nekiFixture, want int) []string {
	t.Helper()
	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		uids, err := readShardUIDs(ctx, db)
		if err == nil && len(uids) >= want {
			t.Logf("nekiverify: the ROUTER now sees %d shards WITH WRITABLE PRIMARIES: %v", len(uids), uids)
			return uids
		}
		if err != nil {
			t.Logf("nekiverify: shard census not readable yet (%v)", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nekiverify: context cancelled waiting for the router to see the new shard")
		case <-time.After(15 * time.Second):
		}
	}
	t.Fatalf("nekiverify: the router did not report %d shards with writable primaries within 10 minutes — the "+
		"control plane may still report them ready, which is exactly the disagreement this function waits out", want)
	return nil
}

// readShardUIDs returns the uids of shards that are not merely PRESENT but
// have a writable primary.
//
// A THIRD readiness view, after the control plane's `ready` flag and the
// router's shard census. Measured 2026-09-11: with both shards listed by
// `__neki.list_shards()`, `CREATE TABLE` still failed —
//
//	ERROR: no healthy sidecars available for shard shm7yc44lg0ie8
//	with type SIDECAR_TYPE_PRIMARY (SQLSTATE NK205)
//
// — because the shard existed and its primary did not yet serve writes.
// `has_writable_primary` is the column that says so, and gating on it is
// the difference between a fixture that builds and one that races.
//
// Note the ORDER BY is by uid and therefore ALPHABETICAL, not by creation
// order. Nothing may infer "the original shard" from position here; see
// [readAuthoritativeShardGroup], which exists because that inference was
// made once and was wrong.
func readShardUIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT uid FROM __neki.list_shards() WHERE has_writable_primary ORDER BY uid`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var uids []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		uids = append(uids, uid)
	}
	return uids, rows.Err()
}

// readAuthoritativeShardGroup returns the shard group the cluster already
// treats as authoritative.
//
// A fresh Neki database ships with one shard, and that shard is already the
// authoritative one — the role cannot be reassigned by a later
// set_data_topology. Any topology this fixture declares therefore has to
// KEEP the existing authoritative group rather than nominate one, so it is
// read back rather than guessed.
func readAuthoritativeShardGroup(ctx context.Context, t *testing.T, fx *nekiFixture) string {
	t.Helper()
	db := openFixtureDB(t, fx)
	defer func() { _ = db.Close() }()

	var group string
	const q = `SELECT ((__neki.get_data_topology())::jsonb) ->> 'authoritative_shard_group'`
	if err := db.QueryRowContext(ctx, q).Scan(&group); err != nil {
		t.Fatalf("nekiverify: read authoritative_shard_group: %v", err)
	}
	if group == "" {
		t.Fatal("nekiverify: the fresh database reports no authoritative_shard_group, so there is nothing to " +
			"preserve and set_data_topology will refuse whichever one we nominate")
	}
	t.Logf("nekiverify: existing authoritative shard group is %q — preserving it", group)
	return group
}
