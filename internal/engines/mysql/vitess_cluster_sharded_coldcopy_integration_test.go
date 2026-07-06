//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Sharded-source VStream COLD-COPY correctness against a genuinely
// multi-shard keyspace (the discovery-by-default fix).
//
// THE BUG this pins: on a sharded source keyspace, the VStream
// snapshot cold-copy (sync start's snapshot→CDC handoff;
// OpenSnapshotStream → openVStreamSnapshotStreamFrom →
// fromBeginningVStreamPos) built its VGTID from the shard list
// resolveVStreamShards handed it. With no explicit `vstream_shards`
// and auto-discovery off, that list silently defaulted to ["-"] — the
// UNSHARDED convention — so the cold-copy asked vtgate to COPY a
// keyspace-wide {Shard:"-"} VGTID that a SHARDED keyspace has no
// such shard for. vtgate rejected it with FailedPrecondition
// ("shard provided in VGTID, -, not found in the <ks> keyspace") and
// the copy delivered ZERO rows — while the cross-shard-collision
// preflight (which always DiscoverShards) correctly saw [-80,80-], so
// guard and copy disagreed.
//
// THE FIX: resolveVStreamShards now auto-discovers by default —
// SHOW VITESS_SHARDS yields the real [-80,80-] for a sharded keyspace
// (and a single "-" for an unsharded one, byte-identical to before).
//
// This lives under the `vitessreshard` tag because that is the only
// harness that produces a GENUINELY multi-shard keyspace
// (startVitessReshardCluster + Reshard); the cheap `vstream`
// (vttestserver) tag cannot serve a sharded VStream COPY. It reshards a
// keyspace 1->2 to reach the [-80,80-] layout the cold-copy must
// discover — the reshard is scaffolding, not the thing under test (the
// cold-copy runs AFTER the cutover, on the settled 2-shard layout).
//
// Usage (Windows; see CLAUDE.md / docs/dev/development.md):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vitessreshard' -v -count=1 -timeout=25m \
//	  -run 'TestVitessReshard_ShardedColdCopy' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVitessReshard_ShardedColdCopy_AllShardsCopied reshards a keyspace
// to a genuine 2-shard layout with a hash-vindexed table whose rows
// scatter across both shards, then runs the VStream snapshot cold-copy
// with NO explicit `vstream_shards` (so resolveVStreamShards MUST
// discover the [-80,80-] layout). The oracle: every seeded row from
// EVERY shard is copied exactly once — the FailedPrecondition /
// zero-rows failure is gone.
func TestVitessReshard_ShardedColdCopy_AllShardsCopied(t *testing.T) {
	// Reach a genuinely 2-shard keyspace via the harness's PROVEN path:
	// boot UNSHARDED, seed, then reshard 1->2 + SwitchTraffic. (Booting a
	// multi-shard source directly reparents the second shard unreliably on
	// this harness; resharding from "-" is the tested flow — the same one
	// TestVitessReshard_ProofOfReshardability pins.) After SwitchTraffic the
	// keyspace presents cleanly as [-80,80-] — exactly the layout the
	// cold-copy must auto-discover; the drained source "-" shard drops out
	// of SHOW VITESS_SHARDS (asserted by the guard below), so no double copy.
	c := startVitessReshardCluster(t, "-")
	defer c.terminate()

	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE account (
		id    BIGINT       NOT NULL,
		owner VARCHAR(128) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	vrApplySQL(t, c.mysqlDSN, `ALTER VSCHEMA ON `+vrKeyspace+`.account ADD VINDEX hash(id) USING hash`)
	time.Sleep(3 * time.Second) // schema-tracker settle

	// Seed on the unsharded shard; the reshard then scatters these rows
	// across -80 / 80- by the hash vindex. 200 rows near-certainly land on
	// both shards; the per-shard guard below asserts it actually did.
	const totalRows = 200
	var sb strings.Builder
	sb.WriteString("INSERT INTO account (id, owner) VALUES ")
	for i := 1; i <= totalRows; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "(%d,'owner-%d')", i, i)
	}
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true", sb.String())
	time.Sleep(2 * time.Second)

	// Reshard 1 -> 2 and switch all traffic to the new layout.
	c.addTargetShards(t, "-80", "80-")
	if out, err := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "coldcopy", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-"); err != nil {
		t.Fatalf("Reshard create: %v\n%s", err, out)
	}
	c.waitReshardRunning(t, "coldcopy")
	if _, err := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "coldcopy", "--target-keyspace", vrKeyspace); err != nil {
		t.Fatalf("Reshard SwitchTraffic: %v", err)
	}
	// The target primaries become routable a beat after SwitchTraffic
	// returns; wait before reading (the harness documents this window).
	c.waitReshardPrimariesRoutable(t, "account")

	// Guard: the keyspace now presents as exactly [-80,80-] AND the rows
	// genuinely span both shards. Without this a single-shard distribution
	// could pass the oracle without exercising the cross-shard cold-copy.
	shards := vrShowShards(t, c.mysqlDSN)
	if len(shards) != 2 {
		t.Fatalf("keyspace has shards %v; want 2 (-80,80-) — not exercising a sharded source", shards)
	}
	for _, sh := range shards {
		if n := shardScopedCount(t, c, sh, "account"); n == 0 {
			t.Fatalf("shard %q holds 0 of the %d seeded rows — the hash-vindexed seed did not span both shards, so this run would not catch the cross-shard cold-copy bug (per-shard distribution off)",
				sh, totalRows)
		}
	}

	// THE COLD-COPY under test: open the VStream snapshot COPY with NO
	// explicit `vstream_shards`, so resolveVStreamShards must auto-discover
	// the [-80,80-] layout. Pre-fix it silently defaulted to ["-"], built a
	// keyspace-wide {Shard:"-"} VGTID, and vtgate rejected it with
	// FailedPrecondition ("shard provided in VGTID, -, not found"), copying
	// ZERO rows. Post-fix discovery hands the COPY both shards.
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none",
		c.mysqlDSN, c.grpcAddr,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream (sharded source, discovery-by-default): %v", err)
	}
	defer func() { _ = stream.Close() }()

	accountTable := &ir.Table{
		Name: "account",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "owner", Type: ir.Varchar{Length: 128}},
		},
	}
	// The FailedPrecondition cold-copy failure surfaces on this drain
	// pre-fix (either ReadRows errors or the channel closes empty with
	// stream.Rows.Err() set).
	rowsCh, err := stream.Rows.ReadRows(ctx, accountTable)
	if err != nil {
		t.Fatalf("ReadRows(account): %v", err)
	}

	seen := make(map[int64]bool, totalRows)
	for row := range rowsCh {
		id, ok := vrAsInt64(row["id"])
		if !ok {
			t.Fatalf("account row has non-integer id: %#v", row["id"])
		}
		seen[id] = true
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("snapshot COPY error after drain (the FailedPrecondition wrong-shard-list bug surfaces here): %v", err)
	}

	// ORACLE: every row from EVERY shard copied exactly once. A zero /
	// short count is the sharded-source cold-copy bug (rows from one or
	// both shards never streamed).
	if len(seen) != totalRows {
		t.Fatalf("cold-copy delivered %d distinct rows; want %d — rows from one or more shards were dropped (sharded-source cold-copy bug)",
			len(seen), totalRows)
	}
	for i := int64(1); i <= totalRows; i++ {
		if !seen[i] {
			t.Fatalf("cold-copy missing id=%d — a shard's rows were not copied", i)
		}
	}
	t.Logf("ORACLE PASSED: 2-shard keyspace %v cold-copied all %d rows via discovery-by-default (no vstream_shards set); FailedPrecondition gone",
		shards, totalRows)
}

// shardScopedCount returns the row count of table on a single shard via
// vtgate "keyspace:shard" targeting (the DSN-rewrite trick shardScopedIDs
// uses). Range sharding puts each row on exactly one shard, so summing
// these equals the scatter count; a zero on either shard means the seed
// did not span both shards.
func shardScopedCount(t *testing.T, c *vitessReshardCluster, sh, table string) int {
	t.Helper()
	dsn := strings.Replace(c.mysqlDSN, "/"+vrKeyspace+"?", "/"+vrKeyspace+":"+sh+"?", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open shard %q: %v", sh, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("shard %q count(%s): %v", sh, table, err)
	}
	return n
}
