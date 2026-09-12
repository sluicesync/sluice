//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Tier-2 coverage item #2: CDC into a sharded target, graded on CONTENT.
//
// # Why a checksum and not a row count
//
// The item says "with the ordered-content checksum rather than a row count",
// and the distinction is the whole test. A router that duplicated one row onto
// two shards and dropped another shows the SAME count as a correct one. A
// router that applied an UPDATE to the pre-image rather than the post-image
// shows the same count too. Counting proves the population size; only the
// content proves the population.
//
// # What this drives, and why it is the applier rather than a full sync
//
// The change stream goes through the real [ChangeApplier] — the same
// ApplyBatch a continuous sync uses — against the live sharded fixture. That
// is deliberate and it is the honest scope: everything ABOVE the applier
// (reader, dispatch, position persistence) is engine-neutral pipeline code
// already covered against ordinary Postgres, and nothing about it changes when
// the target shards. What changes when the target shards is what the applier
// emits and what the router does with it — per-shard ON CONFLICT, per-shard
// routing of an UPDATE's new key, a DELETE that must reach the shard the row
// actually lives on. So this test is named for the applier lane, not for
// "end-to-end sync", because a name that claimed the latter would be read as
// covering the reader too.
//
// # The independent expected value
//
// The expected content is computed in Go FROM THE CHANGE STREAM — the same
// events the applier consumed, folded by the test's own model of what an
// insert/update/delete means. It is not a second read of the target, and not a
// count the target reports about itself. If the applier and the router agree
// with each other but disagree with the stream, this fails.
//
// # Anti-vacuity
//
// Two arms, because this test can pass for the wrong reason in two ways. If
// every row happened to route to ONE shard, the run proves nothing about
// sharding — so the tenants are discovered to be on distinct shards first
// (the same [tenantsOnDistinctShards] the refusal suite uses) and the final
// per-shard census must show both shards non-empty. And if the stream were
// empty or the applier silently skipped everything, an empty target would
// checksum equal to an empty model — so the model must carry rows, asserted
// before the comparison.
func nekiCDCIntoShardedTarget(ctx context.Context, t *testing.T, db *sql.DB, fx *nekiFixture, tenantA, tenantB int) {
	t.Helper()

	t.Run("CDC applies into a sharded target with byte-exact content", func(t *testing.T) {
		// A table of our own so the refusal suite's rows cannot bleed in.
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS cdc_content`); err != nil {
			t.Fatalf("drop: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			CREATE TABLE cdc_content (
				tenant_id BIGINT NOT NULL,
				id        BIGINT NOT NULL,
				payload   TEXT   NOT NULL,
				PRIMARY KEY (tenant_id, id)
			)`); err != nil {
			t.Fatalf("create cdc_content: %v", err)
		}
		// The shard key is INSIDE the primary key — the supported shape. The
		// unsupported one is what TestNekiverify_ShardedRefusalPremises
		// grades; this test is about content on a table that is allowed to
		// take writes.

		opened, err := Engine{}.OpenChangeApplier(ctx, fx.dsn)
		if err != nil {
			t.Fatalf("open applier: %v", err)
		}
		defer func() {
			if c, ok := opened.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()

		// The BATCHED interface on purpose: ApplyBatch is what a real sync
		// uses, and the batch path is where a sharded target's per-shard
		// ON CONFLICT actually gets exercised. A type assertion rather than a
		// cast, so a future engine that stops offering it fails here loudly
		// instead of silently degrading this test to the serial path.
		applier, ok := opened.(ir.BatchedChangeApplier)
		if !ok {
			t.Fatal("the postgres applier no longer implements ir.BatchedChangeApplier; this test drives the " +
				"batch path because that is what a continuous sync uses")
		}
		if err := applier.EnsureControlTable(ctx); err != nil {
			t.Fatalf("ensure control table on the sharded target: %v", err)
		}

		// The stream. Deliberately NOT a pile of inserts: the interesting
		// cases are an UPDATE that rewrites a non-key column (which must land
		// on the row's own shard) and a DELETE (which must find it there).
		// Both tenants appear so every operation class crosses a shard
		// boundary at least once.
		stream := []ir.Change{
			insertRow(tenantA, 1, "a-one"),
			insertRow(tenantB, 2, "b-two"),
			insertRow(tenantA, 3, "a-three"),
			insertRow(tenantB, 4, "b-four"),
			updateRow(tenantA, 1, "a-one", "a-one-updated"),
			updateRow(tenantB, 4, "b-four", "b-four-updated"),
			deleteRow(tenantA, 3, "a-three"),
		}

		// The model: what the stream MEANS, folded independently of the
		// target. This is the expected value the checksum is compared to.
		model := map[[2]int64]string{}
		for _, c := range stream {
			switch e := c.(type) {
			case ir.Insert:
				model[rowKey(e.Row)] = e.Row["payload"].(string)
			case ir.Update:
				delete(model, rowKey(e.Before))
				model[rowKey(e.After)] = e.After["payload"].(string)
			case ir.Delete:
				delete(model, rowKey(e.Before))
			}
		}
		if len(model) == 0 {
			t.Fatal("the model folded to zero rows — an empty target would checksum equal to it and this " +
				"test would pass having proven nothing")
		}

		ch := make(chan ir.Change, len(stream))
		for _, c := range stream {
			ch <- c
		}
		close(ch)
		if err := applier.ApplyBatch(ctx, "nekiverify-cdc-content", ch, 10); err != nil {
			t.Fatalf("ApplyBatch into the sharded target: %v", err)
		}

		wantSum, wantRows := checksumModel(model)
		gotSum, gotRows := checksumTarget(ctx, t, db)

		if gotRows != wantRows {
			t.Errorf("row COUNT differs: target %d, stream model %d", gotRows, wantRows)
		}
		if gotSum != wantSum {
			t.Errorf("ordered-content checksum MISMATCH.\n  target: %s (%d rows)\n  model:  %s (%d rows)\n"+
				"The counts alone would not have caught a duplicate-plus-drop or an update applied to the "+
				"pre-image; this compares every column of every row against what the change stream means.",
				gotSum, gotRows, wantSum, wantRows)
			dumpTargetRows(ctx, t, db)
		}

		// Anti-vacuity: the rows must genuinely be spread. A run where
		// everything landed on one shard would have exercised no routing.
		perShard := map[string]int{}
		for _, uid := range fx.shards {
			if _, err := db.ExecContext(ctx, `SET __neki.shard = '`+uid+`'`); err != nil {
				t.Fatalf("pin to shard %s: %v", uid, err)
			}
			var n int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM cdc_content`).Scan(&n); err != nil {
				t.Fatalf("per-shard count on %s: %v", uid, err)
			}
			perShard[uid] = n
		}
		if _, err := db.ExecContext(ctx, `RESET __neki.shard`); err != nil {
			t.Fatalf("reset shard pin: %v", err)
		}
		populated := 0
		for _, n := range perShard {
			if n > 0 {
				populated++
			}
		}
		if populated < 2 {
			t.Errorf("every surviving row landed on ONE shard (%v) — the content check above passed, but it "+
				"proved nothing about a SHARDED target", perShard)
		}
		t.Logf("content verified across %d shards: %v (checksum %s)", populated, perShard, gotSum)
	})
}

func insertRow(tenant, id int, payload string) ir.Insert {
	return ir.Insert{
		Schema: "public", Table: "cdc_content",
		Row: ir.Row{"tenant_id": int64(tenant), "id": int64(id), "payload": payload},
	}
}

func updateRow(tenant, id int, before, after string) ir.Update {
	return ir.Update{
		Schema: "public", Table: "cdc_content",
		Before: ir.Row{"tenant_id": int64(tenant), "id": int64(id), "payload": before},
		After:  ir.Row{"tenant_id": int64(tenant), "id": int64(id), "payload": after},
	}
}

func deleteRow(tenant, id int, payload string) ir.Delete {
	return ir.Delete{
		Schema: "public", Table: "cdc_content",
		Before: ir.Row{"tenant_id": int64(tenant), "id": int64(id), "payload": payload},
	}
}

func rowKey(r ir.Row) [2]int64 {
	return [2]int64{r["tenant_id"].(int64), r["id"].(int64)}
}

// checksumModel hashes the change stream's meaning. Ordered by key so the
// digest is independent of map iteration and of the order the router happens
// to return rows in — a content comparison must not be an ordering test.
func checksumModel(model map[[2]int64]string) (digest string, rowCount int) {
	keys := make([][2]int64, 0, len(model))
	for k := range model {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%d|%d|%s\n", k[0], k[1], model[k])
	}
	return hex.EncodeToString(h.Sum(nil)), len(keys)
}

// checksumTarget hashes what the sharded target actually holds, read back
// through the router — the scatter-gather every reader would see.
func checksumTarget(ctx context.Context, t *testing.T, db *sql.DB) (digest string, rowCount int) {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, id, payload FROM cdc_content ORDER BY tenant_id, id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = rows.Close() }()

	h := sha256.New()
	n := 0
	for rows.Next() {
		var tenant, id int64
		var payload string
		if err := rows.Scan(&tenant, &id, &payload); err != nil {
			t.Fatalf("scan: %v", err)
		}
		fmt.Fprintf(h, "%d|%d|%s\n", tenant, id, payload)
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

// dumpTargetRows prints what the target holds when the checksum fails. A bare
// "digests differ" is nearly useless for diagnosing a routing bug, and this
// suite runs weekly and unattended — the failure has to carry its own
// evidence or the next person re-provisions a cluster to get it.
func dumpTargetRows(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, id, payload FROM cdc_content ORDER BY tenant_id, id`)
	if err != nil {
		t.Logf("(could not dump rows: %v)", err)
		return
	}
	defer func() { _ = rows.Close() }()
	var b strings.Builder
	for rows.Next() {
		var tenant, id int64
		var payload string
		if err := rows.Scan(&tenant, &id, &payload); err != nil {
			break
		}
		fmt.Fprintf(&b, "\n  (%d, %d) = %q", tenant, id, payload)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(&b, "\n  (iteration ended early: %v)", err)
	}
	t.Logf("target content at failure:%s", b.String())
}
