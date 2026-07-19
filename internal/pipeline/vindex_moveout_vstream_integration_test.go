//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Audit 2026-07-19 M2-2 — the DURABLE PREMISE PIN for the "vindex-column
// --where" not-a-hazard conclusion. The audit flagged that a filtered
// `sync --where` on a VINDEX (sharding-key) column of a SHARDED
// Vitess/PlanetScale keyspace was neither tested nor guarded — a move-OUT (a
// row leaving the filter's scope) on such a column would be a CROSS-SHARD move,
// and if VStream didn't deliver the move-OUT before-image the way route()
// expects, the now-out-of-scope row could silently leak on the target.
//
// GROUND TRUTH (this test, on a real 2-shard Vitess keyspace): a `--where` on
// the vindex column can produce a move-OUT ONLY via an in-place UPDATE of the
// vindex column — and Vitess REFUSES that ("VT12001: unsupported: you cannot
// UPDATE primary vindex columns"). A row cannot change its shard key in place,
// so the move-OUT (and thus the silent-leak hazard) CANNOT occur. M2-2 is
// therefore a documented caveat, not a code guard (docs/operator/filtered-
// subset-migration.md). A DELETE (in scope → target DELETE) and a re-INSERT
// with a new vindex value (out of scope → dropped) are both classified
// correctly by route(); a move-OUT driven by a NON-vindex column in the same
// predicate is an in-place same-shard UPDATE that VStream delivers with both
// images normally. So the vindex axis specifically is safe by construction.
//
// This test is the durable PIN of that premise: if a future Vitess version ever
// ALLOWS an in-place vindex-column UPDATE, it fails loudly (the else branch),
// signaling that M2-2 must be revisited (the cross-shard move-OUT delivery would
// then need real observation + a guard).

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestVindexColumnMoveOut_GroundTruth_M2_2(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startShardedVTTestServer(t, "commerce", 2)
	defer cleanup()
	_ = grpcEndpoint // used only if the vindex UPDATE turns out to be allowed

	// A sharded table whose PRIMARY VINDEX is on `tenant` — so `tenant` is the
	// sharding key and a `--where tenant IN (...)` filters on the vindex column
	// (the M2-2 case). hash(tenant) scatters rows across both shards.
	applySQL(t, mysqlDSN, `CREATE TABLE accounts (
		id      BIGINT       NOT NULL,
		tenant  BIGINT       NOT NULL,
		payload VARCHAR(64)  NOT NULL,
		PRIMARY KEY (id)
	)`)
	applySQL(t, mysqlDSN, `ALTER VSCHEMA ON commerce.accounts ADD VINDEX hash(tenant) USING hash`)
	// Seed across shards: tenants 1,2 in a would-be filter scope; 7 out.
	applySQL(t, mysqlDSN+"&multiStatements=true", `INSERT INTO accounts (id, tenant, payload) VALUES
		(1, 1, 'seed-1'), (2, 2, 'seed-2'), (3, 7, 'seed-3')`)
	time.Sleep(2 * time.Second)

	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open vtgate: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// GROUND TRUTH: can a row move OUT of a vindex-column filter's scope via an
	// in-place UPDATE of the vindex column? That is the ONLY way a `--where` on
	// the vindex column produces a move-OUT.
	_, updErr := db.ExecContext(ctx, "UPDATE accounts SET tenant = 99 WHERE id = 1")
	if updErr != nil {
		t.Logf("GROUND TRUTH (M2-2): Vitess REJECTS an in-place UPDATE of the vindex column on a sharded keyspace: %v", updErr)
		t.Logf("=> a `--where` on the vindex column CANNOT produce a move-OUT (the row can't change its shard key in place), so the silent-leak hazard cannot occur. M2-2 = a documented caveat, not a code guard.")
		return
	}

	// If we reach here, Vitess ALLOWED the vindex-column UPDATE — the hazard is
	// possible, and we must observe what VStream delivers for the move-OUT.
	t.Logf("GROUND TRUTH (M2-2): Vitess ALLOWED the vindex-column UPDATE (tenant 1->99) on a sharded keyspace. The move-OUT hazard IS possible; a code guard (refuse or verified-safe) is warranted. Next: observe the VStream move-OUT delivery on grpc=%s.", grpcEndpoint)
	t.Fatalf("vindex-column UPDATE was allowed — extend this test to observe the VStream move-OUT delivery and add the guard accordingly (M2-2 Phase B)")
}
