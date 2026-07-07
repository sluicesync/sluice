//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SHOW VITESS_TABLETS shard cross-check against a real vtgate. vttestserver is
// a HEALTHY cluster, so it can't reproduce the v0.99.195 omission (that needs
// SHOW VITESS_SHARDS to transiently drop a keyspace) — but it proves the two
// invariants the cross-check rests on: the tablet path actually runs against a
// live vtgate, and on a healthy cluster the reconciled union EQUALS the
// VITESS_SHARDS result with NO spurious discrepancy. The recovery behavior
// itself is unit-pinned in reconcileShardSources (the omission can't be forced
// on vttestserver) and live-validated by the operator on real PlanetScale.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_TabletCrossCheck' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestVStream_TabletCrossCheck_MatchesShardsOnHealthyCluster(t *testing.T) {
	mysqlDSN, _, keyspace, cleanup := startVTTestServerWithShards(t, 2)
	defer cleanup()

	cfg, err := parseDSN(mysqlDSN)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// The vtgate reports SHOW VITESS_SHARDS immediately once the combo is up; the
	// tablets may take an extra beat to all reach SERVING. Poll the tablet path
	// until both shards show up (or the deadline) so the assertion isn't racing
	// the combo's tablet-warmup.
	var fromTablets map[string][]string
	for deadline := time.Now().Add(45 * time.Second); ; {
		fromTablets, err = discoverAllShardsViaTablets(ctx, cfg)
		if err == nil && len(fromTablets[keyspace]) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("SHOW VITESS_TABLETS never surfaced 2 serving shards for %q (last: %v, err %v)",
				keyspace, fromTablets, err)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("SHOW VITESS_TABLETS serving shards for %q: %v", keyspace, fromTablets[keyspace])

	fromShards, err := discoverAllShardsViaShards(ctx, cfg)
	if err != nil {
		t.Fatalf("discoverAllShardsViaShards: %v", err)
	}
	if len(fromShards[keyspace]) != 2 {
		t.Fatalf("SHOW VITESS_SHARDS shards for %q = %v; want 2 (-80, 80-)", keyspace, fromShards[keyspace])
	}

	// Reconciled discovery must equal the VITESS_SHARDS result for our keyspace on
	// a healthy cluster — the cross-check is additive, never a mutation of the
	// correct case.
	union, err := discoverAllShards(ctx, cfg)
	if err != nil {
		t.Fatalf("discoverAllShards (reconciled): %v", err)
	}
	if !reflect.DeepEqual(union[keyspace], fromShards[keyspace]) {
		t.Errorf("reconciled union for %q = %v; want it to equal the VITESS_SHARDS result %v on a healthy cluster",
			keyspace, union[keyspace], fromShards[keyspace])
	}

	// And no spurious discrepancy WARN for our keyspace when both sources agree.
	_, ds := reconcileShardSources(fromShards, fromTablets)
	if d := findDiscrepancy(ds, keyspace); d != nil {
		t.Errorf("spurious discrepancy for %q on a healthy cluster: %+v", keyspace, *d)
	}
}
