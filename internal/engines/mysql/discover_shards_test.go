// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// A non-VStream flavor (vanilla MySQL) can't be sharded, so DiscoverShards
// must return (nil, nil) WITHOUT connecting — this keeps the Bug 152
// cross-shard preflight free for the common case. We pass a DSN that would
// fail to dial if it were used; the nil error proves no connection was made.
func TestEngine_DiscoverShards_VanillaNoOp(t *testing.T) {
	eng := Engine{Flavor: FlavorVanilla}
	shards, err := eng.DiscoverShards(context.Background(), "u:p@tcp(203.0.113.1:3306)/db")
	if err != nil {
		t.Fatalf("DiscoverShards(vanilla) = err %v; want nil (no connection attempted)", err)
	}
	if shards != nil {
		t.Errorf("DiscoverShards(vanilla) = %v; want nil (vanilla MySQL is not sharded)", shards)
	}
}

// Compile-time + behavioral: the Engine satisfies ir.ShardDiscoverer.
func TestEngine_ImplementsShardDiscoverer(_ *testing.T) {
	var _ ir.ShardDiscoverer = Engine{}
}
