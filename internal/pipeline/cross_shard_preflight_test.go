// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// xsShardSource is a fake ir.Engine (via embedded stubEngine) that also
// implements ir.ShardDiscoverer, returning a canned shard list / error.
type xsShardSource struct {
	stubEngine
	shards []string
	err    error
}

func (s xsShardSource) DiscoverShards(context.Context, string) ([]string, error) {
	return s.shards, s.err
}

func xsPKTable(name string) *ir.Table {
	return &ir.Table{
		Name:       name,
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func xsUniqueTable(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 100}}},
		Indexes: []*ir.Index{{Name: "u_email", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}}}},
	}
}

func xsKeylessTable(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "msg", Type: ir.Text{Size: ir.TextLong}}},
	}
}

func xsSchema(tables ...*ir.Table) *ir.Schema { return &ir.Schema{Tables: tables} }

func TestPreflightCrossShardCollision(t *testing.T) {
	ctx := context.Background()
	multi := xsShardSource{shards: []string{"-80", "80-"}}

	t.Run("multi-shard + PK + no discriminator + no opt-out → refuse", func(t *testing.T) {
		err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(xsPKTable("orders")), false, false)
		if !errors.Is(err, errCrossShardCollisionRefused) {
			t.Fatalf("got %v; want errCrossShardCollisionRefused", err)
		}
	})

	t.Run("multi-shard + UNIQUE index (no PK) → refuse", func(t *testing.T) {
		err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(xsUniqueTable("users")), false, false)
		if !errors.Is(err, errCrossShardCollisionRefused) {
			t.Fatalf("got %v; want refusal for a UNIQUE-only table", err)
		}
	})

	t.Run("--inject-shard-column engaged → pass (discriminator solves it)", func(t *testing.T) {
		if err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(xsPKTable("orders")), true, false); err != nil {
			t.Fatalf("got %v; want nil (shard column engaged)", err)
		}
	})

	t.Run("--allow-cross-shard-merge → pass (explicit opt-out)", func(t *testing.T) {
		if err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(xsPKTable("orders")), false, true); err != nil {
			t.Fatalf("got %v; want nil (opt-out)", err)
		}
	})

	t.Run("multi-shard + keyless table only → pass (at-least-once, no overwrite)", func(t *testing.T) {
		if err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(xsKeylessTable("events")), false, false); err != nil {
			t.Fatalf("got %v; want nil (keyless table can't collide)", err)
		}
	})

	t.Run("multi-shard, keyless first + PK second → refuse naming the PK table (skip-keyless path)", func(t *testing.T) {
		err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(xsKeylessTable("events"), xsPKTable("orders")), false, false)
		if !errors.Is(err, errCrossShardCollisionRefused) {
			t.Fatalf("got %v; want refusal", err)
		}
		if !strings.Contains(err.Error(), "orders") {
			t.Errorf("refusal should name the collision-capable table %q; got %v", "orders", err)
		}
	})

	t.Run("single shard → pass (no merge)", func(t *testing.T) {
		single := xsShardSource{shards: []string{"-"}}
		if err := preflightCrossShardCollision(ctx, single, "dsn", xsSchema(xsPKTable("orders")), false, false); err != nil {
			t.Fatalf("got %v; want nil (single shard, no merge)", err)
		}
	})

	t.Run("zero shards → pass", func(t *testing.T) {
		none := xsShardSource{shards: nil}
		if err := preflightCrossShardCollision(ctx, none, "dsn", xsSchema(xsPKTable("orders")), false, false); err != nil {
			t.Fatalf("got %v; want nil (no shards reported)", err)
		}
	})

	t.Run("source is not a ShardDiscoverer → pass (non-sharded engine)", func(t *testing.T) {
		if err := preflightCrossShardCollision(ctx, stubEngine{}, "dsn", xsSchema(xsPKTable("orders")), false, false); err != nil {
			t.Fatalf("got %v; want nil (engine doesn't implement ShardDiscoverer)", err)
		}
	})

	t.Run("discovery error → refuse (fail-closed silent-loss guard)", func(t *testing.T) {
		boom := xsShardSource{err: errors.New("vtgate unreachable")}
		err := preflightCrossShardCollision(ctx, boom, "dsn", xsSchema(xsPKTable("orders")), false, false)
		if !errors.Is(err, errCrossShardCollisionRefused) {
			t.Fatalf("got %v; want fail-closed refusal on discovery error", err)
		}
	})

	t.Run("nil/empty schema → pass", func(t *testing.T) {
		if err := preflightCrossShardCollision(ctx, multi, "dsn", nil, false, false); err != nil {
			t.Fatalf("got %v; want nil (nil schema)", err)
		}
		if err := preflightCrossShardCollision(ctx, multi, "dsn", xsSchema(), false, false); err != nil {
			t.Fatalf("got %v; want nil (empty schema)", err)
		}
	})
}
