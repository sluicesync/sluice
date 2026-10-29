// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyPrimaryVindex grades the functional-type decision: the key
// fixes the shard only through a vindex computed from its columns. A lookup
// primary, and a type the list does not know, are non-functional.
func TestClassifyPrimaryVindex(t *testing.T) {
	cases := []struct {
		typ  string
		want ir.ShardRoutingKind
	}{
		{"hash", ir.ShardRoutingVindex},
		{"HASH", ir.ShardRoutingVindex},
		{"xxhash", ir.ShardRoutingVindex},
		{"unicode_loose_md5", ir.ShardRoutingVindex},
		{"binary_md5", ir.ShardRoutingVindex},
		{"numeric", ir.ShardRoutingVindex},
		{"multicol", ir.ShardRoutingVindex},
		{"lookup_unique", ir.ShardRoutingNonFunctional},
		{"consistent_lookup_unique", ir.ShardRoutingNonFunctional},
		{"lookup_hash_unique", ir.ShardRoutingNonFunctional},
		{"my_custom_vindex", ir.ShardRoutingNonFunctional},
		{"", ir.ShardRoutingNonFunctional},
	}
	for _, tc := range cases {
		got := classifyPrimaryVindex(columnVindex{columns: []string{"id"}, typ: tc.typ})
		if got.Kind != tc.want || got.VindexType != tc.typ || len(got.PrimaryVindex) != 1 {
			t.Errorf("type %q: routing = %+v; want kind %d carrying the type and columns", tc.typ, got, tc.want)
		}
	}
}

// TestStreamShards_ResolvesAsTheReaderDoes pins the door's shard input to the
// reader's: an explicit vstream_shards pin wins without a connection (the
// host here is unreachable, so discovery — which would report the whole
// keyspace, or nothing — is never asked), and a non-VStream flavor is not
// sharded.
func TestStreamShards_ResolvesAsTheReaderDoes(t *testing.T) {
	ctx := context.Background()
	const base = "root@tcp(127.0.0.1:1)/ks2?vstream_endpoint=127.0.0.1:2&vstream_transport=plaintext&vstream_auth=none"
	for _, tc := range []struct {
		pin  string
		want string
	}{
		{"-80", "-80"},
		{"-80,80-", "-80,80-"},
	} {
		shards, sharded, err := Engine{Flavor: FlavorVitess}.StreamShards(ctx, base+"&vstream_shards="+tc.pin)
		if err != nil || !sharded || strings.Join(shards, ",") != tc.want {
			t.Errorf("pin %q: shards=%v sharded=%v err=%v; want %s, sharded, no error", tc.pin, shards, sharded, err, tc.want)
		}
	}
	shards, sharded, err := Engine{Flavor: FlavorVanilla}.StreamShards(ctx, "root@tcp(127.0.0.1:1)/db")
	if err != nil || sharded || shards != nil {
		t.Errorf("vanilla: shards=%v sharded=%v err=%v; want not sharded, no connection", shards, sharded, err)
	}
}
