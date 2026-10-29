// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// StreamShards implements [ir.ShardRoutingReader]: the shards a VStream
// change stream opened on dsn reads, resolved exactly as the reader resolves
// them at stream open ([resolveVStreamShards]) — the DSN's explicit
// `vstream_shards` pin if it carries one (no connection), else the
// discovered layout. A non-VStream flavor is not sharded and answers
// sharded=false without connecting.
//
// This is deliberately NOT [Engine.DiscoverShards], which reports the whole
// keyspace and ignores the pin: a stream pinned to one shard merges nothing,
// and judging it against the keyspace's other shards would refuse a stream
// that cannot reorder anything.
func (e Engine) StreamShards(ctx context.Context, dsn string) (shards []string, sharded bool, err error) {
	if !e.Flavor.usesVStream() {
		return nil, false, nil
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, true, fmt.Errorf("mysql: stream shards: %w", err)
	}
	shards, err = resolveVStreamShards(ctx, cfg)
	if err != nil {
		return nil, true, err
	}
	return shards, true, nil
}

// functionalVindexTypes are the Vitess vindex types that compute a row's
// keyspace id from the vindex columns alone, so the column values fix the
// shard. Every other type — the lookup family (lookup_unique,
// consistent_lookup_unique, lookup_hash_unique, …), which places a row by
// a table it consults, and any type this list does not know — is judged
// non-functional (refused unless asserted), the fail-safe direction for a
// list that can fall behind Vitess.
var functionalVindexTypes = map[string]bool{
	"binary":               true,
	"binary_md5":           true,
	"hash":                 true,
	"xxhash":               true,
	"numeric":              true,
	"numeric_static_map":   true,
	"reverse_bits":         true,
	"unicode_loose_md5":    true,
	"unicode_loose_xxhash": true,
	"multicol":             true,
	"null":                 true,
	"region_experimental":  true,
	"region_json":          true,
	"cfc":                  true,
}

// ShardRouting implements [ir.ShardRoutingReader] for the VStream flavors
// (PlanetScale / Vitess): each named table's routing in the SOURCE keyspace's
// VSchema, read through vtgate with `SHOW VSCHEMA VINDEXES ON` — the same
// statement [readPrimaryVindexColumns] reads for the target-side replay-key
// probe, here with the vindex Type as well. The pipeline's cross-shard
// row-move door (CROSS-SHARD-VINDEX-NOT-IN-PK) compares the primary vindex
// with the table's primary key.
//
// Each answer below was measured on vttestserver (mysql80), on a sharded
// keyspace:
//
//   - a table with column vindexes: one row per vindex, the PRIMARY first
//     (the fact [readPrimaryVindexColumns] records), so the first row's
//     columns and Type are what the table is sharded by. A secondary
//     (lookup) vindex does not move a row's shard and is not reported. A
//     primary vindex whose Type is not in [functionalVindexTypes] (measured:
//     a lookup_unique primary reports Type lookup_unique) →
//     [ir.ShardRoutingNonFunctional].
//   - a reference table (`"type": "reference"`, every shard holds every
//     row): zero rows, no error → [ir.ShardRoutingEveryShard]. Measured by
//     hand on a vttestserver started with a schema dir (a reference table
//     cannot be declared through `ALTER VSCHEMA` on a sharded keyspace, and
//     the test harness has no schema dir), so it is not pinned in CI. The
//     premise is fail-safe: if vtgate answered otherwise the table would
//     land in Unknown or the error branch, both refused; only whether the
//     assertion flag covers it depends on it.
//   - a table the VSchema does not know (Error 1146 VT05005, which vtgate
//     returns even for a table its schema tracker lists in SHOW VSCHEMA
//     TABLES) → [ir.ShardRoutingUnknown].
//
// Any other error fails the whole read: the door refuses on it rather than
// guess. UNVERIFIED PREMISE: that a PlanetScale database role may run
// `SHOW VSCHEMA VINDEXES` was never measured (only vttestserver and a
// self-hosted vtgate were); if PlanetScale refuses it, every merged
// PlanetScale stream refuses here (fail-closed, loud) and the refusal names
// the per-shard remedy and the assertion flag. Filed as
// PLANETSCALE-SHOW-VSCHEMA-ACCESS-UNMEASURED.
func (e Engine) ShardRouting(ctx context.Context, dsn string, tables []string) (map[string]ir.ShardRouting, error) {
	if !e.Flavor.usesVStream() {
		return nil, fmt.Errorf("mysql: shard routing: the %s flavor has no VSchema to read", e.Name())
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, fmt.Errorf("mysql: shard routing: %w", err)
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql: shard routing: the source DSN names no keyspace")
	}
	db, err := openDB(ctx, cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql: shard routing: %w", err)
	}
	defer func() { _ = db.Close() }()

	out := make(map[string]ir.ShardRouting, len(tables))
	for _, t := range tables {
		vindexes, err := readColumnVindexesTyped(ctx, db, cfg.DBName, t)
		switch {
		case errors.Is(err, errNoVSchemaEntry):
			out[t] = ir.ShardRouting{Kind: ir.ShardRoutingUnknown}
		case err != nil:
			return nil, fmt.Errorf("mysql: shard routing: read the VSchema vindexes of %s.%s: %w", cfg.DBName, t, err)
		case len(vindexes) == 0:
			out[t] = ir.ShardRouting{Kind: ir.ShardRoutingEveryShard}
		default:
			out[t] = classifyPrimaryVindex(vindexes[0])
		}
	}
	return out, nil
}

// classifyPrimaryVindex is the type decision, split out so every type is
// graded without a cluster.
func classifyPrimaryVindex(v columnVindex) ir.ShardRouting {
	kind := ir.ShardRoutingNonFunctional
	if functionalVindexTypes[strings.ToLower(v.typ)] {
		kind = ir.ShardRoutingVindex
	}
	return ir.ShardRouting{Kind: kind, PrimaryVindex: v.columns, VindexType: v.typ}
}
