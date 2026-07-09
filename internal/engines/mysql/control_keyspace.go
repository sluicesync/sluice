// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// # --control-keyspace auto-detection (the sidecar-keyspace feature)
//
// A SHARDED PlanetScale/Vitess keyspace requires a primary vindex on every
// table, so sluice's vindex-less CDC control tables can't live there — they go
// in a separate UNSHARDED "sidecar" keyspace. The operator can name that
// keyspace with --control-keyspace, but for the common case (a PlanetScale
// database whose default keyspace is unsharded and named after the DB) sluice
// auto-detects it. The decision matrix is factored into the pure
// [selectControlKeyspace] so it is unit-testable without a live vtgate; the live
// enumeration lives in [Engine.ResolveControlKeyspace] /
// [discoverUnshardedKeyspaces].

// selectControlKeyspace is the pure --control-keyspace decision function.
// Inputs are already-gathered facts (no I/O) so the whole matrix is unit-pinned
// without a vtgate:
//
//   - explicitFlag != "" → use it verbatim (operator override), for ANY target.
//   - targetShardCount <= 1 → "" : an unsharded (or non-Vitess) target keeps its
//     control tables in the data keyspace — unchanged behaviour.
//   - sharded target, flag unset → auto-select the SOLE unsharded sidecar
//     keyspace (candidateKeyspaces minus the data keyspace):
//   - exactly one candidate → use it.
//   - zero candidates → loud refusal (operator must create/name one).
//   - more than one candidate → loud refusal (ambiguous; operator must choose).
//
// candidateKeyspaces is every UNSHARDED keyspace the vtgate serves; the data
// keyspace is filtered out here (it is sharded in this branch, but the filter is
// defensive and keeps the function total).
func selectControlKeyspace(targetShardCount int, candidateKeyspaces []string, dataKeyspace, explicitFlag string) (string, error) {
	if explicitFlag != "" {
		return explicitFlag, nil
	}
	if targetShardCount <= 1 {
		return "", nil
	}
	candidates := make([]string, 0, len(candidateKeyspaces))
	for _, k := range candidateKeyspaces {
		if k != dataKeyspace {
			candidates = append(candidates, k)
		}
	}
	sort.Strings(candidates)
	switch len(candidates) {
	case 1:
		return candidates[0], nil
	case 0:
		return "", fmt.Errorf(
			"target keyspace %q is sharded but no unsharded sidecar keyspace was found; "+
				"create one or pass --control-keyspace", dataKeyspace,
		)
	default:
		return "", fmt.Errorf(
			"target keyspace %q is sharded and multiple unsharded keyspaces exist (%s); "+
				"pass --control-keyspace to choose", dataKeyspace, strings.Join(candidates, ", "),
		)
	}
}

// ResolveControlKeyspace returns the control keyspace to use for a sync against
// the target identified by dsn. It is the CLI's single entry point (called
// before [Engine.WithControlKeyspace] by every sync-family command):
//
//   - explicitFlag set → returned verbatim, no vtgate query (override).
//   - non-VStream target (vanilla MySQL) → "" without connecting: control tables
//     stay in the data database, byte-identical to today.
//   - VStream target, flag unset → auto-detect (see [selectControlKeyspace]).
//
// A shard-discovery failure (endpoint isn't really Vitess, transient error) is
// NOT fatal: it falls back to "" (unchanged behaviour) with a DEBUG log. A
// genuinely sharded target then still fails LOUDLY at CREATE TABLE (VT09001)
// rather than silently — the loud-failure tenet holds either way. The only
// loud refusals here are the sharded-but-ambiguous cases surfaced by
// selectControlKeyspace.
func (e Engine) ResolveControlKeyspace(ctx context.Context, dsn, explicitFlag string) (string, error) {
	if explicitFlag != "" {
		return explicitFlag, nil
	}
	if !e.Flavor.usesVStream() {
		return "", nil
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return "", fmt.Errorf("mysql: resolve control keyspace: %w", err)
	}
	targetShardCount, unsharded, err := discoverUnshardedKeyspaces(ctx, cfg)
	if err != nil {
		slog.DebugContext(ctx, "mysql: control-keyspace auto-detect skipped (shard discovery failed)",
			slog.String("data_keyspace", cfg.DBName),
			slog.String("reason", err.Error()))
		return "", nil
	}
	resolved, err := selectControlKeyspace(targetShardCount, unsharded, cfg.DBName, "")
	if err != nil {
		return "", err
	}
	if resolved != "" {
		slog.InfoContext(ctx, "auto-selected control keyspace (target keyspace is sharded)",
			slog.String("control_keyspace", resolved),
			slog.String("data_keyspace", cfg.DBName))
	}
	return resolved, nil
}

// discoverUnshardedKeyspaces returns the connected keyspace's shard count plus
// the names of every UNSHARDED keyspace the vtgate serves. It derives both from
// [discoverAllShardsForKeyspace], which reconciles SHOW VITESS_SHARDS with the
// SHOW VITESS_TABLETS serving-shard cross-check (so a keyspace SHOW VITESS_SHARDS
// silently drops still surfaces here) and adds the fresh-keyspace
// retry-on-empty. If a future PlanetScale scoping change hides sibling keyspaces
// from BOTH statements, swap the enumeration here for SHOW KEYSPACES + a
// per-keyspace classification — [selectControlKeyspace] and its callers are
// unaffected.
func discoverUnshardedKeyspaces(ctx context.Context, cfg *gomysql.Config) (targetShardCount int, unsharded []string, err error) {
	// Retry-on-empty (2a): a fresh keyspace can transiently report zero shards
	// for cfg.DBName; ride that the same bounded way the reader's enumeration
	// does before classifying the target as unsharded (which resolves to "").
	shardMap, err := discoverAllShardsForKeyspace(ctx, cfg, cfg.DBName)
	if err != nil {
		return 0, nil, err
	}
	for ks, shards := range shardMap {
		if len(shards) == 1 && shards[0] == "-" {
			unsharded = append(unsharded, ks)
		}
	}
	return len(shardMap[cfg.DBName]), unsharded, nil
}
