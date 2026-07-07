// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// # SHOW VITESS_TABLETS shard cross-check (the silently-dropped-shard fix)
//
// Live-confirmed on real PlanetScale (v0.99.195): `SHOW VITESS_SHARDS` can
// PERSISTENTLY omit an entire sharded keyspace's shards that `SHOW
// VITESS_TABLETS` reports fully SERVING — a DB with a default unsharded
// keyspace + two 4-shard keyspaces (`sks`, `sks2`) had `SHOW VITESS_SHARDS`
// drop `sks2` entirely for 6+ minutes while tablets/keyspaces showed all three
// throughout. Per-keyspace the omission is all-or-nothing (0 or all N shards),
// but the DB-level result is INCOMPLETE-not-empty — the target keyspace can be
// present-and-complete while a SIBLING is silently dropped — so the existing
// retry-on-empty ([discoverAllShardsForKeyspace]) never fires. Copying against
// the omitted keyspace would silently miss every one of its shards: a
// silent-partial-copy risk.
//
// The fix cross-checks SHOW VITESS_SHARDS against SHOW VITESS_TABLETS and takes
// the UNION per keyspace, so no serving shard either source reports is ever
// dropped. The cross-check is additive safety, not a new hard dependency: if
// SHOW VITESS_TABLETS is unavailable (older vtgate, perms) discovery falls back
// to the VITESS_SHARDS-only result — today's behavior.

// shardDiscrepancy names a per-keyspace disagreement between the two
// shard-discovery sources, surfaced as a loud WARN by [discoverAllShards] (the
// tenet: a silently-dropped serving shard is a silent-partial-copy risk, so it
// gets named). Both slices are sorted+deduped for stable log output.
type shardDiscrepancy struct {
	keyspace string
	// recoveredFromTablets is the set of SERVING shards SHOW VITESS_TABLETS
	// reported that SHOW VITESS_SHARDS omitted for this keyspace — the observed
	// v0.99.195 under-reporting bug. Absent this cross-check they'd be silently
	// dropped.
	recoveredFromTablets []string
	// shardsWithoutServingTablet is the set of shards SHOW VITESS_SHARDS
	// reported for this keyspace that have NO serving tablet in SHOW
	// VITESS_TABLETS — kept in the union (we never drop a shard a source
	// reports), but flagged as possibly-unstreamable.
	shardsWithoutServingTablet []string
}

// reconcileShardSources merges the two shard-discovery sources into a per-
// keyspace UNION and reports the discrepancies. It is a PURE function (no I/O)
// so the union+flag policy is unit-pinned without a vtgate.
//
// Policy: for every keyspace present in EITHER source, the result shard set is
// (VITESS_SHARDS shards) ∪ (VITESS_TABLETS SERVING shards), deduped and sorted.
// A shard either source reports is NEVER dropped.
//
// Discrepancy flagging:
//   - recoveredFromTablets: SERVING shards tablets reported that shards omitted
//     (the observed bug).
//   - shardsWithoutServingTablet: shards SHOW VITESS_SHARDS reported that have
//     no serving tablet — flagged ONLY for keyspaces tablets actually
//     enumerated. A keyspace tablets never mentions at all (a system keyspace
//     the tablets query doesn't surface, or a transient) is not flagged
//     shard-by-shard, so the WARN can't false-fire on keyspaces the cross-check
//     simply has no opinion on.
func reconcileShardSources(fromShards, fromTablets map[string][]string) (map[string][]string, []shardDiscrepancy) {
	keyspaces := make(map[string]struct{}, len(fromShards)+len(fromTablets))
	for ks := range fromShards {
		keyspaces[ks] = struct{}{}
	}
	for ks := range fromTablets {
		keyspaces[ks] = struct{}{}
	}

	out := make(map[string][]string, len(keyspaces))
	var discrepancies []shardDiscrepancy
	for ks := range keyspaces {
		shardsSet := sliceToSet(fromShards[ks])
		tabletsSet := sliceToSet(fromTablets[ks])
		_, tabletsHasKeyspace := fromTablets[ks]

		union := make(map[string]struct{}, len(shardsSet)+len(tabletsSet))
		for s := range shardsSet {
			union[s] = struct{}{}
		}
		for s := range tabletsSet {
			union[s] = struct{}{}
		}
		out[ks] = sortedSetSlice(union)

		var recovered, withoutServing []string
		for s := range tabletsSet {
			if _, ok := shardsSet[s]; !ok {
				recovered = append(recovered, s)
			}
		}
		// Only flag missing-serving-tablet shards when tablets has an opinion on
		// this keyspace; otherwise we'd WARN about every shard of a keyspace the
		// tablets query never enumerated (e.g. a system keyspace).
		if tabletsHasKeyspace {
			for s := range shardsSet {
				if _, ok := tabletsSet[s]; !ok {
					withoutServing = append(withoutServing, s)
				}
			}
		}
		if len(recovered) > 0 || len(withoutServing) > 0 {
			sort.Strings(recovered)
			sort.Strings(withoutServing)
			discrepancies = append(discrepancies, shardDiscrepancy{
				keyspace:                   ks,
				recoveredFromTablets:       recovered,
				shardsWithoutServingTablet: withoutServing,
			})
		}
	}
	// Deterministic discrepancy order for stable logs + test assertions.
	sort.Slice(discrepancies, func(i, j int) bool { return discrepancies[i].keyspace < discrepancies[j].keyspace })
	return out, discrepancies
}

// sliceToSet turns a shard-name slice into a set (nil slice → empty set).
func sliceToSet(in []string) map[string]struct{} {
	set := make(map[string]struct{}, len(in))
	for _, s := range in {
		set[s] = struct{}{}
	}
	return set
}

// sortedSetSlice materialises a shard-name set as a sorted slice.
func sortedSetSlice(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// discoverAllShardsViaTablets runs SHOW VITESS_TABLETS and returns the distinct
// SERVING shards per keyspace — the second, cross-checking shard-discovery
// source [discoverAllShards] reconciles against [discoverAllShardsViaShards].
//
// The authoritative shard set is the distinct (Keyspace, Shard) where State ==
// "SERVING": each shard shows up as ~3 tablet rows (1 PRIMARY + 2 REPLICA), so
// the set is deduped. We accept ANY serving tablet type — NOT PRIMARY-only — so
// a momentary reparent gap (no serving PRIMARY for a beat) doesn't drop a shard.
//
// Connection hygiene matches [discoverAllShardsViaShards]: openDB strips
// sluice's vstream_* DSN flags and Clone()s cfg, so the caller's cfg is intact
// and vtgate's parser never sees an unknown SET (Bug 126).
func discoverAllShardsViaTablets(ctx context.Context, cfg *gomysql.Config) (map[string][]string, error) {
	db, err := openDB(ctx, cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("open mysql for tablet-based shard discovery: %w", err)
	}
	defer func() { _ = db.Close() }()

	const tabletsStmt = "SHOW VITESS_TABLETS"
	rows, err := db.QueryContext(ctx, tabletsStmt)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", tabletsStmt, err)
	}
	defer func() { _ = rows.Close() }()

	return parseVitessTablets(rows)
}

// parseVitessTablets scans a SHOW VITESS_TABLETS result set into raw rows and
// delegates the SERVING-shard reduction to [servingShardsFromTablets]. It reads
// columns BY NAME (via [rows.Columns]) rather than fixed positions, so an added
// or reordered vtgate column can't silently shift the parse onto the wrong
// field.
func parseVitessTablets(rows *sql.Rows) (map[string][]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("SHOW VITESS_TABLETS columns: %w", err)
	}
	holders := make([]sql.NullString, len(cols))
	scan := make([]any, len(cols))
	for i := range scan {
		scan[i] = &holders[i]
	}
	var raw [][]string
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return nil, fmt.Errorf("scan SHOW VITESS_TABLETS row: %w", err)
		}
		row := make([]string, len(cols))
		for i := range holders {
			row[i] = holders[i].String
		}
		raw = append(raw, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SHOW VITESS_TABLETS rows: %w", err)
	}
	return servingShardsFromTablets(cols, raw)
}

// vitessTabletColumns locates the Keyspace/Shard/State columns by NAME in a SHOW
// VITESS_TABLETS header, returning their indexes. Scanning by name (not fixed
// position) means an added or reordered vtgate column can't silently shift the
// parse. A missing expected column is a loud error rather than a silent
// mis-parse — the cross-check refuses to guess.
func vitessTabletColumns(cols []string) (ksIdx, shardIdx, stateIdx int, err error) {
	ksIdx, shardIdx, stateIdx = -1, -1, -1
	for i, c := range cols {
		switch c {
		case "Keyspace":
			ksIdx = i
		case "Shard":
			shardIdx = i
		case "State":
			stateIdx = i
		}
	}
	if ksIdx < 0 || shardIdx < 0 || stateIdx < 0 {
		return -1, -1, -1, fmt.Errorf(
			"SHOW VITESS_TABLETS missing expected column(s) Keyspace/Shard/State (got %v)", cols,
		)
	}
	return ksIdx, shardIdx, stateIdx, nil
}

// servingShardsFromTablets reduces raw SHOW VITESS_TABLETS rows (each aligned to
// cols) to the distinct SERVING shards per keyspace: filter to State=="SERVING"
// (case-insensitive), dedupe the ~3 tablet rows per shard, and sort. It is a
// PURE function so the parse is unit-pinned without a live vtgate.
func servingShardsFromTablets(cols []string, rows [][]string) (map[string][]string, error) {
	ksIdx, shardIdx, stateIdx, err := vitessTabletColumns(cols)
	if err != nil {
		return nil, err
	}
	seen := map[string]map[string]struct{}{}
	for _, row := range rows {
		// Defensive: a short row (fewer values than the header) can't be indexed
		// at the resolved positions; skip it rather than panic.
		if len(row) <= ksIdx || len(row) <= shardIdx || len(row) <= stateIdx {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(row[stateIdx]), "SERVING") {
			continue
		}
		ks := strings.TrimSpace(row[ksIdx])
		shard := strings.TrimSpace(row[shardIdx])
		if ks == "" || shard == "" {
			continue
		}
		if seen[ks] == nil {
			seen[ks] = map[string]struct{}{}
		}
		seen[ks][shard] = struct{}{}
	}
	out := make(map[string][]string, len(seen))
	for ks, shards := range seen {
		out[ks] = sortedSetSlice(shards)
	}
	return out, nil
}

// discoverAllShards runs BOTH shard-discovery sources and reconciles them into a
// per-keyspace UNION, so no serving shard either source reports is dropped — the
// fix for `SHOW VITESS_SHARDS` silently omitting a fully-SERVING keyspace's
// shards (v0.99.195). It returns a map of every keyspace the vtgate serves to
// its shard names; shared by [discoverShards] (which filters to one keyspace)
// and the --control-keyspace auto-detect ([Engine.ResolveControlKeyspace] via
// [discoverUnshardedKeyspaces]), which needs the full keyspace inventory.
//
// Resilience (the cross-check is additive, never a new hard dependency):
//   - both sources fail → surface the primary (SHOW VITESS_SHARDS) error, as
//     before the cross-check existed.
//   - only the tablets cross-check fails (older vtgate, perms, transient) →
//     fall back to the VITESS_SHARDS-only result with a WARN; byte-identical to
//     the pre-cross-check behavior.
//   - only SHOW VITESS_SHARDS fails → use the tablet-derived SERVING set rather
//     than failing discovery outright (tablets was authoritative-complete in the
//     observed bug), with a WARN.
func discoverAllShards(ctx context.Context, cfg *gomysql.Config) (map[string][]string, error) {
	fromShards, shardsErr := discoverAllShardsViaShards(ctx, cfg)
	fromTablets, tabletsErr := discoverAllShardsViaTablets(ctx, cfg)

	switch {
	case shardsErr != nil && tabletsErr != nil:
		// Both failed — the cross-check never masks a real discovery failure.
		return nil, shardsErr
	case tabletsErr != nil:
		slog.WarnContext(ctx, "mysql/vstream: SHOW VITESS_TABLETS shard cross-check unavailable; "+
			"using SHOW VITESS_SHARDS alone (a serving shard SHARDS omits cannot be recovered)",
			slog.String("err", tabletsErr.Error()))
		return fromShards, nil
	case shardsErr != nil:
		slog.WarnContext(ctx, "mysql/vstream: SHOW VITESS_SHARDS failed; "+
			"falling back to the SHOW VITESS_TABLETS serving-shard set",
			slog.String("err", shardsErr.Error()))
		return fromTablets, nil
	}

	union, discrepancies := reconcileShardSources(fromShards, fromTablets)
	for _, d := range discrepancies {
		if len(d.recoveredFromTablets) > 0 {
			slog.WarnContext(ctx, fmt.Sprintf(
				"mysql/vstream: SHOW VITESS_SHARDS under-reported keyspace %q — "+
					"recovered %v serving shard(s) via SHOW VITESS_TABLETS",
				d.keyspace, d.recoveredFromTablets,
			))
		}
		if len(d.shardsWithoutServingTablet) > 0 {
			slog.WarnContext(ctx, fmt.Sprintf(
				"mysql/vstream: keyspace %q shard(s) %v from SHOW VITESS_SHARDS have no SERVING tablet "+
					"in SHOW VITESS_TABLETS — kept in the shard set but possibly unstreamable",
				d.keyspace, d.shardsWithoutServingTablet,
			))
		}
	}
	return union, nil
}
