// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"sort"
	"strconv"

	gomysql "github.com/go-sql-driver/mysql"
)

// Cross-table VStream cold-copy concurrency (ADR-0099). The ADR-0095
// auto-shard COPY copies each table one at a time over a SINGLE vtgate
// VStream — bounded memory, but a single un-splittable read stream that
// leaves the source/target/network N-fold under-utilized. ADR-0097's
// write-side fan-out only reached ~1.4× on this path because the write
// workers starve behind that one read stream.
//
// The real lever (validated: ~4× near-linear) is N INDEPENDENT read
// streams, each on a DIFFERENT table. This file internalizes that: it
// partitions the in-scope table set into K disjoint groups and the
// concurrent pump (cdc_vstream_copy_concurrency_pump.go) runs one
// single-table auto-shard sub-sequence per group on its own VStream.
// The per-shard GTID-set-min stitch (stitchSnapshotMin) is
// parallelism-agnostic, so the snapshot→CDC handoff is unchanged.

const (
	// defaultCopyTableParallelism is the VSTREAM cross-table COPY stream
	// count when the operator sets no knob. It is 1 (sequential single-stream
	// auto-shard, byte-identical to ADR-0095/0098) and — unlike the native
	// binlog path, whose defaultNativeCopyTableParallelism flipped to auto-4
	// in the perf-parity gap-3 chunk — it DELIBERATELY stays 1:
	//
	//   - K is not persisted in the position token (ADR-0099 §5 kept the
	//     ADR-0098 token shape); a resume must re-derive the SAME table→stream
	//     partition with the SAME K. A default flip would silently change the
	//     effective K for any durable-watermark job resumed across the upgrade
	//     boundary → the in-progress cursor's table lands in a different
	//     stream → missed or double-copied table, the silent-loss class the
	//     ADR's operator contract exists to prevent. (The native path has no
	//     cross-process resume, which is why ITS flip is safe.)
	//   - K > 1 is K INDEPENDENT causally-unordered vtgate sessions whose
	//     handoff rests on the set-min stitch + its loud-refusal divergence
	//     path (ADR-0099 §4) — sound, but a refusal class K = 1 structurally
	//     never hits; not a surprise to spring on every cold-start.
	//   - Each stream gets a --max-buffer-bytes/K sub-budget (the §2 liveness
	//     fix); a default flip would silently quarter every cold-start's
	//     per-stream buffer.
	//   - Each stream is a full vtgate gRPC COPY session (the original cost
	//     rationale; contrast ADR-0097's fan-out, whose workers are cheap so
	//     it defaults to 4).
	//
	// Recorded as a deliberate absence in docs/dev/perf-parity-matrix.md;
	// the sequential path INFO-logs the knob so the throughput is one flag
	// away, never a hidden ceiling. This is the zero-value-safe default (the
	// v0.99.51 trap): every constructor / test / non-DSN caller that gets the
	// Go zero value gets sequential behavior, NOT "zero streams = copies
	// nothing".
	defaultCopyTableParallelism = 1

	// maxCopyTableParallelism caps an operator-supplied parallelism so a
	// typo (vstream_copy_table_parallelism=1000) can't open a thousand
	// concurrent vtgate streams. The connection-budget preflight clamps
	// further against K × fan-out-degree and --max-target-connections.
	maxCopyTableParallelism = 32
)

// vstreamCopyTableParallelismFromDSN reads the optional
// vstream_copy_table_parallelism DSN parameter — the number of CONCURRENT
// single-table COPY streams the auto-shard cold-copy runs (ADR-0099). Each
// stream is an independent vtgate VStream over a disjoint subset of the
// in-scope tables. Absent ⇒ defaultCopyTableParallelism (1, sequential).
// A malformed value is a LOUD error (the loud-failure tenet: an operator
// who set the knob deserves to know it didn't parse), NOT a silent
// fallback to sequential.
//
// ADR-0118 finding 4 precedence: an explicit --vstream-copy-table-parallelism
// CLI flag (recorded via SetVStreamCopyTableParallelismOverride, value > 0)
// WINS over the DSN param. The DSN form is still read (and still validated
// loudly) when no CLI override is set, so existing DSN-only setups are
// byte-identical. The CLI override path skips DSN parsing entirely — an
// explicit flag means the operator chose the value for this run.
//
// The returned value is the RAW operator intent; resolveCopyTableParallelism
// clamps it to the table count + the ceiling.
func vstreamCopyTableParallelismFromDSN(cfg *gomysql.Config) (int, error) {
	if cli := int(vstreamCopyTableParallelismOverride.Load()); cli > 0 {
		return cli, nil
	}
	v := cfg.Params["vstream_copy_table_parallelism"]
	if v == "" {
		return defaultCopyTableParallelism, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf(
			"mysql/vstream: invalid vstream_copy_table_parallelism %q (want a positive integer, e.g. 4; 0 or 1 means sequential): %w",
			v, err,
		)
	}
	return n, nil
}

// resolveCopyTableParallelism maps the raw operator-supplied parallelism to
// the effective stream count for a copy of nTables tables (ADR-0099). It is
// the zero-value-safe resolver (the v0.99.51 trap), exactly mirroring
// resolveCopyFanoutDegree's shape:
//
//	n <= 1            → 1   (sequential single-stream auto-shard — the safe
//	                         default the Go zero value resolves to)
//	n  > 1            → min(n, nTables, maxCopyTableParallelism)
//
// There is no value that produces "0 streams = copies nothing". K is never
// more than the table count (an empty stream group is wasteful, and we
// avoid double-assignment by construction), and never more than the
// ceiling. nTables <= 1 always collapses to 1 (one table never benefits
// from cross-table concurrency).
func resolveCopyTableParallelism(n, nTables int) int {
	if nTables < 1 {
		nTables = 1
	}
	switch {
	case n <= 1:
		return 1
	case n > maxCopyTableParallelism:
		n = maxCopyTableParallelism
	}
	if n > nTables {
		n = nTables
	}
	return n
}

// tableSizeEstimator returns an estimated byte size for an unqualified
// table name, used to size-balance the partition (ADR-0099 §1). A nil
// estimator (or one returning ok=false for a table) makes
// partitionTablesForStreams fall back to deterministic round-robin over
// the sorted table list. The estimate need not be exact — it only steers
// which group a large table lands in so one stream isn't the long pole.
type tableSizeEstimator func(table string) (bytes int64, ok bool)

// partitionTablesForStreams splits the in-scope tables into k disjoint
// groups, one per concurrent COPY stream (ADR-0099 §1). It is a
// DETERMINISTIC PURE FUNCTION of (tables, k, sizes): the same inputs always
// produce the same partition, regardless of the order tables is passed in
// (it sorts a copy first). That determinism is the load-bearing
// cold-start-vs-resume stability invariant — on resume each stream must
// re-derive the SAME table→stream assignment it had on cold-start, or the
// persisted cursor's in-progress table could land in a different stream
// than the one that re-derives it (silent miss or double-copy).
//
// Coverage + disjointness (the load-bearing silent-loss invariant): every
// in-scope table lands in EXACTLY ONE group — none dropped (a silently
// un-copied table), none duplicated (a table double-produced into one
// shared rowBuffer queue from two pumps). Pinned hard by unit test.
//
// Balancing: when sizes is non-nil and returns an estimate for the tables,
// it uses a longest-processing-time greedy (assign each table, largest
// first, to the currently-smallest group) so the BLOB table doesn't make
// one stream the long pole. When sizes is nil or has no estimate, it falls
// back to round-robin over the SORTED table list — still deterministic, no
// size dependency. Both variants preserve coverage/disjointness/determinism.
//
// k is clamped to [1, len(tables)]: never more streams than tables (no
// empty group), never zero. The returned groups preserve, WITHIN each
// group, the orchestrator's filtered schema order is NOT required — the
// per-table ReadRows↔pump coupling needs each table copied by exactly one
// stream in some order, and the orchestrator drains tables in its own
// schema order independently. We keep each group's internal order sorted
// for determinism.
func partitionTablesForStreams(tables []string, k int, sizes tableSizeEstimator) [][]string {
	if len(tables) == 0 {
		return nil
	}
	if k < 1 {
		k = 1
	}
	if k > len(tables) {
		k = len(tables)
	}

	// Sort a COPY so the assignment is independent of the caller's order —
	// the determinism / stability invariant (ADR-0099 §5).
	sorted := make([]string, len(tables))
	copy(sorted, tables)
	sort.Strings(sorted)

	groups := make([][]string, k)

	// Size-balanced greedy when estimates are available for ALL tables;
	// otherwise deterministic round-robin. We require estimates for every
	// table (not just some) so the balancing is well-defined and
	// deterministic — a partial-estimate mix would make the greedy depend
	// on which tables happened to have stats, a non-deterministic surface.
	type sized struct {
		name string
		size int64
	}
	estimates := make([]sized, 0, len(sorted))
	haveAll := sizes != nil
	for _, t := range sorted {
		var b int64
		if sizes != nil {
			var ok bool
			b, ok = sizes(t)
			if !ok {
				haveAll = false
				break
			}
		}
		estimates = append(estimates, sized{name: t, size: b})
	}

	if haveAll {
		// Longest-processing-time greedy: largest tables first, each to the
		// currently-smallest group. Deterministic because the input is the
		// sorted list and ties break by the already-sorted name order
		// (stable sort by descending size keeps equal-size tables in sorted
		// name order), and the smallest-group selection breaks ties by the
		// lowest group index.
		sort.SliceStable(estimates, func(i, j int) bool {
			return estimates[i].size > estimates[j].size
		})
		loads := make([]int64, k)
		for _, e := range estimates {
			minG := 0
			for g := 1; g < k; g++ {
				if loads[g] < loads[minG] {
					minG = g
				}
			}
			groups[minG] = append(groups[minG], e.name)
			loads[minG] += e.size
		}
	} else {
		// Round-robin over the sorted list — deterministic, no size
		// dependency.
		for i, t := range sorted {
			groups[i%k] = append(groups[i%k], t)
		}
	}

	// Keep each group's internal order sorted for determinism (the greedy
	// reordered by size).
	for g := range groups {
		sort.Strings(groups[g])
	}
	return groups
}
