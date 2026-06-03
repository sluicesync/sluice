// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Streamer-side mid-run filter mutation for MySQL Phase 2 live add-table
// (ADR-0034). The orchestrator records new tables to the per-target
// `sluice_cdc_state.live_added_tables` column on a successful
// `sluice schema add-table --no-drain TABLE`; the running streamer's
// poll goroutine reads that column on its existing tick cadence and
// merges any new entries into the dispatch filter additively.
//
// The base [TableFilter] (operator's `--include-table` /
// `--exclude-table`) is never mutated. The merge is performed at
// dispatch time via [changeAllowedWithLiveAdd], which permits a change
// when EITHER the base filter allows the table OR the live-added set
// contains its unqualified name. This preserves the principle of
// least surprise: the operator's safety-isolation filter still excludes
// what it always excluded; live-adds are explicit additive grants.
//
// Concurrency model:
//   - One writer (the poll goroutine) and many readers (the dispatch
//     filter goroutine + tests).
//   - State lives in an [atomic.Pointer] to a map; the writer allocates
//     a fresh map on every update and atomically swaps. Readers do
//     lock-free loads. Map values are never mutated post-swap.
//   - Initial value (no live-adds yet) is a nil pointer — the dispatch
//     filter treats it as "empty set", so the hot path stays
//     allocation-free until the first add-table fires.

package pipeline

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// liveAddedTablesReader is the optional applier-side surface the
// streamer's poll goroutine consults for ADR-0034 filter-flip
// propagation. Engines that record live-added tables on the per-target
// `sluice_cdc_state` row (MySQL) implement it; engines without the
// concept (Postgres uses publication-add instead) leave it
// unimplemented. The streamer's poll skips the read when the type
// assertion fails.
//
// Returns the comma-joined list parsed into a deduplicated slice of
// unqualified table names, sorted lexicographically for deterministic
// log output. Empty / NULL column → empty slice. Tolerant of the
// column being missing (legacy pre-v0.27.0 control table) — returns
// empty slice.
type liveAddedTablesReader interface {
	ReadLiveAddedTables(ctx context.Context, streamID string) ([]string, error)
}

// liveAddedTablesWriter is the optional applier-side surface the
// add-table orchestrator calls to record a new table on the per-target
// control row (ADR-0034). MySQL implements; PG doesn't (publication-add
// is the equivalent on PG).
//
// Idempotent: the implementation appends `tableName` to the existing
// comma-separated list and deduplicates. A re-run after a partial
// failure (column written, streamer hadn't polled yet) lands cleanly.
type liveAddedTablesWriter interface {
	RecordLiveAddedTable(ctx context.Context, streamID, tableName string) error
}

// liveAddedFilter is the streamer-side state for ADR-0034. It holds the
// atomically-swappable set of tables that have been live-added to this
// stream's scope mid-run. The dispatch filter consults [Contains] on
// every event; the poll goroutine [Set]s the set when the cdc-state
// column changes.
//
// Zero value is usable: a freshly-constructed liveAddedFilter has no
// live-added tables (Contains returns false for everything).
type liveAddedFilter struct {
	// set holds *map[string]struct{}. nil pointer = empty set; a
	// non-nil pointer points at an immutable map (the writer always
	// allocates a fresh map and atomically swaps).
	set atomic.Pointer[map[string]struct{}]
}

// Contains reports whether tableName is in the live-added set.
// Lock-free; safe to call concurrently with Set.
func (f *liveAddedFilter) Contains(tableName string) bool {
	if f == nil {
		return false
	}
	m := f.set.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[tableName]
	return ok
}

// Set replaces the live-added set with the values in tables. Allocates
// a fresh map and atomically swaps; existing readers that hold a
// pointer to the previous map are unaffected.
//
// Empty / nil tables clears the set (Contains returns false for
// everything). The streamer doesn't currently use the clear path —
// live-adds are append-only — but the API supports it for symmetry.
func (f *liveAddedFilter) Set(tables []string) {
	if len(tables) == 0 {
		var empty map[string]struct{}
		f.set.Store(&empty)
		return
	}
	m := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		m[t] = struct{}{}
	}
	f.set.Store(&m)
}

// Snapshot returns the current set as a sorted slice. Used for log
// output and tests — the hot dispatch path uses Contains, not this.
func (f *liveAddedFilter) Snapshot() []string {
	if f == nil {
		return nil
	}
	m := f.set.Load()
	if m == nil || len(*m) == 0 {
		return nil
	}
	out := make([]string, 0, len(*m))
	for k := range *m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// filterChangesWithLiveAdd is the live-add-aware companion to
// [filterChanges]. The hot-path event check consults the base filter
// AND the live-added set: a change is allowed if EITHER admits it.
// When both base and live-add are empty (the common case for streams
// with no filter and no live-adds yet), the function returns the input
// channel verbatim — same zero-overhead shape as [filterChanges].
//
// The two-input shape lets the caller swap one without touching the
// other; the streamer wires the operator-supplied [TableFilter] as
// `base` and the running [liveAddedFilter] (whose contents change
// over the run) as `live`.
func filterChangesWithLiveAdd(ctx context.Context, in <-chan ir.Change, base TableFilter, live *liveAddedFilter) <-chan ir.Change {
	// Fast path: no base filter and no live-add infra → pass-through.
	// `live` may still be non-nil but empty; we let it through to the
	// goroutine path because subsequent live-adds need to take effect
	// without restarting the stream.
	if base.IsEmpty() && live == nil {
		return in
	}
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				if !changeAllowedWithLiveAdd(c, base, live) {
					slog.DebugContext(
						ctx, "cdc event dropped by table filter",
						slog.String("table", c.QualifiedName()),
					)
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// changeAllowedWithLiveAdd is the OR-merged dispatch decision for
// ADR-0034: a change passes if the base filter allows it OR its
// unqualified table name is in the live-added set.
//
// Source-tx boundary events ([ir.TxBegin], [ir.TxCommit]) bypass both
// (same shape as [changeAllowed]) — they're applier-internal signals,
// not per-table data.
func changeAllowedWithLiveAdd(c ir.Change, base TableFilter, live *liveAddedFilter) bool {
	switch c.(type) {
	case ir.TxBegin, ir.TxCommit:
		return true
	}
	name := c.QualifiedName()
	// Strip "schema." prefix if present — filter patterns target
	// unqualified names, same convention as [changeAllowed].
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			name = name[i+1:]
			break
		}
	}
	if base.Allows(name) {
		return true
	}
	return live.Contains(name)
}

// pollLiveAddedTables runs alongside [pollStopSignal] and refreshes the
// streamer's [liveAddedFilter] when the cdc-state column changes.
// Same poll cadence as the stop-signal poll (defaults to 5s; tests
// override via [setPollIntervalForTest]).
//
// reader is typed as [liveAddedTablesReader] — the optional applier
// surface MySQL implements. Callers that pass a non-conforming applier
// should skip calling this entirely.
//
// The poll skips errors quietly (logs WARN, continues): the column
// might be missing on a pre-v0.27.0 control table, or a transient
// connection blip might trip the read. The next tick retries; the
// streamer's correctness doesn't depend on getting the live-add update
// instantly — the operator's add-table run already wrote the column,
// and a few seconds of poll lag is the documented best-effort shape
// (ADR-0034 § "best-effort caveat").
func pollLiveAddedTables(pollCtx context.Context, reader liveAddedTablesReader, streamID string, target *liveAddedFilter) {
	t := time.NewTicker(loadPollIntervalForTest())
	defer t.Stop()

	prev := ""
	for {
		select {
		case <-pollCtx.Done():
			return
		case <-t.C:
		}
		tables, err := reader.ReadLiveAddedTables(pollCtx, streamID)
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			slog.WarnContext(
				pollCtx, "live-added-tables poll failed; will retry on next tick",
				slog.String("err", err.Error()),
			)
			continue
		}
		joined := strings.Join(tables, ",")
		if joined == prev {
			continue
		}
		prev = joined
		target.Set(tables)
		slog.InfoContext(
			pollCtx, "live-added tables observed; merging into dispatch filter (ADR-0034)",
			slog.String("stream_id", streamID),
			slog.String("tables", joined),
		)
	}
}
