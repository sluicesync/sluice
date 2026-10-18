// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Streamer-side mid-run filter mutation for MySQL Phase 2 live add-table
// (ADR-0034). The orchestrator records new tables to the per-target
// `sluice_cdc_state.live_added_tables` column on a successful
// `sluice schema add-table --no-drain TABLE`; the running streamer's
// poll goroutine reads that column on its existing tick cadence and
// merges any new entries into the dispatch filter additively.
//
// The base [migcore.TableFilter] (operator's `--include-table` /
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
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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

// liveAddedTablesRemover takes a table out of the live-added set: the
// `schema add-table --abandon` path (Bug 321 R6 A). MySQL and Postgres
// implement it.
type liveAddedTablesRemover interface {
	RemoveLiveAddedTable(ctx context.Context, streamID, tableName string) error
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

	// refusal is the LIVE-ADDED-TABLE-EXCLUDED refusal the poll raised when
	// the set gained a table an explicit --exclude-table names (R6-S1);
	// refused is closed when it is set, and the dispatch filter forwards
	// nothing from then on. Created lazily so the zero value stays usable.
	refusal    atomic.Pointer[error]
	refuseOnce sync.Once
	refusedMu  sync.Mutex
	refused    chan struct{}
}

// refusedChan is closed once [liveAddedFilter.refuse] has run. A nil filter
// returns nil, which a select never receives from.
func (f *liveAddedFilter) refusedChan() <-chan struct{} {
	if f == nil {
		return nil
	}
	f.refusedMu.Lock()
	defer f.refusedMu.Unlock()
	if f.refused == nil {
		f.refused = make(chan struct{})
	}
	return f.refused
}

// refuse records the refusal and closes refusedChan; only the first call
// counts.
func (f *liveAddedFilter) refuse(err error) {
	f.refuseOnce.Do(func() {
		f.refusal.Store(&err)
		f.refusedChan() // ensure it exists before closing it
		f.refusedMu.Lock()
		close(f.refused)
		f.refusedMu.Unlock()
	})
}

// refusalErr is the recorded refusal, nil while there is none.
func (f *liveAddedFilter) refusalErr() error {
	if f == nil {
		return nil
	}
	if p := f.refusal.Load(); p != nil {
		return *p
	}
	return nil
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
//
// There is no pass-through for an empty filter: control tables are out of
// scope even when nothing else is ([tableAllowedWithLiveAdd], GC-43 (e)).
// The pass-through this function used to have was never taken by the
// streamer anyway, whose live-added set is always non-nil.
//
// The two-input shape lets the caller swap one without touching the
// other; the streamer wires the operator-supplied [migcore.TableFilter] as
// `base` and the running [liveAddedFilter] (whose contents change
// over the run) as `live`.
//
// Once the live-added set's poll has raised LIVE-ADDED-TABLE-EXCLUDED
// ([liveAddedFilter.refuse]), the stage forwards nothing more and closes its
// output, like the other refusing intercepts; [Streamer.phaseSettleDispatch]
// surfaces the refusal. Nothing it received after the refusal reaches the
// applier, so the persisted position never moves past a change the stream
// saw while the conflict stood.
func filterChangesWithLiveAdd(ctx context.Context, in <-chan ir.Change, base migcore.TableFilter, live *liveAddedFilter) <-chan ir.Change {
	out := make(chan ir.Change)
	refused := live.refusedChan()
	go func() {
		defer close(out)
		for {
			select {
			case <-refused:
				return
			case c, ok := <-in:
				if !ok {
					return
				}
				if live.refusalErr() != nil {
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
func changeAllowedWithLiveAdd(c ir.Change, base migcore.TableFilter, live *liveAddedFilter) bool {
	switch c.(type) {
	case ir.TxBegin, ir.TxCommit:
		return true
	}
	// Strip "schema." prefix if present — filter patterns target
	// unqualified names, same convention as [changeAllowed]. The scan
	// lives in [ir.UnqualifiedTableName] rather than inline here because
	// the MySQL reader's scope predicate has to reach the SAME answer:
	// it split at the first dot while this scanned from the last, so for
	// a table named `a.b` the two disagreed and a refusal the reader
	// skipped as out-of-scope had its boundary forwarded from here
	// (audit 2026-09-09 VF0909C-3). Behaviour is unchanged on this side.
	return tableAllowedWithLiveAdd(ir.UnqualifiedTableName(c.QualifiedName()), base, live)
}

// tableAllowedWithLiveAdd is the name-based core of the dispatch filter,
// shared with the reader-side scope predicate (Bug 246) so the reader's
// policy checks and the dispatch filter can never disagree about a table.
//
// sluice's own control tables are never in scope, whatever the filter says
// (GC-43 (e)). The roster ([appliershared.IsControlTable]) kept them out of
// every schema reader, so no target ever gets one created — but nothing kept
// their CHANGES out of the stream. The source heartbeat
// (--source-heartbeat-interval) writes sluice_heartbeat on the source; on a
// MySQL source those rows are in the binlog, reached the applier, found no
// such table on the target and were counted in the skip ledger, so `sync
// health` reported SKIPPING and exited 1, telling the operator to `schema
// add-table` sluice's bookkeeping. A source that is itself a sluice target
// carries sluice_cdc_state and its siblings the same way. Excluding them
// HERE reaches both halves of the scope at once: every reader that accepts
// the predicate drops them at the source ([Streamer.wireCDCScopePredicate]),
// and the dispatch filter drops them for every reader that does not. The
// transaction's TxBegin/TxCommit still pass, so a heartbeat still moves the
// persisted position — which is what it is for.
//
// The roster carries only fixed names, so a heartbeat table renamed with
// --source-heartbeat-table-name is not on it. The streamer marks that name
// sluice-owned in its filter ([Streamer.customSourceHeartbeatTable]); base
// refuses it in Allows, and it is checked again here so a live-add naming
// it cannot readmit it either.
func tableAllowedWithLiveAdd(unqualified string, base migcore.TableFilter, live *liveAddedFilter) bool {
	return scopeAdmits(unqualified, base, live.Contains)
}

// scopeAdmits is the stream's effective scope, the one rule the dispatch
// filter, the reader's scope predicate, the INCLUDE-TABLE-SCOPE-DRIFT door
// and the unique-key door all ask: sluice's own tables never; a table the
// operator's filter allows; and a table added to the stream (live-added: on
// every source since Bug 321 R6 A, by any completed `schema add-table`)
// unless an explicit --exclude-table names it. The exclusion winning over the
// live-add is what ADR-0034 promised ("the operator's safety-isolation
// filter still excludes what it always excluded") and what this function did
// not do before R6: it admitted a live-added table its --exclude-table named.
//
// The exclusion winning is a backstop, not a state a stream runs in: a
// live-added table an explicit exclude names is a contradiction every start
// and the running stream refuse (LIVE-ADDED-TABLE-EXCLUDED, R6-S1,
// [liveAddedExcluded]), because the add told the operator the table is in
// scope and the exclusion silently applies none of its changes.
func scopeAdmits(unqualified string, base migcore.TableFilter, liveAdded func(string) bool) bool {
	if appliershared.IsControlTable(unqualified) || base.IsSluiceOwned(unqualified) {
		return false
	}
	if base.Allows(unqualified) {
		return true
	}
	return !base.ExcludesExplicitly(unqualified) && liveAdded(unqualified)
}

// LiveAddedTableExcludedMarker names the refusal of a live-added table an
// explicit --exclude-table names (R6-S1).
const LiveAddedTableExcludedMarker = "LIVE-ADDED-TABLE-EXCLUDED"

// liveAddedExcluded returns, sorted, the live-added tables an explicit
// --exclude-table names — the contradiction LIVE-ADDED-TABLE-EXCLUDED
// refuses. sluice's own tables are never live-added and are skipped.
func liveAddedExcluded(base migcore.TableFilter, liveAdded []string) []string {
	var out []string
	for _, t := range liveAdded {
		if appliershared.IsControlTable(t) || base.IsSluiceOwned(t) {
			continue
		}
		if base.ExcludesExplicitly(t) {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// liveAddedCheck is what the LIVE-ADDED-TABLE-EXCLUDED check knows about a
// stream: its id, its operator filter, and the engine names its pasteable
// `schema add-table` remedies carry.
type liveAddedCheck struct {
	streamID       string
	base           migcore.TableFilter
	source, target string // engine names
}

// liveAddedCheckFor is the stream's [liveAddedCheck].
func (s *Streamer) liveAddedCheckFor(streamID string) liveAddedCheck {
	return liveAddedCheck{streamID: streamID, base: s.Filter, source: s.Source.Name(), target: s.Target.Name()}
}

// refusal is the LIVE-ADDED-TABLE-EXCLUDED refusal of tables. It carries
// [ir.ErrIncludeTableScopeDrift]: a restart reads the same control row and
// the same flags, so it is terminal, and in the fleet supervisor's
// do-not-restart roster. running says the stream stopped mid-run rather than
// at its start.
func (c liveAddedCheck) refusal(tables []string, running bool) error {
	list := strings.Join(tables, ", ")
	what := "Nothing has been applied by this run."
	if running {
		// The poll sees the add up to one poll interval after it was
		// recorded; changes in between were dropped as excluded, so keeping
		// the table needs its copy redone, not just the flag narrowed.
		what = fmt.Sprintf("The running stream stopped as soon as it saw the add, forwarding nothing it received after "+
			"that; changes to the table that reached it between the add and that moment (at most one poll interval, the "+
			"window every live add has — ADR-0034) were dropped as excluded, so to keep the table, also redo its copy: "+
			"empty the target table and run `sluice schema add-table <table> --source-driver %s --source <SOURCE-DSN> --target-driver %s --target <TARGET-DSN> --stream-id %s --yes` "+
			"once --exclude-table no longer names it.", c.source, c.target, c.streamID)
	}
	return fmt.Errorf("pipeline: %w: %s: stream %q has %s in its live-added set — `sluice schema add-table` brought "+
		"it into the stream — and an --exclude-table pattern names it. The two contradict each other: the add said the "+
		"table is in the stream's scope, and the exclusion would apply none of its changes, so its target copy would go "+
		"stale while the stream reported healthy. Decide which you meant: to keep the table, narrow --exclude-table so "+
		"no pattern matches it; to take it out, run `sluice schema add-table <table> --abandon --source-driver %s --source <SOURCE-DSN> --target-driver %s --target <TARGET-DSN> --stream-id %s --yes`, "+
		"which removes it from the live-added set. Then start again. %s",
		ir.ErrIncludeTableScopeDrift, LiveAddedTableExcludedMarker, c.streamID, list, c.source, c.target, c.streamID, what)
}

// phaseReadLiveAddedScope reads the stream's live-added set at a start, on
// every dispatch branch (cold and warm) before anything is copied, dropped or
// applied, and does two things with it:
//
//   - LIVE-ADDED-TABLE-EXCLUDED (R6-S1): a live-added table an explicit
//     --exclude-table names refuses the start;
//   - the cold start's copy scope (R7b): it records the set in
//     s.startLiveAdded, which [Streamer.copyFilter] adds to the filter, so a
//     cold start copies every table the stream will apply. Before R7b it
//     copied only what the flags named, then streamed a live-added table from
//     its new snapshot onto a target copy missing everything that changed
//     while the stream was stopped, at exit 0.
//
// A read failure fails the start (restartable): a copy scope that silently
// lost the set is the defect itself. A target without the set has nothing to
// add. --reset-target-data deletes the control row the set lives on before its
// cold start, so the set is discarded rather than copied; the WARN names what
// that leaves unreplicated.
func (s *Streamer) phaseReadLiveAddedScope(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	s.startLiveAdded = nil
	reader, ok := applier.(liveAddedTablesReader)
	if !ok {
		return nil
	}
	tables, err := reader.ReadLiveAddedTables(ctx, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: read the stream's live-added tables: %w", err))
	}
	if s.ResetTargetData {
		var discarded []string
		for _, t := range tables {
			if !s.Filter.Allows(t) {
				discarded = append(discarded, t)
			}
		}
		if len(discarded) > 0 {
			sort.Strings(discarded)
			slog.WarnContext(ctx, "--reset-target-data discards the stream's live-added set with its control row: these tables "+
				"`schema add-table` brought in are neither copied nor applied from here on, and their target tables are left "+
				"as they are (stale). Name them in --include-table or `schema add-table` them again to keep replicating them",
				slog.String("stream_id", streamID), slog.String("tables", strings.Join(discarded, ", ")))
		}
		return nil
	}
	if excluded := liveAddedExcluded(s.Filter, tables); len(excluded) > 0 {
		slog.ErrorContext(ctx, "start refused: "+LiveAddedTableExcludedMarker, slog.String("stream_id", streamID),
			slog.String("tables", strings.Join(excluded, ", ")))
		return s.liveAddedCheckFor(streamID).refusal(excluded, false)
	}
	s.startLiveAdded = tables
	return nil
}

// copyFilter is the cold start's copy scope: the operator's filter plus the
// live-added tables [Streamer.phaseReadLiveAddedScope] read — the same set
// [scopeAdmits] applies, so the copy, the publication scope it derives and
// the apply agree (R7b).
func (s *Streamer) copyFilter() migcore.TableFilter {
	return s.Filter.WithAddedTables(s.startLiveAdded)
}

// wireCDCScopePredicate hands a CDC reader the sync's EFFECTIVE table scope
// (the operator filter merged with live-adds) via the optional
// [ir.CDCScopePredicateSetter] surface, so reader-side POLICY checks — the
// MySQL XA refusal — agree with the dispatch filter one stage downstream
// (Bug 246: without this the refusal fired for tables the sync excludes, a
// working configuration, and the tripped stream re-refused forever because
// excluding the table changed nothing the reader could see). Wired at every
// reader-open site — cold start, warm resume, and both multidb paths — the
// same enumeration the poll-interval setter carries. The closure reads the
// live-add set through an atomic pointer (nil until the apply sidecars
// create it — correct, since no live-add can exist before they start), so
// it is safe from the reader's pump goroutine. Type-assert/silent-ignore
// like every optional reader surface: engines without reader-side policy
// checks don't implement it.
func (s *Streamer) wireCDCScopePredicate(r ir.CDCReader) {
	setter, ok := r.(ir.CDCScopePredicateSetter)
	if !ok {
		return
	}
	setter.SetCDCScopePredicate(func(_, table string) bool {
		return tableAllowedWithLiveAdd(table, s.Filter, s.liveFilterRef.Load())
	})
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
//
// A set that gains a table the stream's explicit --exclude-table names raises
// LIVE-ADDED-TABLE-EXCLUDED ([observeLiveAdded]) and the poll ends: the
// dispatch filter stops the stream.
func pollLiveAddedTables(pollCtx context.Context, reader liveAddedTablesReader, check liveAddedCheck, target *liveAddedFilter) {
	t := time.NewTicker(loadPollIntervalForTest())
	defer t.Stop()

	streamID := check.streamID
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
		if !observeLiveAdded(pollCtx, check, target, tables) {
			return
		}
		slog.InfoContext(
			pollCtx, "live-added tables observed; merging into dispatch filter (ADR-0034)",
			slog.String("stream_id", streamID),
			slog.String("tables", joined),
		)
	}
}

// observeLiveAdded installs a read of the live-added set into target, or —
// when an explicit --exclude-table names one of its tables — raises
// LIVE-ADDED-TABLE-EXCLUDED on target instead and reports false (R6-S1). The
// refusal is raised before the set is installed, and the dispatch filter
// checks it before forwarding each change, so no change the stream receives
// from then on is applied or acknowledged.
func observeLiveAdded(ctx context.Context, check liveAddedCheck, target *liveAddedFilter, tables []string) bool {
	if excluded := liveAddedExcluded(check.base, tables); len(excluded) > 0 {
		slog.ErrorContext(ctx, "stream stopping: "+LiveAddedTableExcludedMarker, slog.String("stream_id", check.streamID),
			slog.String("tables", strings.Join(excluded, ", ")))
		target.refuse(check.refusal(excluded, true))
		return false
	}
	target.Set(tables)
	return true
}
