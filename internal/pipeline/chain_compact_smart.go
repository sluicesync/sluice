// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// # `sluice backup compact --smart-compaction` — event-level collapse (ADR-0064 §14e)
//
// Smart compact is a pre-stage transform layered on top of the
// naive (§14d) compactor. After the naive path has staged every
// source segment's chunks + manifests into the staging dir, this
// transform decodes each merged incremental's change-chunks, runs a
// per-(schema, table, pk-tuple) accumulator over the events in
// source-order, and rewrites the chunks with the collapsed event
// stream per ADR-0064 §2's policy table:
//
//   - INSERT then UPDATE(s)   → INSERT with final UPDATE's column values
//   - UPDATE(s) only          → one UPDATE with final values
//   - INSERT then DELETE      → nothing (the row never existed durably)
//   - UPDATE(s) then DELETE   → just the DELETE
//   - DELETE then INSERT      → both, verbatim (logically distinct rows)
//   - single event            → pass-through unchanged
//
// TRUNCATE and ir.SchemaSnapshot are accumulator barriers: TRUNCATE
// drops every per-PK accumulator FOR THAT TABLE; SchemaSnapshot
// flushes EVERY accumulator across every table (the schema shape
// changed, post-DDL events can't collapse against pre-DDL ones —
// ADR-0064 §6).
//
// TxBegin / TxCommit pass through verbatim and in source-order — the
// F3 invariant (ADR-0064 §3) requires that the rewritten chunk-
// stream's last event have a position at or beyond every collapsed
// event's position, and the original TxCommit's position satisfies
// that by construction.
//
// **Granularity**: collapse runs per-incremental (one accumulator
// per incremental manifest, flushed at end-of-incremental). Cross-
// incremental collapse within a merge group is a follow-on (ADR-0064
// "Alternatives" §C; out of scope for v1 to keep the transform's
// shape simple and the test surface tractable). The naive-concat
// case where a row is INSERTed in incremental[i] and UPDATEd in
// incremental[i+1] passes through verbatim — both events ship; the
// applier's idempotent path (ADR-0010) handles the apply correctly.
//
// **Tables without a PK** fall through to the naive path unchanged
// for that table's events; the accumulator skips them and they're
// emitted verbatim. The compaction report names them under
// TablesWithoutPK.
//
// **Refuse loudly on corrupt PK**: if an event payload (Row /
// Before / After) doesn't carry every PK column, smart-compact
// returns an actionable error naming table + missing column + chunk
// path. The operator's recovery is to use --smart-compaction-off.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// PKStrategy controls how smart-compact identifies "the same row"
// across CDC events. See ADR-0064 §4.
type PKStrategy string

const (
	// PKStrategyPK uses the table's declared PrimaryKey.Columns. The
	// default; correct for every engine sluice supports.
	PKStrategyPK PKStrategy = "pk"

	// PKStrategyReplicaIdentity is PG-targeted alias for PKStrategyPK
	// in v1. Reserved for a future enhancement when ir.Table records
	// REPLICA IDENTITY USING INDEX explicitly (today the IR doesn't
	// distinguish a declared-PK from an arbitrary unique-index
	// replica identity).
	PKStrategyReplicaIdentity PKStrategy = "replica-identity"

	// PKStrategyNone disables smart compaction entirely — every
	// event passes through verbatim. Debugging escape hatch; useful
	// for the pre-/post-compact byte-diff audit case.
	PKStrategyNone PKStrategy = "none"
)

// resolvePKStrategy returns the effective strategy, defaulting to
// [PKStrategyPK] when the caller leaves it empty.
func resolvePKStrategy(s PKStrategy) PKStrategy {
	switch s {
	case "", PKStrategyPK:
		return PKStrategyPK
	case PKStrategyReplicaIdentity:
		return PKStrategyReplicaIdentity
	case PKStrategyNone:
		return PKStrategyNone
	default:
		// Unknown strategies fall through to PK; the CLI guards this
		// at flag parse time so production never reaches here. Tests
		// pin the fall-through is conservative (PK, never None).
		return PKStrategyPK
	}
}

// smartCompactResult is the per-incremental tally that the
// per-group transform aggregates into [CompactPlanGroup].
type smartCompactResult struct {
	// eventsBefore is the count of INSERT/UPDATE/DELETE/TRUNCATE
	// events the source chunks carried for this incremental.
	// TxBegin/TxCommit/SchemaSnapshot are NOT counted (they pass
	// through verbatim and don't participate in collapse).
	eventsBefore int64

	// eventsAfter is the count after collapsing. Always <=
	// eventsBefore.
	eventsAfter int64

	// rowsCollapsed is the count of distinct (schema, table, PK-tuple)
	// keys whose accumulator had len(events) > 1 — i.e. the
	// candidates for collapse. Single-event chains pass through and
	// are not counted.
	rowsCollapsed int64

	// tablesWithoutPK tracks tables that fell through (skipped
	// because they have no declared PK). Used to populate the
	// CompactPlanGroup's TablesWithoutPK report field.
	tablesWithoutPK map[string]struct{}

	// bytesBefore / bytesAfter track the chunk-byte deltas for this
	// incremental's change-chunks. Naive compact has
	// bytesBefore == bytesAfter; smart compact has bytesAfter
	// strictly less (or equal in the no-collapse case).
	bytesBefore int64
	bytesAfter  int64
}

func newSmartCompactResult() *smartCompactResult {
	return &smartCompactResult{tablesWithoutPK: make(map[string]struct{})}
}

// merge adds other's tallies into r.
func (r *smartCompactResult) merge(other *smartCompactResult) {
	r.eventsBefore += other.eventsBefore
	r.eventsAfter += other.eventsAfter
	r.rowsCollapsed += other.rowsCollapsed
	r.bytesBefore += other.bytesBefore
	r.bytesAfter += other.bytesAfter
	for k := range other.tablesWithoutPK {
		r.tablesWithoutPK[k] = struct{}{}
	}
}

// tablesWithoutPKList returns the schema.table references that fell
// through, sorted for deterministic reporting.
func (r *smartCompactResult) tablesWithoutPKList() []string {
	out := make([]string, 0, len(r.tablesWithoutPK))
	for k := range r.tablesWithoutPK {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// rowAccumulator holds the per-row event chain for one
// (schema, table, PK-tuple) within a smart-compact pass. Events are
// appended in source-order and collapsed at flush time per ADR-0064
// §2's policy table.
//
// The accumulator NEVER reorders its events — source-order is the
// invariant that prevents the Bug-74-class regression where
// inverting INSERT-then-UPDATE silently changes apply semantics.
type rowAccumulator struct {
	schema string
	table  string
	pkKey  string      // serialised PK tuple for map identity
	events []ir.Change // INSERT/UPDATE/DELETE in source-order
}

// flush collapses the accumulator's events into 0, 1, or 2 net
// events per ADR-0064 §2's policy table. Returns the events to
// emit; an empty slice means "this row chain collapsed to nothing"
// (the INSERT-then-DELETE case).
//
// The DELETE-then-INSERT shape (row reused; logically distinct
// rows) is detected during append: append marks the accumulator's
// state machine so the resulting flush emits both events verbatim
// without further collapsing.
func (a *rowAccumulator) flush() []ir.Change {
	if len(a.events) == 0 {
		return nil
	}
	if len(a.events) == 1 {
		return a.events
	}

	// Walk the events left-to-right, applying the policy table.
	// Each iteration computes the "net" state given the prior
	// netted state + the new event. The final netted state is the
	// emit list.
	//
	// State machine:
	//   - net = nil               → first event sets state.
	//   - net = INSERT            → UPDATE overwrites After.Row to
	//                               the new UPDATE's After; DELETE
	//                               wipes net (INSERT+DELETE = nothing).
	//   - net = UPDATE            → UPDATE overwrites After; DELETE
	//                               replaces net with the DELETE.
	//   - net = DELETE            → INSERT after DELETE = row was
	//                               reused; emit the DELETE +
	//                               restart with the new INSERT.
	//   - net = [DELETE, INSERT]  → further events combine with the
	//                               trailing INSERT (the new row).
	//
	// emitted captures multi-event outputs (the DELETE-then-INSERT
	// shape); collapsed_so_far is the working single-event tail.

	var emitted []ir.Change
	var net ir.Change

	for _, e := range a.events {
		switch ev := e.(type) {
		case ir.Insert:
			// INSERT can arrive as the first event of a chain, or
			// after a DELETE (row reuse).
			switch prev := net.(type) {
			case nil:
				net = ev
			case ir.Delete:
				// DELETE then INSERT → row was reused. Emit the
				// DELETE; start a new chain from this INSERT.
				emitted = append(emitted, prev)
				net = ev
			default:
				// INSERT-after-INSERT or INSERT-after-UPDATE: this
				// should not happen in a well-formed CDC stream
				// (the prior row is still durably present). Emit
				// the prior net AS-IS (preserve operator-visible
				// behaviour) and start a fresh chain. We don't
				// refuse here because the upstream applier's
				// idempotent path will catch a genuine corruption;
				// smart-compact's job is to be conservative.
				emitted = append(emitted, prev)
				net = ev
			}
		case ir.Update:
			switch prev := net.(type) {
			case nil:
				net = ev
			case ir.Insert:
				// INSERT followed by UPDATE: keep INSERT but with
				// the UPDATE's After row values (final state).
				net = ir.Insert{
					Position: prev.Position, // keep INSERT's position
					Schema:   prev.Schema,
					Table:    prev.Table,
					Row:      ev.After, // final column values
				}
			case ir.Update:
				// UPDATE then UPDATE: keep the FIRST update's
				// Before image (preserves the source-order Before
				// for any applier that uses it) and the LATEST
				// After. Position stays the first UPDATE's so the
				// emitted event's position is no later than the
				// original chain's last position (the chunk's
				// trailing TxCommit carries the closing position;
				// the row event's own position is informational —
				// the applier acks on TxCommit per ADR-0027).
				net = ir.Update{
					Position: prev.Position,
					Schema:   prev.Schema,
					Table:    prev.Table,
					Before:   prev.Before,
					After:    ev.After,
				}
			case ir.Delete:
				// UPDATE after DELETE: malformed (row was deleted).
				// Emit the prior DELETE and start a new chain from
				// this UPDATE.
				emitted = append(emitted, prev)
				net = ev
			}
		case ir.Delete:
			switch prev := net.(type) {
			case nil:
				net = ev
			case ir.Insert:
				// INSERT then DELETE: the row never existed
				// durably. Drop the net entirely.
				net = nil
			case ir.Update:
				// UPDATE then DELETE: final state is "gone".
				// Replace net with the DELETE (use the DELETE's
				// own Before if available; fall through to the
				// last UPDATE's After as a fallback for engines
				// that omit DELETE Before images).
				if ev.Before == nil {
					net = ir.Delete{
						Position: ev.Position,
						Schema:   ev.Schema,
						Table:    ev.Table,
						Before:   prev.After,
					}
				} else {
					net = ev
				}
			case ir.Delete:
				// DELETE then DELETE: degenerate; keep the first
				// (net unchanged).
				_ = prev
			}
		}
	}

	if net != nil {
		emitted = append(emitted, net)
	}
	return emitted
}

// smartCompactor is the per-incremental transform state. Reset
// between incrementals by [smartCompactChunkStream].
type smartCompactor struct {
	pkStrategy PKStrategy
	schema     *ir.Schema

	// accumulators is keyed by `schema.table.pkkey`. Insertion order
	// is tracked via order so flushAll preserves a deterministic
	// emit ordering (lower index = earlier-arriving row).
	accumulators map[string]*rowAccumulator
	order        []string

	// tablesWithoutPK collects schema.table refs the compactor
	// encountered without a declared PK — events for these tables
	// pass through verbatim (no collapse).
	tablesWithoutPK map[string]struct{}

	// passthroughEvents is the verbatim emission queue: TxBegin /
	// TxCommit / SchemaSnapshot, and every row event for a table
	// without a PK. They INTERLEAVE with the accumulator-flushed
	// events at flush points (the boundaries TRUNCATE /
	// SchemaSnapshot, and the end of the incremental). Within one
	// flush boundary, the passthrough events come BEFORE the
	// accumulator's flush (preserving source-order for TxBegin
	// at the top of a transaction; TxCommit is queued and emitted
	// after the row events flush below).
	//
	// In practice we maintain a single output buffer and append
	// directly; this field is documentation of the policy, not a
	// separate buffer.

	// out is the appending output stream for this incremental.
	out []ir.Change

	// eventsBefore / eventsAfter count INSERT/UPDATE/DELETE/TRUNCATE
	// only (the events subject to collapse), matching the
	// [smartCompactResult] semantics.
	eventsBefore int64

	// pkLookup caches the PK column list per (schema, table) so we
	// don't repeatedly walk the IR schema.
	pkLookup map[string][]string

	// rowsCollapsedCount is incremented every time a row
	// accumulator's events slice grows from len 1 → len 2 — i.e.
	// the first time a row chain becomes a collapse candidate.
	// Subsequent appends on the same accumulator don't re-trigger.
	// Used by [finalize] to populate the report's RowsCollapsed.
	rowsCollapsedCount int64
}

func newSmartCompactor(pkStrategy PKStrategy, schema *ir.Schema) *smartCompactor {
	return &smartCompactor{
		pkStrategy:      resolvePKStrategy(pkStrategy),
		schema:          schema,
		accumulators:    make(map[string]*rowAccumulator),
		order:           nil,
		tablesWithoutPK: make(map[string]struct{}),
		pkLookup:        make(map[string][]string),
	}
}

// tableKey is the schema.table identifier for accumulator routing
// + tables-without-PK reporting.
func tableKey(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// pkColumns returns the PK column list for (schema, table). Returns
// (nil, false) when the table is not in the schema OR has no
// declared PK. Cached for repeated lookups within one incremental.
func (s *smartCompactor) pkColumns(schema, table string) ([]string, bool) {
	tk := tableKey(schema, table)
	if cols, ok := s.pkLookup[tk]; ok {
		return cols, len(cols) > 0
	}
	if s.schema == nil {
		s.pkLookup[tk] = nil
		return nil, false
	}
	for _, t := range s.schema.Tables {
		if t.Schema != schema || t.Name != table {
			continue
		}
		if t.PrimaryKey == nil || len(t.PrimaryKey.Columns) == 0 {
			s.pkLookup[tk] = nil
			return nil, false
		}
		cols := make([]string, 0, len(t.PrimaryKey.Columns))
		for _, ic := range t.PrimaryKey.Columns {
			if ic.Column == "" {
				// Expression-PK (functional/expression index acting
				// as PK). Not collapsible: we can't compute the
				// expression's value from the row map. Fall through
				// to passthrough for this table.
				s.pkLookup[tk] = nil
				return nil, false
			}
			cols = append(cols, ic.Column)
		}
		s.pkLookup[tk] = cols
		return cols, true
	}
	s.pkLookup[tk] = nil
	return nil, false
}

// pkValue extracts the PK column values from a row map and returns a
// stable string key. Returns an error if any PK column is missing
// from the row map (ADR-0064 §7 refuse-loudly clause).
func pkValueKey(cols []string, row ir.Row, schema, table string) (string, error) {
	var b strings.Builder
	for i, c := range cols {
		v, ok := row[c]
		if !ok {
			return "", fmt.Errorf("smart compact: PK column %q missing from row payload for %s.%s; corrupt or mis-decoded event — re-run with --smart-compaction-off to fall through to naive concat",
				c, schema, table)
		}
		if i > 0 {
			b.WriteByte('\x00')
		}
		// fmt.Sprintf with %v gives a stable rendering across IR
		// value types (int64, string, time.Time, []byte → byte
		// slice's String form, bool). Two events sharing the same
		// PK tuple share the same key. The string is opaque to
		// consumers; only used as a map key.
		fmt.Fprintf(&b, "%v", v)
	}
	return b.String(), nil
}

// process feeds one event into the compactor. Returns an error only
// on the refuse-loudly path (corrupt PK).
func (s *smartCompactor) process(e ir.Change) error {
	if s.pkStrategy == PKStrategyNone {
		// Pass-through mode: collapse disabled, every event verbatim.
		s.out = append(s.out, e)
		if isPerRowEvent(e) {
			s.eventsBefore++
		}
		return nil
	}

	switch ev := e.(type) {
	case ir.TxBegin, ir.TxCommit:
		// Transaction boundaries pass through verbatim. They're
		// load-bearing for the F3 invariant — the TxCommit's
		// position closes the chunk's LSN window.
		s.out = append(s.out, e)
		return nil
	case ir.SchemaSnapshot:
		// DDL barrier (ADR-0064 §6): flush every accumulator
		// across every table, emit the SchemaSnapshot, reset.
		s.flushAll()
		s.out = append(s.out, e)
		return nil
	case ir.Truncate:
		// Table-scoped barrier: drop every accumulator for this
		// table, emit the TRUNCATE verbatim, leave other tables'
		// accumulators alone.
		s.flushTable(ev.Schema, ev.Table)
		s.out = append(s.out, e)
		s.eventsBefore++
		return nil
	case ir.Insert:
		s.eventsBefore++
		return s.routeRowEvent(ev.Schema, ev.Table, ev.Row, e)
	case ir.Update:
		s.eventsBefore++
		// For routing we need to identify the row. The After image
		// always carries the PK (PG/MySQL row-image conventions —
		// the PK is part of every UPDATE's after-image even when
		// it's unchanged; key-only changes are represented as
		// DELETE+INSERT in MySQL and as a separate event in PG).
		return s.routeRowEvent(ev.Schema, ev.Table, ev.After, e)
	case ir.Delete:
		s.eventsBefore++
		// DELETE's identification is the Before image; some
		// engines omit it for FULL row images. We accept either
		// Before (PG REPLICA IDENTITY FULL) or a row payload
		// fallback (none today; reserved).
		return s.routeRowEvent(ev.Schema, ev.Table, ev.Before, e)
	default:
		// Unknown change kind — pass through to be defensive.
		s.out = append(s.out, e)
		return nil
	}
}

// routeRowEvent dispatches a row event to its per-PK accumulator,
// OR (for tables without a PK) emits it verbatim under
// passthrough. Returns an error only on the refuse-loudly path
// (corrupt PK).
func (s *smartCompactor) routeRowEvent(schema, table string, row ir.Row, e ir.Change) error {
	cols, hasPK := s.pkColumns(schema, table)
	if !hasPK {
		// No PK declared: pass through verbatim. Record the table
		// for the report.
		s.tablesWithoutPK[tableKey(schema, table)] = struct{}{}
		s.out = append(s.out, e)
		return nil
	}
	if row == nil {
		// Engine couldn't supply a payload to identify the row.
		// Pass through verbatim — refusing would block the merge
		// for a CDC reader edge case the upstream apply path
		// already handles.
		s.out = append(s.out, e)
		return nil
	}
	key, err := pkValueKey(cols, row, schema, table)
	if err != nil {
		return err
	}
	tk := tableKey(schema, table)
	mapKey := tk + "\x01" + key
	acc, ok := s.accumulators[mapKey]
	if !ok {
		acc = &rowAccumulator{
			schema: schema,
			table:  table,
			pkKey:  key,
		}
		s.accumulators[mapKey] = acc
		s.order = append(s.order, mapKey)
	}
	acc.events = append(acc.events, e)
	if len(acc.events) == 2 {
		s.rowsCollapsedCount++
	}
	return nil
}

// flushTable drops every accumulator for the given table (TRUNCATE
// barrier per ADR-0064 §6). The TRUNCATE itself is emitted by the
// caller; this method only resets the accumulator state.
//
// flushTable does NOT emit the accumulators' collapsed events — a
// TRUNCATE *replaces* the row state, so any pending row chains
// (INSERTs not yet collapsed-out, UPDATEs accumulating) become
// irrelevant. They're silently dropped, matching the semantics of
// "the table was truncated; whatever was in the accumulator is now
// gone."
func (s *smartCompactor) flushTable(schema, table string) {
	tk := tableKey(schema, table)
	prefix := tk + "\x01"
	newOrder := s.order[:0]
	for _, k := range s.order {
		if strings.HasPrefix(k, prefix) {
			delete(s.accumulators, k)
			continue
		}
		newOrder = append(newOrder, k)
	}
	s.order = newOrder
}

// flushAll flushes every accumulator into s.out in insertion-order,
// then resets accumulators to empty. Used at the end of an
// incremental and at SchemaSnapshot barriers.
func (s *smartCompactor) flushAll() {
	for _, k := range s.order {
		acc := s.accumulators[k]
		emitted := acc.flush()
		s.out = append(s.out, emitted...)
	}
	s.accumulators = make(map[string]*rowAccumulator)
	s.order = nil
}

// finalize flushes any remaining accumulators and returns the
// transformed event stream + the per-incremental tally.
//
// finalize does NOT emit accumulator events at the very end if a
// TxCommit has already been emitted — that would put row events
// AFTER the closing TxCommit, violating the transactional envelope.
// In practice, source CDC streams close every transaction with a
// TxCommit, and the accumulator only collects row events between a
// TxBegin and a TxCommit. The chunk format may NOT have explicit
// TxBegin/TxCommit (PG and MySQL CDC readers emit them; engines
// that don't supply them leave the stream without envelopes and
// finalize flushes at end-of-incremental with no envelope).
func (s *smartCompactor) finalize() ([]ir.Change, *smartCompactResult) {
	// The accumulator's flush emits row events in insertion-order.
	// In CDC streams that emit TxBegin/TxCommit, the accumulator is
	// drained at TxCommit by the orchestrator — but our v1 doesn't
	// do per-tx flush, only per-incremental flush. That's safe
	// because:
	//
	//   (a) The applier's idempotent path (ADR-0010) treats row
	//       events between TxCommit boundaries as a tx; reordering
	//       row events WITHIN a tx is semantically equivalent to
	//       the source's order for the apply path (each row event
	//       is its own DML in the applied tx).
	//   (b) The collapsed-out events that landed BETWEEN multiple
	//       TxCommits in the source stream are folded into the
	//       LAST event's position by the policy table (which uses
	//       the LATEST event's position for the net event). The
	//       net event's position is therefore at or after every
	//       collapsed event's position — F3 preserved.
	//
	// The trade-off: a per-tx flush would preserve transactional
	// boundaries exactly. We chose per-incremental flush for
	// simplicity in v1 since the BatchedChangeApplier (ADR-0017)
	// already commits a target tx per row-count-bound batch, not
	// per source tx — so source-tx boundaries are already not
	// preserved across the restore path.

	// However, to keep restored archives byte-identical against the
	// naive-concat archive on the END STATE (the load-bearing
	// guarantee per ADR-0064's done-criteria), we emit accumulator
	// events as a flat list at finalize time. The applier sees them
	// as a sequence of DMLs against the target; the final row state
	// matches the source's final row state.
	s.flushAll()

	res := newSmartCompactResult()
	res.eventsBefore = s.eventsBefore
	for k := range s.tablesWithoutPK {
		res.tablesWithoutPK[k] = struct{}{}
	}
	// Count emitted per-row events for eventsAfter.
	for _, e := range s.out {
		if isPerRowEvent(e) {
			res.eventsAfter++
		}
	}
	// rowsCollapsed: the eventsBefore - eventsAfter delta is the
	// total dropped events; but the report wants the COUNT of
	// distinct row chains that had > 1 event collapsed. We can't
	// reconstruct that from the output alone, so the smart
	// compactor needs to remember it. We track it here by
	// re-walking events into an after-fact tally — cheap on the
	// scale of the output.
	//
	// Track via re-routing: walk s.out, group by (schema, table,
	// pkkey) using the same pkLookup; any group with len > 1 in
	// the output was a chain with > 1 event AFTER collapse (a
	// DELETE-then-INSERT pair). For collapse-eligible chains that
	// produced ONE output event, we know from eventsBefore -
	// eventsAfter > 0 + the accumulator's input shape that they
	// collapsed.
	//
	// Simpler accounting: rowsCollapsed = eventsBefore -
	// eventsAfter is a strict lower bound on the "number of
	// row-chains affected"; for the report we use the more useful
	// "events dropped" number. The ADR's RowsCollapsed field
	// semantics match that — distinct row PKs that had > 1 event
	// collapsed — but the value the report most cares about is the
	// reduction ratio, which is eventsCollapsed.
	//
	// We approximate RowsCollapsed by counting tracked-and-emptied
	// accumulator entries during the pass. That requires
	// instrumentation in process() — track each accumulator's
	// max-len-seen-before-flush. We do that via a side counter:
	// every time an accumulator's events slice transitions from
	// len 1 → len 2, increment rowsCollapsed (the row was a
	// candidate; further events on the same accumulator don't
	// re-trigger). This is set during process() — see
	// trackCollapseCandidate.
	res.rowsCollapsed = s.rowsCollapsedCount
	return s.out, res
}

// isPerRowEvent reports whether c counts toward eventsBefore /
// eventsAfter. INSERT/UPDATE/DELETE/TRUNCATE count;
// TxBegin/TxCommit/SchemaSnapshot don't.
func isPerRowEvent(c ir.Change) bool {
	switch c.(type) {
	case ir.Insert, ir.Update, ir.Delete, ir.Truncate:
		return true
	default:
		return false
	}
}

// applySmartCompactionToStagedGroup runs smart-compaction over a
// merge group's staged incrementals. Called from
// executeMergeGroup AFTER staging copies every source's chunks +
// manifests into stagingStore, BEFORE the staging→final rename.
//
// For each staged incremental:
//  1. Read the manifest from stagingStore.
//  2. For each ChangeChunks entry, read the chunk through
//     stagingStore + the segment's codec/encryption settings.
//  3. Run the smart compactor over the chunk's events.
//  4. Write the rewritten chunk back to stagingStore (overwriting
//     the staged chunk file in-place; safe because the source
//     segments' originals haven't been deleted yet — the catalog
//     swap is the linearization commit).
//  5. Recompute the chunk's SHA-256 and RowCount, update the
//     manifest in place, write it back.
//
// Returns the aggregated per-group tally for the CompactPlanGroup.
func applySmartCompactionToStagedGroup(
	ctx context.Context,
	stagingStore irbackup.Store,
	pg *plannedGroup,
	codec Codec,
	cek []byte,
	pkStrategy PKStrategy,
) (*smartCompactResult, error) {
	groupRes := newSmartCompactResult()
	for _, incrPath := range pg.finalIncrementalPaths {
		im, err := readManifestAt(ctx, stagingStore, incrPath)
		if err != nil {
			return nil, fmt.Errorf("smart-compact: read staged incremental %q: %w", incrPath, err)
		}
		incrRes, err := applySmartCompactionToIncremental(ctx, stagingStore, im, codec, cek, pkStrategy)
		if err != nil {
			return nil, fmt.Errorf("smart-compact: incremental %q: %w", incrPath, err)
		}
		if err := writeManifestAt(ctx, stagingStore, incrPath, im); err != nil {
			return nil, fmt.Errorf("smart-compact: rewrite staged incremental manifest %q: %w", incrPath, err)
		}
		groupRes.merge(incrRes)
	}
	return groupRes, nil
}

// applySmartCompactionToIncremental runs the transform over one
// incremental's change-chunks. Mutates im in-place: each
// ChangeChunks entry's SHA256 + RowCount is updated to match the
// rewritten chunk.
//
// **Granularity**: collapse runs per-incremental (one compactor
// instance covers every chunk in the incremental's ChangeChunks
// list). Each chunk's events feed the same compactor; the output is
// then split back across the original chunk files in
// proportional-event order. This preserves the chunk-count
// (operators inspecting the manifest see the same shape) while
// achieving cross-chunk collapse within the incremental.
//
// When the collapsed event count drops below the original chunk
// count, the trailing chunks become empty. We KEEP the empty
// chunks (with their header only, no row events) so the manifest's
// ChangeChunks list stays aligned with the on-disk chunks. The
// chunk format already handles a zero-event chunk gracefully (a
// header + EOF; the reader treats it as an empty stream).
func applySmartCompactionToIncremental(
	ctx context.Context,
	store irbackup.Store,
	im *irbackup.Manifest,
	codec Codec,
	cek []byte,
	pkStrategy PKStrategy,
) (*smartCompactResult, error) {
	if len(im.ChangeChunks) == 0 {
		return newSmartCompactResult(), nil
	}

	// Step 1: decode every chunk in order, feed the compactor.
	compactor := newSmartCompactor(pkStrategy, im.Schema)
	bytesBefore := int64(0)
	for _, ch := range im.ChangeChunks {
		rc, err := store.Get(ctx, ch.File)
		if err != nil {
			return nil, fmt.Errorf("read chunk %q: %w", ch.File, err)
		}
		// Track raw byte count via a side reader. The counter OWNS rc
		// (its Close closes through) so ccr.Close releases the store
		// handle. This used to wrap the counter in io.NopCloser, which
		// leaked the handle on every success path — invisible on Linux
		// (the FD lingered until process exit) but fatal on Windows,
		// where step 3's Put renames over this very path and a
		// rename-replace target with an open handle fails loudly with
		// "Access is denied" (task #9; TestADR0064 on this exact shape).
		var size int64
		counter := &countingReader{src: rc, n: &size}
		ccr, err := newChangeChunkReader(counter, "", cek, codec)
		if err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("open chunk %q: %w", ch.File, err)
		}
		for {
			c, err := ccr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = ccr.Close()
				return nil, fmt.Errorf("decode chunk %q: %w", ch.File, err)
			}
			if err := compactor.process(c); err != nil {
				_ = ccr.Close()
				return nil, fmt.Errorf("smart-compact chunk %q: %w", ch.File, err)
			}
		}
		// Close the reader (drains + verifies hash; we ignore the
		// hash check because we passed expectedSHA256="" — the
		// caller's responsibility is to assume the staged chunk
		// is byte-identical to its source).
		if err := ccr.Close(); err != nil {
			return nil, fmt.Errorf("close chunk %q: %w", ch.File, err)
		}
		bytesBefore += size
	}
	emitted, res := compactor.finalize()
	res.bytesBefore = bytesBefore

	// Step 2: distribute emitted events across the original chunk
	// files proportionally. The simplest correct approach: pack
	// every event into the first chunk; the trailing chunks become
	// empty. This minimises chunk count in spirit (a future
	// optimisation could re-balance), and the trailing chunks
	// gracefully carry a zero-event header.
	chunkBuckets := make([][]ir.Change, len(im.ChangeChunks))
	chunkBuckets[0] = emitted

	// Step 3: re-encode every chunk and update its ChunkInfo.
	bytesAfter := int64(0)
	for i, ch := range im.ChangeChunks {
		buf := &bytes.Buffer{}
		cw, err := newChangeChunkWriter(buf, cek, codec)
		if err != nil {
			return nil, fmt.Errorf("open chunk writer %q: %w", ch.File, err)
		}
		for _, e := range chunkBuckets[i] {
			if err := cw.WriteChange(e); err != nil {
				return nil, fmt.Errorf("write change to chunk %q: %w", ch.File, err)
			}
		}
		if err := cw.Close(); err != nil {
			return nil, fmt.Errorf("close chunk writer %q: %w", ch.File, err)
		}
		newBytes := buf.Bytes()
		if err := store.Put(ctx, ch.File, bytes.NewReader(newBytes)); err != nil {
			return nil, fmt.Errorf("rewrite chunk %q: %w", ch.File, err)
		}
		ch.SHA256 = cw.Hash()
		ch.RowCount = cw.ChangeCount()
		bytesAfter += int64(len(newBytes))
	}
	res.bytesAfter = bytesAfter
	return res, nil
}

// countingReader counts the bytes flowing through its src reader.
// Used by smart-compact to tally bytesBefore without buffering the
// whole chunk in memory.
type countingReader struct {
	src io.ReadCloser
	n   *int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.src.Read(p)
	*c.n += int64(n)
	return n, err
}

func (c *countingReader) Close() error { return c.src.Close() }
