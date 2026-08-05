// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

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
//   - INSERT then UPDATE(s)   → INSERT with the UNION of the column values
//   - UPDATE(s) only          → one UPDATE with the UNION of the values
//   - INSERT then DELETE      → nothing (the row never existed durably)
//   - UPDATE(s) then DELETE   → just the DELETE
//   - DELETE then INSERT      → both, verbatim (logically distinct rows)
//   - single event            → pass-through unchanged
//
// "UNION", not "the final values", and the distinction is load-bearing
// (audit 2026-08-01 S1). An after-image can be PARTIAL — Postgres omits an
// unchanged out-of-line TOAST column from the new tuple, where an absent key
// means "preserve the target's existing value". Taking the last after-image
// verbatim dropped exactly those columns: on the INSERT arm the column has no
// existing value to preserve and lands NULL/default, and on the UPDATE arm the
// event that wrote the value has been collapsed away, so what the target
// preserves is the value from before it. See [mergeAfterImage].
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
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
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

	// tablesUnmatched tracks tables whose change-event qualifier matched
	// NO table in the manifest schema (or matched ambiguously). This is a
	// DEFECT bucket, not an expected one: every event for such a table
	// passes through uncollapsed, so the compaction silently does nothing.
	// Kept separate from tablesWithoutPK because reporting it as "no
	// primary key" is what hid Bug 223 for three releases.
	tablesUnmatched map[string]struct{}

	// bytesBefore / bytesAfter track the chunk-byte deltas for this
	// incremental's change-chunks. Naive compact has
	// bytesBefore == bytesAfter; smart compact has bytesAfter
	// strictly less (or equal in the no-collapse case).
	bytesBefore int64
	bytesAfter  int64
}

func newSmartCompactResult() *smartCompactResult {
	return &smartCompactResult{
		tablesWithoutPK: make(map[string]struct{}),
		tablesUnmatched: make(map[string]struct{}),
	}
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
	for k := range other.tablesUnmatched {
		r.tablesUnmatched[k] = struct{}{}
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

// tablesUnmatchedList returns the schema.table references whose qualifier
// matched no manifest table, sorted for deterministic reporting.
func (r *smartCompactResult) tablesUnmatchedList() []string {
	out := make([]string, 0, len(r.tablesUnmatched))
	for k := range r.tablesUnmatched {
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
// mergeAfterImage overlays a later after-image on an earlier one and returns a
// NEW row — neither input is mutated, since both are still referenced by the
// un-collapsed events the caller may yet emit.
//
// Later wins for every column the later image CARRIES; a column only the
// earlier image carries is preserved. That is what makes collapsing two events
// into one lossless when either is partial (audit 2026-08-01 S1).
//
// # The premise, named because the whole fix rests on it
//
// This is correct only if an absent column means "unchanged" and never "set to
// NULL" — otherwise the union would resurrect a stale value over a deliberate
// SET NULL. That is the ONE thing the fix depends on, and it is checked rather
// than assumed: postgres.decodeTuple maps pgoutput's 'n' (null) to a PRESENT
// key with a nil value and omits ONLY 'u' (unchanged TOAST), with an
// unrecognised marker a loud error so a future wire addition cannot quietly
// join the omitted set.
//
// Both halves were already pinned SEPARATELY, which is not the same as pinning
// the argument. postgres.TestNullAndUnchangedToastAreDistinguishable now binds
// them, and says in the test why this package depends on it.
// TestMergeAfterImage_AbsentMeansUnchangedNotNull pins this end.
//
// No claim is made here about which engines emit partial images. It does not
// matter: on a complete after-image the union is an identity operation, so the
// merge is correct for a source that always sends full rows and for one that
// does not, without anyone having to maintain a list.
//
// Schema changes cannot smuggle a stale column through: ir.SchemaSnapshot is
// an accumulator barrier that flushes every table (ADR-0064 §6), so no merge
// ever spans a DDL boundary.
func mergeAfterImage(earlier, later ir.Row) ir.Row {
	if earlier == nil {
		return later
	}
	if later == nil {
		return earlier
	}
	merged := make(ir.Row, len(earlier)+len(later))
	for k, v := range earlier {
		merged[k] = v
	}
	for k, v := range later {
		merged[k] = v
	}
	return merged
}

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
				//
				// UNION, not replace (audit 2026-08-01 S1). An
				// after-image can be PARTIAL: Postgres omits an
				// unchanged out-of-line TOAST column from the new
				// tuple, and an absent key means "preserve the
				// target's existing value". An INSERT has no
				// existing value to preserve, so taking ev.After
				// verbatim wrote NULL/default over the column.
				net = ir.Insert{
					Position: prev.Position, // keep INSERT's position
					Schema:   prev.Schema,
					Table:    prev.Table,
					Row:      mergeAfterImage(prev.Row, ev.After),
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
				//
				// After is a UNION for the same reason as the
				// INSERT arm above (audit 2026-08-01 S1): the
				// column an earlier UPDATE wrote and a later one
				// left unchanged is present in the earlier
				// after-image and absent from the later one.
				// Taking the last one verbatim dropped it, and
				// because the earlier UPDATE has been collapsed
				// away there is nothing left to re-apply it — the
				// target keeps its PRE-update value.
				net = ir.Update{
					Position: prev.Position,
					Schema:   prev.Schema,
					Table:    prev.Table,
					Before:   prev.Before,
					After:    mergeAfterImage(prev.After, ev.After),
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

	// tablesUnmatched collects schema.table refs whose qualifier matched no
	// manifest table at all (or matched ambiguously) — the DEFECT half of
	// the old single bucket. See [pkLookupStatus].
	tablesUnmatched map[string]struct{}

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
	pkLookup map[string]pkLookupResult

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
		tablesUnmatched: make(map[string]struct{}),
		pkLookup:        make(map[string]pkLookupResult),
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

// pkLookupStatus is why a (schema, table) lookup did or did not yield a
// PK column list. The three not-found reasons need OPPOSITE operator
// responses, and conflating them is what hid Bug 223 for three releases:
// "this table declares no primary key" is expected and actionable by the
// operator, while "the event's qualifier matched no table in the manifest"
// is a sluice bug that makes the whole collapse silently inert.
type pkLookupStatus int

const (
	pkFound          pkLookupStatus = iota // collapsible
	pkNoneDeclared                         // table found, no usable PK — expected
	pkTableUnmatched                       // no manifest table matched — a defect
	pkAmbiguous                            // several unqualified tables share the name
)

// pkLookupResult is the cached outcome of one (schema, table) lookup.
type pkLookupResult struct {
	cols   []string
	status pkLookupStatus
}

// pkColumns returns the PK column list for a change event's (schema,
// table), and why not when it cannot.
//
// # The qualifier mismatch this has to bridge (Bug 223 / roadmap item 119)
//
// A MySQL change event carries Schema = the DATABASE name, because that is
// what the binlog gives it. The manifest's IR records Schema = "" for a
// single-database MySQL source: the schema reader's namespaceName() returns
// the database name only in multi-database mode. So an exact comparison can
// never succeed on the most common MySQL shape, every table fell through to
// passthrough, and smart compaction was INERT on every MySQL-family source
// while reporting those tables as having "no primary key" — including tables
// with a plain INT PRIMARY KEY.
//
// The fix is deliberately NOT a name-only fallback, which the item filed as
// the hazard: picking the wrong table's PK columns yields a wrong collapse,
// and a wrong collapse is silent data loss on a BACKUP path — strictly worse
// than the inertness. The rule instead reads the recorded schema for what it
// means:
//
//   - An exact (schema, name) match always wins, so a multi-database or
//     Postgres manifest — where the qualifier is real — behaves exactly as
//     before.
//   - Only when NO exact match exists does an UNQUALIFIED manifest table
//     (Schema == "") match by name. An empty recorded schema means the reader
//     had exactly one namespace, so within one manifest such a name is unique
//     by construction.
//   - If more than one unqualified table somehow shares the name, it REFUSES
//     to pick and reports the ambiguity in its own bucket. Belt and braces:
//     the construction argument above says this cannot happen, and the whole
//     point of this item is that an argument like that had already been wrong
//     once.
func (s *smartCompactor) pkColumns(schema, table string) ([]string, pkLookupStatus) {
	tk := tableKey(schema, table)
	if r, ok := s.pkLookup[tk]; ok {
		return r.cols, r.status
	}
	res := s.lookupPKUncached(schema, table)
	s.pkLookup[tk] = res
	return res.cols, res.status
}

func (s *smartCompactor) lookupPKUncached(schema, table string) pkLookupResult {
	if s.schema == nil {
		return pkLookupResult{status: pkTableUnmatched}
	}
	var (
		exact       *ir.Table
		unqualified []*ir.Table
	)
	for _, t := range s.schema.Tables {
		if t == nil || t.Name != table {
			continue
		}
		switch t.Schema {
		case schema:
			exact = t
		case "":
			unqualified = append(unqualified, t)
		}
	}
	match := exact
	if match == nil {
		switch len(unqualified) {
		case 0:
			return pkLookupResult{status: pkTableUnmatched}
		case 1:
			match = unqualified[0]
		default:
			return pkLookupResult{status: pkAmbiguous}
		}
	}
	if match.PrimaryKey == nil || len(match.PrimaryKey.Columns) == 0 {
		return pkLookupResult{status: pkNoneDeclared}
	}
	cols := make([]string, 0, len(match.PrimaryKey.Columns))
	for _, ic := range match.PrimaryKey.Columns {
		if ic.Column == "" {
			// Expression-PK (functional/expression index acting as PK).
			// Not collapsible: we can't compute the expression's value
			// from the row map. Passthrough, and it is genuinely a
			// "no usable PK" case rather than a lookup failure.
			return pkLookupResult{status: pkNoneDeclared}
		}
		cols = append(cols, ic.Column)
	}
	return pkLookupResult{cols: cols, status: pkFound}
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
	cols, status := s.pkColumns(schema, table)
	if status != pkFound {
		// Not collapsible: pass through verbatim, and record the table
		// in the bucket that says WHY. The two buckets need opposite
		// operator responses — see [pkLookupStatus].
		key := tableKey(schema, table)
		if status == pkNoneDeclared {
			s.tablesWithoutPK[key] = struct{}{}
		} else {
			s.tablesUnmatched[key] = struct{}{}
		}
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

// flushCollapsedInsideClosingTx drains the accumulators into the output
// stream, placing the collapsed row events INSIDE the incremental's last
// transaction rather than after its closing TxCommit.
//
// Roadmap item 101, and the reason [finalize]'s own doc comment already
// said finalize "does NOT emit accumulator events at the very end if a
// TxCommit has already been emitted" — the code did exactly that. A bare
// [flushAll] appends at the very end, so the rewritten stream's last
// position-bearing event became a collapsed row event, and a collapsed
// event deliberately carries its chain's FIRST position (see
// [rowAccumulator.flush]). Any incremental that touched one row in more
// than one transaction therefore ENDED BELOW its own recorded
// EndPosition — which chain_restore.go's F1 tail-truncation backstop
// reads as a truncated change-list and refuses (SLUICE-E-BACKUP-INCOMPLETE).
// A `backup compact --smart-compaction` could thus leave a chain that no
// longer restores, on the most ordinary shape smart compaction exists for.
//
// Holding the closing TxCommit back costs nothing else: the collapsed
// rows land inside the last transaction (a tighter envelope than before,
// not a looser one), and the closing position is once again the stream's
// last event.
//
// A stream with no transactional envelope — an engine whose CDC reader
// emits no TxBegin/TxCommit — has no closing commit to hold back and
// keeps the previous shape.
func (s *smartCompactor) flushCollapsedInsideClosingTx() {
	last := len(s.out) - 1
	if last < 0 {
		s.flushAll()
		return
	}
	if _, ok := s.out[last].(ir.TxCommit); !ok {
		s.flushAll()
		return
	}
	closing := s.out[last]
	s.out = s.out[:last]
	s.flushAll()
	s.out = append(s.out, closing)
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
	//   (b) F3 (the rewritten stream must not END below the
	//       incremental's closing position) is preserved by holding
	//       the closing TxCommit back until after the flush — see
	//       [smartCompactor.flushCollapsedInsideClosingTx]. It is NOT
	//       preserved by the policy table: a collapsed event
	//       deliberately carries its chain's FIRST position (see
	//       [rowAccumulator.flush]), not its last. An earlier version
	//       of this comment claimed the opposite and the mismatch was
	//       load-bearing — roadmap item 101.
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
	s.flushCollapsedInsideClosingTx()

	res := newSmartCompactResult()
	res.eventsBefore = s.eventsBefore
	for k := range s.tablesWithoutPK {
		res.tablesWithoutPK[k] = struct{}{}
	}
	for k := range s.tablesUnmatched {
		res.tablesUnmatched[k] = struct{}{}
	}
	// A qualifier that matched no manifest table means every event for
	// that table passed through uncollapsed — the compaction ran and did
	// nothing. That is a defect, not a table property, so it is said out
	// loud rather than folded into a report line the operator has to go
	// looking for. Bug 223 survived three releases precisely because its
	// only symptom was a quiet "no primary key" line about tables that
	// had one.
	if len(s.tablesUnmatched) > 0 {
		slog.Warn(
			"smart compaction: change events name tables that are not in the backup's recorded schema, "+
				"so their rows could not be collapsed and passed through verbatim; the compaction did "+
				"nothing for them",
			slog.Any("tables", res.tablesUnmatchedList()),
		)
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
	codec blobcodec.Codec,
	cek []byte,
	pkStrategy PKStrategy,
) (*smartCompactResult, error) {
	groupRes := newSmartCompactResult()
	for _, incrPath := range pg.finalIncrementalPaths {
		im, err := lineage.ReadManifestAt(ctx, stagingStore, incrPath)
		if err != nil {
			return nil, fmt.Errorf("smart-compact: read staged incremental %q: %w", incrPath, err)
		}
		incrRes, err := applySmartCompactionToIncremental(ctx, stagingStore, im, codec, cek, pkStrategy)
		if err != nil {
			return nil, fmt.Errorf("smart-compact: incremental %q: %w", incrPath, err)
		}
		if err := lineage.WriteManifestAt(ctx, stagingStore, incrPath, im); err != nil {
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
	codec blobcodec.Codec,
	cek []byte,
	pkStrategy PKStrategy,
) (*smartCompactResult, error) {
	if len(im.ChangeChunks) == 0 {
		return newSmartCompactResult(), nil
	}

	// Step 1: decode every chunk in order, feed the compactor.
	compactor := newSmartCompactor(pkStrategy, im.Schema)
	bytesBefore := int64(0)
	for chIdx, ch := range im.ChangeChunks {
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
		// ADR-0152: the chunk's position binding is derived from the
		// OWNING manifest's recorded FormatVersion + identity — nil for
		// pre-v5 chains — and the rewrite below re-binds under the same
		// (path-stable, identity-stable) manifest.
		ccr, err := blobcodec.NewChangeChunkReader(counter, "", cek, codec, irbackup.ChangeChunkAADFor(im, ch, chIdx))
		if err != nil {
			_ = rc.Close()
			// Decrypt-at-open: a tampered/spliced encrypted change chunk met
			// during compaction fails its GCM tag here → coded refusal, the
			// same SLUICE-E-BACKUP-CHUNK-AUTH-FAILED the restore paths emit
			// (parity across restore/replay/compact; SEC-1).
			return nil, lineage.CodeChunkAuthError(fmt.Errorf("open chunk %q: %w", ch.File, err))
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
		cw, err := blobcodec.NewChangeChunkWriter(buf, cek, codec, irbackup.ChangeChunkAADFor(im, ch, i))
		if err != nil {
			return nil, fmt.Errorf("open chunk writer %q: %w", ch.File, err)
		}
		for _, e := range chunkBuckets[i] {
			if err := cw.WriteChange(e); err != nil {
				if errors.Is(err, blobcodec.ErrChunkLineTooLong) {
					// Collapsing merges after-images as a UNION of their
					// columns (see [mergeAfterImage]), so a collapsed event
					// can serialize larger than any of the events it replaced
					// — a chain whose events the source backup wrote happily.
					// Refusing is right (the rewritten chunk would not restore),
					// but the remedy is specific to this path and the codec
					// cannot know it.
					return nil, fmt.Errorf("write collapsed change to chunk %q: %w\n\n"+
						"This row's events were each individually writable; collapsing them merged "+
						"their column values into one larger row. Re-run with --smart-compaction-off "+
						"to keep the naive concatenation, which leaves the events uncollapsed and "+
						"within the limit", ch.File, err)
				}
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
