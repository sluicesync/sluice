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

	// peakChunkBufferBytes is the high-water mark of the EMITTED bytes held
	// in memory for a not-yet-written output chunk (roadmap item 130). It is
	// the number [chunkStreamSink] bounds, and it is reported rather than
	// merely asserted in a test because the bound degrades on a pre-ceiling
	// chain — see [chunkStreamSink] for when and why.
	peakChunkBufferBytes int64

	// chainsEvicted counts row chains the retention cap drained early
	// (roadmap item 127). Non-zero means the collapse was PARTIAL: the
	// output is correct, but some chains that could have collapsed did
	// not. Reported rather than left to be inferred from the ratio,
	// because "compacted less well than it could have" and "had nothing
	// to compact" are the same number otherwise.
	chainsEvicted int64
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
	r.chainsEvicted += other.chainsEvicted
	// A peak is a MAXIMUM, not a total: two incrementals compacted one after
	// the other never hold their buffers at the same time.
	if other.peakChunkBufferBytes > r.peakChunkBufferBytes {
		r.peakChunkBufferBytes = other.peakChunkBufferBytes
	}
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

	// bytes is the running [estimateChangeBytes] total for events. Kept
	// incrementally so the retention cap never has to re-walk the chain.
	bytes int64
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

// changeSink receives the compactor's emitted events, in order, as they are
// produced. It is the seam roadmap item 130 needed: the compactor used to
// append every emitted event to a slice it held for the whole incremental, so
// the pass peaked at O(retention cap + OUTPUT) — and for an incremental with no
// collapse opportunity output ≈ input, which is the case item 127's cap did
// least for.
//
// Two implementations, and the difference between them is the whole point:
// [sliceSink] collects (what every unit test wants, and what the code used to
// do unconditionally), while [chunkStreamSink] writes straight into the
// incremental's chunk slots so nothing is retained beyond the chunk being
// filled.
type changeSink interface {
	emit(ir.Change) error
}

// sliceSink collects the emitted stream in memory. This is the pre-item-130
// behaviour, kept for the unit tests that assert on the whole output at once —
// a test corpus is small by construction, and asserting on a slice is far
// clearer than asserting on re-decoded chunk bytes.
type sliceSink struct {
	out []ir.Change
}

func (s *sliceSink) emit(c ir.Change) error {
	s.out = append(s.out, c)
	return nil
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

	// sink receives the emitted stream in order. It used to be an
	// `out []ir.Change` that grew for the whole incremental — the OTHER
	// unbounded structure item 127 named and did not fix (roadmap item 130).
	// Never nil: [newSmartCompactor] installs a [sliceSink].
	sink changeSink

	// heldCommit is the one-slot lookahead that replaces the old
	// "splice before the trailing TxCommit" slice surgery. See
	// [smartCompactor.pushCollapsed] for why the compactor holds a
	// trailing TxCommit back rather than emitting it eagerly.
	heldCommit ir.Change

	// eventsBefore / eventsAfter count INSERT/UPDATE/DELETE/TRUNCATE
	// only (the events subject to collapse), matching the
	// [smartCompactResult] semantics. eventsAfter is tallied as events
	// reach the sink, because there is no longer an output slice to walk.
	eventsBefore int64
	eventsAfter  int64

	// pkLookup caches the PK column list per (schema, table) so we
	// don't repeatedly walk the IR schema.
	pkLookup map[string]pkLookupResult

	// rowsCollapsedCount is incremented every time a row
	// accumulator's events slice grows from len 1 → len 2 — i.e.
	// the first time a row chain becomes a collapse candidate.
	// Subsequent appends on the same accumulator don't re-trigger.
	// Used by [finalize] to populate the report's RowsCollapsed.
	rowsCollapsedCount int64

	// maxRetainedBytes bounds what [accumulators] may hold before
	// [maybeEvict] starts draining chains early. See
	// [defaultMaxRetainedBytes] for what the number means and does not
	// mean. Zero disables the cap (tests that assert the pre-cap shape).
	maxRetainedBytes int64

	// retainedBytes is the current [estimateChangeBytes] total across
	// every live accumulator; peakRetainedBytes is its high-water mark,
	// which is what the retention gate asserts on.
	retainedBytes     int64
	peakRetainedBytes int64

	// chainsEvicted counts chains drained early by the cap. Reported so
	// an operator can tell a partially-collapsed incremental from a
	// fully-collapsed one rather than inferring it from the ratio.
	chainsEvicted int64
}

// defaultMaxRetainedBytes bounds the ESTIMATED SERIALIZED size of the event
// chains smart compaction holds in memory at once (roadmap item 127).
//
// # What was unbounded, and what this does and does not bound
//
// The compactor retained every input event until a flush barrier — and the
// only barriers are TRUNCATE, SchemaSnapshot and end-of-incremental, so an
// incremental with none of those retained ALL of them. Measured by the
// 2026-08-04 audit: 100k events → 261 MiB, and a 100:1-collapsing corpus →
// 248 MiB, i.e. NO relief from collapsing, because the inputs are held until
// flush however well they collapse. ~2.6 GiB extrapolated at 1M events.
//
// This cap bounds the retained-INPUT term. The emitted-OUTPUT term — which
// used to be an `out []ir.Change` holding every emitted event for the whole
// incremental, so the peak was O(cap + output) — is bounded separately by
// [chunkStreamSink] (roadmap item 130). Read that type for what the output
// bound actually is, since it degrades on a chain written before the per-chunk
// byte ceiling existed.
//
// # The number is in estimated WIRE bytes, not Go heap bytes
//
// [estimateChangeBytes] approximates the serialized size of an event. Live Go
// heap for the same events is a MULTIPLE of that — maps, interface boxes and
// string headers dominate a small event, and the audit's 261 MiB for 100k
// events implies roughly 2.6 KiB of heap apiece for events whose wire form is
// far smaller. So 32 MiB of retained wire bytes is a low-hundreds-of-MiB heap
// ceiling, not a 32 MiB one. Stated because a reader who takes this number for
// a heap budget would be wrong by an order of magnitude.
const defaultMaxRetainedBytes int64 = 32 << 20

// estimateChangeBytes approximates one event's serialized size. It is an
// ESTIMATE by design — marshalling every event just to size it would cost more
// than the cap saves — but it is not a guess: TestEstimateChangeBytes_TracksReal
// MarshalledSize pins it against real json.Marshal output across the value
// families this format carries, so the cap cannot silently drift into meaning
// nothing.
func estimateChangeBytes(c ir.Change) int64 {
	const envelope = 64 // kind tag, schema/table keys, position, punctuation
	switch ev := c.(type) {
	case ir.Insert:
		return envelope + int64(len(ev.Schema)+len(ev.Table)) + estimateRowBytes(ev.Row)
	case ir.Update:
		return envelope + int64(len(ev.Schema)+len(ev.Table)) + estimateRowBytes(ev.Before) + estimateRowBytes(ev.After)
	case ir.Delete:
		return envelope + int64(len(ev.Schema)+len(ev.Table)) + estimateRowBytes(ev.Before)
	default:
		return envelope
	}
}

// estimateRowBytes approximates a row map's serialized size: each key plus its
// value's own width, with a flat allowance for the fixed-width scalars whose
// rendering does not vary much.
func estimateRowBytes(r ir.Row) int64 {
	var n int64
	for k, v := range r {
		n += int64(len(k)) + 4 // key, quotes, colon, comma
		switch tv := v.(type) {
		case string:
			n += int64(len(tv)) + 2
		case []byte:
			// Rendered through an encodeValue envelope, roughly base64.
			n += int64(len(tv))*4/3 + 16
		case nil:
			n += 4
		default:
			// int64 / float64 / bool / time.Time and friends.
			n += 24
		}
	}
	return n
}

func newSmartCompactor(pkStrategy PKStrategy, schema *ir.Schema) *smartCompactor {
	return &smartCompactor{
		pkStrategy:       resolvePKStrategy(pkStrategy),
		schema:           schema,
		accumulators:     make(map[string]*rowAccumulator),
		order:            nil,
		tablesWithoutPK:  make(map[string]struct{}),
		tablesUnmatched:  make(map[string]struct{}),
		pkLookup:         make(map[string]pkLookupResult),
		maxRetainedBytes: defaultMaxRetainedBytes,
		sink:             &sliceSink{},
	}
}

// push hands one event to the sink as an ORDINARY emission — a pass-through
// event that carries its own real position (TxBegin / TxCommit /
// SchemaSnapshot / TRUNCATE / a row event for a table that cannot collapse).
//
// It releases any held closing commit first, so an ordinary event that follows
// a TxCommit still follows it in the output.
func (s *smartCompactor) push(e ir.Change) error {
	if err := s.releaseHeldCommit(); err != nil {
		return err
	}
	if _, closing := e.(ir.TxCommit); closing {
		// Hold it back: see [smartCompactor.pushCollapsed].
		s.heldCommit = e
		return nil
	}
	return s.deliver(e)
}

// pushCollapsed hands a flushed chain's events to the sink WITHOUT releasing a
// held closing commit — so collapsed events land before it.
//
// # Why the compactor holds a trailing TxCommit back (roadmap item 101, third time)
//
// A collapsed event carries its chain's FIRST position (see
// [rowAccumulator.flush]), so emitting one after the incremental's closing
// TxCommit leaves the rewritten stream ENDING BELOW its own recorded
// EndPosition — which chain_restore.go's F1 tail-truncation backstop reads as a
// truncated change-list and refuses (SLUICE-E-BACKUP-INCOMPLETE). A
// `backup compact` could thus leave a chain that no longer restores.
//
// The invariant is "the closing position is last at EVERY point in the pass",
// not "at finalize". That distinction has now been learned twice: the original
// [flushCollapsedInsideClosingTx] enforced it only at finalize and item 127's
// eviction path got out from under it by appending to the same slice, which is
// why `evict` grew its own splice. With the output streaming there is no slice
// to splice, so the one-slot lookahead here IS the enforcement, for every
// flusher at once — eviction, the PK-change barrier, the DDL barrier and
// finalize. That is a deliberate unification, not an accident: the PK-change
// barrier and the DDL barrier previously appended collapsed events AFTER a
// trailing commit, which was the same F3 hole `evict` had.
func (s *smartCompactor) pushCollapsed(events []ir.Change) error {
	for _, e := range events {
		if err := s.deliver(e); err != nil {
			return err
		}
	}
	return nil
}

// releaseHeldCommit emits the held closing commit, if there is one.
func (s *smartCompactor) releaseHeldCommit() error {
	if s.heldCommit == nil {
		return nil
	}
	e := s.heldCommit
	s.heldCommit = nil
	return s.deliver(e)
}

// deliver is the single door to the sink; every emitted event passes through it
// so the eventsAfter tally cannot drift from what was actually written.
func (s *smartCompactor) deliver(e ir.Change) error {
	if isPerRowEvent(e) {
		s.eventsAfter++
	}
	return s.sink.emit(e)
}

// tableKey is the schema.table identifier an OPERATOR reads — the
// tables-without-PK and tables-unmatched report buckets. It is deliberately
// human-shaped and deliberately NOT used for routing: see [tableRoutingKey].
func tableKey(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// tableRoutingKey is the INJECTIVE identifier used for accumulator routing and
// the pkLookup cache — the PG-side twin of the [pkValueKey] collision (audit
// 2026-08-05 B-5).
//
// `schema + "." + table` is ambiguous the moment an identifier contains a dot,
// which Postgres and MySQL both permit in a quoted name: schema `a.b` table `c`
// and schema `a` table `b.c` render identically. That collapses two tables into
// one accumulator namespace AND poisons the pkLookup cache, so the second table
// silently inherits the first's primary key — a wrong key means a wrong
// collapse, which on a backup path is silent loss.
//
// Kept separate from [tableKey] rather than length-prefixing that one, because
// the display form is read by operators in the compaction report and a
// length-prefixed rendering there would be gibberish.
func tableRoutingKey(schema, table string) string {
	var b strings.Builder
	writeLengthPrefixed(&b, schema)
	writeLengthPrefixed(&b, table)
	return b.String()
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
	tk := tableRoutingKey(schema, table)
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
// # Injectivity is the whole contract (audit 2026-08-05 B-5)
//
// This used to render each value with %v and join with \x00, which is NOT
// injective: ("a\x00","b") and ("a","\x00b") produced the same key, and so did
// (int64(1),"2") and ("1",int64(2)). Two DIFFERENT rows then shared one
// accumulator, so an INSERT of one and a DELETE of the other netted to nothing
// and the compacted backup silently lost a row. The NUL case needs a
// NUL-significant collation (utf8mb4_bin) to be reachable; the cross-type case
// needs only a column whose decode varies.
//
// Now each component is LENGTH-PREFIXED and TYPE-TAGGED, which makes the
// encoding unambiguously parseable and therefore injective.
//
// The type tag errs in the SAFE direction deliberately. If one column's value
// ever decodes as []byte in one event and string in another, tagging SPLITS
// that row's chain in two — less collapse, both halves emitted in order, output
// still correct. Merging two distinct rows is the direction that loses data,
// and that is the one this makes impossible.
func pkValueKey(cols []string, row ir.Row, schema, table string) (string, error) {
	var b strings.Builder
	for _, c := range cols {
		v, ok := row[c]
		if !ok {
			return "", fmt.Errorf("smart compact: PK column %q missing from row payload for %s.%s; corrupt or mis-decoded event — re-run with --smart-compaction-off to fall through to naive concat",
				c, schema, table)
		}
		writeLengthPrefixed(&b, fmt.Sprintf("%T\x1f%v", v, v))
	}
	return b.String(), nil
}

// writeLengthPrefixed appends s to b as `<len>\x1e<s>`. The length is decimal
// and \x1e cannot appear in a decimal, so the boundary is unambiguous however
// the payload is spelled — which is what makes a concatenation of these
// injective.
func writeLengthPrefixed(b *strings.Builder, s string) {
	fmt.Fprintf(b, "%d\x1e%s", len(s), s)
}

// process feeds one event into the compactor. Returns an error only
// on the refuse-loudly path (corrupt PK).
func (s *smartCompactor) process(e ir.Change) error {
	if s.pkStrategy == PKStrategyNone {
		// Pass-through mode: collapse disabled, every event verbatim.
		if isPerRowEvent(e) {
			s.eventsBefore++
		}
		return s.push(e)
	}

	switch ev := e.(type) {
	case ir.TxBegin, ir.TxCommit:
		// Transaction boundaries pass through verbatim. They're
		// load-bearing for the F3 invariant — the TxCommit's
		// position closes the chunk's LSN window.
		return s.push(e)
	case ir.SchemaSnapshot:
		// DDL barrier (ADR-0064 §6): flush every accumulator
		// across every table, emit the SchemaSnapshot, reset.
		if err := s.flushAll(); err != nil {
			return err
		}
		return s.push(e)
	case ir.Truncate:
		// Table-scoped barrier: drop every accumulator for this
		// table, emit the TRUNCATE verbatim, leave other tables'
		// accumulators alone.
		s.flushTable(ev.Schema, ev.Table)
		s.eventsBefore++
		return s.push(e)
	case ir.Insert:
		s.eventsBefore++
		return s.routeRowEvent(ev.Schema, ev.Table, ev.Row, e)
	case ir.Update:
		s.eventsBefore++
		// A PK-CHANGING update is a two-key barrier, not a routable row
		// event (audit 2026-08-05 A-3). See [smartCompactor.pkChangeBarrier]
		// for the defect this closes and why the previous comment here —
		// "key-only changes are represented as DELETE+INSERT in MySQL and as
		// a separate event in PG" — was false of sluice's own readers.
		if handled, err := s.pkChangeBarrier(ev, e); handled || err != nil {
			return err
		}
		// Otherwise the After image identifies the row: the PK is part of
		// every UPDATE's after-image even when unchanged.
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
		return s.push(e)
	}
}

// pkChangeBarrier handles an UPDATE that MOVES a row from one primary key to
// another. It reports whether it consumed the event.
//
// # The defect (audit 2026-08-05 A-3): a compacted backup that resurrects a
// deleted row
//
// The Update arm used to route on the After image alone, justified by a comment
// asserting that "key-only changes are represented as DELETE+INSERT in MySQL
// and as a separate event in PG". Both halves are contradicted by sluice's own
// readers: the MySQL CDC reader emits ONE ir.Update whose narrowed Before
// carries the OLD key and whose After carries the new one (`cdc_reader.go`
// says exactly that), Postgres emits one UpdateMessage carrying an OldTuple,
// and [laneapply.PKChangedUpdate] exists precisely because the appliers
// receive them.
//
// So on INSERT(1) / UPDATE(1→2) / DELETE(2), the INSERT bucketed under key 1
// while the UPDATE and DELETE both bucketed under key 2. Key 2's chain
// collapsed to the DELETE; key 1's chain still emitted its INSERT. The
// compacted stream was INSERT(1) + DELETE(2) — replaying it leaves row 1 ALIVE
// where the source has no rows. `backup verify` could not see it: compaction
// re-stamps the very hashes verify checks.
//
// # Why the barrier is scoped to the two KEYS, not the whole table
//
// The audit proposed the Truncate pattern (flush the whole table). Two keys is
// sufficient and strictly less disruptive: the only identities this event
// disturbs are the one it vacates and the one it occupies. Flushing the table
// would stop every unrelated row in it from collapsing whenever one row is
// re-keyed, which is a real cost on the workload smart compaction exists for
// — pinned by TestSmartCompact_PKChangeUpdate_UnrelatedChainsStillCollapse.
//
// Both keys are flushed EMITTING, not dropped: the vacated key's pending chain
// is real history that must still ship. That is the difference from
// [smartCompactor.flushTable], where a TRUNCATE genuinely makes pending chains
// irrelevant.
//
// # The named premise
//
// A nil Before means this cannot be detected, and such an event routes on After
// exactly as before. That is safe for the engines sluice supports because a nil
// Before means the key did NOT change: MySQL's binlog always carries a before
// image, and Postgres emits an OldTuple precisely WHEN the key changes. Stated
// rather than assumed — if an engine is ever added whose reader omits the
// before image on a key change, this barrier goes blind and A-3 returns.
func (s *smartCompactor) pkChangeBarrier(ev ir.Update, e ir.Change) (bool, error) {
	if ev.Before == nil || ev.After == nil {
		return false, nil
	}
	cols, status := s.pkColumns(ev.Schema, ev.Table)
	if status != pkFound {
		return false, nil // not collapsible anyway; routeRowEvent reports why
	}
	oldKey, err := pkValueKey(cols, ev.Before, ev.Schema, ev.Table)
	if err != nil {
		// A before-image missing a PK column cannot be compared. Fall through
		// to the normal path rather than refusing: the Before may legitimately
		// be narrowed, and routeRowEvent keys off After.
		return false, nil //nolint:nilerr // deliberate: see comment above
	}
	newKey, err := pkValueKey(cols, ev.After, ev.Schema, ev.Table)
	if err != nil {
		return false, err
	}
	if oldKey == newKey {
		return false, nil // an ordinary in-place update
	}

	tk := tableRoutingKey(ev.Schema, ev.Table)
	if err := s.flushKeyEmitting(tk + "\x01" + oldKey); err != nil {
		return true, err
	}
	if err := s.flushKeyEmitting(tk + "\x01" + newKey); err != nil {
		return true, err
	}
	return true, s.push(e)
}

// flushKeyEmitting drains one accumulator into the output stream and drops it.
// Unlike [smartCompactor.flushTable] the events are EMITTED, because the chain
// is real history rather than state a TRUNCATE made irrelevant.
func (s *smartCompactor) flushKeyEmitting(mapKey string) error {
	acc, ok := s.accumulators[mapKey]
	if !ok {
		return nil
	}
	if err := s.pushCollapsed(acc.flush()); err != nil {
		return err
	}
	s.retainedBytes -= acc.bytes
	delete(s.accumulators, mapKey)
	for i, k := range s.order {
		if k == mapKey {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return nil
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
		return s.push(e)
	}
	if row == nil {
		// Engine couldn't supply a payload to identify the row.
		// Pass through verbatim — refusing would block the merge
		// for a CDC reader edge case the upstream apply path
		// already handles.
		return s.push(e)
	}
	key, err := pkValueKey(cols, row, schema, table)
	if err != nil {
		return err
	}
	// Make room BEFORE retaining the event, so the ceiling is one the
	// compactor actually stays under rather than one it overshoots by an
	// event and then recovers from. This also has to run before the
	// accumulator lookup: eviction can drop the very chain this event belongs
	// to, and a pointer taken first would be stale.
	sz := estimateChangeBytes(e)
	if err := s.reserve(sz); err != nil {
		return err
	}

	tk := tableRoutingKey(schema, table)
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
	acc.bytes += sz
	s.retainedBytes += sz
	if s.retainedBytes > s.peakRetainedBytes {
		s.peakRetainedBytes = s.retainedBytes
	}
	if len(acc.events) == 2 {
		s.rowsCollapsedCount++
	}
	return nil
}

// reserve makes room for an incoming event, draining chains early when
// retaining it would push [smartCompactor.retainedBytes] past the cap — so an
// incremental with no TRUNCATE and no DDL cannot retain itself entirely
// (roadmap item 127).
//
// # What is sacrificed, and the order that minimises it
//
// A cap on a collapsing transform has to give something up: the compactor
// cannot flush a chain mid-way without forgoing the collapse it exists to
// perform, and flushing early changes WHICH events collapse — an observable
// output change, not merely a memory one. So the eviction order is chosen to
// spend the cheapest chains first:
//
//  1. SINGLE-EVENT chains, in insertion order. These cost NOTHING: a
//     one-event accumulator's flush emits that event verbatim, which is
//     exactly what it would have emitted at finalize, and insertion order is
//     the order flushAll would have used. This matters more than it looks —
//     an incremental with no collapse opportunity is ALL single-event chains,
//     so the workload with the worst retention is also the one where the cap
//     changes nothing observable.
//  2. Then the LARGEST remaining chains. A big chain is where the memory
//     actually is, and evicting one loses the collapse of one row rather than
//     of many.
//
// Correctness under eviction rests on one property: a chain evicted at time T
// is emitted at T, and any later event for the same PK starts a fresh
// accumulator that is emitted after it. Per-PK source order therefore holds,
// which is what ADR-0064 §2's policy table is defined over. The events simply
// collapse in two groups instead of one — always a superset of the events full
// collapse would emit, never a different end state.
//
// Draining to a LOW-WATER mark rather than to exactly the cap is what keeps
// this from degenerating: without hysteresis a corpus that sits at the cap
// would re-sort its accumulator set on every subsequent event.
//
// The one case the ceiling cannot hold is a SINGLE event whose own estimated
// size exceeds the cap: there is nothing left to evict, so it is retained
// anyway. Peak is therefore bounded by max(cap, largest single event), and the
// chunk format's per-row limit ([blobcodec.MaxChunkLineBytes], 64 MiB) is what
// bounds that second term.
func (s *smartCompactor) reserve(incoming int64) error {
	if s.maxRetainedBytes <= 0 || s.retainedBytes+incoming <= s.maxRetainedBytes {
		return nil
	}
	lowWater := s.maxRetainedBytes / 2
	fits := func() bool { return s.retainedBytes+incoming <= lowWater }

	// Pass 1: the free ones, oldest first.
	survivors := s.order[:0]
	for _, k := range s.order {
		acc := s.accumulators[k]
		if !fits() && len(acc.events) == 1 {
			if err := s.evict(k, acc); err != nil {
				return err
			}
			continue
		}
		survivors = append(survivors, k)
	}
	s.order = survivors
	if fits() {
		return nil
	}

	// Pass 2: the largest of what is left. Sorting a copy keeps s.order's
	// insertion ordering intact for the survivors, which flushAll relies on.
	bySize := make([]string, len(s.order))
	copy(bySize, s.order)
	sort.SliceStable(bySize, func(i, j int) bool {
		return s.accumulators[bySize[i]].bytes > s.accumulators[bySize[j]].bytes
	})
	evicted := make(map[string]struct{}, len(bySize))
	for _, k := range bySize {
		if fits() {
			break
		}
		if err := s.evict(k, s.accumulators[k]); err != nil {
			return err
		}
		evicted[k] = struct{}{}
	}
	if len(evicted) == 0 {
		return nil
	}
	survivors = s.order[:0]
	for _, k := range s.order {
		if _, gone := evicted[k]; !gone {
			survivors = append(survivors, k)
		}
	}
	s.order = survivors
	return nil
}

// evict drains one chain into the output stream and drops it. The caller is
// responsible for removing k from s.order.
//
// The F3 position invariant this used to enforce with its own slice splice now
// lives in [smartCompactor.pushCollapsed]'s one-slot lookahead, which covers
// every flusher rather than this one — read that comment for why the invariant
// is "the closing position is last at EVERY point in the pass".
func (s *smartCompactor) evict(k string, acc *rowAccumulator) error {
	if err := s.pushCollapsed(acc.flush()); err != nil {
		return err
	}
	s.retainedBytes -= acc.bytes
	delete(s.accumulators, k)
	s.chainsEvicted++
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
	// Must be the ROUTING key, matching what routeRowEvent built the accumulator
	// map keys from — a prefix scan against the display form would silently
	// match the wrong table's chains (or none).
	tk := tableRoutingKey(schema, table)
	prefix := tk + "\x01"
	newOrder := s.order[:0]
	for _, k := range s.order {
		if strings.HasPrefix(k, prefix) {
			s.retainedBytes -= s.accumulators[k].bytes
			delete(s.accumulators, k)
			continue
		}
		newOrder = append(newOrder, k)
	}
	s.order = newOrder
}

// flushAll flushes every accumulator into the sink in insertion-order,
// then resets accumulators to empty. Used at the end of an
// incremental and at SchemaSnapshot barriers.
//
// Collapsed events go out through [smartCompactor.pushCollapsed], so a held
// closing TxCommit stays held and lands after them — which is what used to be
// spelled out as `flushCollapsedInsideClosingTx`'s pop-flush-reappend dance.
func (s *smartCompactor) flushAll() error {
	for _, k := range s.order {
		acc := s.accumulators[k]
		if err := s.pushCollapsed(acc.flush()); err != nil {
			return err
		}
	}
	s.accumulators = make(map[string]*rowAccumulator)
	s.order = nil
	s.retainedBytes = 0
	return nil
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
func (s *smartCompactor) finalize() (*smartCompactResult, error) {
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
	//       [smartCompactor.pushCollapsed]. It is NOT
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
	if err := s.flushAll(); err != nil {
		return nil, err
	}
	// The last thing out of the compactor is the closing commit the
	// one-slot lookahead has been holding — see [smartCompactor.pushCollapsed].
	if err := s.releaseHeldCommit(); err != nil {
		return nil, err
	}

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
	// eventsAfter is tallied at [smartCompactor.deliver] rather than by walking
	// an output slice, because there is no longer an output slice to walk.
	res.eventsAfter = s.eventsAfter
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
	res.chainsEvicted = s.chainsEvicted
	if s.chainsEvicted > 0 {
		// Said out loud for the same reason the tablesUnmatched warning
		// above exists: an operator comparing two compactions of similar
		// chains deserves to know one of them ran into its memory ceiling
		// rather than being left to read it out of a ratio.
		slog.Info(
			"smart compaction: the retained-event ceiling was reached, so some row chains were "+
				"written out before they could finish collapsing; the output is correct but the "+
				"compaction is partial",
			slog.Int64("chains_evicted", s.chainsEvicted),
			slog.Int64("peak_retained_bytes", s.peakRetainedBytes),
			slog.Int64("max_retained_bytes", s.maxRetainedBytes),
		)
	}
	return res, nil
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
// then split back across the original chunk files under a byte budget.
// This preserves the chunk-count (operators inspecting the manifest see
// the same shape) while achieving cross-chunk collapse within the
// incremental.
//
// When the collapsed stream needs fewer chunks than the incremental
// had, the trailing chunks become empty. We KEEP the empty
// chunks (with their header only, no row events) so the manifest's
// ChangeChunks list stays aligned with the on-disk chunks. The
// chunk format already handles a zero-event chunk gracefully (a
// header + EOF; the reader treats it as an empty stream).
//
// # Read and write interleave, and that is the constraint (roadmap item 130)
//
// The pass READS every staged chunk and WRITES back over the same paths, so
// streaming output during the read phase risks overwriting a staged input chunk
// that has not been read yet. [chunkStreamSink] resolves that with a
// write-behind cursor rather than by moving the output somewhere else; the two
// escape routes that were rejected, and why, are recorded there.
func applySmartCompactionToIncremental(
	ctx context.Context,
	store irbackup.Store,
	im *irbackup.Manifest,
	codec blobcodec.Codec,
	cek []byte,
	pkStrategy PKStrategy,
) (*smartCompactResult, error) {
	return applySmartCompactionToIncrementalSized(ctx, store, im, codec, cek, pkStrategy, 0)
}

// smartCompactUnboundedChunkBudget disables [chunkStreamSink]'s roll entirely,
// so every emitted event is packed into the first chunk — the pre-item-130
// shape, and the ONLY thing that produces it.
//
// It exists as the anti-vacuity control for the memory gate: a peak-under-the-
// budget assertion proves nothing unless the same corpus, unbudgeted, blows
// past it. Nothing in the tree passes it outside those tests, and it is
// deliberately a NEGATIVE sentinel rather than zero — zero means "the default"
// (the v0.99.51 zero-value trap: a field whose zero value turns the protection
// off silently opts out every future caller).
const smartCompactUnboundedChunkBudget int64 = -1

// applySmartCompactionToIncrementalSized is [applySmartCompactionToIncremental]
// with the per-chunk output budget as a parameter. chunkBudget of 0 means
// [DefaultBackupChunkBytes]; see [smartCompactUnboundedChunkBudget] for the
// only other value anything passes.
func applySmartCompactionToIncrementalSized(
	ctx context.Context,
	store irbackup.Store,
	im *irbackup.Manifest,
	codec blobcodec.Codec,
	cek []byte,
	pkStrategy PKStrategy,
	chunkBudget int64,
) (*smartCompactResult, error) {
	if len(im.ChangeChunks) == 0 {
		return newSmartCompactResult(), nil
	}
	if chunkBudget == 0 {
		chunkBudget = DefaultBackupChunkBytes
	}

	// Step 1: decode every chunk in order, feeding the compactor — which now
	// streams its output straight back into the chunk slots as it goes,
	// instead of accumulating the whole incremental's emitted stream.
	sink := &chunkStreamSink{
		ctx:    ctx,
		store:  store,
		im:     im,
		codec:  codec,
		cek:    cek,
		budget: chunkBudget,
	}
	compactor := newSmartCompactor(pkStrategy, im.Schema)
	compactor.sink = sink
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
		// This chunk's staged bytes are no longer being read, so its slot
		// becomes writable — the write-behind cursor advancing by one.
		if err := sink.inputChunkRead(chIdx + 1); err != nil {
			return nil, err
		}
	}

	// Step 2: drain the accumulators. Their events go through the same sink,
	// so the tail of the incremental is distributed exactly like the rest.
	res, err := compactor.finalize()
	if err != nil {
		return nil, err
	}
	res.bytesBefore = bytesBefore

	// Step 3: close the slot being filled and write a header-only chunk into
	// every slot the collapsed stream did not need.
	if err := sink.finish(); err != nil {
		return nil, err
	}
	res.bytesAfter = sink.bytesAfter
	res.peakChunkBufferBytes = sink.peakBuffered
	if sink.degraded() {
		// The bound degraded. Said out loud rather than left to be inferred,
		// for the same reason the eviction notice above is: an operator whose
		// compaction used more memory than the ceiling advertises deserves to
		// know which property gave way. See [chunkStreamSink].
		slog.Info(
			"smart compaction: an input change chunk was larger than the per-chunk output budget, so "+
				"the emitted stream buffered past it; this chain predates the per-chunk byte ceiling "+
				"and its compaction peak is bounded by its largest chunk rather than by the budget",
			slog.Int64("peak_chunk_buffer_bytes", sink.peakBuffered),
			slog.Int64("chunk_budget_bytes", sink.budget),
		)
	}
	return res, nil
}

// chunkStreamSink writes the compactor's emitted events straight into the
// incremental's existing chunk slots, rolling to the next slot on a byte
// budget, so nothing is retained beyond the chunk currently being filled.
//
// # The constraint, and the escape route chosen (roadmap item 130)
//
// The compaction pass READS every staged chunk (step 1) and WRITES back over
// the same paths (step 3). Streaming output during the read phase therefore
// risks overwriting a staged input chunk that has not been read yet — on
// Windows it fails loudly, since a rename-replace over an open handle is
// "Access is denied"; elsewhere it is silent input corruption. Three ways out
// were available:
//
//  1. Write the output to FRESH paths. Rejected: [irbackup.ChangeChunkAAD]
//     deliberately binds the chunk's own File and index as its anti-splice
//     property, so a new path is a new binding, and chunk paths are referenced
//     by every surface that reads a manifest.
//  2. Stage the output to temp paths and move them into place. Rejected: a
//     full extra read+write of every chunk, on object storage, on a
//     maintenance path.
//  3. **A write-behind cursor** — the one implemented here. Output slot i is
//     written only once input chunk i has been fully read and CLOSED, which is
//     exactly the condition that makes overwriting it safe. No extra I/O and
//     no rebinding.
//
// # What that bounds, and what it costs
//
// A slot cannot be written early even when it is full, so the emitted bytes
// for a still-open input chunk pile into the current slot. Writing:
//
//	B = the per-slot budget ([DefaultBackupChunkBytes])
//	I = the largest input chunk's uncompressed byte count
//
// the peak buffered output is bounded by max(B, I) — one chunk, never the whole
// incremental, which is what it was before. When I ≤ B the bound IS the budget;
// otherwise it is that chain's largest chunk, which is still O(one chunk) but
// larger than the budget claims, so it is logged when it happens rather than
// asserted away.
//
// # I ≤ B is NOT something this code may assume, and that is checked, not hoped
//
// The tempting version of this comment says "chains written by a recent binary
// cap a change chunk at [DefaultBackupChunkBytes], so I ≤ B". Ground-truthed
// against the code, that is false in two ways worth writing down:
//
//   - The change-chunk byte ceiling (audit 2026-08-05 C-3) is on `main` and
//     UNRELEASED, so no published binary has ever written a capped change
//     chunk.
//   - It reached ONE of the two lanes that write them. `incremental.go` rolls
//     on `writer.BytesWritten() >= DefaultBackupChunkBytes`; `stream.go`'s
//     `changeChunkBuffer.processChange` — the continuous `backup stream run`
//     lane, which produces most change chunks in practice — still rolls on the
//     event COUNT alone. Its rollover-level `RolloverMaxBytes` bounds a
//     rollover at a transaction boundary, not a chunk.
//
// So the degraded case is the NORMAL case today, not a legacy-chain edge, and
// nothing here is allowed to depend on the tight bound. That is why the
// degradation is logged and pinned rather than argued away, and why the gate
// measures both. The stream lane's missing ceiling is its own sibling gap, not
// this type's to close.
//
// # Slot exhaustion
//
// Output bytes are ≤ input bytes (collapsing a chain emits at most the union of
// its events' columns and fewer envelopes), so N slots of budget B suffice
// whenever the input averages ≤ B per chunk. When it does not, the LAST slot
// absorbs the remainder without a budget — the same degraded bound, not a
// refusal, because refusing would make a chain uncompactable rather than
// merely expensive.
//
// # Why the byte budget is load-bearing and not a tidiness measure
//
// The encrypted [blobcodec.ChangeChunkWriter] buffers the whole compressed
// chunk in memory and GCM-encrypts it at Close — one tag per chunk IS the
// format. So bounding the compactor's own output slice while still packing
// every event into one chunk would bound nothing at all for an encrypted chain.
// Distributing across the slots is what makes the bound real on the shape that
// needs it most, which is why the gate covers plaintext and encrypted alike.
type chunkStreamSink struct {
	ctx    context.Context
	store  irbackup.Store
	im     *irbackup.Manifest
	codec  blobcodec.Codec
	cek    []byte
	budget int64

	// slot is the chunk index currently being filled; w/buf are its open
	// writer and destination buffer (nil until the first event lands).
	slot int
	w    *blobcodec.ChangeChunkWriter
	buf  *bytes.Buffer

	// readThrough is the number of input chunks fully read AND closed. The
	// write-behind rule is `slot < readThrough`.
	readThrough int

	bytesAfter int64

	// peakBuffered is the high-water mark of one slot's accepted UNCOMPRESSED
	// bytes — the accounting hook the memory gate asserts on, because
	// runtime.MemStats is too noisy to gate on. It is an upper bound on what
	// is actually held: the writer keeps the COMPRESSED form (and, encrypted,
	// its ciphertext), both smaller than the bytes accepted.
	peakBuffered int64

	// maxEvent is the largest single event accepted. The budget is checked
	// AFTER a write, so a slot legitimately overshoots by at most one event;
	// without this number "the bound degraded" and "the last event was a wide
	// one" are the same observation, and the notice below would cry wolf on
	// every ordinary compaction.
	maxEvent int64
}

// degraded reports whether the peak exceeded what the budget can account for —
// budget plus the one-event overshoot that checking after the write implies.
// Anything beyond that came from a slot the write-behind rule or slot
// exhaustion would not let it roll out of. See the type doc.
func (k *chunkStreamSink) degraded() bool {
	return k.budget > 0 && k.peakBuffered > k.budget+k.maxEvent
}

func (k *chunkStreamSink) emit(c ir.Change) error {
	if k.w == nil {
		if err := k.open(); err != nil {
			return err
		}
	}
	file := k.im.ChangeChunks[k.slot].File
	before := k.w.BytesWritten()
	if err := k.w.WriteChange(c); err != nil {
		if errors.Is(err, blobcodec.ErrChunkLineTooLong) {
			// Collapsing merges after-images as a UNION of their columns (see
			// [mergeAfterImage]), so a collapsed event can serialize larger
			// than any of the events it replaced — a chain whose events the
			// source backup wrote happily. Refusing is right (the rewritten
			// chunk would not restore), but the remedy is specific to this
			// path and the codec cannot know it.
			return fmt.Errorf("write collapsed change to chunk %q: %w\n\n"+
				"This row's events were each individually writable; collapsing them merged "+
				"their column values into one larger row. Re-run with --smart-compaction-off "+
				"to keep the naive concatenation, which leaves the events uncollapsed and "+
				"within the limit", file, err)
		}
		return fmt.Errorf("write change to chunk %q: %w", file, err)
	}
	n := k.w.BytesWritten()
	if n > k.peakBuffered {
		k.peakBuffered = n
	}
	if d := n - before; d > k.maxEvent {
		k.maxEvent = d
	}
	return k.maybeRoll()
}

// inputChunkRead records that input chunks [0, n) have been read and closed,
// then rolls if the slot now under the cursor has reached its budget.
func (k *chunkStreamSink) inputChunkRead(n int) error {
	k.readThrough = n
	return k.maybeRoll()
}

// maybeRoll closes the current slot and advances when the slot is full, a
// further slot exists, and the write-behind rule permits writing this one.
func (k *chunkStreamSink) maybeRoll() error {
	switch {
	case k.w == nil, k.budget <= 0:
		return nil
	case k.w.BytesWritten() < k.budget:
		return nil
	case k.slot+1 >= len(k.im.ChangeChunks):
		// No slot left to roll into: the last one carries the remainder.
		return nil
	case k.slot >= k.readThrough:
		// Its input chunk is still open. Overwriting it now would destroy
		// bytes step 1 has not read.
		return nil
	}
	return k.closeSlot()
}

// finish closes the slot being filled and writes a header-only chunk into every
// slot the collapsed stream did not reach, so the manifest's ChangeChunks list
// stays aligned with what is on disk.
func (k *chunkStreamSink) finish() error {
	for k.slot < len(k.im.ChangeChunks) {
		if k.w == nil {
			if err := k.open(); err != nil {
				return err
			}
		}
		if err := k.closeSlot(); err != nil {
			return err
		}
	}
	return nil
}

func (k *chunkStreamSink) open() error {
	ch := k.im.ChangeChunks[k.slot]
	k.buf = &bytes.Buffer{}
	// ADR-0152: the rewrite re-binds under the SAME (path-stable,
	// identity-stable) manifest and the SAME chunk index the reader used.
	w, err := blobcodec.NewChangeChunkWriter(k.buf, k.cek, k.codec, irbackup.ChangeChunkAADFor(k.im, ch, k.slot))
	if err != nil {
		return fmt.Errorf("open chunk writer %q: %w", ch.File, err)
	}
	k.w = w
	return nil
}

// closeSlot finalises the current slot: encode, store, re-stamp, re-read, and
// advance the cursor.
func (k *chunkStreamSink) closeSlot() error {
	ch := k.im.ChangeChunks[k.slot]
	if err := k.w.Close(); err != nil {
		return fmt.Errorf("close chunk writer %q: %w", ch.File, err)
	}
	newBytes := k.buf.Bytes()
	if err := k.store.Put(k.ctx, ch.File, bytes.NewReader(newBytes)); err != nil {
		return fmt.Errorf("rewrite chunk %q: %w", ch.File, err)
	}
	ch.SHA256 = k.w.Hash()
	ch.RowCount = k.w.ChangeCount()
	// Both numbers above came from the compactor's OWN writer, and nothing
	// on this path ever read the object back — so every later consumer
	// would check the store's bytes against a figure the writer asserted
	// rather than one anything observed (audit 2026-08-04 C-4). Read it
	// back through the store with the real reader before moving on; one
	// extra read of a chunk this pass has already read once, on the only
	// maintenance path that rewrites restore-critical bytes.
	if err := verifyRewrittenChangeChunk(k.ctx, k.store, ch, k.cek, k.codec, irbackup.ChangeChunkAADFor(k.im, ch, k.slot)); err != nil {
		return fmt.Errorf("re-read rewritten chunk %q: %w", ch.File, err)
	}
	k.bytesAfter += int64(len(newBytes))
	k.w = nil
	k.buf = nil
	k.slot++
	return nil
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
