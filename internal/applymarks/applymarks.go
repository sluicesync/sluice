// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package applymarks is the engine-neutral core of ADR-0190's exactly-once
// apply marks: a restart skips the changes of an interrupted source
// transaction that already reached the target.
//
// # The problem it closes (GC-38 (l))
//
// A restart re-delivers every change after the persisted position, and every
// apply path relies on ADR-0010's idempotent UPSERT to make that harmless.
// It is not harmless when a crash left a PREFIX of a source transaction on
// the target: the replay meets a uniqueness constraint the transaction's own
// later changes already moved (a unique value freed and reused, a swap
// through a temporary, a primary-key change), fails with 23505 / 1062, and
// fails again on every restart because the position never passed the
// transaction.
//
// # The mechanism, in one paragraph
//
// Every change a reader delivers carries a stable [ir.ApplyID]. When a
// change of a NON-IDEMPOTENT class applies — a table with a secondary
// uniqueness constraint, a primary-key change, a keyless table — the applier
// writes a [Mark] for each key it touches IN THE SAME TARGET TRANSACTION as
// the change's row write. The lane router (and trivially the serial paths)
// applies the changes to any one key in source order, so the changes of a
// transaction that reached a key before a crash are a PREFIX of that key's
// changes: a mark naming (TxID, Seq) proves every change of TxID to that key
// with a Seq at or below it is on the target. On restart every change —
// marked class or not — consults the marks for its keys and is skipped when
// one proves it applied.
//
// # The rule every branch below serves
//
// A mark can only ever cause a skip of a change the target already holds;
// ANY doubt resolves to applying the change, which is today's behaviour — a
// loud collision at worst, never a silent skip. So: a change with no identity
// never skips; a change whose key cannot be computed never skips and writes
// no mark; a mark whose scope digest differs from the running stream's
// refuses instead of skipping; and when a mark names the SAME ordinal as the
// replayed change, the two changes' digests must agree or the stream refuses
// with [MismatchMarker] (the tripwire: the independent expected value on the
// skip path — it catches a reader that renumbered a transaction).
//
// This package touches no database. The engine owns the mark table's SQL and
// calls in at four points: [Tracker.Load] when an apply run starts,
// [Tracker.Decide] before each change dispatches, [Tracker.Plan] inside each
// target transaction, and [Tracker.Committed] after it commits.
package applymarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// MismatchMarker is the grep-stable token on every refusal this package
// raises: a mark whose identity says a replayed change was already applied
// but whose evidence disagrees with the change — the ordinal tripwire, or a
// row-filter scope that changed since the mark was written.
const MismatchMarker = "APPLY-MARK-MISMATCH"

// UnavailableMarker is the grep-stable token of the WARN an engine logs when
// the mark table cannot be created, read or written (a PlanetScale
// safe-migrations branch, a DML-only role, a --schema-already-applied target
// that lacks it). The run then applies without marks: today's behaviour,
// operator decision 2 in ADR-0190 — no new refusal.
const UnavailableMarker = "APPLY-MARKS-UNAVAILABLE"

// UntrustedMarker is the grep-stable token of the WARN a run logs when a
// durable mark names the replayed change's own transaction but that
// transaction is NOT the first one this run delivered. Marks only ever exist
// for the first transaction after the persisted position (ADR-0190 amendment
// B), so such a mark means the position moved behind it — the 2026-09-28
// SchemaSnapshot regression was one way — and it is not evidence: the change
// applies (a loud collision at worst, never a silent skip).
const UntrustedMarker = "APPLY-MARK-UNTRUSTED"

// Mark is one row of the sluice_cdc_apply_marks control table: the last
// change of transaction TxID that reached one key of one target table.
type Mark struct {
	// Table is the qualified target table the change was routed to.
	Table string
	// KeyDigest is [KeyDigest] of the target primary-key values; empty for
	// a keyless table, whose mark is per table.
	KeyDigest string
	// TxID and Seq are the change's [ir.ApplyID].
	TxID string
	Seq  uint64
	// ChangeDigest is [ChangeDigest] of the change — the tripwire's
	// evidence when a replay presents the same ordinal.
	ChangeDigest string
	// ScopeDigest is the stream's row-filter scope when the mark was
	// written; a replay under a different scope may number its changes
	// differently, so it refuses rather than trusting the mark.
	ScopeDigest string
}

// slot is a mark's identity within one stream: the table's primary key
// (stream_id, table_name, key_digest) minus the stream, which a Tracker
// fixes.
type slot struct {
	table string
	key   string
}

func (m Mark) slot() slot { return slot{table: m.Table, key: m.KeyDigest} }

// Subject is what the ENGINE resolved about a row change's target: the
// target-side facts this package cannot know.
type Subject struct {
	// Table is the qualified, routed target table (the form the engine's
	// own caches key on). It is the mark's table_name.
	Table string
	// PK is the target table's ordered primary-key column list; empty for a
	// keyless table.
	PK []string
	// SecondaryUnique reports that the target table can refuse a row on
	// something other than its primary key — the lane router's
	// RouteScopeTable probe. An engine whose probe failed answers true (a
	// mark written unnecessarily costs a write; one omitted costs the
	// exactly-once guarantee).
	SecondaryUnique bool
}

// Decision is [Tracker.Decide]'s verdict for one change.
type Decision struct {
	// Skip reports that a durable mark proves the change already applied.
	// The caller must not dispatch it.
	Skip bool
	// Marks are the marks the change must write, in the same target
	// transaction as its row write, if (and only if) the dispatch actually
	// applied it. Empty for an idempotent class.
	Marks []Mark
}

// Tracker is one applier's view of its stream's apply marks. It is safe for
// concurrent use: the ADR-0104/0105 lanes call Decide and Committed from W
// goroutines while the coordinator plans the checkpoint's GC.
//
// Two sets drive garbage collection. dirty holds every transaction with
// marks durably on the target (loaded at start, or committed since); closed
// holds every transaction whose source commit the stream has passed. A mark
// is needed only until the persisted position passes its transaction, so the
// position write that passes it deletes it, in the same target transaction.
type Tracker struct {
	enabled atomic.Bool

	mu       sync.Mutex
	streamID string
	scope    string
	// loaded is the skip evidence: the stream's marks as the target held
	// them when this apply run started. Never mutated after Load, so the
	// lanes read it without the lock (Load happens-before the lanes start).
	loaded       map[slot]Mark
	loadedTables map[string]bool
	dirty        map[string]bool
	open         map[string]bool
	closed       map[string]bool
	swept        bool

	// firstTx is the first identity-carrying transaction this apply run
	// delivered — the only one a loaded mark may vouch for (see consult).
	// First writer wins; Load resets it.
	firstTx         atomic.Pointer[string]
	warnedUntrusted atomic.Bool
	skipped         atomic.Int64
}

// Load resets the tracker to the stream's durable marks and enables it. The
// engine calls it at the start of every apply run (Apply / ApplyBatch), so a
// retry that re-enters after a failed transaction always decides from what
// the target actually holds — never from in-memory state a rolled-back
// transaction left behind.
func (t *Tracker) Load(streamID, scope string, marks []Mark) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streamID = streamID
	t.scope = scope
	t.loaded = make(map[slot]Mark, len(marks))
	t.loadedTables = map[string]bool{}
	t.dirty = map[string]bool{}
	t.open = map[string]bool{}
	t.closed = map[string]bool{}
	t.swept = false
	for _, m := range marks {
		t.loaded[m.slot()] = m
		t.loadedTables[m.Table] = true
		t.dirty[m.TxID] = true
	}
	t.skipped.Store(0)
	t.firstTx.Store(nil)
	t.warnedUntrusted.Store(false)
	t.enabled.Store(true)
}

// Disable turns the tracker into a no-op: every change applies and no mark is
// written — today's behaviour exactly. The engine calls it when the mark
// table is unavailable ([UnavailableMarker]).
func (t *Tracker) Disable() {
	t.enabled.Store(false)
}

// Enabled reports whether marks are in force for this run.
func (t *Tracker) Enabled() bool { return t.enabled.Load() }

// StreamID is the stream the loaded marks belong to.
func (t *Tracker) StreamID() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.streamID
}

// Skipped is the number of changes this run skipped on mark evidence.
func (t *Tracker) Skipped() int64 { return t.skipped.Load() }

// LogRunSummary emits the run's INFO total of skipped changes (each skip is
// also named at DEBUG). Silent when nothing was skipped — the ordinary case.
func (t *Tracker) LogRunSummary(ctx context.Context, engine string) {
	if n := t.skipped.Load(); n > 0 {
		slog.InfoContext(ctx, engine+": applier: apply marks skipped changes a previous run had already applied "+
			"(an interrupted source transaction replayed onto its own committed prefix; ADR-0190)",
			slog.String("stream_id", t.StreamID()), slog.Int64("skipped", n))
	}
}

// Decide reports whether c must be skipped and, when it applies, which marks
// it must write. A non-nil error is a [MismatchMarker] refusal: terminal, the
// stream stops loudly naming the table, the key and both identities.
//
// Only an identity-carrying row change can skip or mark. Every change of an
// identity-carrying transaction CHECKS the marks — that is what stops a
// replayed change on an idempotent, unmarked path from resurrecting a key a
// later marked change of the same transaction moved away.
func (t *Tracker) Decide(c ir.Change, s Subject) (Decision, error) {
	d, involved, err := t.verdict(c, s)
	if !involved {
		return Decision{}, nil
	}
	id := ir.ApplyIDOf(c)
	t.noteOpen(id.TxID)
	if d.Skip {
		t.skipped.Add(1)
		slog.Debug("apply-marks: skipping a change the target already holds",
			slog.String("table", s.Table), slog.String("tx_id", id.TxID), slog.Uint64("seq", id.Seq))
	}
	return d, err
}

// Skips reports whether [Tracker.Decide] would skip c, with no side effect but
// noting the run's first delivered transaction (and the once-per-run
// [UntrustedMarker] WARN).
// The lane coordinator asks it at ROUTE time so a change the marks prove
// already applied does not advance rows_applied; a refusal answers false
// (the apply path raises it).
func (t *Tracker) Skips(c ir.Change, s Subject) bool {
	d, _, err := t.verdict(c, s)
	return err == nil && d.Skip
}

// WouldMark reports, with no side effect but those of [Tracker.Skips], whether [Tracker.Decide] would have
// c write a mark — or would refuse it. The lane coordinator asks it at ROUTE
// time to decide whether the change needs a mark fence (see [LaneFence]); a
// refusal answers true because fencing first costs only a drain, and the
// apply path raises the refusal either way.
func (t *Tracker) WouldMark(c ir.Change, s Subject) bool {
	d, _, err := t.verdict(c, s)
	return err != nil || (!d.Skip && len(d.Marks) > 0)
}

// verdict is [Tracker.Decide] without its bookkeeping. involved is false
// when the change neither consults nor writes a mark — no identity, the
// tracker disabled, or an idempotent class on a table with nothing on record.
func (t *Tracker) verdict(c ir.Change, s Subject) (d Decision, involved bool, err error) {
	if !t.enabled.Load() {
		return Decision{}, false, nil
	}
	id := ir.ApplyIDOf(c)
	if id.IsZero() {
		return Decision{}, false, nil
	}
	// Every apply path consults the tracker for its changes in source order
	// before any later change reaches it (the lane coordinator at route
	// time), so the first identity seen is the first transaction delivered.
	t.firstTx.CompareAndSwap(nil, &id.TxID)
	marked, pkChange := s.classOf(c)
	checked := t.loadedTables[s.Table]
	if !marked && !checked {
		// Idempotent class and nothing on record for the table: no mark to
		// consult and none to write, so skip the digest work (and the lock)
		// entirely. This is the PK-only steady state, and the reason it
		// costs nothing.
		return Decision{}, false, nil
	}
	keys := changeKeys(c, s.PK)
	if len(keys) == 0 {
		// No computable key (a malformed image): nothing can prove the
		// change applied and nothing can be marked. Apply it.
		return Decision{}, true, nil
	}
	digest := ChangeDigest(c, s.Table, s.PK)
	if checked {
		skip, err := t.consult(id, s.Table, keys, digest)
		if err != nil || skip {
			return Decision{Skip: skip}, true, err
		}
	}
	if !marked {
		return Decision{}, true, nil
	}
	if !pkChange && len(keys) > 1 {
		// A non-PK-changing update presents the same key twice (before and
		// after); one mark covers it.
		keys = keys[:1]
	}
	marks := make([]Mark, 0, len(keys))
	for _, k := range keys {
		marks = append(marks, Mark{
			Table: s.Table, KeyDigest: k,
			TxID: id.TxID, Seq: id.Seq,
			ChangeDigest: digest, ScopeDigest: t.scope,
		})
	}
	return Decision{Marks: marks}, true, nil
}

// consult checks the loaded marks for every key the change touches. One key
// whose mark proves the change applied is enough (the per-key prefix
// property); a mark that CONTRADICTS the change refuses, and a refusal wins
// over any skip evidence from another key.
func (t *Tracker) consult(id ir.ApplyID, table string, keys []string, digest string) (skip bool, err error) {
	for _, k := range keys {
		m, ok := t.loaded[slot{table: table, key: k}]
		if !ok || m.TxID != id.TxID {
			// No mark, or a mark of another transaction: a TxID is unique per
			// source transaction, so a foreign one proves nothing about this
			// change.
			continue
		}
		if first := t.firstTx.Load(); first == nil || *first != m.TxID {
			// Amendment B as a runtime check: marks only exist for the first
			// transaction after the persisted position, so a mark of THIS
			// transaction when it was not delivered first means the position
			// moved behind it — and the earlier transaction just replayed may
			// have re-created what the mark says is done. Not evidence.
			t.warnUntrusted(m, first)
			continue
		}
		if m.ScopeDigest != t.scope {
			return false, scopeMismatch(t.streamID, m, id, t.scope)
		}
		switch {
		case m.Seq > id.Seq:
			skip = true
		case m.Seq == id.Seq:
			if m.ChangeDigest != digest {
				return false, ordinalMismatch(t.streamID, m, id, digest)
			}
			skip = true
		}
	}
	return skip, nil
}

// classOf reports whether the change belongs to a marked (non-idempotent)
// class, and whether it is a primary-key-changing update (which marks BOTH
// its before-key and its after-key).
func (s Subject) classOf(c ir.Change) (marked, pkChange bool) {
	if u, ok := c.(ir.Update); ok && len(s.PK) > 0 && laneapply.PKChangedUpdate(u, s.PK) {
		pkChange = true
	}
	return s.SecondaryUnique || len(s.PK) == 0 || pkChange, pkChange
}

// noteOpen records that the stream is inside transaction txID. The serial
// paths close it with [Tracker.CloseOpen] at the transaction's commit.
func (t *Tracker) noteOpen(txID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.open[txID] = true
}

// CloseOpen records that the stream passed the commit of every transaction it
// has decided a change for since the last close. The serial apply paths call
// it on a delivered [ir.TxCommit], BEFORE the position write that persists
// it, so that write deletes those transactions' marks. Transactions arrive
// whole and in order on every identity-carrying source, so this is exactly
// the transaction the commit closes.
func (t *Tracker) CloseOpen() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for tx := range t.open {
		t.closed[tx] = true
		delete(t.open, tx)
	}
	t.sweepLocked()
}

// CloseTxs records that the stream passed the commits of txIDs — the lane
// coordinator's form of [Tracker.CloseOpen], naming the transactions a
// checkpoint boundary covers.
func (t *Tracker) CloseTxs(txIDs []string) {
	if len(txIDs) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, tx := range txIDs {
		t.closed[tx] = true
		delete(t.open, tx)
	}
	t.sweepLocked()
}

// sweepLocked is the restart sweep, run once per apply run at its FIRST
// transaction close: every loaded mark is closed with it, so the position
// write that passes the first re-delivered transaction deletes them all.
//
// The invariant it rests on: marks only ever exist for the FIRST transaction
// after the persisted position. The serial and per-change paths write the
// position at every transaction's commit, in the same target transaction as
// the commit's mark deletion, so their marks never outlive the one
// transaction in flight; the lane barrier writes marks only after its
// pre-barrier checkpoint has persisted the position up to the barrier's own
// transaction; and a lane writes a transaction's marks only once the
// coordinator's mark fence has done the same for it ([LaneFence], ADR-0190
// amendment A) — and the next transaction's fence moves the position past it,
// deleting them. So at the first close, every loaded mark is either that
// transaction's —
// now passed — or stale, left behind by a position that moved without this
// binary's bookkeeping (an older binary, a run with marks unavailable, an
// external position write). Stale marks are harmless to the skip rule (a
// TxID never recurs) but would otherwise live forever. Callers hold t.mu.
func (t *Tracker) sweepLocked() {
	if t.swept {
		return
	}
	t.swept = true
	for _, m := range t.loaded {
		t.closed[m.TxID] = true
		delete(t.open, m.TxID)
	}
}

// Pending accumulates the marks of one target transaction, coalesced per key
// (the last change to a key in the transaction wins — it carries the highest
// Seq, which is all a later skip needs). The zero value is ready to use. Not
// safe for concurrent use; each target transaction owns its own.
type Pending struct {
	marks map[slot]Mark
	order []slot
}

// Add records ms for the transaction.
func (p *Pending) Add(ms []Mark) {
	for _, m := range ms {
		if p.marks == nil {
			p.marks = map[slot]Mark{}
		}
		s := m.slot()
		if _, seen := p.marks[s]; !seen {
			p.order = append(p.order, s)
		}
		p.marks[s] = m
	}
}

// Reset empties the accumulator (a rolled-back transaction's marks die with
// it).
func (p *Pending) Reset() {
	p.marks = nil
	p.order = nil
}

// Len is the number of distinct keys pending.
func (p *Pending) Len() int { return len(p.order) }

// TxMarks is one target transaction's mark bookkeeping: the marks its applied
// changes owe, and — once computed and executed on the transaction — the
// plan to retire when it commits. An engine keeps one on each batch
// transaction handle; the zero value is ready to use.
type TxMarks struct {
	pending Pending
	plan    *Plan
}

// Add records the marks of a change the transaction applied.
func (m *TxMarks) Add(ms []Mark) { m.pending.Add(ms) }

// Plan computes the transaction's mark writes the FIRST time it is asked
// (ok true) and records them for [TxMarks.Committed]; a later call answers
// ok false, because the writes are already on the transaction. The batch
// loop asks twice on a boundary flush — once with the position write (gc
// true), once at commit — and only the first executes.
func (m *TxMarks) Plan(t *Tracker, gc bool) (Plan, bool) {
	if m.plan != nil {
		return Plan{}, false
	}
	pl := t.Plan(&m.pending, gc)
	m.plan = &pl
	m.pending.Reset()
	return pl, true
}

// Committed retires the executed plan after the transaction committed.
func (m *TxMarks) Committed(t *Tracker) {
	if m.plan != nil {
		t.Committed(*m.plan)
	}
}

// Plan is what one target transaction must write to the mark table: the
// pending marks to upsert and the transactions whose marks to delete. An
// engine executes it on the transaction's own connection before COMMIT and
// reports the commit with [Tracker.Committed].
type Plan struct {
	Upserts []Mark
	Deletes []string

	// closing is the closed set the plan was computed against, retired on
	// commit.
	closing []string
}

// Empty reports whether the plan writes nothing.
func (p Plan) Empty() bool { return len(p.Upserts) == 0 && len(p.Deletes) == 0 }

// Plan computes the mark writes for one target transaction. gc is true when
// the transaction also writes the stream's position: every closed transaction
// is then passed by that position, so its durable marks are deleted and any of
// its marks still pending are dropped (they could never be consulted — a
// transaction behind the persisted position is never re-delivered). With gc
// false (a mid-transaction flush, a lane commit) every pending mark is
// written.
func (t *Tracker) Plan(p *Pending, gc bool) Plan {
	if !t.enabled.Load() {
		return Plan{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var pl Plan
	if p != nil {
		for _, s := range p.order {
			m := p.marks[s]
			if gc && t.closed[m.TxID] {
				continue
			}
			pl.Upserts = append(pl.Upserts, m)
		}
	}
	if gc {
		for tx := range t.closed {
			pl.closing = append(pl.closing, tx)
			if t.dirty[tx] {
				pl.Deletes = append(pl.Deletes, tx)
			}
		}
		sort.Strings(pl.Deletes)
	}
	return pl
}

// Committed retires a plan whose transaction committed: its upserted
// transactions are now durably marked, and its deleted (and closed-but-never-
// written) transactions are gone for good. Not called for a rolled-back
// transaction — the next apply run's [Tracker.Load] re-reads the truth.
func (t *Tracker) Committed(pl Plan) {
	if !t.enabled.Load() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range pl.Upserts {
		t.dirty[m.TxID] = true
	}
	for _, tx := range pl.closing {
		delete(t.dirty, tx)
		delete(t.closed, tx)
	}
}

// changeKeys returns the key digests a row change touches: the after-image's
// key and, when it differs (a primary-key change) or the after-image lacks
// it, the before-image's. A keyless table has one table-wide key, the empty
// string. A key column absent from an image makes that image's key
// uncomputable; the result may then be empty.
func changeKeys(c ir.Change, pk []string) []string {
	if len(pk) == 0 {
		return []string{""}
	}
	var images []ir.Row
	switch v := c.(type) {
	case ir.Insert:
		images = []ir.Row{v.Row}
	case ir.Update:
		images = []ir.Row{v.After, v.Before}
	case ir.Delete:
		images = []ir.Row{v.Before}
	}
	var keys []string
	for _, row := range images {
		k, ok := KeyDigest(row, pk)
		if !ok {
			continue
		}
		if len(keys) == 0 || keys[0] != k {
			keys = append(keys, k)
		}
	}
	return keys
}

// KeyDigest is the hex SHA-256 of a row's primary-key values, canonicalized
// by VALUE the way the lane router hashes them
// ([laneapply.WriteCanonicalKeyValue]) so a row decoded as int64 on one
// delivery and uint64 on another is still one key. ok is false when a key
// column is absent from the row.
func KeyDigest(row ir.Row, pk []string) (string, bool) {
	if row == nil {
		return "", false
	}
	h := sha256.New()
	for _, col := range pk {
		v, present := row[col]
		if !present {
			return "", false
		}
		laneapply.WriteCanonicalKeyValue(h, v)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), true
}

// ChangeDigest is the hex SHA-256 of a row change as the tripwire compares
// it: its operation, its table, its key(s) and its after-image (a Delete's
// before-key stands in for an image it does not carry). Deterministic across
// re-deliveries of the same change, because every value it reads is decoded
// by the same reader from the same source bytes.
func ChangeDigest(c ir.Change, table string, pk []string) string {
	h := sha256.New()
	writeString(h, table)
	switch v := c.(type) {
	case ir.Insert:
		writeString(h, "I")
		writeRow(h, v.Row)
	case ir.Update:
		writeString(h, "U")
		writeBefore(h, v.Before, pk)
		writeRow(h, v.After)
	case ir.Delete:
		writeString(h, "D")
		writeBefore(h, v.Before, pk)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// writeBefore writes the before-image's identity: its key digest, or — for a
// keyless table, which has no key to name the row — the whole image.
func writeBefore(h hash.Hash, before ir.Row, pk []string) {
	if len(pk) == 0 {
		writeRow(h, before)
		return
	}
	k, _ := KeyDigest(before, pk)
	writeString(h, k)
}

// writeRow writes a row's columns in name order.
func writeRow(h hash.Hash, row ir.Row) {
	names := make([]string, 0, len(row))
	for n := range row {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		writeString(h, n)
		writeValue(h, row[n])
		_, _ = h.Write([]byte{0})
	}
}

// writeValue writes one value in a deterministic, type-tagged form covering
// the IR value contract (docs/value-types.md). An unforeseen kind falls back
// to its type name and %v rendering: deterministic for the value kinds a
// reader produces, and a non-deterministic one could only ever make the
// tripwire refuse, never skip.
func writeValue(h hash.Hash, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = io.WriteString(h, "N")
	case []byte:
		_, _ = io.WriteString(h, "b")
		_, _ = h.Write([]byte(strconv.Itoa(len(t))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write(t)
	case string:
		_, _ = io.WriteString(h, "s")
		_, _ = io.WriteString(h, strconv.Itoa(len(t)))
		_, _ = io.WriteString(h, ":"+t)
	case float64:
		_, _ = io.WriteString(h, "f"+strconv.FormatFloat(t, 'g', -1, 64))
	case float32:
		_, _ = io.WriteString(h, "f"+strconv.FormatFloat(float64(t), 'g', -1, 32))
	case time.Time:
		_, _ = io.WriteString(h, "t"+t.Format(time.RFC3339Nano))
	case json.Number:
		_, _ = io.WriteString(h, "n"+t.String())
	case []any:
		_, _ = io.WriteString(h, "[")
		for _, e := range t {
			writeValue(h, e)
			_, _ = io.WriteString(h, ",")
		}
		_, _ = io.WriteString(h, "]")
	case []string:
		_, _ = io.WriteString(h, "S[")
		for _, e := range t {
			writeValue(h, e)
			_, _ = io.WriteString(h, ",")
		}
		_, _ = io.WriteString(h, "]")
	case map[string]any:
		_, _ = io.WriteString(h, "{")
		writeRow(h, t)
		_, _ = io.WriteString(h, "}")
	default:
		// Integers of every width and bool share the lane router's
		// value-canonical encoding.
		laneapply.WriteCanonicalKeyValue(h, v)
	}
}

// writeString writes a length-prefixed string so adjacent fields cannot run
// together.
func writeString(h hash.Hash, s string) {
	_, _ = io.WriteString(h, strconv.Itoa(len(s))+":"+s)
}

// WarnUnavailable is the one WARN every engine logs when the mark table
// cannot be used this run (operator decision 2: WARN and apply without marks —
// never a new refusal). cause names why.
func WarnUnavailable(ctx context.Context, engine, streamID string, cause error) {
	msg := "unknown"
	if cause != nil {
		msg = cause.Error()
	}
	slog.WarnContext(ctx, engine+": applier: "+UnavailableMarker+": the sluice_cdc_apply_marks table cannot be used, so "+
		"this run applies WITHOUT exactly-once apply marks (ADR-0190). Nothing is lost: a restart after a crash in the "+
		"middle of a source transaction replays it as sluice always has, and may stop loudly on a unique collision. To "+
		"enable the marks, let sluice create the table (or, on a MySQL-family target, ship the DDL `sluice control-tables "+
		"ddl` prints) and grant this role SELECT, INSERT, UPDATE and DELETE on it",
		slog.String("stream_id", streamID), slog.String("cause", msg))
}

// warnUntrusted logs the [UntrustedMarker] WARN, once per apply run. first
// is the run's first delivered transaction (nil only if none was noted).
func (t *Tracker) warnUntrusted(m Mark, first *string) {
	if !t.warnedUntrusted.CompareAndSwap(false, true) {
		return
	}
	firstTx := ""
	if first != nil {
		firstTx = *first
	}
	slog.Warn("apply-marks: "+UntrustedMarker+": a durable apply mark names a replayed change's own transaction, but "+
		"that transaction was not the first this run re-delivered — marks only ever exist for the first transaction "+
		"after the persisted position, so the position moved behind it and the mark is not evidence. The change is "+
		"APPLIED, not skipped: nothing is skipped silently, but the re-apply is not always loud — a unique collision stops "+
		"the stream, while on a keyless table it can add a duplicate row (ADR-0089 at-least-once), so compare that table "+
		"against the source. Report this with the log around it (ADR-0190 amendment B)",
		slog.String("stream_id", t.streamID), slog.String("mark_tx_id", m.TxID),
		slog.String("first_tx_id", firstTx), slog.String("table", m.Table))
}

// RefusalError is the terminal refusal this package raises. It is terminal
// ([ir.TerminalError]): a retry replays the same transaction onto the same
// marks and refuses the same way.
type RefusalError struct {
	msg string
}

func (e *RefusalError) Error() string { return e.msg }

// Terminal implements [ir.TerminalError].
func (e *RefusalError) Terminal() bool { return true }

// ErrMismatch is the sentinel every [RefusalError] matches with errors.Is.
var ErrMismatch = errors.New(MismatchMarker)

// Is lets errors.Is(err, ErrMismatch) find a refusal.
func (e *RefusalError) Is(target error) bool { return target == ErrMismatch }

func ordinalMismatch(streamID string, m Mark, id ir.ApplyID, digest string) error {
	return &RefusalError{msg: fmt.Sprintf(
		"%s: stream %q replayed change %s#%d to table %s, key %s, and the apply mark written when that change "+
			"first reached the target names the same ordinal but a DIFFERENT change (mark digest %s, replayed %s). "+
			"The source's re-delivery numbered this transaction's changes differently, so the mark cannot say which "+
			"of them the target already holds. Refusing rather than skipping: a skip could drop a change the target "+
			"never received. Recover with `sync start --restart-from-scratch`, which re-copies the target and clears "+
			"the stream's marks",
		MismatchMarker, streamID, id.TxID, id.Seq, m.Table, shortDigest(m.KeyDigest), shortDigest(m.ChangeDigest), shortDigest(digest),
	)}
}

func scopeMismatch(streamID string, m Mark, id ir.ApplyID, scope string) error {
	return &RefusalError{msg: fmt.Sprintf(
		"%s: stream %q replayed change %s#%d to table %s, key %s, whose apply mark was written under a different "+
			"row-filter scope (mark %q, current %q). A changed --where can change which of a transaction's changes the "+
			"source delivers and so how they are numbered; the mark cannot be trusted to name this change. Refusing "+
			"rather than skipping. Re-run with the --where the stream was established with, or re-copy with "+
			"`sync start --restart-from-scratch`",
		MismatchMarker, streamID, id.TxID, id.Seq, m.Table, shortDigest(m.KeyDigest), m.ScopeDigest, scope,
	)}
}

// shortDigest abbreviates a hex digest for an operator-facing message; the
// empty key of a keyless table renders as such.
func shortDigest(d string) string {
	switch {
	case d == "":
		return "(table-wide: keyless)"
	case len(d) > 12:
		return d[:12]
	}
	return d
}
