// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// # A window's schema versions replay where they were recorded
// (KEY-COLLATION-REVERTED-WINDOW-RESTORE, KEY-COLLATION-LOOSENED-WINDOW)
//
// An incremental's SchemaHistory holds each schema version its change
// stream observed, anchored at the source position of the DDL. Chain
// restore used to push every one of them onto the applier BEFORE the
// window's first row, so the applier's active schema for every row was the
// window's LAST version. The applier's key-collation guard
// (KEY-COLLATION-MERGE) arms from that active schema — so a window that
// changed a key `utf8mb4_0900_ai_ci` → `utf8mb4_bin`, wrote 'A' beside 'a',
// moved it out of the way and changed the key back armed from the reverted
// `_ci` shape, equal to the target's, and replayed 'A' unguarded onto row
// 1's 'a'. Measured on MySQL 8.0 (MINIMAL and FULL): target row 1 held 'A'
// where the source holds 'a', restore exit 0.
//
// A live stream never had this problem: the reader emits each version in
// stream order, ahead of the rows written under it. So the restore now does
// what the live stream does — [historyReplay] releases each version
// immediately before the first change recorded at or after its anchor, by
// the source engine's own causal test ([ir.PositionOrderer], the one the
// applier's schema-history resume uses), and only at a transaction boundary.
// A version anchored after the window's last change is released at the end,
// so the next window — and a `sync start` resuming at the chain's end —
// still finds it.
//
// UNVERIFIED PREMISE: a schema version is anchored outside every
// transaction of the window, so releasing it only at a transaction boundary
// never moves it past a row written under it. True of MySQL, whose DDL
// implicitly commits; NOT of Postgres, whose DDL is transactional (an ALTER
// and rows written under it can share a transaction) — but a Postgres
// version is anchored at LSN 0/0 and so is released ahead of the whole
// window anyway (GC-44 F23). Pinned only for the MySQL shape.
//
// The rows a window writes BEFORE its first version are written under the
// shape the window started with, and nothing in the window records that
// shape. A key LOOSENED inside the window (`_bin` → `_ci` after 'A' beside
// 'a' moved away) applies its delta to the target before any row, so 'A'
// met 'a' on a `_ci` key with no version armed at all (measured on MySQL
// 8.0, MINIMAL and FULL: target row 1 held 'A', or was gone with the key as
// the primary key, exit 0). So the chain walk also releases, ahead of the
// window, the shape the previous link recorded for every table whose key
// collation the window changes ([windowStartVersions]).
//
// The orderer comes from [ChainRestore.SourceEngine] (cmd/sluice wires the
// engine registry; the orchestrator imports no engine), or from the Target
// when it is the recorded source engine. When the anchors cannot be ordered
// — no orderer for the recorded source engine, or one that does not place a
// version before a later version of the same table — and the window changes
// a key column's collation, which rows each version governed cannot be
// known, and the restore refuses before replaying anything
// (KEY-COLLATION-HISTORY-UNORDERED) rather than arm the guard from the wrong
// one. `sync from-backup` places no version between a window's changes at
// all, so it applies the same refusal to every window
// ([RefuseUnreplayableKeyCollationHistory]).

// keyCollationHistoryUnorderedMarker names the refusal.
const keyCollationHistoryUnorderedMarker = "KEY-COLLATION-HISTORY-UNORDERED"

// # A version names its table the way the window's changes do (Bug 306)
//
// The applier keys the shape it arms the key-collation guard from by the
// namespace and table a version names, and looks it up under the namespace
// the row names. A MySQL full manifest records its tables with NO namespace
// (a single-database read is flat: "the database the stream is bound to"),
// while every change the binlog reader records names that database. A
// window-start version built from such a manifest therefore landed under
// `kd` while the window's rows looked up `<db>.kd`: the lookup missed, the
// guard never armed, and 'A' merged into 'a' at exit 0 — whenever the
// window recorded no version of the table to borrow the name from, which is
// exactly a window whose last event for the table is the loosening ALTER
// (measured on MySQL 8.4, both capture lanes, MINIMAL and FULL, PRIMARY and
// UNIQUE key; the target's schema history held the seed under '').
//
// So a version that names no namespace takes the one the window's changes
// name, learned from the window's first table-scoped change
// ([historyReplay.admit]). When such a version is due at a boundary before
// that change has been seen, the boundary and what follows it are held —
// until the transaction's first row (which names the namespace) or its
// commit (an empty transaction: nothing in it for a version to govern, so it
// passes and the versions wait for the next boundary). A flat manifest is a
// single-namespace stream, so any table's row names it; that premise is
// CHECKED, not assumed: a later change of a table whose version took the
// namespace, naming another, refuses (KEY-COLLATION-VERSION-NAMESPACE)
// rather than let the lookup miss again.

// keyCollationVersionNamespaceMarker names the refusal raised when a change
// names a namespace other than the one a namespace-less version took.
const keyCollationVersionNamespaceMarker = "KEY-COLLATION-VERSION-NAMESPACE"

// historyReplay releases a window's recorded schema versions into its
// replayed change stream. Producer goroutine only.
type historyReplay struct {
	backupID string
	pending  []ir.SchemaSnapshot
	orderer  ir.PositionOrderer // nil: unorderable, everything released up front
	keys     *ir.Schema         // the segment's recorded schema, for its unique keys
	inTx     bool

	// namespace is the one the window's changes name, once learned
	// (namespaceKnown); bound holds the tables whose namespace-less version
	// took it, for the check that a later change of them names the same.
	namespace      string
	namespaceKnown bool
	bound          map[string]bool

	// held is non-nil while changes wait for the namespace a due version
	// needs (held[0], when present, is the boundary it is due at);
	// startHeld marks the versions [historyReplay.start] would release
	// ahead of every change as part of what waits.
	held      []ir.Change
	startHeld bool

	// windowEnd are the versions released after everything else
	// ([historyReplay.finish]).
	windowEnd []ir.SchemaSnapshot
}

// positionOrdererFor is the position orderer of the engine a chain was
// recorded from: [ChainRestore.SourceEngine]'s, else the Target's when it
// is that engine, else none.
func (r *ChainRestore) positionOrdererFor(sourceEngine string) ir.PositionOrderer {
	var eng ir.Engine
	if r.SourceEngine != nil {
		eng, _ = r.SourceEngine(sourceEngine)
	}
	if eng == nil && r.Target != nil && r.Target.Name() == sourceEngine {
		eng = r.Target
	}
	orderer, _ := eng.(ir.PositionOrderer)
	return orderer
}

// newHistoryReplay builds the replay for snaps (in recorded order), ordered
// by orderer — the recorded source engine's ([ChainRestore.positionOrdererFor];
// nil when there is none). m is the segment's manifest.
func newHistoryReplay(backupID, sourceEngine string, orderer ir.PositionOrderer, m *irbackup.Manifest, snaps []ir.SchemaSnapshot) (*historyReplay, error) {
	h := &historyReplay{backupID: backupID, pending: snaps, orderer: orderer}
	if m != nil {
		h.keys = m.Schema
	}
	if h.orderer == nil {
		why := fmt.Sprintf("its schema versions cannot be ordered against its changes (no position orderer for source engine %q)", sourceEngine)
		if err := refuseKeyCollationHistory(backupID, why, m, snaps); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// start releases what precedes every change: with no orderer, everything;
// with one, a version recorded with no position (nothing to order it by,
// so it keeps its old place ahead of the window). When one of them names
// no namespace, they wait for the window's first table-scoped change
// instead ([historyReplay.admit]).
func (h *historyReplay) start(push func(ir.Change) error) error {
	wait, err := h.dueNeedsNamespace(nil)
	if err != nil {
		return err
	}
	if wait {
		h.held, h.startHeld = []ir.Change{}, true
		return nil
	}
	return h.releaseStart(push)
}

// releaseStart is [historyReplay.start]'s release.
func (h *historyReplay) releaseStart(push func(ir.Change) error) error {
	if h.orderer == nil {
		return h.releaseAll(push)
	}
	kept := h.pending[:0]
	for _, s := range h.pending {
		if s.Position.Token != "" {
			kept = append(kept, s)
			continue
		}
		if err := push(s); err != nil {
			return err
		}
	}
	h.pending = kept
	return nil
}

// before releases, ahead of c, every pending version anchored at or before
// c's position — when c opens a transaction or stands outside one. c itself
// is not pushed. A version released while an EARLIER version of the same
// table stays pending means the orderer could not place that earlier one (a
// file/pos ServerUUID mismatch, a VStream shard missing from a position):
// the window is unorderable there, and if the two disagree on a key
// collation it refuses.
func (h *historyReplay) before(c ir.Change, push func(ir.Change) error) error {
	if !h.track(c) || h.orderer == nil || len(h.pending) == 0 {
		return nil
	}
	pos := c.Pos()
	if pos.Token == "" {
		return nil
	}
	// stranded holds the versions left pending so far in this pass — each
	// recorded BEFORE any later version of the same table this pass
	// releases. "The same table" is [sameVersionTable]'s: a version naming
	// no namespace is the table of that name in any (Bug 306 — keyed by the
	// literal namespace, a window-start version naming none was never
	// compared with the window's own versions naming the database).
	// UNVERIFIED PREMISE: that the manifest lists a table's versions in
	// position order, so "earlier in pending" means "earlier in the stream"
	// (KEY-COLLATION-STRANDED-VERSION-ORDER).
	var stranded []int
	drop := map[int]bool{}
	var kept []int
	for i, s := range h.pending {
		at, err := h.orderer.PositionAtOrAfter(pos, s.Position)
		if err != nil {
			return fmt.Errorf("order schema-history version %s.%s at %s against change at %s: %w",
				s.Schema, s.Table, s.Position.Token, pos.Token, err)
		}
		if !at {
			stranded = append(stranded, i)
			kept = append(kept, i)
			continue
		}
		still := stranded[:0:0]
		for _, j := range stranded {
			if !sameVersionTable(h.pending[j], s) {
				still = append(still, j)
				continue
			}
			earlier := h.pending[j]
			why := fmt.Sprintf("its schema version at %s was not placed before the change at %s although a later version of the table was",
				earlier.Position.Token, pos.Token)
			if err := refuseKeyCollationHistory(h.backupID, why, &irbackup.Manifest{Schema: h.keys}, []ir.SchemaSnapshot{earlier, s}); err != nil {
				return err
			}
			// They agree, so the earlier one changes nothing the guard reads —
			// but released later it would leave the applier's active schema
			// on the OLDER shape for the rest of the window (review of
			// d6ecb939, finding 6). It is dropped.
			drop[j] = true
		}
		stranded = still
		if err := push(s); err != nil {
			return err
		}
	}
	next := make([]ir.SchemaSnapshot, 0, len(kept))
	for _, j := range kept {
		if !drop[j] {
			next = append(next, h.pending[j])
		}
	}
	h.pending = next
	return nil
}

// track records c's effect on the transaction state and reports whether c
// is a boundary: it opens a transaction or stands outside one.
func (h *historyReplay) track(c ir.Change) bool {
	boundary := h.isBoundary(c)
	switch c.(type) {
	case ir.TxBegin:
		h.inTx = true
	case ir.TxCommit:
		h.inTx = false
	}
	return boundary
}

// isBoundary is [historyReplay.track]'s answer, without recording c.
func (h *historyReplay) isBoundary(c ir.Change) bool {
	if _, ok := c.(ir.TxCommit); ok {
		return false
	}
	return !h.inTx
}

// admit hands c on — deliver, after [historyReplay.before] has released what
// is due ahead of it — unless a version naming no namespace is due and the
// window's namespace is not known yet: then c waits, with what follows it,
// until a table-scoped change names the namespace (the versions take it, and
// everything held goes on in order) or the transaction commits empty (it
// goes on, and the versions stay due). See "A version names its table the
// way the window's changes do".
func (h *historyReplay) admit(c ir.Change, push, deliver func(ir.Change) error) error {
	if h.held == nil {
		wait, err := h.dueNeedsNamespace(c)
		if err != nil {
			return err
		}
		if !wait {
			return h.pass(c, push, deliver, true)
		}
		h.held = []ir.Change{}
	}
	h.held = append(h.held, c)
	if namespace, _, ok := tableScoped(c); ok {
		h.bindNamespace(namespace)
		return h.flush(push, deliver, true)
	}
	if _, ok := c.(ir.TxCommit); ok {
		return h.flush(push, deliver, false)
	}
	return nil
}

// finish ends the window: anything still held goes on (the window never
// named its namespace, so its versions keep none — there is no row of it for
// them to govern), then every pending version, then the window-end versions.
func (h *historyReplay) finish(push, deliver func(ir.Change) error) error {
	if h.held != nil {
		if err := h.flush(push, deliver, true); err != nil {
			return err
		}
	}
	if err := h.releaseAll(push); err != nil {
		return err
	}
	for _, s := range h.windowEnd {
		if err := push(s); err != nil {
			return err
		}
	}
	h.windowEnd = nil
	return nil
}

// flush hands on the held changes in order: with release, the versions due
// ahead of them go first (the start versions, then whatever
// [historyReplay.before] releases at the held boundary); without, only the
// transaction state is recorded, and the versions stay due.
func (h *historyReplay) flush(push, deliver func(ir.Change) error, release bool) error {
	held := h.held
	h.held = nil
	if release && h.startHeld {
		h.startHeld = false
		if err := h.releaseStart(push); err != nil {
			return err
		}
	}
	for _, c := range held {
		if err := h.pass(c, push, deliver, release); err != nil {
			return err
		}
	}
	if h.startHeld {
		// The start versions still wait for the namespace.
		h.held = []ir.Change{}
	}
	return nil
}

// pass hands c on: release decides whether the versions due ahead of it go
// first.
func (h *historyReplay) pass(c ir.Change, push, deliver func(ir.Change) error, release bool) error {
	if release {
		if err := h.before(c, push); err != nil {
			return err
		}
	} else {
		h.track(c)
	}
	if err := h.checkNamespace(c); err != nil {
		return err
	}
	return deliver(c)
}

// dueNeedsNamespace reports whether a version naming no namespace would be
// released ahead of c (c nil: by [historyReplay.start]) while the window's
// namespace is still unknown.
func (h *historyReplay) dueNeedsNamespace(c ir.Change) (bool, error) {
	if h.namespaceKnown {
		return false, nil
	}
	var pos ir.Position
	if c != nil {
		if !h.isBoundary(c) || h.orderer == nil {
			return false, nil
		}
		if pos = c.Pos(); pos.Token == "" {
			return false, nil
		}
	}
	for _, s := range h.pending {
		if s.Schema != "" {
			continue
		}
		// A version released up front goes at start, never at a change.
		upFront := h.orderer == nil || s.Position.Token == ""
		if c == nil {
			if upFront {
				return true, nil
			}
			continue
		}
		if upFront {
			continue
		}
		at, err := h.orderer.PositionAtOrAfter(pos, s.Position)
		if err != nil {
			return false, fmt.Errorf("order schema-history version %s at %s against change at %s: %w",
				s.Table, s.Position.Token, pos.Token, err)
		}
		if at {
			return true, nil
		}
	}
	return false, nil
}

// bindNamespace gives every version naming no namespace the one the
// window's changes name.
func (h *historyReplay) bindNamespace(namespace string) {
	h.namespace, h.namespaceKnown = namespace, true
	if h.bound == nil {
		h.bound = map[string]bool{}
	}
	for _, versions := range [][]ir.SchemaSnapshot{h.pending, h.windowEnd} {
		for i := range versions {
			if versions[i].Schema == "" {
				versions[i].Schema = namespace
				h.bound[versions[i].Table] = true
			}
		}
	}
}

// checkNamespace refuses a change of a table whose namespace-less version
// took the window's namespace, when the change names another: the version
// would be filed where the change's lookup does not look, and the guard it
// arms would not arm (Bug 306). A flat manifest is a single-namespace
// stream, so this never fires on a stream sluice records; it is the check
// on that premise.
func (h *historyReplay) checkNamespace(c ir.Change) error {
	namespace, table, ok := tableScoped(c)
	if !ok || !h.namespaceKnown || !h.bound[table] || namespace == h.namespace {
		return nil
	}
	return fmt.Errorf("%w: %s: incremental %s: a schema version of table %s recorded no namespace and took %q, the one "+
		"the window's first change named, but a change of the table names %q, so the shape that version records would "+
		"not govern it and rows the source keeps apart could merge. Refusing before that change is applied; take a fresh "+
		"full backup and restore from it",
		ir.ErrKeyCollationMerge, keyCollationVersionNamespaceMarker, h.backupID, table, h.namespace, namespace)
}

// tableScoped is the namespace and table a change names, for the changes
// that name one.
func tableScoped(c ir.Change) (namespace, table string, ok bool) {
	switch v := c.(type) {
	case ir.Insert:
		return v.Schema, v.Table, true
	case ir.Update:
		return v.Schema, v.Table, true
	case ir.Delete:
		return v.Schema, v.Table, true
	case ir.Truncate:
		return v.Schema, v.Table, true
	}
	return "", "", false
}

// sameVersionTable reports whether two versions are of the same table: the
// same name, and the same namespace when both name one.
func sameVersionTable(a, b ir.SchemaSnapshot) bool {
	return a.Table == b.Table && (a.Schema == "" || b.Schema == "" || a.Schema == b.Schema)
}

// releaseAll releases every pending version (the window's end).
func (h *historyReplay) releaseAll(push func(ir.Change) error) error {
	for _, s := range h.pending {
		if err := push(s); err != nil {
			return err
		}
	}
	h.pending = nil
	return nil
}

// windowStartVersions is, for every table whose key collation the window
// changes — its delta, or a recorded version, disagrees with the shape the
// previous link recorded — that previous shape, anchored at the window's
// start, so the rows written before the window's first version of it are
// guarded under the collation they were written under
// (KEY-COLLATION-LOOSENED-WINDOW). parent nil (no previous link known)
// seeds nothing. Only those tables are seeded: a version is a durable row in
// the target's schema history, and a table whose key did not change gains
// nothing from one.
//
// A seed names the namespace the parent recorded — none, for a MySQL
// manifest — and one naming none takes the namespace the window's changes
// name when it is released ([historyReplay.admit], Bug 306). It used to
// borrow one only from a recorded version of the table in the window, so a
// window recording none (its last event for the table the loosening ALTER)
// filed the seed where the rows' lookup never looked.
func windowStartVersions(parent *ir.Schema, m *irbackup.Manifest, snaps []ir.SchemaSnapshot) []ir.SchemaSnapshot {
	if parent == nil || m == nil {
		return nil
	}
	var seeds []ir.SchemaSnapshot
	for _, p := range parent.Tables {
		if p == nil {
			continue
		}
		hit := false
		for _, d := range m.SchemaDelta {
			if d != nil && d.Kind == irbackup.SchemaDeltaAlterTable && sameTable(p, d.Schema, d.Table) &&
				migcore.DeltaChangesKeyCollation([]*irbackup.SchemaDeltaEntry{d}) {
				hit = true
				break
			}
		}
		for _, s := range snaps {
			if hit {
				break
			}
			if sameTable(p, s.Schema, s.Table) && versionChangesKeyCollation(p, withRecordedIndexes(s.IR, parent), false) {
				hit = true
			}
		}
		if hit {
			seeds = append(seeds, ir.SchemaSnapshot{Position: m.StartPosition, Schema: p.Schema, Table: p.Name, IR: p})
		}
	}
	return seeds
}

// windowEndVersions is, for every window-start version, the shape the
// window recorded for its table, anchored at the window's end. The window's
// own versions need not reach that shape: a window whose last event for the
// table is the ALTER records no version after it (Bug 306), and without one
// the window-start shape — the one the window CHANGED — would stay the
// applier's active shape for every later window, and for a `sync start`
// resuming at the chain's end, arming the guard from a collation the table
// no longer has. The recorded shape is the basis the next window's reader
// starts from, so the applier and the reader agree at the seam. A window
// with no end position, or a table the window no longer records, gets none.
func windowEndVersions(m *irbackup.Manifest, seeds []ir.SchemaSnapshot) []ir.SchemaSnapshot {
	if m == nil || m.Schema == nil || m.EndPosition.Token == "" {
		return nil
	}
	var ends []ir.SchemaSnapshot
	for _, s := range seeds {
		for _, t := range m.Schema.Tables {
			if t != nil && sameTable(t, s.Schema, s.Table) {
				ends = append(ends, ir.SchemaSnapshot{Position: m.EndPosition, Schema: s.Schema, Table: s.Table, IR: t})
				break
			}
		}
	}
	return ends
}

// sameTable reports whether t is the table named schema.table (an empty
// schema on either side matches).
func sameTable(t *ir.Table, schema, table string) bool {
	return t.Name == table && (t.Schema == "" || schema == "" || t.Schema == schema)
}

// versionChangesKeyCollation reports whether a change stream's version
// declares a key collation different from known — a shape the schema reader
// recorded, or an earlier version. The version is always the AFTER side of
// [migcore.KeyCollationChange]: a version that records no collation (every
// VStream version) is no evidence of a change, and comparing it the other way
// round read a recorded `utf8mb4_0900_ai_ci` against its "" as one, refusing
// every VStream window of a character-keyed table on the broker (review of
// d6ecb939, finding 1). Between two versions both sides must record the
// collation: the reverse check asks the same question of known, so an
// unrecorded side on either end is not a change. A table where one key
// column is unrecorded on one side and another on the other side would read
// as a change, a loud false refusal. UNVERIFIED PREMISE: that no reader
// produces that mix — each records collations for every column or for none.
// Nothing asserts it per reader; the cost if it breaks is a refusal, not a
// merge.
func versionChangesKeyCollation(known, version *ir.Table, bothVersions bool) bool {
	if !migcore.KeyCollationChange(known, version) {
		return false
	}
	return !bothVersions || migcore.KeyCollationChange(version, known)
}

// RefuseUnreplayableKeyCollationHistory refuses an incremental that changes
// a key column's collation, for a replay that cannot place each schema
// version ahead of the changes written under it — why says which. `sync
// from-backup` is one: its change stream carries the source's events only,
// so its applier never sees a window's versions at all and its key-collation
// guard is never armed from one (KEY-COLLATION-REVERTED-WINDOW-RESTORE and
// KEY-COLLATION-LOOSENED-WINDOW, both measured on the broker: target row 1
// held 'A' where the source holds 'a', exit 0).
func RefuseUnreplayableKeyCollationHistory(link *lineage.SegmentRecord, why string) error {
	snaps, err := decodeSchemaHistory(link)
	if err != nil {
		return fmt.Errorf("schema history: %w", err)
	}
	return refuseKeyCollationHistory(lineage.ManifestBackupID(link.Manifest), why, link.Manifest, snaps)
}

// refuseKeyCollationHistory refuses when the window m changes a key
// column's collation: its delta does ([migcore.DeltaChangesKeyCollation] —
// a loosened key applied ahead of rows written under the stricter one), two
// consecutive versions of one table disagree (a change and its revert), or
// a version disagrees with the segment's recorded schema (a drain-commit,
// which records its parent's schema and no delta). A version that records
// no collation (VStream) is no evidence either way
// (KEY-COLLATION-VSTREAM-UNSTAMPED). The recorded schema also supplies the
// unique keys: a change stream's version carries the primary key at most
// (the MySQL binlog reader's knows no UNIQUE index).
func refuseKeyCollationHistory(backupID, why string, m *irbackup.Manifest, snaps []ir.SchemaSnapshot) error {
	var keys *ir.Schema
	if m != nil {
		keys = m.Schema
		if migcore.DeltaChangesKeyCollation(m.SchemaDelta) {
			return keyCollationHistoryRefusal(backupID, "a table its schema delta changes", why)
		}
	}
	// prev is each version seen so far with its table's recorded indexes;
	// the one compared with s is the latest of the same table
	// ([sameVersionTable] — a window-start version naming no namespace and
	// the window's versions naming the database are one table, Bug 306).
	type seen struct {
		v   ir.SchemaSnapshot
		cur *ir.Table
	}
	var prev []seen
	for _, s := range snaps {
		qn := s.Schema + "." + s.Table
		cur := withRecordedIndexes(s.IR, keys)
		for i := len(prev) - 1; i >= 0; i-- {
			if !sameVersionTable(prev[i].v, s) {
				continue
			}
			if versionChangesKeyCollation(prev[i].cur, cur, true) {
				return keyCollationHistoryRefusal(backupID, qn, why)
			}
			break
		}
		prev = append(prev, seen{s, cur})
		if keys != nil {
			for _, k := range keys.Tables {
				if k != nil && sameTable(k, s.Schema, s.Table) && versionChangesKeyCollation(k, cur, false) {
					return keyCollationHistoryRefusal(backupID, qn, why)
				}
			}
		}
	}
	return nil
}

// restoreKeyCollationRemedy gives a KEY-COLLATION-MERGE the applier raised
// during a chain restore the remedy `restore` can run. The applier's own —
// give the target key the source's collation and restart, or re-copy the
// table with --restart-from-scratch — is written for the commands that
// resume a stream, and neither half runs here (Bug 308): `restore` has no
// --restart-from-scratch, and it does not resume, so a re-run starts over
// from the full backup onto the rows already restored (measured on MySQL
// 8.4: LOAD-DATA-ROWS-SKIPPED, after the operator had ALTERed the key as
// told). So the restore's remedy REPLACES the applier's, it is not appended
// after it:
//
//   - inside a window that itself changes a key's collation (a loosening
//     delta, a changed-and-reverted key), a fresh full backup taken after
//     the key change, restored into an empty target. No other remedy
//     converges: every restore of this chain re-applies the window's change
//     to the target ahead of the rows written under the earlier collation
//     (review of d6ecb939, finding 4);
//   - otherwise, a restore into an empty target whose key column(s) can
//     carry what the guard compared under ([ir.KeyCollationMergeError.Needs]).
//
// A KEY-COLLATION-MERGE that is no [ir.KeyCollationMergeError] (the guard
// could not be built for the key, KEY-COLLATION-GUARD-UNSUPPORTED) names no
// command; inside such a window it gets the fresh-full sentence after its
// own, as before.
func restoreKeyCollationRemedy(link *lineage.SegmentRecord, err error) error {
	if !errors.Is(err, ir.ErrKeyCollationMerge) {
		return err
	}
	window := windowChangesKeyCollation(link)
	var merge *ir.KeyCollationMergeError
	if !errors.As(err, &merge) {
		if !window {
			return err
		}
		return fmt.Errorf("%w. %s", err, windowKeyCollationRemedy(link))
	}
	return ir.WithKeyCollationMergeRemedy(err, func(e *ir.KeyCollationMergeError) string {
		if window {
			return windowKeyCollationRemedy(link)
		}
		needs := "the collation the source compares them under"
		if e.Needs != "" {
			needs = e.Needs
		}
		return "Restore the chain into an empty target whose key column(s) carry " + needs + ". `sluice restore` " +
			"does not resume: running it again starts over from the full backup, onto the rows already restored, so " +
			"altering this target's key and re-running does not converge"
	})
}

// windowChangesKeyCollation reports whether link's window itself changes a
// key column's collation ([refuseKeyCollationHistory]'s test).
func windowChangesKeyCollation(link *lineage.SegmentRecord) bool {
	if link == nil || link.Manifest == nil {
		return false
	}
	snaps, err := decodeSchemaHistory(link)
	return err == nil && refuseKeyCollationHistory("", "", link.Manifest, snaps) != nil
}

// windowKeyCollationRemedy is the remedy inside such a window.
func windowKeyCollationRemedy(link *lineage.SegmentRecord) string {
	return fmt.Sprintf("Take a fresh full backup after the key change and restore it into an empty target. "+
		"Incremental %s changes a key column's collation inside its own window, so every restore of this chain "+
		"re-applies that change ahead of the rows written under the earlier collation and refuses here again; "+
		"altering the target's key does not help, because `sluice restore` does not resume and a re-run starts "+
		"over from the full backup", lineage.ManifestBackupID(link.Manifest))
}

// keyCollationHistoryRefusal is the KEY-COLLATION-HISTORY-UNORDERED error.
func keyCollationHistoryRefusal(backupID, what, why string) error {
	return fmt.Errorf("%w: %s: incremental %s changes a key column's collation (%s), and %s, so which rows were "+
		"written under which collation cannot be told; replaying them under either could merge rows the source keeps "+
		"apart. Refusing before any change of the window is applied; restarting refuses again, and changing the "+
		"target key's collation does not help. Take a new full backup after the key change, then replay the new "+
		"chain: `sluice sync from-backup run --reset-target-data --yes` against it (or `sluice restore` it)",
		ir.ErrKeyCollationMerge, keyCollationHistoryUnorderedMarker, backupID, what, why)
}

// withRecordedIndexes is t with the indexes keys records for the same table
// added, so a UNIQUE key the change stream's version does not carry still
// counts as a key. t itself is not modified.
func withRecordedIndexes(t *ir.Table, keys *ir.Schema) *ir.Table {
	if t == nil || keys == nil {
		return t
	}
	for _, k := range keys.Tables {
		if k == nil || !sameTable(k, t.Schema, t.Name) {
			continue
		}
		cp := *t
		cp.Indexes = append(append([]*ir.Index{}, t.Indexes...), k.Indexes...)
		if cp.PrimaryKey == nil {
			cp.PrimaryKey = k.PrimaryKey
		}
		return &cp
	}
	return t
}
