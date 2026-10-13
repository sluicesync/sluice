// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
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

// historyReplay releases a window's recorded schema versions into its
// replayed change stream. Producer goroutine only.
type historyReplay struct {
	backupID string
	pending  []ir.SchemaSnapshot
	orderer  ir.PositionOrderer // nil: unorderable, everything released up front
	keys     *ir.Schema         // the segment's recorded schema, for its unique keys
	inTx     bool
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
// so it keeps its old place ahead of the window).
func (h *historyReplay) start(push func(ir.Change) error) error {
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
	boundary := !h.inTx
	switch c.(type) {
	case ir.TxBegin:
		h.inTx = true
	case ir.TxCommit:
		h.inTx = false
		boundary = false
	}
	if !boundary || h.orderer == nil || len(h.pending) == 0 {
		return nil
	}
	pos := c.Pos()
	if pos.Token == "" {
		return nil
	}
	held := map[string]ir.SchemaSnapshot{}
	kept := h.pending[:0]
	for _, s := range h.pending {
		at, err := h.orderer.PositionAtOrAfter(pos, s.Position)
		if err != nil {
			return fmt.Errorf("order schema-history version %s.%s at %s against change at %s: %w",
				s.Schema, s.Table, s.Position.Token, pos.Token, err)
		}
		qn := s.Schema + "." + s.Table
		if !at {
			if _, ok := held[qn]; !ok {
				held[qn] = s
			}
			kept = append(kept, s)
			continue
		}
		if earlier, ok := held[qn]; ok {
			why := fmt.Sprintf("its schema version at %s was not placed before the change at %s although a later version of the table was",
				earlier.Position.Token, pos.Token)
			if err := refuseKeyCollationHistory(h.backupID, why, &irbackup.Manifest{Schema: h.keys}, []ir.SchemaSnapshot{earlier, s}); err != nil {
				return err
			}
		}
		if err := push(s); err != nil {
			return err
		}
	}
	h.pending = kept
	return nil
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
func windowStartVersions(parent *ir.Schema, m *irbackup.Manifest, snaps []ir.SchemaSnapshot) []ir.SchemaSnapshot {
	if parent == nil || m == nil {
		return nil
	}
	changed := map[string]bool{}
	for _, d := range m.SchemaDelta {
		if d != nil && d.Kind == irbackup.SchemaDeltaAlterTable && migcore.DeltaChangesKeyCollation([]*irbackup.SchemaDeltaEntry{d}) {
			changed[d.Table] = true
		}
	}
	var seeds []ir.SchemaSnapshot
	for _, p := range parent.Tables {
		if p == nil {
			continue
		}
		hit := changed[p.Name]
		for _, s := range snaps {
			if hit {
				break
			}
			if sameTable(p, s.Schema, s.Table) && keyCollationsDisagree(p, withRecordedIndexes(s.IR, parent)) {
				hit = true
			}
		}
		if !hit {
			continue
		}
		schema := p.Schema
		for _, s := range snaps {
			if sameTable(p, s.Schema, s.Table) && s.Schema != "" {
				schema = s.Schema
				break
			}
		}
		seeds = append(seeds, ir.SchemaSnapshot{Position: m.StartPosition, Schema: schema, Table: p.Name, IR: p})
	}
	return seeds
}

// sameTable reports whether t is the table named schema.table (an empty
// schema on either side matches).
func sameTable(t *ir.Table, schema, table string) bool {
	return t.Name == table && (t.Schema == "" || schema == "" || t.Schema == schema)
}

// keyCollationsDisagree reports whether a and b declare a different
// collation for a key column either records ([migcore.KeyCollationChange],
// both ways).
func keyCollationsDisagree(a, b *ir.Table) bool {
	return migcore.KeyCollationChange(a, b) || migcore.KeyCollationChange(b, a)
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
	prev := map[string]*ir.Table{}
	for _, s := range snaps {
		qn := s.Schema + "." + s.Table
		cur := withRecordedIndexes(s.IR, keys)
		if p, ok := prev[qn]; ok && keyCollationsDisagree(p, cur) {
			return keyCollationHistoryRefusal(backupID, qn, why)
		}
		prev[qn] = cur
		if keys != nil {
			for _, k := range keys.Tables {
				if k != nil && sameTable(k, s.Schema, s.Table) && keyCollationsDisagree(k, cur) {
					return keyCollationHistoryRefusal(backupID, qn, why)
				}
			}
		}
	}
	return nil
}

// keyCollationHistoryRefusal is the KEY-COLLATION-HISTORY-UNORDERED error.
func keyCollationHistoryRefusal(backupID, what, why string) error {
	return fmt.Errorf("%w: %s: incremental %s changes a key column's collation (%s), and %s, so which rows were "+
		"written under which collation cannot be told; replaying them under either could merge rows the source keeps "+
		"apart. Refusing before any change of the window is applied. A chain restore whose source engine orders its "+
		"positions guards each row under the collation recorded at its position; otherwise take a fresh full backup "+
		"after the key change",
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
