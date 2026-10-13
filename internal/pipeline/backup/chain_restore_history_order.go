// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// # A window's schema versions replay where they were recorded
// (KEY-COLLATION-REVERTED-WINDOW-RESTORE)
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
// applier's schema-history resume uses), and only at a transaction boundary
// (a DDL is never inside a MySQL transaction, and a version released inside
// one would split it). A version anchored after the window's last change is
// released at the end, so the next window — and a `sync start` resuming at
// the chain's end — still finds it.
//
// The orderer comes from [ChainRestore.SourceEngine] (cmd/sluice wires the
// engine registry; the orchestrator imports no engine), or from the Target
// when it is the recorded source engine. When the anchors cannot be ordered
// (no orderer for the recorded source engine), the versions are released up
// front as before — unless the window holds two versions of one table whose
// KEY collations differ: then which rows each governed cannot be known, and
// the restore refuses before replaying anything
// (KEY-COLLATION-HISTORY-UNORDERED) rather than arm the guard from the wrong
// one. `sync from-backup` places no version between a window's changes at
// all, so it applies the same refusal to every window
// ([RefuseUnreplayableKeyCollationHistory]).

// keyCollationHistoryUnorderedMarker names the refusal.
const keyCollationHistoryUnorderedMarker = "KEY-COLLATION-HISTORY-UNORDERED"

// historyReplay releases a window's recorded schema versions into its
// replayed change stream. Producer goroutine only.
type historyReplay struct {
	pending []ir.SchemaSnapshot
	orderer ir.PositionOrderer // nil: unorderable, everything released up front
	inTx    bool
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
// nil when there is none). keys is the segment's recorded schema
// ([refuseKeyCollationHistory]).
func newHistoryReplay(backupID, sourceEngine string, orderer ir.PositionOrderer, keys *ir.Schema, snaps []ir.SchemaSnapshot) (*historyReplay, error) {
	h := &historyReplay{pending: snaps, orderer: orderer}
	if h.orderer == nil {
		if err := refuseUnorderedKeyCollationHistory(backupID, sourceEngine, keys, snaps); err != nil {
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
// is not pushed.
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
	kept := h.pending[:0]
	for _, s := range h.pending {
		at, err := h.orderer.PositionAtOrAfter(pos, s.Position)
		if err != nil {
			return fmt.Errorf("order schema-history version %s.%s at %s against change at %s: %w",
				s.Schema, s.Table, s.Position.Token, pos.Token, err)
		}
		if !at {
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

// refuseUnorderedKeyCollationHistory is the restore's refusal when the
// window's versions cannot be ordered against its changes.
func refuseUnorderedKeyCollationHistory(backupID, sourceEngine string, keys *ir.Schema, snaps []ir.SchemaSnapshot) error {
	return refuseKeyCollationHistory(backupID, fmt.Sprintf(
		"its schema versions cannot be ordered against its changes (no position orderer for source engine %q)", sourceEngine,
	), keys, snaps)
}

// RefuseUnreplayableKeyCollationHistory refuses an incremental whose
// SchemaHistory holds two versions of one table that disagree on a key
// column's collation, for a replay that cannot place each version ahead of
// the changes written under it — why says which. `sync from-backup` is
// one: its change stream carries the source's events only, so its applier
// never sees a window's versions at all, and its key-collation guard is
// never armed from one (KEY-COLLATION-REVERTED-WINDOW-RESTORE, measured on
// the broker: target row 1 held 'A' where the source holds 'a', exit 0).
func RefuseUnreplayableKeyCollationHistory(link *lineage.SegmentRecord, why string) error {
	snaps, err := decodeSchemaHistory(link)
	if err != nil {
		return fmt.Errorf("schema history: %w", err)
	}
	return refuseKeyCollationHistory(lineage.ManifestBackupID(link.Manifest), why, link.Manifest.Schema, snaps)
}

// refuseKeyCollationHistory refuses when two versions of one table in snaps
// disagree on a key column's collation ([migcore.KeyCollationChange], either
// way — a version recording no collation for a key the other names counts).
// keys is the segment's recorded schema: a change stream's version carries
// the primary key at most (the MySQL binlog reader's knows no UNIQUE index),
// so the recorded schema's unique indexes say which other columns are keys.
func refuseKeyCollationHistory(backupID, why string, keys *ir.Schema, snaps []ir.SchemaSnapshot) error {
	first := map[string]*ir.Table{}
	for _, s := range snaps {
		qn := s.Schema + "." + s.Table
		cur := withRecordedIndexes(s.IR, keys)
		prev, ok := first[qn]
		if !ok {
			first[qn] = cur
			continue
		}
		changed, unknown := migcore.KeyCollationChange(prev, cur)
		if !changed && !unknown {
			continue
		}
		return fmt.Errorf("%w: %s: incremental %s records more than one key collation for %s, and %s, so which rows "+
			"were written under which collation cannot be told; replaying them under either could merge rows the "+
			"source keeps apart. Refusing before any change of the window is applied. Take a fresh full backup after "+
			"the key change and restore from it", ir.ErrKeyCollationMerge, keyCollationHistoryUnorderedMarker, backupID, qn, why)
	}
	return nil
}

// withRecordedIndexes is t with the indexes keys records for the same table
// added, so a UNIQUE key the change stream's version does not carry still
// counts as a key. t itself is not modified.
func withRecordedIndexes(t *ir.Table, keys *ir.Schema) *ir.Table {
	if t == nil || keys == nil {
		return t
	}
	for _, k := range keys.Tables {
		if k == nil || k.Name != t.Name || (k.Schema != "" && t.Schema != "" && k.Schema != t.Schema) {
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
