// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Smart compaction hid shape (B) — silently, since v0.85.0. On a pre-v0.138.0
// Postgres chain incremental 1 ends on a committed transaction T and
// incremental 2 re-delivers T whole (the old EndPosition was T's commit START).
// When incremental 1 also touched one of T's keyed rows earlier, collapse
// flushes that row at its FIRST position, so incremental 1's last ROW position
// drops below T while incremental 2's first row stays at T: the door's (B)
// stops firing, and a replay applies T's keyless rows twice (the review
// measured 4 audit_log rows against the source's 2). Collapse can only lower
// the last-row position on Postgres, so it can only HIDE (B), never invent it.

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// smartLSNComparator orders the `pos()` positions these tests write (the
// Postgres token shape, LSN in hex), as the Postgres engine orders its own.
type smartLSNComparator struct{}

func smartLSN(p ir.Position) (uint64, error) {
	i := strings.Index(p.Token, `"lsn":"0/`)
	if i < 0 {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseUint(strings.TrimSuffix(p.Token[i+len(`"lsn":"0/`):], `"}`), 16, 64)
}

func (smartLSNComparator) PrecedesOrEqual(a, b ir.Position) (bool, error) {
	x, err := smartLSN(a)
	if err != nil {
		return false, err
	}
	y, err := smartLSN(b)
	if err != nil {
		return false, err
	}
	return x <= y, nil
}

// redeliveredBoundaryRows is the review's chain (zz5): incremental 1 commits
// INSERT users(id) at s+1, then T at s+2 (a same-key UPDATE of that row and a
// KEYLESS audit_log INSERT); incremental 2 re-delivers T, then commits T3.
func redeliveredBoundaryRows(start uint64, j int) (out []ir.Change, end uint64) {
	id := int64(start)
	if j == 1 {
		t1, t2 := start+1, start+2
		return []ir.Change{
			ir.TxBegin{Position: pos(t1)},
			ir.Insert{Position: pos(t1), Schema: "public", Table: "users", Row: ir.Row{"id": id, "name": "a"}},
			ir.TxCommit{Position: pos(t1)},
			ir.TxBegin{Position: pos(t2)},
			ir.Update{Position: pos(t2), Schema: "public", Table: "users", Before: ir.Row{"id": id}, After: ir.Row{"id": id, "name": "b"}},
			ir.Insert{Position: pos(t2), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup-me"}},
			ir.TxCommit{Position: pos(t2)},
		}, t2
	}
	t3 := start + 1
	return []ir.Change{
		ir.TxBegin{Position: pos(start)},
		ir.Update{Position: pos(start), Schema: "public", Table: "users", Before: ir.Row{"id": id - 2}, After: ir.Row{"id": id - 2, "name": "b"}},
		ir.Insert{Position: pos(start), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup-me"}},
		ir.TxCommit{Position: pos(start)},
		ir.TxBegin{Position: pos(t3)},
		ir.Insert{Position: pos(t3), Schema: "public", Table: "users", Row: ir.Row{"id": int64(9000) + id, "name": "z"}},
		ir.TxCommit{Position: pos(t3)},
	}, t3
}

// TestSmartCompaction_RefusesToHideShapeB: with the source engine's comparator
// (what the CLI passes), smart compaction refuses a chain the door refuses as
// shape (B), BEFORE copying anything, and leaves it as it was — still refused.
// The independent expected value is the input chain's own (B) verdict.
func TestSmartCompaction_RefusesToHideShapeB(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, severedCompactSchema(), nil, redeliveredBoundaryRows)
	requireShapeB := func(when string) {
		t.Helper()
		chain, err := lineage.BuildLineageChain(ctx, store, nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSevered(t, NewSeveredTransactionDoor(store, smartLSNComparator{}, nil).Check(ctx, chain), "at or before the last rows")
		_ = when
	}
	requireShapeB("before compaction (control)")
	files := len(store.data)

	// The order arrives the way the CLI supplies it: through the resolver,
	// from the source engine the catalog records.
	_, err := CompactChain(ctx, store, CompactOpts{
		MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
		Now: func() time.Time { return now.Add(10 * time.Hour) },
		PositionOrder: func(engine string) (ir.PositionMonotonicChecker, bool) {
			if engine != "postgres" {
				return nil, false
			}
			return smartLSNComparator{}, true
		},
	})
	if codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction || !strings.Contains(err.Error(), "nothing was copied") {
		t.Fatalf("smart compaction did not refuse, before copying, a chain carrying shape (B): %v", err)
	}
	requireShapeB("after the refused compaction")
	if len(store.data) != files {
		t.Fatalf("the refused run changed the store: %d files before, %d after", files, len(store.data))
	}
}

// TestSmartCompaction_BeltSeesShapeBLost isolates the pre-swap belt for (B):
// the merged output of a (B)-carrying chain, judged with the comparator, LOST
// the finding — exactly what the belt refuses. Built by running the rewrite
// with the pre-copy judge given no comparator, then judging with one.
func TestSmartCompaction_BeltSeesShapeBLost(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, severedCompactSchema(), nil, redeliveredBoundaryRows)
	before, _, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// No comparator: neither gate can see (B) — the stated reach — so the
	// compaction completes and erases it.
	if _, err := CompactChain(ctx, store, CompactOpts{
		MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-smart" },
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after, _, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// The source segments were swept, so rebuild the pre-swap state the belt
	// sees: a fresh copy of the input chain plus the merged segment's files.
	fresh := newMemStore()
	seedSmartCompactLineageWithSchemaAndEnc(t, fresh, now, severedCompactSchema(), nil, redeliveredBoundaryRows)
	for k, v := range store.data {
		if strings.HasPrefix(k, mergedSegmentDirPrefix) {
			fresh.data[k] = v
		}
	}
	ids := incrementalIDs(t, fresh, before, after)
	err = verdictChange(ctx, fresh, before, after, ids, smartLSNComparator{})
	if codeOf(err) != sluicecode.CodeBackupChainUnreadable || !strings.Contains(err.Error(), "lost: [B(") {
		t.Fatalf("the belt did not refuse a rewrite that LOST the (B) finding: %v", err)
	}
	// Without a comparator the belt cannot see (B): the stated reach.
	if err := verdictChange(ctx, fresh, before, after, ids, nil); err != nil {
		t.Fatalf("without a comparator (B) is unjudged, so there is nothing to compare: %v", err)
	}
}
