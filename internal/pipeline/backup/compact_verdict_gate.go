// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"log/slog"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Smart compaction rewrites change chunks, and the severed-transaction door
// judges a chain BY its change chunks — so a rewrite can change the door's
// verdict. Two rounds of the F-E1-SEVERED-TAIL-REPLAY work each shipped such
// a change (b953b09d hid shape (A) behind a closing pair; e9cbfc42 left a
// trailing unframed TRUNCATE's chain ending below EndPosition), and both were
// found by review. The two gates here make the property mechanical:
//
//   - refuseSeveredSmartInputs: BEFORE any merge group is copied, an
//     incremental the door judges severed refuses the run. Nothing is
//     written, so a refusal leaves no `seg-merged-*` copy behind.
//   - refuseVerdictRegression: AFTER the rewrite and BEFORE the catalog
//     swap, the door is run over the chain as it is and as it would be; a
//     chain it passes today that it would refuse after the swap refuses the
//     swap. A compaction must never turn a restorable chain into an
//     unrestorable one. The independent expected value is the pre-compaction
//     chain's own verdict.
//
// SCOPE: smart compaction only. Naive compaction moves chunks verbatim, so
// the door reads the same bytes (and the pairing across the merged segment's
// removed fulls is the same incremental order). Smart compaction refuses
// encrypted chains, so both gates read plaintext chunks with no key.

// refuseSeveredSmartInputs refuses a smart compaction whose merge groups
// include an incremental the severed-transaction door judges severed (an
// older binary's window that ends inside an open source transaction), before
// anything is copied. [smartCompactor.keepsTheSeveredVerdict] is the second
// layer, over the whole decoded stream.
func refuseSeveredSmartInputs(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, planned []plannedGroup) error {
	sources := map[string]bool{}
	for i := range planned {
		if planned[i].plan.MergedSegmentID == "" || planned[i].span[0].fullMani.ChainEncryption != nil {
			continue // an encrypted group is refused by the smart pass itself, with its own message
		}
		for _, m := range planned[i].span {
			sources[cat.Segments[m.catIdx].SegmentID] = true
		}
	}
	if len(sources) == 0 {
		return nil
	}
	links, err := lineage.BuildLineageChainFromCatalog(ctx, store, cat, nil)
	if err != nil {
		return fmt.Errorf("backup compact: read the chain to judge its incrementals: %w", err)
	}
	door := NewSeveredTransactionDoor(store, nil, nil)
	for i := range links {
		if !isIncrementalLink(&links[i]) || links[i].Segment == nil || !sources[links[i].Segment.SegmentID] {
			continue
		}
		e, err := door.edgesOf(ctx, &links[i])
		if err != nil {
			return fmt.Errorf("backup compact: judge incremental %s before compacting it: %w", lineage.ManifestBackupID(links[i].Manifest), err)
		}
		if e.endsOpen {
			return severedSmartInputError(lineage.ManifestBackupID(links[i].Manifest), e.openAt)
		}
	}
	return nil
}

// refuseVerdictRegression runs the severed-transaction door over the chain the
// catalog describes now and the one the post-compaction catalog would
// describe, and refuses the swap when the first passes and the second does not.
func refuseVerdictRegression(ctx context.Context, store irbackup.Store, current, prospective *lineage.Catalog) error {
	// An unwalkable chain on either side is the pre-swap readability gate's
	// (verifyChainReadable) to refuse; this gate compares door verdicts, and a
	// chain the door already refuses before compaction has nothing to regress.
	if before := doorVerdictOf(ctx, store, current); !before.walks || before.refused {
		return nil
	}
	after := doorVerdictOf(ctx, store, prospective)
	if !after.walks || !after.refused {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupChainUnreadable,
		"this is a defect in sluice's smart compaction, not in the chain: compact with --smart-compaction-off, and report it",
		fmt.Errorf("backup compact: pre-swap check: the smart-compacted chain would be refused by restore although the chain restores today, so the catalog was NOT swapped and nothing was deleted; the refusal the compacted chain would get: %w", after.refusal))
}

// doorVerdict is the severed-transaction door's verdict on one chain: walks is
// false when the chain does not walk at all, refusal is the door's refusal.
type doorVerdict struct {
	walks   bool
	refused bool
	refusal error
}

func doorVerdictOf(ctx context.Context, store irbackup.Store, cat *lineage.Catalog) doorVerdict {
	links, werr := lineage.BuildLineageChainFromCatalog(ctx, store, cat, nil)
	if werr != nil {
		return doorVerdict{}
	}
	refusal := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, links)
	return doorVerdict{walks: true, refused: refusal != nil, refusal: refusal}
}

// removeUncommittedMergedDirs deletes the `seg-merged-*` copies a compaction
// run created before it refused, so a refused run does not leak a full copy
// of each merge group (three refused runs grew a test store from 11 files to
// 38). A dir the catalog references is never touched — with a fixed merged
// segment id (tests) a retry could otherwise name a live segment. Best
// effort: a failed delete WARNs, the refusal stands.
func removeUncommittedMergedDirs(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, dirs []string) {
	live := map[string]bool{}
	for i := range cat.Segments {
		live[cat.Segments[i].Dir] = true
	}
	for _, dir := range dirs {
		if dir == "" || live[dir] {
			continue
		}
		if err := sweepSegmentSubdir(context.WithoutCancel(ctx), store, dir); err != nil {
			slog.WarnContext(
				ctx, "backup compact: the refused run's merged copy could not be removed; it is unreferenced and safe to delete",
				slog.String("dir", dir),
				slog.String("err", err.Error()),
			)
		}
	}
}

func severedSmartInputError(backupID string, at any) error {
	return sluicecode.Wrap(sluicecode.CodeBackupChainSeveredTransaction,
		"take a new full backup (`sluice backup full`) and compact that chain; this one carries a source transaction across two incrementals, and no rewrite of it can be replayed exactly",
		fmt.Errorf("smart compaction refused: incremental %s ends inside an open source transaction (TxBegin at %+v; F-E1-SEVERED-TAIL-REPLAY shape A, written by a `backup stream` stop or cancel on an older sluice), and collapsing it would change what restore and `sync from-backup` judge on — nothing was copied, swapped or deleted", backupID, at))
}
