// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Smart compaction rewrites change chunks, and the severed-transaction door
// judges a chain BY its change chunks, so a rewrite can change the door's
// verdict in either direction: hide a finding (b953b09d closed an open
// transaction and erased shape (A); collapse lowers an incremental's last-row
// position and erases shape (B) — smart compaction has done that since
// v0.85.0) or invent one (e9cbfc42 left a healthy TRUNCATE-tailed chain ending
// below EndPosition). Two gates, both smart-compaction only (naive compaction
// moves chunk bytes verbatim, so the door reads the same evidence):
//
//   - refuseFindingsInRewrittenLinks, BEFORE any merge group is copied:
//     a shape-(A) or shape-(B) finding that involves an incremental the run
//     would rewrite refuses the run. Such a chain is already unreplayable,
//     and rewriting it can only move or hide the evidence. Nothing is
//     written, so the refusal leaves no `seg-merged-*` copy behind.
//   - refuseVerdictChange, AFTER the rewrite and BEFORE the catalog swap:
//     the door's findings on the rewritten incrementals, before and after,
//     must be the SAME SET — compared link by link, so an unrelated refused
//     link elsewhere in the chain neither disables the gate nor trips it.
//     Any finding gained (pass→refuse) or lost (refuse→pass) refuses the
//     swap. The independent expected value is the input's own findings.
//
// REACH, stated so it is not read as broader: shape (B) is judged only with
// a comparator, which comes from the chain's SOURCE engine through
// [CompactOpts.Comparator] (the CLI resolves it from the registry; Postgres is
// the one engine that orders positions today). Without one — a caller that
// passes none, a MySQL chain — neither gate can see (B), exactly as restore
// cannot. Encrypted chains are refused by smart compaction itself, so both
// gates read plaintext chunks with no key.

// rewrittenIncrementals returns the BackupIDs of the incrementals in the
// segments the planned merge groups will rewrite (encrypted groups excluded:
// the smart pass refuses them with its own message).
func rewrittenIncrementals(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, planned []plannedGroup) (map[string]bool, []lineage.SegmentRecord, error) {
	sources := map[string]bool{}
	for i := range planned {
		if planned[i].plan.MergedSegmentID == "" || planned[i].span[0].fullMani.ChainEncryption != nil {
			continue
		}
		for _, m := range planned[i].span {
			sources[cat.Segments[m.catIdx].SegmentID] = true
		}
	}
	if len(sources) == 0 {
		return nil, nil, nil
	}
	links, err := lineage.BuildLineageChainFromCatalog(ctx, store, cat, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("backup compact: read the chain to judge its incrementals: %w", err)
	}
	ids := map[string]bool{}
	for i := range links {
		if isIncrementalLink(&links[i]) && links[i].Segment != nil && sources[links[i].Segment.SegmentID] {
			ids[lineage.ManifestBackupID(links[i].Manifest)] = true
		}
	}
	return ids, links, nil
}

// involves reports whether finding f touches a link in ids.
func involves(links []lineage.SegmentRecord, f doorFinding, ids map[string]bool) bool {
	if ids[lineage.ManifestBackupID(links[f.link].Manifest)] {
		return true
	}
	return f.pair >= 0 && ids[lineage.ManifestBackupID(links[f.pair].Manifest)]
}

// refuseFindingsInRewrittenLinks is the pre-copy judge.
func refuseFindingsInRewrittenLinks(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, planned []plannedGroup, cmp ir.PositionMonotonicChecker) error {
	ids, links, err := rewrittenIncrementals(ctx, store, cat, planned)
	if err != nil || len(ids) == 0 {
		return err
	}
	for _, f := range NewSeveredTransactionDoor(store, cmp, nil).findings(ctx, links) {
		if !involves(links, f, ids) {
			continue
		}
		switch f.shape {
		case shapeSevered, shapeRedelivered:
			return sluicecode.Wrap(sluicecode.CodeBackupChainSeveredTransaction,
				"take a new full backup (`sluice backup full`) and compact that chain; this one carries a source transaction across two incrementals, and no rewrite of it can be replayed exactly",
				fmt.Errorf("smart compaction refused before copying anything: %w — collapsing these incrementals would move or hide that evidence, so restore and `sync from-backup` would replay the transaction twice instead of refusing; nothing was copied, swapped or deleted", f.err))
		case shapeUndecodable:
			return fmt.Errorf("backup compact: judge an incremental before compacting it: %w", f.err)
		case shapeEndPast:
			// Preserved by the rewrite (the closing pair is never stamped past
			// the input's last position) and compared by refuseVerdictChange.
		}
	}
	return nil
}

// verdictKey names a finding by the BackupIDs of the links it involves, which
// a rewrite preserves.
func verdictKey(links []lineage.SegmentRecord, f doorFinding) string {
	pair := ""
	if f.pair >= 0 {
		pair = lineage.ManifestBackupID(links[f.pair].Manifest)
	}
	return fmt.Sprintf("%s(%s,%s)", f.shape, lineage.ManifestBackupID(links[f.link].Manifest), pair)
}

// rewrittenVerdicts is the set of findings that involve a rewritten link,
// keyed by [verdictKey]. walks is false when the chain does not walk at all.
func rewrittenVerdicts(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, ids map[string]bool, cmp ir.PositionMonotonicChecker) (set map[string]bool, walks bool) {
	links, werr := lineage.BuildLineageChainFromCatalog(ctx, store, cat, nil)
	if werr != nil {
		return nil, false
	}
	set = map[string]bool{}
	for _, f := range NewSeveredTransactionDoor(store, cmp, nil).findings(ctx, links) {
		if involves(links, f, ids) {
			set[verdictKey(links, f)] = true
		}
	}
	return set, true
}

// refuseVerdictChange is the pre-swap belt.
func refuseVerdictChange(ctx context.Context, store irbackup.Store, current, prospective *lineage.Catalog, planned []plannedGroup, cmp ir.PositionMonotonicChecker) error {
	ids, _, err := rewrittenIncrementals(ctx, store, current, planned)
	if err != nil || len(ids) == 0 {
		return err
	}
	return verdictChange(ctx, store, current, prospective, ids, cmp)
}

// verdictChange is the belt's comparison over the incrementals in ids.
func verdictChange(ctx context.Context, store irbackup.Store, current, prospective *lineage.Catalog, ids map[string]bool, cmp ir.PositionMonotonicChecker) error {
	// An unwalkable chain on either side is the pre-swap readability gate's
	// (verifyChainReadable) to refuse; this gate compares door findings.
	before, walks := rewrittenVerdicts(ctx, store, current, ids, cmp)
	if !walks {
		return nil
	}
	after, walks := rewrittenVerdicts(ctx, store, prospective, ids, cmp)
	if !walks {
		return nil
	}
	var gained, lost []string
	for k := range after {
		if !before[k] {
			gained = append(gained, k)
		}
	}
	for k := range before {
		if !after[k] {
			lost = append(lost, k)
		}
	}
	if len(gained) == 0 && len(lost) == 0 {
		return nil
	}
	sort.Strings(gained)
	sort.Strings(lost)
	return sluicecode.Wrap(sluicecode.CodeBackupChainUnreadable,
		"this is a defect in sluice's smart compaction, not in the chain: compact with --smart-compaction-off, and report it",
		fmt.Errorf("backup compact: pre-swap check: smart compaction would change what restore's severed-transaction check finds on the rewritten incrementals (gained: [%s]; lost: [%s]) — a gained finding makes a restorable chain unrestorable, a lost one hides a transaction restore would replay twice — so the catalog was NOT swapped and nothing was deleted",
			strings.Join(gained, " "), strings.Join(lost, " ")))
}

// removeUncommittedMergedDirs deletes the `seg-merged-*` copies a compaction
// run created before it refused, so a refused run does not leak a full copy
// of each merge group.
//
// Which dirs are LIVE is decided from the store, not from the in-memory
// catalog: commitCompactedChain mutates the in-memory catalog before it writes
// it, so a CAS conflict on that write would make the merged dir look live and
// leak it — while an ambiguous-success write (the Put landed, then errored)
// really did make it live, and deleting it would destroy the chain. So the
// stored catalog is re-read; a dir it references, or that the catalog
// referenced before this run, is never touched; and if it cannot be re-read,
// nothing is deleted (a WARN names the dirs). Pinned both ways by
// TestCompactChain_CatalogWriteFailure_CleansOrKeepsByTheStoredCatalog.
func removeUncommittedMergedDirs(ctx context.Context, store irbackup.Store, preRunDirs map[string]bool, dirs []string) {
	if len(dirs) == 0 {
		return
	}
	ctx = context.WithoutCancel(ctx)
	stored, _, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		slog.WarnContext(
			ctx, "backup compact: the refused run could not re-read the lineage catalog, so its merged copies were NOT removed; delete any listed dir the catalog does not reference",
			slog.Any("dirs", dirs),
			slog.String("err", err.Error()),
		)
		return
	}
	live := map[string]bool{}
	for d := range preRunDirs {
		live[d] = true
	}
	if stored != nil {
		for i := range stored.Segments {
			live[stored.Segments[i].Dir] = true
		}
	}
	for _, dir := range dirs {
		if dir == "" || live[dir] {
			continue
		}
		if err := sweepSegmentSubdir(ctx, store, dir); err != nil {
			slog.WarnContext(
				ctx, "backup compact: the refused run's merged copy could not be removed; it is unreferenced and safe to delete",
				slog.String("dir", dir),
				slog.String("err", err.Error()),
			)
		}
	}
}

// segmentDirs is the set of segment dirs cat references.
func segmentDirs(cat *lineage.Catalog) map[string]bool {
	out := map[string]bool{}
	for i := range cat.Segments {
		out[cat.Segments[i].Dir] = true
	}
	return out
}
