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
// transaction and erased shape (A); collapse erases shape (B) two ways —
// lowering the first incremental's last-row position, and collapsing the
// second's leading re-delivered row away, which raises its first-row
// position — and has since v0.85.0) or invent one (e9cbfc42 left a healthy
// TRUNCATE-tailed chain ending below EndPosition). Two gates, both smart-compaction only (naive compaction
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
// the chain's SOURCE engine's position order, which CompactChain resolves
// from the catalog through [CompactOpts.PositionOrder] (the CLI injects the
// registry; Postgres is the one engine that orders positions today). A
// chain whose engine has no order, a chain that records no engine, and a
// caller that injects no resolver all leave (B) unjudged — exactly as restore
// on that engine — and each says so at INFO and in
// [CompactResult.ShapeBJudged]; an engine the resolver does not know refuses.
// The pair arm of [involves] matters: a (B) pair can cross a merge-group
// boundary, one link rewritten and one not
// (TestSmartCompaction_CrossGroupPairShapeB). Encrypted chains are refused by
// smart compaction itself, so both gates read plaintext chunks with no key.

// prepareSmartGates resolves the source engine's position order once, from
// the catalog CompactChain already loaded, records whether shape (B) can be
// judged, and runs the pre-copy judge. opts.Comparator is set for the
// pre-swap belt that runs later.
func prepareSmartGates(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, planned []plannedGroup, opts *CompactOpts, res *CompactResult) error {
	cmp, judged, err := resolveSmartPositionOrder(ctx, cat.SourceEngine, opts)
	if err != nil {
		return err
	}
	opts.Comparator, res.ShapeBJudged = cmp, judged
	return refuseFindingsInRewrittenLinks(ctx, store, cat, planned, cmp)
}

// resolveSmartPositionOrder decides how smart compaction judges shape (B).
// No silent degrade: every way of NOT judging it is said at INFO and recorded
// in [CompactResult.ShapeBJudged], and the one way that would leave a
// judgeable chain unjudged without anyone deciding so — an engine this build
// does not know — refuses.
func resolveSmartPositionOrder(ctx context.Context, engine string, opts *CompactOpts) (cmp ir.PositionMonotonicChecker, judged bool, err error) {
	switch {
	case opts.Comparator != nil:
		return opts.Comparator, true, nil
	case opts.PositionOrder == nil:
		slog.InfoContext(ctx, "backup compact: smart compaction was given no position-order resolver, so shape (B) of the severed-transaction check is NOT judged on the incrementals it rewrites")
		return nil, false, nil
	case engine == "":
		slog.InfoContext(ctx, "backup compact: the chain records no source engine, so shape (B) of the severed-transaction check is NOT judged on the incrementals smart compaction rewrites")
		return nil, false, nil
	}
	cmp, known := opts.PositionOrder(engine)
	if !known {
		return nil, false, fmt.Errorf("backup compact: SMART-COMPACTION-SOURCE-ENGINE-UNKNOWN: the chain's source engine %q is not known to this sluice, so smart compaction cannot tell whether a re-delivered transaction (shape B) would be hidden by its rewrite; compact with --smart-compaction-off (naive compaction moves the evidence verbatim), or with a sluice that knows the engine", engine)
	}
	if cmp == nil {
		slog.InfoContext(ctx, "backup compact: the chain's source engine has no position order, so shape (B) of the severed-transaction check is NOT judged — the same as restore on this engine",
			slog.String("source_engine", engine))
		return nil, false, nil
	}
	return cmp, true, nil
}

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
				"to compact this chain anyway, use `--smart-compaction-off`: naive compaction moves the change chunks verbatim, so the evidence restore refuses on is kept; to get a chain that can be replayed, take a new full backup (`sluice backup full`) — this one carries a source transaction across two incrementals, and no rewrite of it replays exactly",
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
