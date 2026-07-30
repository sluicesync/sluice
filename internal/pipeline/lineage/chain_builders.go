// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"fmt"
	"sort"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ChainNotRestorableHint is the standalone remedy the structural-lineage
// refusal carries alongside its prose — the ONE definition both `restore`
// and `backup verify` report, so a script branching on the coded hint gets
// the same answer from either command.
const ChainNotRestorableHint = "this chain cannot be restored as it stands — read the underlying error, which names the shape and the repair. " +
	"A branching lineage is most often a FORK from two concurrent chain writers (overlapping `backup incremental` " +
	"crons); that repair is lossless. A prune/compact whose survivors no longer form one chain, and a crash between " +
	"an incremental's manifest write and its lineage.json append, produce the same shape"

// invalidLineage codes a STRUCTURAL refusal from the lineage walk — the
// chain does not hold together — with [sluicecode.CodeBackupManifestInvalid],
// which is the refusal class the docs table already publishes for it and the
// code `backup verify` already reports over the same directory.
//
// Bug 219 is what its absence cost. The walk's refusals predate the exit-3
// refusal taxonomy (they shipped v0.67.0; the taxonomy arrived v0.99.175 and
// seeded five call sites, none of them here), so restore reported a FORKED
// lineage — the shape two overlapping `backup incremental` crons produce —
// as an uncoded exit 1 while `backup verify` reported exit 3 with the code
// on the identical directory, and while restore itself reported exit 3 with
// that same code for the sibling shapes (schema-hash mismatch, BackupID
// recompute). One code, two machine contracts on one command, keyed on which
// shape raised it: loud to a human, invisible to the DR script that followed
// docs/operator/error-codes.md and branched on `SLUICE-E-*`.
//
// Every structural refusal in [BuildLineageChainFromCatalog] goes through
// this except two, both deliberate and both pinned by
// TestRestoreRefusalMachineContract_MatchesVerify:
//
//   - The two link READS. A store GET can fail transiently (an S3 5xx, a
//     timeout) and exit 3 promises "a re-run will not help", so this shape
//     keeps its uncoded connect-phase failure.
//   - [blobcodec.ValidateRecordedCodec]. `backup verify` reaches that same
//     check through [ListAllSegmentManifests] — its own pre-walk listing,
//     which runs BEFORE the walk and returns it uncoded — so coding it here
//     alone would leave restore at 3 and verify at 1 and simply INVERT the
//     asymmetry Bug 219 is. Coding it belongs with that listing, for every
//     command that reads through it, and that is a wider decision than this.
//
// Prose is untouched at every site: [sluicecode.Wrap] delegates Error(), so
// the human-facing message an operator reads is byte-identical.
func invalidLineage(err error) error {
	return sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid, ChainNotRestorableHint, err)
}

// ValidateBoundary is THE single boundary-monotonicity invariant used
// at every lineage adjacency — intra-segment (exact) and
// segment-to-segment (monotonic). A regressed or gapped boundary is a
// LOUD refusal, never a silent partial (ADR-0046 §3 / loud-failure
// tenet).
func ValidateBoundary(cmp ir.PositionMonotonicChecker, prevEnd, curStart ir.Position, exact bool, prevLabel, curLabel string) error {
	if prevEnd.Engine == "" && prevEnd.Token == "" {
		return nil // legacy v0.16 full with no recorded position
	}
	if exact {
		if curStart != prevEnd {
			return fmt.Errorf(
				"lineage boundary mismatch at %s: StartPosition %+v does not equal preceding %s EndPosition %+v — refusing to silently assemble a gapped/regressed restore (DR data)",
				curLabel, curStart, prevLabel, prevEnd,
			)
		}
		return nil
	}
	// Inter-segment: prevEnd must precede-or-equal curStart (no
	// regression). Exact contiguity is NOT required (S >= P_N).
	if curStart == prevEnd {
		return nil
	}
	if cmp != nil {
		le, err := cmp.PrecedesOrEqual(prevEnd, curStart)
		if err != nil {
			return fmt.Errorf("lineage boundary at %s: cannot prove monotonic vs preceding %s (DR data, refusing): %w",
				curLabel, prevLabel, err)
		}
		if !le {
			return fmt.Errorf(
				"lineage boundary REGRESSION at %s: StartPosition %+v precedes preceding %s EndPosition %+v — refusing to silently assemble a gapped/regressed restore (DR data)",
				curLabel, curStart, prevLabel, prevEnd,
			)
		}
		return nil
	}
	// No comparator: fall back to the structural same-engine guarantee
	// (the rotation FSM already hard-asserted S>=P_N at write time).
	if curStart.Engine != prevEnd.Engine {
		return fmt.Errorf("lineage boundary at %s: engine %q != preceding %s engine %q (DR data, refusing)",
			curLabel, curStart.Engine, prevLabel, prevEnd.Engine)
	}
	return nil
}

// ValidateFirstIncrementalBoundary validates a segment's full ->
// first-incremental boundary, tolerating the ADR-0067 overlap. A
// rotation-opened segment KEEPS the (P_N, S] window in its incrementals,
// so its first incremental legitimately starts at P_N, which PRECEDES
// the full's anchor (fullEnd == S). Two properties are checked:
//
//  1. INTEGRITY: the first incremental must start exactly at the
//     segment's recorded coverage start (coverageStart ==
//     IncrementalCoverageStart, or StartPosition when unset). This is
//     what lets the no-comparator path trust the overlap: the rotation
//     FSM hard-asserted P_N <= S when it recorded coverageStart = P_N at
//     write time, so a first incremental that matches coverageStart is
//     known-good even when restore can't re-order positions. It also
//     catches a tampered/corrupt first incremental. Prune keeps
//     coverageStart in sync when it trims leading incrementals (see
//     PruneChain), so this does not spuriously fire post-prune.
//  2. NO FORWARD GAP: the first incremental must start at-or-before the
//     full's end -- the (firstStart, fullEnd] overlap re-applies
//     idempotently on restore (ADR-0010); a first incremental AFTER the
//     full's end would leave (fullEnd, firstStart) uncovered (a silent
//     data gap -> loud refusal). For a never-rotated segment firstStart
//     == fullEnd and this is the historical exact match.
func ValidateFirstIncrementalBoundary(cmp ir.PositionMonotonicChecker, fullEnd, recordedCoverage, firstStart ir.Position, segLabel string) error {
	if fullEnd.Engine == "" && fullEnd.Token == "" {
		return nil // legacy full with no recorded position (historical tolerance)
	}
	// Where the first incremental must start:
	//   - rotated segment (recordedCoverage set): the kept-overlap start
	//     P_N (which precedes the full's anchor fullEnd == S);
	//   - never-rotated segment (recordedCoverage unset): the full's own
	//     end — the historical contiguous chain. We compare against the
	//     full MANIFEST's EndPosition (authoritative), NOT the catalog's
	//     StartPosition, which legacy/rebuilt catalogs may leave unset.
	rotated := recordedCoverage.Engine != "" || recordedCoverage.Token != ""
	expected := fullEnd
	if rotated {
		expected = recordedCoverage
	}
	if firstStart != expected {
		return fmt.Errorf(
			"lineage boundary mismatch at %s: first incremental StartPosition %+v does not equal the expected start %+v — refusing to silently assemble a gapped/regressed restore (DR data)",
			segLabel, firstStart, expected,
		)
	}
	if !rotated {
		return nil // exact match against the full's end — historical behavior
	}
	// Rotated: the kept (recordedCoverage, fullEnd] overlap re-applies
	// idempotently on restore (ADR-0010). Require no FORWARD gap — the
	// coverage start must be at-or-before the full's end; a coverage start
	// AHEAD of the full would leave (fullEnd, recordedCoverage) uncovered.
	if recordedCoverage == fullEnd {
		return nil
	}
	if cmp != nil {
		le, err := cmp.PrecedesOrEqual(recordedCoverage, fullEnd)
		if err != nil {
			return fmt.Errorf("lineage %s: cannot prove first incremental start %+v <= full end %+v (DR data, refusing): %w",
				segLabel, recordedCoverage, fullEnd, err)
		}
		if !le {
			return fmt.Errorf(
				"lineage boundary mismatch at %s: first incremental StartPosition %+v is AHEAD of the segment full's end %+v — a forward gap would lose events between them (DR data, refusing)",
				segLabel, recordedCoverage, fullEnd,
			)
		}
		return nil
	}
	// No comparator: the rotation FSM hard-asserted P_N <= S from the live
	// source at write time when it recorded recordedCoverage; require the
	// same engine as the structural guarantee.
	if recordedCoverage.Engine != fullEnd.Engine {
		return fmt.Errorf("lineage %s: first incremental engine %q != full engine %q (DR data, refusing)",
			segLabel, recordedCoverage.Engine, fullEnd.Engine)
	}
	return nil
}

// BuildLineageChain walks the lineage segment-by-segment and returns a
// flat ordered link list (each segment's full followed by its
// incrementals in chain order), validated by the SINGLE
// [ValidateBoundary] invariant at every adjacency — intra-segment and
// segment-to-segment alike. A malformed lineage (missing full,
// out-of-order, position regression/gap across any boundary, branching,
// cyclic) is a LOUD refusal — never a silent partial (ADR-0046 §3 /
// loud-failure tenet).
// cmp, when non-nil, is a [ir.PositionMonotonicChecker] for the
// lineage's SOURCE engine (typically the target engine when restoring
// same-engine — positions are then comparable). It enforces the
// inter-segment no-regression check; nil degrades to the structural
// same-engine guarantee (the rotation FSM already hard-asserted
// S>=P_N at write time via the live source engine).
func BuildLineageChain(ctx context.Context, store irbackup.Store, cmp ir.PositionMonotonicChecker) ([]SegmentRecord, error) {
	cat, err := ResolveLineage(ctx, store)
	if err != nil {
		return nil, err
	}
	return BuildLineageChainFromCatalog(ctx, store, cat, cmp)
}

// BuildLineageChainFromCatalog is [BuildLineageChain] against a SUPPLIED
// catalog instead of the one on disk. Same walk, same single
// [ValidateBoundary] invariant — the only difference is where the
// structural record comes from.
//
// It exists for the chain-maintenance readability gate (Bug 214): compact
// builds its post-compact catalog IN MEMORY and must prove the chain that
// catalog describes actually reads BEFORE it commits the swap, so a
// maintenance run that would leave an unreadable chain refuses while every
// pre-compaction byte is still on disk. Reading through the on-disk
// lineage.json cannot answer that question — the swap is what would put it
// there.
func BuildLineageChainFromCatalog(ctx context.Context, store irbackup.Store, cat *Catalog, cmp ir.PositionMonotonicChecker) ([]SegmentRecord, error) {
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return nil, invalidLineage(fmt.Errorf("lineage restorable_from_segment=%d out of range [0,%d) — corrupt lineage",
			cat.RestorableFromSegment, len(cat.Segments)))
	}

	var out []SegmentRecord
	var prevLink *SegmentRecord // last link of the previously-walked segment
	for si := cat.RestorableFromSegment; si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		// Uncoded on purpose — one of [invalidLineage]'s two carve-outs:
		// verify reaches this same check uncoded through its own pre-walk
		// [ListAllSegmentManifests], so coding it here alone would invert
		// Bug 219's asymmetry instead of closing it.
		if err := blobcodec.ValidateRecordedCodec(seg.Codec); err != nil {
			return nil, err
		}
		ss := seg.Store(store)

		// Segment full. The READ failure is the other carve-out: a store GET
		// can fail transiently and exit 3 says "a re-run will not help".
		// Everything below it is structural.
		fm, err := ReadManifestAt(ctx, ss, seg.FullManifestPath)
		if err != nil {
			return nil, fmt.Errorf("segment %d (%s) full %q: %w", si, seg.SegmentID, seg.FullManifestPath, err)
		}
		if k := CanonicalKind(fm.Kind); k != irbackup.BackupKindFull {
			return nil, invalidLineage(fmt.Errorf("segment %d (%s) full manifest %q has kind %q; expected full",
				si, seg.SegmentID, seg.FullManifestPath, fm.Kind))
		}
		fullRec := SegmentRecord{
			ManifestRecord: ManifestRecord{Path: seg.FullManifestPath, Manifest: fm},
			Segment:        seg,
		}
		// Segment-to-segment boundary (MONOTONIC, not exact): the
		// prior segment's last link EndPosition (P_N) must
		// precede-or-equal this segment's recorded StartPosition (S
		// from lineage.json — the rotation anchor, NOT the full
		// manifest's empty StartPosition field). SAME validator as the
		// intra-segment boundary below, `exact=false`.
		if prevLink != nil {
			if err := ValidateBoundary(cmp, prevLink.Manifest.EndPosition, seg.IncrementalCoverageStartOrStart(), false,
				fmt.Sprintf("segment %d last link %s", si-1, ManifestBackupID(prevLink.Manifest)),
				fmt.Sprintf("segment %d (%s) incremental coverage start", si, seg.SegmentID)); err != nil {
				return nil, invalidLineage(err)
			}
		}
		out = append(out, fullRec)
		prevLink = &out[len(out)-1]

		// Intra-segment incremental chain. The lineage records the
		// ordered incremental paths; validate the parent-link + the
		// SAME boundary invariant against the running prev link.
		seenInc := make(map[string]string, len(seg.Incrementals))
		parentID := ManifestBackupID(fm)
		for ii, ip := range seg.Incrementals {
			im, err := ReadManifestAt(ctx, ss, ip)
			if err != nil {
				return nil, fmt.Errorf("segment %d (%s) incremental %q: %w", si, seg.SegmentID, ip, err)
			}
			if k := CanonicalKind(im.Kind); k != irbackup.BackupKindIncremental {
				return nil, invalidLineage(fmt.Errorf("segment %d incremental %q has kind %q; expected incremental",
					si, ip, im.Kind))
			}
			id := ManifestBackupID(im)
			if prevPath, dup := seenInc[id]; dup {
				return nil, invalidLineage(fmt.Errorf("segment %d duplicate incremental BackupID %q (paths %q and %q)",
					si, id, prevPath, ip))
			}
			seenInc[id] = ip
			if im.ParentBackupID != "" && im.ParentBackupID != parentID {
				// Cause + recovery, in that order. The pre-2026-07-29 wording
				// advised "if a prune/compact produced it, the surviving links
				// no longer form one chain" — and the shape that actually
				// produces this in the field is neither: two overlapping
				// `backup incremental` crons each committing off the same
				// parent. Sending an operator to audit a maintenance run that
				// never happened is the v0.104.3 false-accusation class again,
				// so the concurrent-writer fork is named FIRST and the
				// (genuinely lossless) repair is named at all.
				return nil, invalidLineage(fmt.Errorf("segment %d incremental %q parent %q does not chain off preceding link %q — "+
					"branching/mis-stitched lineage. Most likely a FORK: two chain writers (overlapping `backup "+
					"incremental` cron entries, or a `backup incremental` racing a `backup stream`) each committed "+
					"an incremental off the same parent — pre-v0.104.7 both such runs exited 0. A crash between an "+
					"incremental's manifest write and its lineage.json append, or a `backup prune`/`backup compact` "+
					"whose survivors no longer form one chain, produce the same shape. "+
					"REPAIR (lossless for the fork case — the siblings captured the same window): remove the "+
					"ORPHANED sibling's entry from lineage.json, keeping whichever sibling the LATER incrementals "+
					"chain off, re-upload it, and re-run `backup verify` before relying on the chain",
					si, ip, im.ParentBackupID, parentID))
			}
			if ii == 0 {
				// Full -> first-incremental boundary. ADR-0067: a
				// rotation-opened segment KEEPS the (P_N, S] overlap, so
				// its first incremental starts at IncrementalCoverageStart
				// (P_N), which PRECEDES the full's anchor (full.End == S).
				// Tolerate that backward overlap (it re-applies
				// idempotently on restore); refuse only a FORWARD gap
				// (first incremental starting AFTER the full's end would
				// lose events). For a never-rotated segment the coverage
				// start == full.End and this is the historical exact match.
				if err := ValidateFirstIncrementalBoundary(cmp, fm.EndPosition, seg.IncrementalCoverageStart, im.StartPosition,
					fmt.Sprintf("segment %d (%s)", si, seg.SegmentID)); err != nil {
					return nil, invalidLineage(err)
				}
			} else if err := ValidateBoundary(cmp, prevLink.Manifest.EndPosition, im.StartPosition, true,
				fmt.Sprintf("segment %d link %d", si, ii),
				fmt.Sprintf("segment %d incremental %s", si, id)); err != nil {
				return nil, invalidLineage(err)
			}
			out = append(out, SegmentRecord{
				ManifestRecord: ManifestRecord{Path: ip, Manifest: im},
				Segment:        seg,
			})
			prevLink = &out[len(out)-1]
			parentID = id
		}
	}
	return out, nil
}

// SameEngineComparator returns eng as an [ir.PositionMonotonicChecker]
// IFF eng implements it AND eng.Name() matches the lineage's recorded
// source engine (positions are engine-native — a MySQL target cannot
// order PG LSNs). Otherwise nil (the inter-segment check degrades to
// the structural guarantee; the write-time S>=P_N hard-fail remains
// the authoritative monotonicity gate). Best-effort: a lineage-read
// hiccup yields nil rather than failing the restore here (the
// subsequent BuildLineageChain surfaces real lineage errors).
func SameEngineComparator(ctx context.Context, store irbackup.Store, eng ir.Engine) ir.PositionMonotonicChecker {
	chk, ok := eng.(ir.PositionMonotonicChecker)
	if !ok {
		return nil
	}
	cat, err := ResolveLineage(ctx, store)
	if err != nil || cat.SourceEngine == "" || cat.SourceEngine != eng.Name() {
		return nil
	}
	return chk
}

// BuildBrokerChain is the backup-broker (Phase 4.5) entry point. It
// walks the full lineage — single OR multi-segment — and returns the
// flat link list. The broker's apply loop skips every link whose
// manifest Kind is BackupKindFull (`broker.go::replayNewIncrementals`),
// so segment-N+1's rotation full is auto-skipped and the broker
// continues with the new segment's incremental tail. ADR-0067's
// born-contiguous rotation guarantees the new segment's first
// incremental covers the `(P_N, S]` overlap from the prior segment's
// end position; ADR-0010's idempotent applier handles the brief
// re-application of changes that landed before the broker last
// advanced its `last_applied_backup_id`. Phase 4.5 originally deferred
// multi-segment broker following pending validation that the existing
// chain-walker + idempotent-applier infrastructure actually covered
// the seam; Round D's soak (2026-05-31) characterized the gap and
// this commit closes it. Same comparator semantics as the single-
// segment path — nil is fine because the rotation FSM's write-time
// S>=P_N hard-assert is the authoritative monotonicity gate.
// Broker call sites reach this through [brokerChainCache], which
// memoizes the walk across ticks so an idle tick is O(1) store GETs
// instead of O(chain-length).
func BuildBrokerChain(ctx context.Context, store irbackup.Store) ([]SegmentRecord, error) {
	return BuildLineageChain(ctx, store, nil)
}

// DetectAmbiguousDeltas returns a non-nil error when the slice
// contains a clearly unsupportable pattern (today: drop+add of the
// same column name within a single incremental, which the design
// doc names as ambiguous and recommends "force fresh full").
func DetectAmbiguousDeltas(deltas []*irbackup.SchemaDeltaEntry) error {
	for _, d := range deltas {
		if d.Kind != irbackup.SchemaDeltaAlterTable {
			continue
		}
		if d.Before == nil || d.After == nil {
			continue
		}
		// Build column-name sets for before / after.
		bef := make(map[string]bool, len(d.Before.Columns))
		for _, c := range d.Before.Columns {
			bef[c.Name] = true
		}
		aft := make(map[string]bool, len(d.After.Columns))
		for _, c := range d.After.Columns {
			aft[c.Name] = true
		}
		// "Drop+add of the same name" wouldn't show up at the
		// incremental boundary (the diff only sees the start and end
		// shape). The genuine ambiguous case is a column-rename: a
		// before column missing from after AND an after column missing
		// from before, with different names. Surface that as
		// ambiguous so the operator can disambiguate.
		var dropped, added []string
		for name := range bef {
			if !aft[name] {
				dropped = append(dropped, name)
			}
		}
		for name := range aft {
			if !bef[name] {
				added = append(added, name)
			}
		}
		// Single drop + single add is the rename ambiguity. Multiple
		// of either is a more complex shape that's still
		// data-dependent; for v1 we stay conservative and refuse just
		// the rename pattern.
		if len(dropped) == 1 && len(added) == 1 {
			sort.Strings(dropped)
			sort.Strings(added)
			return fmt.Errorf(
				"table %q has dropped column %q and added column %q within one incremental window; ambiguous (rename vs independent edits)",
				d.Table, dropped[0], added[0],
			)
		}
	}
	return nil
}

// AddedColumns returns the columns in after but not in before,
// preserving after's declaration order.
func AddedColumns(before, after *ir.Table) []*ir.Column {
	if after == nil {
		return nil
	}
	beforeNames := map[string]bool{}
	if before != nil {
		for _, c := range before.Columns {
			beforeNames[c.Name] = true
		}
	}
	out := make([]*ir.Column, 0, len(after.Columns))
	for _, c := range after.Columns {
		if !beforeNames[c.Name] {
			out = append(out, c)
		}
	}
	return out
}

// ManifestBackupID returns m's stored or computed BackupID. Pre-
// Phase-3 manifests have an empty BackupID; we compute it on demand
// so chain code can identify links uniformly.
func ManifestBackupID(m *irbackup.Manifest) string {
	if m == nil {
		return ""
	}
	if m.BackupID != "" {
		return m.BackupID
	}
	return irbackup.ComputeBackupID(m)
}
