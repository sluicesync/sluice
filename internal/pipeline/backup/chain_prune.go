// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// # `sluice backup prune` — lineage retention pruning (ADR-0046 §4)
//
// Reframed (zero users) from the grafted "parent-link re-stitch +
// tombstone bookkeeping" onto the native segment-list model. Prune is
// now a list/segment-local operation:
//
//   - Drop leading WHOLE segments (their full + every incremental):
//     a segment full is a self-contained snapshot at its anchor S, so
//     the oldest KEPT segment's full is always a complete restore
//     base — dropping older whole segments is unconditionally
//     restore-safe. This is the ONLY shape prune ever commits.
//   - NEVER drop leading incrementals WITHIN the oldest kept segment:
//     that SEVERS the chain. The claim that used to stand here — "the
//     segment full + the remaining incrementals still form a
//     contiguous chain (the boundary invariant prune always
//     preserved)" — was false in both of its senses: the trim breaks
//     the parent-link chain (the first survivor still records a parent
//     this prune deletes) AND the position boundary (the full stays
//     anchored at S while the first survivor starts at S′ > S, so the
//     events in (S, S′] are gone). The re-stitch that once preserved
//     the parent link was removed in the segment-list reframe and
//     nothing replaced it.
//   - Advance `lineage.json`'s `restorable_from_segment` floor;
//     restore refuses to start before it.
//
// The open (last, uncapped) segment is never fully dropped while it's
// the only segment, and its incrementals are pruned only down to its
// full (the full is always kept — it's the restore base).
//
// # Retention is SEGMENT-granular (roadmap item 100's contract call)
//
// `--keep-incrementals=N` and `--keep-duration` are rounded UP to the
// nearest segment boundary ([segmentAlignedKeep]) before anything is
// planned, so prune can only ever retire whole leading segments. The
// operator's number becomes approximate in the SAFE direction — never
// fewer incrementals than asked, sometimes more — and the run says so at
// INFO with both numbers, because silent over-retention is how "my disk
// did not shrink" starts.
//
// Two alternatives were weighed and rejected. Re-anchoring the floor
// segment's full would make retention exact, but it turns a maintenance
// command into one that reads the SOURCE — surprising and expensive for
// something an operator runs from cron. Refusing every in-place trim
// (the item-95 interim behaviour) is honest, but it leaves the headline
// retention flag unable to succeed on the most common chain shape: a
// single-segment chain, where NO `--keep-incrementals` value can retire
// the segment's base.
//
// Rounding up can leave NOTHING to drop, when the chain has no boundary
// at or above the requested retention — always the case on a
// never-rotated chain, which has no boundary at all. That one case
// refuses, under [pruneTrim.withinSegmentTrimRefusal]: retaining the
// whole chain and reporting a successful prune is the silent no-op the
// INFO line above exists to prevent.
//
// What prune physically deletes: every dropped incremental's change
// chunks + manifest, every dropped whole segment's full manifest + data
// chunks + sub-dir contents, and — for each retired manifest — its detached
// ADR-0154 `.sig` sibling ([retireManifest], audit C-8). lineage.json is
// rewritten in one atomic Put. What it preserves: the oldest kept segment's
// full + every kept incremental + every later segment — and, when the
// dropped segment is the lineage ROOT, the BODY of its `manifest.json`
// ([keepsChainIdentity], roadmap item 95), which is kept for the chain's
// key material and whose signature is retired with the segment.

// prunerOp is the operator-facing verb the readability gate and its
// pre/post-sweep decorators name in their refusals — the CLI command,
// not the package-internal "prune:" error prefix, because the operator
// reading the refusal is holding a `sluice backup prune` invocation.
const prunerOp = "backup prune"

// PruneOpts configures [PruneChain]. Exactly one of KeepIncrementals
// or KeepDuration is required; specifying both is an error.
type PruneOpts struct {
	// KeepIncrementals retains the N most-recent incrementals across
	// the whole lineage. Segment fulls are not counted; the oldest
	// kept incremental's segment full (and every later segment) is
	// always retained as the restore base. Zero means "use
	// KeepDuration".
	KeepIncrementals int

	// KeepDuration retains incrementals whose CreatedAt is newer than
	// (now - duration). Zero means "use KeepIncrementals".
	KeepDuration time.Duration

	// DryRun reports what would be pruned without deleting anything or
	// rewriting lineage.json.
	DryRun bool

	// Now overrides the wall-clock time source for KeepDuration math.
	Now func() time.Time

	// Signer, when non-nil, re-signs the surviving manifests + lineage
	// after a prune of a SIGNED chain (ADR-0154 Q4). Pruning renumbers
	// link positions, so every survivor must be re-signed. When the chain
	// is signed and Signer is nil, prune REFUSES rather than leave a
	// signed chain with stale-position signatures / an unsigned lineage.
	Signer *lineage.Signer

	// Envelope, when non-nil, is the operator's READ envelope for an
	// encrypted chain. Prune never encrypts or decrypts anything — this is
	// consumed only by the readability gate ([verifyChainReadable], shared
	// verbatim with compact), which uses it to prove the pruned chain's CEK
	// still unwraps before and after the destructive sweep. nil (no
	// `--encrypt`) degrades the gate to its identity-only check with a
	// WARN; an encrypted chain has always been prunable without key
	// material and stays so.
	Envelope crypto.EnvelopeEncryption
}

// PruneResult summarises a [PruneChain] run.
type PruneResult struct {
	// Pruned is the list of manifest paths deleted (or, under DryRun,
	// that would be deleted). Paths are segment-Dir-qualified.
	Pruned []string

	// Kept is the list of manifest paths preserved.
	Kept []string

	// ChunksDeleted is the count of chunk files deleted across all
	// pruned manifests. 0 on DryRun.
	ChunksDeleted int

	// SegmentsDropped is the number of whole leading segments removed.
	SegmentsDropped int

	// EarliestRestorableBackupID identifies the first kept incremental
	// (or the oldest kept segment's full when no incrementals remain).
	// After prune, restore-to-position-X requires X >= this entry's
	// StartPosition.
	EarliestRestorableBackupID string

	// RequestedIncrementals is how many incrementals the operator's flag
	// asked to retain (derived from the cutoff for --keep-duration);
	// IncrementalsRetained is how many segment-granular retention
	// actually kept. They differ when the requested boundary fell inside
	// a segment and was rounded UP — see the package doc. Reported so a
	// caller can surface the over-retention rather than leave an operator
	// wondering why the number moved; DryRun reports the ROUNDED plan,
	// exactly as the real run would.
	RequestedIncrementals int
	IncrementalsRetained  int
}

// lineageIncr is one incremental in lineage-flattened order, tagged
// with its owning segment index + in-segment index so the pruner can
// translate a flat keep/drop decision back into per-segment edits.
type lineageIncr struct {
	segIdx   int
	inSegIdx int
	path     string
	manifest *irbackup.Manifest
}

// PruneChain executes a retention prune against the lineage in store.
// See package doc for semantics. Returns the summary or a wrapped
// error on any pre-flight refusal / I/O failure.
func PruneChain(ctx context.Context, store irbackup.Store, opts PruneOpts) (*PruneResult, error) {
	if (opts.KeepIncrementals > 0) == (opts.KeepDuration > 0) {
		return nil, errors.New("prune: exactly one of KeepIncrementals or KeepDuration is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	// Load FOR UPDATE (ADR-0160): the catalog commit below is a CAS on
	// the chain write-generation observed here, so a backup / compact
	// landing mid-prune conflicts loudly instead of being clobbered.
	cat, ok, err := lineage.LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("prune: load lineage catalog: %w", err)
	}
	if !ok {
		return nil, errors.New("prune: lineage.json not found; run `sluice backup verify --rebuild-catalog` first")
	}
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return nil, fmt.Errorf("prune: lineage restorable_from_segment=%d out of range — corrupt lineage", cat.RestorableFromSegment)
	}

	// Bug 243 maintenance door: pruning a chain whose restore is already
	// broken must not be silent (WARN, never a refusal).
	warnIfChainRecordedSchemaMalformed(ctx, "prune", store, cat)

	// ADR-0154 Q4: a signed chain must be re-signed after prune (positions
	// renumber). Refuse loudly when we cannot — never leave a signed chain
	// with an unsigned/stale-signature successor.
	signed, err := lineage.ChainIsSigned(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("prune: probe signed chain: %w", err)
	}
	if err := refuseUnsignableMaintenance("prune", signed, opts.DryRun, opts.Signer); err != nil {
		return nil, err
	}

	// Flatten the currently-restorable incrementals across segments,
	// oldest first. (Reads each incremental manifest for CreatedAt.)
	var flat []lineageIncr
	for si := cat.RestorableFromSegment; si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		ss := seg.Store(store)
		for ii, ip := range seg.Incrementals {
			m, err := lineage.ReadManifestAt(ctx, ss, ip)
			if err != nil {
				return nil, fmt.Errorf("prune: read incremental %q (segment %d): %w", ip, si, err)
			}
			flat = append(flat, lineageIncr{segIdx: si, inSegIdx: ii, path: ip, manifest: m})
		}
	}
	// Defensive stable order by CreatedAt (segments are already
	// ordered; this guards a clock-skew edge).
	sort.SliceStable(flat, func(i, j int) bool {
		if flat[i].segIdx != flat[j].segIdx {
			return flat[i].segIdx < flat[j].segIdx
		}
		return flat[i].inSegIdx < flat[j].inSegIdx
	})

	// Compute how many leading incrementals to drop.
	dropN := 0
	switch {
	case opts.KeepIncrementals > 0:
		if opts.KeepIncrementals >= len(flat) {
			// No-op door 1 of 2 (the other is the keep-duration r0 below; the
			// dry-run return is exempt — read-only by contract). A crashed
			// prune — catalog committed, resignIfSigned never ran — re-run
			// with the key lands on an already-pruned chain and returns here;
			// heal the stale signatures so the published "re-run the
			// maintenance step" remedy is true (see healStaleLineageSignatures).
			return pruneNoOpReturn(ctx, store, cat, opts, "nothing to prune (keep >= incremental count)")
		}
		dropN = len(flat) - opts.KeepIncrementals
	case opts.KeepDuration > 0:
		threshold := now().Add(-opts.KeepDuration)
		for _, e := range flat {
			if e.manifest.CreatedAt.Before(threshold) {
				dropN++
				continue
			}
			break
		}
		if dropN == 0 {
			// No-op door 2 of 2 — see the keep-count r0 above.
			return pruneNoOpReturn(ctx, store, cat, opts, "nothing to prune (all incrementals newer than keep-duration)")
		}
	}

	// Segment-granular retention (roadmap item 100): round the requested
	// retention UP to the nearest segment boundary, so the plan below can
	// only ever retire whole leading segments. Rounding the other way
	// would trim leading incrementals INSIDE the floor segment, which
	// severs it from its restore base.
	requestedDrop, requestedKeep := dropN, len(flat)-dropN
	keepN := segmentAlignedKeep(flat, requestedKeep)
	dropN = len(flat) - keepN

	dropped := flat[:dropN]
	kept := flat[dropN:]

	// The oldest kept incremental's segment is the new restore floor.
	// Every segment strictly before it is dropped WHOLE (full + all
	// incrementals): a segment full is a self-contained snapshot, so
	// the kept floor segment's full is a complete restore base.
	floorSeg := cat.RestorableFromSegment
	if len(kept) > 0 {
		floorSeg = kept[0].segIdx
	} else if len(dropped) > 0 {
		// Everything dropped: keep the LAST segment (its full is the
		// restore base; never drop the only restore anchor).
		floorSeg = len(cat.Segments) - 1
	}

	// Rounding up can round the whole run away: no segment boundary sits
	// at or above the requested retention, so the plan retains everything
	// and the floor has not moved. (A chain whose leading segments hold
	// no incrementals at all is the exception that makes the floor check
	// load-bearing — dropping those is still a real prune.) Reporting
	// success over a run that deleted nothing is the silent no-op this
	// contract exists to avoid, so it keeps item 100's refusal — and it
	// fires for --dry-run too, because a dry run that promises a prune
	// the real run refuses is worse than no dry run.
	if dropN == 0 && floorSeg == cat.RestorableFromSegment {
		survivor := flat[requestedDrop]
		seg := &cat.Segments[survivor.segIdx]
		trim := pruneTrim{
			floor: seg, floorStore: seg.Store(store),
			flat: flat, keepFromInSeg: survivor.inSegIdx,
			keepCount: opts.KeepIncrementals, keepDuration: opts.KeepDuration,
		}
		return nil, trim.withinSegmentTrimRefusal(ctx, survivor)
	}
	if keepN > requestedKeep {
		slog.InfoContext(
			ctx, "prune: retained MORE than requested — the retention boundary was rounded up to a segment boundary",
			slog.String("requested_retention", requestedRetention(opts.KeepIncrementals, opts.KeepDuration)),
			slog.Int("requested_incrementals_retained", requestedKeep),
			slog.Int("incrementals_retained", keepN),
			slog.String("reason", "prune drops only WHOLE leading segments: trimming leading incrementals inside a segment would sever it from its restore base (roadmap item 100), so retention rounds UP to the nearest segment boundary"),
		)
	}

	res := &PruneResult{
		Pruned:                make([]string, 0, len(dropped)),
		RequestedIncrementals: requestedKeep,
		IncrementalsRetained:  keepN,
	}

	// Deletion boundaries. The physical deletes themselves run AFTER the
	// catalog commit below (ADR-0160) — see the post-commit delete pass.
	floor := &cat.Segments[floorSeg]
	floorStore := floor.Store(store)
	keepFromInSeg := 0
	if len(kept) > 0 && kept[0].segIdx == floorSeg {
		keepFromInSeg = kept[0].inSegIdx
	} else if len(kept) == 0 {
		// Everything dropped — keep the floor segment's full only.
		keepFromInSeg = len(floor.Incrementals)
	}
	// The invariant the whole contract rests on, asserted rather than
	// assumed: the committed plan must never trim inside the floor
	// segment while leaving later incrementals there. The two shapes that
	// remain are exactly the restore-safe ones — keepFromInSeg == 0 (the
	// floor segment is untouched and its full is a self-contained base)
	// and an empty survivor set (the floor keeps only its full, so there
	// is no first-incremental boundary left to violate). Unreachable
	// after [segmentAlignedKeep]; it is here because the failure it
	// guards is an unrestorable DR archive discovered at restore time.
	if keepFromInSeg > 0 && len(kept) > 0 {
		return nil, fmt.Errorf(
			"prune: internal invariant violated: the plan would trim %d of the floor segment's %d incrementals in place, which severs the chain (roadmap item 100); refusing before anything is written",
			keepFromInSeg, len(floor.Incrementals),
		)
	}
	// Snapshot the pre-prune catalog shape for the post-commit delete
	// pass: cat.Segments is reassigned below, but the helpers enumerate
	// the ORIGINAL segments/incrementals being dropped. Shallow copy is
	// enough — the original backing array is never mutated.
	origCat := *cat

	// Build the post-prune lineage: drop whole leading segments, trim
	// the floor segment's incrementals, advance the restore floor.
	newSegs := make([]lineage.Segment, 0, len(cat.Segments)-(floorSeg-cat.RestorableFromSegment))
	for si := floorSeg; si < len(cat.Segments); si++ {
		seg := cat.Segments[si]
		if si == floorSeg {
			seg.Incrementals = append([]string(nil), floor.Incrementals[keepFromInSeg:]...)
		}
		newSegs = append(newSegs, seg)
		// Kept-path bookkeeping for the result.
		res.Kept = append(res.Kept, segQualify(seg.Dir, seg.FullManifestPath))
		for _, ip := range seg.Incrementals {
			res.Kept = append(res.Kept, segQualify(seg.Dir, ip))
		}
	}
	// ADR-0067: the floor segment's earliest incremental coverage is what
	// the restore-time within-segment integrity check
	// (validateFirstIncrementalBoundary) validates against. Segment-
	// granular retention leaves a kept floor segment's incrementals
	// untouched, so its recorded value stays correct; the one case that
	// moves is retiring them all, which leaves no coverage at all.
	if len(newSegs) > 0 && len(kept) == 0 {
		// The floor segment retains only its full, so there is no
		// incremental coverage — clear the field (it resolves to
		// StartPosition, the full anchor).
		newSegs[0].IncrementalCoverageStart = ir.Position{}
	}

	cat.Segments = newSegs
	cat.RestorableFromSegment = 0

	if len(kept) > 0 {
		res.EarliestRestorableBackupID = lineage.ManifestBackupID(kept[0].manifest)
	} else if len(newSegs) > 0 {
		// No incrementals kept — the floor segment's full is the
		// earliest restorable point.
		if fm, err := lineage.ReadManifestAt(ctx, newSegs[0].Store(store), newSegs[0].FullManifestPath); err == nil {
			res.EarliestRestorableBackupID = lineage.ManifestBackupID(fm)
		}
	}

	if opts.DryRun {
		// Enumerate (without deleting) what a real run would drop. Neither
		// helper touches storage under DryRun, so neither can fail here.
		_ = pruneWholeSegments(ctx, store, &origCat, floorSeg, true, res)
		_ = pruneFloorLeadingIncrementals(ctx, floor, floorStore, keepFromInSeg, true, res)
		return res, nil
	}
	// The readability gate's FIRST leg (Bug 214 / item 95), against the
	// prospective post-prune catalog and BEFORE the commit. Its placement
	// is the point: prune's deletes all run after the commit, so a refusal
	// here has written no catalog and removed no file — the chain is
	// exactly as restorable as it was when the run started.
	//
	// Roadmap item 100's within-segment trim no longer reaches here — the
	// retention rounding above makes it unplannable, and it carries its
	// own refusal at that point. What remains is the gate's original
	// catch: a missing chain-root manifest, an identity/key mismatch, a
	// structural break the prune did not cause. Those keep the generic
	// decoration, whose recovery is a `cp` of a surviving header rather
	// than a retention change.
	if err := verifyChainReadable(ctx, store, cat, opts.Envelope, prunerOp, "pre-commit"); err != nil {
		return nil, errPreSweepReadability(prunerOp, err)
	}
	// Catalog commit FIRST (the ADR-0160 CAS linearization point, and the
	// same commit-then-sweep order compaction uses): the loud concurrent-
	// writer refusal — or a crash — before this write leaves the chain
	// byte-untouched, and after it leaves only orphaned (already-
	// uncatalogued) files for the delete pass below. The pre-ADR-0160
	// order deleted first, so a failed catalog write stranded a catalog
	// referencing deleted manifests.
	cat.UpdatedAt = now().UTC()
	if err := lineage.WriteLineageCatalog(ctx, store, cat); err != nil {
		return nil, fmt.Errorf("prune: rewrite lineage catalog: %w", err)
	}
	// ADR-0154: re-sign the survivor set at its new positions + lineage.
	if err := resignIfSigned(ctx, store, signed, opts.Signer); err != nil {
		return nil, fmt.Errorf("prune: re-sign pruned chain: %w", err)
	}
	// Post-commit delete pass: everything the new catalog no longer
	// references.
	sweepPrunedOrphans(ctx, store, &origCat, floorSeg, floor, floorStore, keepFromInSeg, res)
	// The gate's SECOND leg, against what is actually on disk. It cannot
	// un-delete; its entire job is to stop a retention run reporting
	// success over a chain it has just made unreadable — the difference
	// between an operator who knows their DR archive is broken and one who
	// finds out at restore time.
	if err := verifyChainReadable(ctx, store, cat, opts.Envelope, prunerOp, "post-sweep"); err != nil {
		return nil, errPostSweepReadability(prunerOp, err)
	}
	slog.InfoContext(
		ctx, "prune: lineage pruned",
		slog.Int("segments_dropped", res.SegmentsDropped),
		slog.Int("manifests_dropped", len(res.Pruned)),
		slog.Int("chunks_deleted", res.ChunksDeleted),
	)
	return res, nil
}

// sweepPrunedOrphans is prune's post-commit delete pass: every manifest,
// `.sig` sibling and chunk the new catalog no longer references.
//
// A per-file failure is NOT a run failure. The catalog commit is the
// linearization point, so by the time this runs the chain is already
// correctly shaped and every object below is uncatalogued — nothing
// resolves it: restore dispatches through the catalog
// ([verifyLineageNeedsWalk]), and the one reader that reaches a manifest
// without the catalog ([lineage.ResolveLineage]'s legacy single-segment
// synthesis) refuses loudly the moment it sees a `seg-*` dir. Returning an
// error would tell an operator their retention run failed when it succeeded
// and merely leaked disk.
//
// It is NOT nothing either, which is the half that was missing (audit F-8).
// Pre-fix every delete error was discarded with `_ =`, so a store that
// silently refused deletes — an expired credential, an object-lock/WORM
// retention policy, a read-only mount — produced a prune that reported
// segments dropped and manifests pruned while the bytes stayed. That is how
// B-6's "restore fails on a missing chunk" became "restore silently returns
// OLDER data": the retired segment's chunks were still there for the
// mis-dispatched read to find. The dispatch bug is fixed, but a maintenance
// command must still say when its sweep did not happen, so the failures are
// collected and named at WARN — the same posture, and the same reasoning,
// as compaction's [sweepOneSupersededSegment]. Pinned by
// TestPruneChain_OrphanSweepDeleteFailure_Warns.
func sweepPrunedOrphans(
	ctx context.Context,
	store irbackup.Store,
	origCat *lineage.Catalog,
	floorSeg int,
	floor *lineage.Segment,
	floorStore irbackup.Store,
	keepFromInSeg int,
	res *PruneResult,
) {
	sweepErr := errors.Join(
		pruneWholeSegments(ctx, store, origCat, floorSeg, false, res),
		pruneFloorLeadingIncrementals(ctx, floor, floorStore, keepFromInSeg, false, res),
	)
	if sweepErr == nil {
		return
	}
	slog.WarnContext(
		ctx, "prune: orphan sweep incomplete — retired backup files remain in the store (the chain is correctly pruned; this is leaked disk, not lost data). A store that refuses deletes — expired credentials, an object-lock/WORM retention policy, a read-only mount — will repeat this every run",
		slog.Int("chunks_deleted", res.ChunksDeleted),
		slog.String("error", sweepErr.Error()),
	)
}

// pruneTrim is the retention the operator REQUESTED, in the terms the
// refusal needs: which segment the request lands in, what it holds, and
// which flag expressed it. It describes the plan segment-granular
// rounding declined to commit, never the one that runs. It exists so
// [PruneChain] can hand the refusal ONE value instead of five positional
// arguments.
type pruneTrim struct {
	floor         *lineage.Segment
	floorStore    irbackup.Store
	flat          []lineageIncr
	keepFromInSeg int

	keepCount    int           // --keep-incrementals; 0 when unused
	keepDuration time.Duration // --keep-duration; 0 when unused
}

// withinSegmentTrimRefusal is roadmap item 100's purpose-built refusal:
// the requested retention would trim LEADING incrementals inside the
// floor segment, and rounding it up to a segment boundary reaches no
// boundary at all — so honouring it segment-granularly would retain the
// whole chain and delete nothing.
//
// It is unconditional by design. The earlier version returned nil when
// it could not PROVE the sever from the survivor's parent link, because
// it decorated a readability-gate failure that already had prose of its
// own; this one fires ahead of any gate, on retention arithmetic that is
// fully known, and there is no second refusal behind it to fall through
// to. Silently succeeding over a run that deletes nothing is the outcome
// the whole contract exists to avoid.
func (p pruneTrim) withinSegmentTrimRefusal(ctx context.Context, survivor lineageIncr) error {
	fullID := "(unreadable)"
	if fm, err := lineage.ReadManifestAt(ctx, p.floorStore, p.floor.FullManifestPath); err == nil {
		fullID = lineage.ManifestBackupID(fm)
	}
	return sluicecode.Wrap(sluicecode.CodeBackupChainUnreadable, withinSegmentTrimHint,
		errors.New(p.withinSegmentTrimProse(survivor, fullID)))
}

// withinSegmentTrimHint is the standalone machine-readable remedy that
// rides alongside the prose (the `hint` slog attribute). Item 100 is the
// THIRD refusal in this arc whose human half was right and whose
// actionable half was missing (items 91 and 96 were the others), so the
// remedy is not optional decoration here — it is the finding.
const withinSegmentTrimHint = "retention is SEGMENT-granular: prune rounds a keep-count UP to the nearest segment boundary, and this chain has none at or above what you asked for, so rounding up would retain everything. Use one of the boundary keep-counts the refusal lists, or a --keep-duration cutoff older than every incremental in the chain. A never-rotated chain has no boundary at all, so --keep-incrementals cannot express retention there. Nothing was deleted."

// withinSegmentTrimProse renders the refusal an operator reads. It has
// three jobs and each is load-bearing: name the SHAPE (a within-segment
// incremental trim, not "the chain does not walk"), state that nothing
// was deleted and the chain is exactly as restorable as before, and name
// the retention that IS safe on THIS chain — computed, not generic.
func (p pruneTrim) withinSegmentTrimProse(survivor lineageIncr, fullID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: refusing %s: it would trim %d of the floor segment's %d incrementals IN PLACE, and a within-segment incremental trim severs the chain. ",
		prunerOp, requestedRetention(p.keepCount, p.keepDuration), p.keepFromInSeg, len(p.floor.Incrementals))
	fmt.Fprintf(&b, "A segment's full is a restore base only together with the incrementals that continue from it: trimming the leading ones leaves the floor segment's full (%s, at %q) anchored where it is while the first survivor %q still records parent %q — a link this very prune would delete — so the events between them are gone. The restore path refuses the result, on the severed parent link and again on the forward position gap. ",
		fullID, segQualify(p.floor.Dir, p.floor.FullManifestPath), segQualify(p.floor.Dir, survivor.path), survivor.manifest.ParentBackupID)
	b.WriteString("So retention is SEGMENT-granular: prune rounds a keep-count UP to the nearest segment boundary and retires only WHOLE leading segments — keeping more than asked, never less. This chain has no segment boundary at or above the retention you asked for, so rounding up would retain the entire chain and delete nothing, and reporting that as a prune is how a bounded archive quietly stops being bounded. ")
	b.WriteString("NOTHING WAS DELETED: this is the pre-commit leg, so no catalog was written and no file removed — the chain is exactly as restorable as it was before this run. ")
	if boundaries := segmentBoundaryKeepCounts(p.flat); len(boundaries) > 0 {
		fmt.Fprintf(&b, "On this chain the --keep-incrementals counts that land on a segment boundary are %s. ", joinKeepCounts(boundaries))
	} else {
		b.WriteString("No --keep-incrementals value lands on a segment boundary on this chain, and on a never-rotated (SINGLE-segment) chain none ever can — the flag cannot express segment granularity there, which is why every --keep-incrementals prune of one refuses. ")
	}
	fmt.Fprintf(&b, "--keep-duration obeys the same rule (its cutoff is rounded up to a boundary too), and a cutoff older than all %d incrementals in the chain always lands on one: it retires them and leaves the newest segment's full as a self-contained restore base. Otherwise leave the chain as it is — nothing about it is broken.", len(p.flat))
	return b.String()
}

// requestedRetention renders the retention the operator ASKED for, in
// the flag they used. Both the refusal and the rounded-up INFO line
// quote it back: a remedy — or an "I kept more than you asked" — only
// means something when it names their own invocation.
func requestedRetention(keepCount int, keepDuration time.Duration) string {
	if keepCount > 0 {
		return fmt.Sprintf("--keep-incrementals=%d", keepCount)
	}
	return fmt.Sprintf("--keep-duration=%s", keepDuration)
}

// segmentBoundaryKeepCounts returns the --keep-incrementals values that
// land the keep/drop split exactly ON a segment boundary, ascending —
// i.e. every count for which prune retires whole segments and touches no
// segment's incrementals in place.
//
// The empty result is itself informative and is exactly item 100's
// reach, derived rather than asserted: on a single-segment chain the
// only in-segment index 0 is the first incremental of the whole lineage,
// whose keep count is "keep everything" (a no-op, not a prune) — so
// there is no boundary-safe count at all, and every --keep-incrementals
// prune of a never-rotated chain trims in place.
func segmentBoundaryKeepCounts(flat []lineageIncr) []int {
	var out []int
	for i := len(flat) - 1; i > 0; i-- {
		if flat[i].inSegIdx == 0 {
			out = append(out, len(flat)-i)
		}
	}
	return out
}

// segmentAlignedKeep rounds a requested retention UP to the nearest
// segment boundary — the whole of roadmap item 100's contract, in one
// function. It is deliberately built on [segmentBoundaryKeepCounts]
// (which the refusal already quotes to the operator) rather than
// re-deriving the boundary test, so the counts prune ACCEPTS and the
// counts it NAMES can never drift apart.
//
// Two counts are boundary-aligned without appearing in that list, and
// both are handled here:
//
//   - keep == 0 — retire every incremental. The floor segment is left
//     holding only its full, so there is no first-incremental boundary
//     to violate. This is `--keep-duration` past everything.
//   - keep == len(flat) — retain the whole chain. It is the answer when
//     NO boundary sits at or above the request, which is always the case
//     on a never-rotated chain; the caller refuses that rather than
//     reporting a prune that deleted nothing.
func segmentAlignedKeep(flat []lineageIncr, keep int) int {
	if keep <= 0 {
		return 0
	}
	for _, b := range segmentBoundaryKeepCounts(flat) {
		if b >= keep {
			return b
		}
	}
	return len(flat)
}

// joinKeepCounts renders a keep-count list for the refusal prose.
func joinKeepCounts(ns []int) string {
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ", ")
}

// pruneWholeSegments physically deletes every segment in
// [cat.RestorableFromSegment, floorSeg): each segment's full manifest +
// its data chunks and every incremental + its change chunks. Records the
// dropped paths and chunk-delete count on res (and bumps
// res.SegmentsDropped per segment). DryRun records the paths without
// deleting. A segment full is a self-contained snapshot, so dropping
// whole segments strictly older than the floor is unconditionally
// restore-safe.
// Per-file delete failures are COLLECTED and returned joined (audit F-8),
// never swallowed — see [PruneChain]'s post-commit sweep for why that is a
// WARN rather than a run failure, and why silence was the wrong posture.
func pruneWholeSegments(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, floorSeg int, dryRun bool, res *PruneResult) error {
	var errs []error
	for si := cat.RestorableFromSegment; si < floorSeg; si++ {
		seg := &cat.Segments[si]
		ss := seg.Store(store)
		// Full + its data chunks.
		if fm, err := lineage.ReadManifestAt(ctx, ss, seg.FullManifestPath); err == nil {
			for _, t := range fm.Tables {
				for _, ch := range t.Chunks {
					res.Pruned = appendChunk(res, ch.File)
					if !dryRun {
						if derr := ss.Delete(ctx, ch.File); derr == nil {
							res.ChunksDeleted++
						} else {
							errs = append(errs, fmt.Errorf("delete %q: %w", segQualify(seg.Dir, ch.File), derr))
						}
					}
				}
			}
		}
		if !dryRun {
			// The chain-identity root keeps its BODY and loses its
			// SIGNATURE — see [retireManifest].
			if err := retireManifest(ctx, ss, seg.FullManifestPath, keepsChainIdentity(seg)); err != nil {
				errs = append(errs, err)
			}
		}
		// Incrementals + their change chunks.
		for _, ip := range seg.Incrementals {
			if im, err := lineage.ReadManifestAt(ctx, ss, ip); err == nil {
				for _, ch := range im.ChangeChunks {
					if !dryRun {
						if derr := ss.Delete(ctx, ch.File); derr == nil {
							res.ChunksDeleted++
						} else {
							errs = append(errs, fmt.Errorf("delete %q: %w", segQualify(seg.Dir, ch.File), derr))
						}
					}
				}
			}
			res.Pruned = append(res.Pruned, segQualify(seg.Dir, ip))
			if !dryRun {
				if err := retireManifest(ctx, ss, ip, false); err != nil {
					errs = append(errs, err)
				}
			}
		}
		res.SegmentsDropped++
	}
	return errors.Join(errs...)
}

// keepsChainIdentity reports whether seg's full manifest IS the
// chain-root `manifest.json` — the one file a whole-segment prune must
// never delete (roadmap item 95, the Bug-214 defect on prune's path).
//
// For the lineage ROOT segment (Dir == "") the segment store IS the
// lineage root, so its FullManifestPath resolves to the chain-root
// manifest. That file is not a redundant copy of segment 0's full: it is
// the CHAIN'S IDENTITY. ADR-0152 binds the chain CEK's wrap to it, and
// for a passphrase chain the Argon2id salt the restore side re-derives
// its KEK from is recorded ONLY there (the CLI's buildReadEnvelope,
// which SILENTLY falls back to a fresh salt when the file is absent —
// which is why the symptom is `unwrap chain cek`, i.e. "wrong
// passphrase", rather than an honest missing-file error). Deleting it
// revokes readability for EVERY remaining segment, including ones prune
// never touched, while the operator holds a perfectly good passphrase.
//
// Prune's reach is larger than compaction's, which is what settles the
// contract question: compaction is occasional maintenance, prune is
// scheduled retention, and it fires whenever retention drops segment 0.
// So the chain keeps a dangling identity HEADER for a root segment that
// has been legitimately retired. That is the right trade — the file is
// small, it is the chain's identity rather than segment 0's data, and
// "we only need it sometimes" is precisely the reasoning that produced
// Bug 214. Unconditional, not "when the chain is encrypted": a
// keep-it-when rule is one future encryption mode or one future reader
// away from the same outage, and nobody notices until a restore.
//
// What survives is an identity HEADER, not a restorable manifest — and
// that distinction is only safe while every reader agrees on it. The
// previous version of this comment said it was "safe by construction"
// because the post-prune catalog never names the file and the one reader
// that could reach it without the catalog ([lineage.ResolveLineage]'s
// legacy single-segment synthesis) refuses on sight of a `seg-*` dir. That
// enumeration was ONE reader short, and had been since it was written:
// [Restore.Run]'s single-manifest path also resolves this file by
// convention, and on the prune-to-floor shape it was handed the retired
// root while `backup verify` walked the catalog floor (audit B-6). The
// claim was true when written, nothing checked it, and it stopped being
// true one dispatch predicate away.
//
// So the enumeration is DERIVED now, not asserted:
// TestRootManifestReaderRoster walks the source of every package that reads
// a manifest at the lineage root by convention and requires each call site
// to be classified — identity-only, or catalog-dispatched — with a reason.
// A new reader of this file fails the build until it says which it is.
// [retireManifest] closes the other half: the retired root keeps its body
// and loses its SIGNATURE, so nothing reports a green signature check over
// a manifest that has left the chain (audit C-8).
func keepsChainIdentity(seg *lineage.Segment) bool {
	return seg.Dir == "" && seg.FullManifestPath == lineage.ManifestFileName
}

// pruneFloorLeadingIncrementals physically deletes the first
// keepFromInSeg incrementals (+ their change chunks and their `.sig`
// siblings) of the floor segment — the incrementals older than the
// retained window whose full is kept as the restore base. Records dropped
// paths + chunk-delete count on res; DryRun records without deleting.
// Per-file delete failures are collected and returned joined (audit F-8).
//
// This is the path a SINGLE-segment signed chain takes when a
// `--keep-duration` cutoff older than the whole chain retires every
// incremental: the floor keeps its full, and each retired incremental's
// signature used to survive it.
func pruneFloorLeadingIncrementals(ctx context.Context, floor *lineage.Segment, floorStore irbackup.Store, keepFromInSeg int, dryRun bool, res *PruneResult) error {
	var errs []error
	for ii := 0; ii < keepFromInSeg; ii++ {
		ip := floor.Incrementals[ii]
		if im, err := lineage.ReadManifestAt(ctx, floorStore, ip); err == nil {
			for _, ch := range im.ChangeChunks {
				if !dryRun {
					if derr := floorStore.Delete(ctx, ch.File); derr == nil {
						res.ChunksDeleted++
					} else {
						errs = append(errs, fmt.Errorf("delete %q: %w", segQualify(floor.Dir, ch.File), derr))
					}
				}
			}
		}
		res.Pruned = append(res.Pruned, segQualify(floor.Dir, ip))
		if !dryRun {
			if err := retireManifest(ctx, floorStore, ip, false); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// pruneNoOpReturn finishes a PruneChain run through one of its r0 no-op
// doors: heal any stale signatures the resignIfSigned crash window left
// behind ([healStaleLineageSignatures] — the doors are exactly where a
// crashed run's re-run lands), then report the full kept set via [r0].
func pruneNoOpReturn(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, opts PruneOpts, why string) (*PruneResult, error) {
	if err := healStaleLineageSignatures(ctx, store, cat, "prune", opts.DryRun, opts.Signer); err != nil {
		return nil, err
	}
	return r0(cat, store, ctx, why)
}

// r0 is the "nothing to prune" early return: report the full kept set
// without mutating anything.
func r0(cat *lineage.Catalog, store irbackup.Store, ctx context.Context, why string) (*PruneResult, error) {
	res := &PruneResult{}
	for si := cat.RestorableFromSegment; si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		res.Kept = append(res.Kept, segQualify(seg.Dir, seg.FullManifestPath))
		for _, ip := range seg.Incrementals {
			res.Kept = append(res.Kept, segQualify(seg.Dir, ip))
		}
		// The request was already satisfied by what is there, so the two
		// retention counts agree at the whole chain — no rounding happened
		// and none was needed.
		res.RequestedIncrementals += len(seg.Incrementals)
		res.IncrementalsRetained += len(seg.Incrementals)
		if res.EarliestRestorableBackupID == "" && len(seg.Incrementals) > 0 {
			if m, err := lineage.ReadManifestAt(ctx, seg.Store(store), seg.Incrementals[0]); err == nil {
				res.EarliestRestorableBackupID = lineage.ManifestBackupID(m)
			}
		}
	}
	slog.InfoContext(ctx, "prune: "+why)
	return res, nil
}

// segQualify renders a segment-relative path as a lineage-root-
// relative one for the result's path lists (operator-facing).
func segQualify(dir, p string) string {
	if dir == "" {
		return p
	}
	return dir + "/" + p
}

// appendChunk records a pruned chunk path (segment-relative) for the
// result; centralised so DryRun and real prune agree on the list.
func appendChunk(res *PruneResult, file string) []string {
	return append(res.Pruned, file)
}

// SchemaHistoryRetentionFloor returns the combined floor below which
// ADR-0049 schema-history rows are safe to compact (DP-2):
//
//	min(liveSafePoint, oldest retained backup chain's resume position)
//
// "Min" is defined by the engine's [ir.PositionOrderer]: the older of
// the two, where "older" = the one the other is at-or-after but the
// reverse is false. Two incomparable positions (partial order, e.g. two
// MySQL GTID sets that diverge) yield a loud error — compaction can't
// pick a safe floor when the candidates can't be ordered, and a guess
// is the Bug-74 silent-loss class this ADR exists to kill.
//
// liveSafePoint is the live persisted ADR-0007 CDC safe-point on the
// target (sluice_cdc_state.source_position for the active stream).
// Callers extract it from the target before invoking this helper —
// chain_prune.go has no target connection. An empty live position
// (no active stream) reduces the floor to the backup-side alone.
//
// Returns the chosen floor and ok=true on success; ok=false when there
// is NO floor to apply (no backups in store and an empty live position
// — the caller must skip compaction in that case, since deleting
// everything would defeat the loud-floor sentinel).
//
// ADR-0049 Chunk D one-shot wiring: the caller passes the resulting
// floor to [ir.SchemaHistoryCompactor.CompactSchemaHistoryBelow] on the
// target's [ir.ChangeApplier]. Compaction is conservative by default
// (locked DP-2 "retain generously"); this helper is the safety wrapper
// that ensures the floor is never above the backup-resume needs.
func SchemaHistoryRetentionFloor(
	ctx context.Context,
	store irbackup.Store,
	liveSafePoint ir.Position,
	orderer ir.PositionOrderer,
) (floor ir.Position, ok bool, err error) {
	if orderer == nil {
		return ir.Position{}, false, errors.New("schema-history floor: orderer is nil; ordering is a correctness primitive (loud-failure tenet)")
	}
	backupFloor, backupOK, err := oldestRetainedBackupResumePosition(ctx, store)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("schema-history floor: load lineage: %w", err)
	}
	liveOK := liveSafePoint.Engine != "" || liveSafePoint.Token != ""

	switch {
	case !backupOK && !liveOK:
		return ir.Position{}, false, nil
	case !backupOK:
		return liveSafePoint, true, nil
	case !liveOK:
		return backupFloor, true, nil
	}
	// Both present: pick the OLDER under the partial order.
	liveAfter, err := orderer.PositionAtOrAfter(liveSafePoint, backupFloor)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("schema-history floor: order live vs backup: %w", err)
	}
	backupAfter, err := orderer.PositionAtOrAfter(backupFloor, liveSafePoint)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("schema-history floor: order backup vs live: %w", err)
	}
	switch {
	case liveAfter && !backupAfter:
		// live > backup → backup is the older (smaller) → it is the
		// safe min-floor.
		return backupFloor, true, nil
	case backupAfter && !liveAfter:
		return liveSafePoint, true, nil
	case liveAfter && backupAfter:
		// Equal positions — either is fine.
		return liveSafePoint, true, nil
	default:
		// Neither is at-or-after the other: incomparable. Loud refuse
		// (Bug-74 class — never guess on a partial order).
		return ir.Position{}, false, fmt.Errorf(
			"schema-history floor: live safe-point %+v and oldest backup resume %+v are incomparable under the engine's partial order; cannot pick a single retention floor (loud-failure tenet)",
			liveSafePoint, backupFloor,
		)
	}
}

// oldestRetainedBackupResumePosition returns the resume position the
// OLDEST currently-retained backup chain in store would resume CDC
// from after restore. For a chain root [full, incr_1, …, incr_N]:
//
//   - If the full has a recorded EndPosition (v0.18.0+), that IS the
//     resume position (snapshot's anchor LSN/GTID + the chain's events
//     forward).
//   - Else falls back to the OLDEST incremental's StartPosition (the
//     pre-v0.18.0 shape, where fulls didn't record EndPosition).
//   - If neither is populated, no usable floor → ok=false.
//
// The "oldest retained" chain is the one at the lineage catalog's
// [lineage.Catalog.RestorableFromSegment] index (the first segment the
// lineage considers restorable post-prune).
func oldestRetainedBackupResumePosition(ctx context.Context, store irbackup.Store) (ir.Position, bool, error) {
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("load lineage: %w", err)
	}
	if !ok || len(cat.Segments) == 0 {
		return ir.Position{}, false, nil
	}
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return ir.Position{}, false, fmt.Errorf("lineage restorable_from_segment=%d out of range", cat.RestorableFromSegment)
	}
	seg := &cat.Segments[cat.RestorableFromSegment]
	segStore := seg.Store(store)
	fm, err := lineage.ReadManifestAt(ctx, segStore, seg.FullManifestPath)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("read oldest full %q: %w", seg.FullManifestPath, err)
	}
	if fm.EndPosition.Engine != "" || fm.EndPosition.Token != "" {
		return fm.EndPosition, true, nil
	}
	// Pre-v0.18.0 fall-back: the oldest incremental's StartPosition.
	if len(seg.Incrementals) == 0 {
		return ir.Position{}, false, nil
	}
	im, err := lineage.ReadManifestAt(ctx, segStore, seg.Incrementals[0])
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("read oldest incremental %q: %w", seg.Incrementals[0], err)
	}
	if im.StartPosition.Engine == "" && im.StartPosition.Token == "" {
		return ir.Position{}, false, nil
	}
	return im.StartPosition, true, nil
}
