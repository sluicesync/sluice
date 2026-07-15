// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// # `sluice backup compact` — naive lineage segment compaction (ADR-0046 §14d)
//
// Compact is the operator-explicit complement to prune (ADR-0046 §4).
// Where prune drops segments off the OLDEST end of a lineage, compact
// concatenates CONSECUTIVE retained segments whose CreatedAt gaps fall
// within a single operator-supplied `--merge-window` duration. Every
// run is operator-initiated; sluice never auto-compacts on a write or
// rotation path.
//
// ## Coverage-gap subdivision (ADR-0087)
//
// After the CreatedAt-window grouping, each group is SUBDIVIDED at every
// boundary where the prior segment's EndPosition does not equal the
// next segment's incremental coverage start ([subdivideAtCoverageGaps]).
// Such a gap appears when a rotation-born segment never committed an
// incremental in its creating session (Bug 139: source idle at stream
// stop, or a crash/end at the rotation boundary) — it carries no
// IncrementalCoverageStart stamp and falls back to its full's anchor S,
// a few WAL bytes past the prior segment's P_N. The (P_N, S] window then
// lives ONLY in that segment's full snapshot, which a byte-level merge
// would drop. Subdivision keeps such a segment in its own merge group
// (with a WARN naming the boundary), so the surrounding contiguous runs
// still merge while NO data is lost and the chain stays restorable. This
// replaces the pre-ADR-0087 behaviour of refusing the WHOLE compact run
// with a corruption-blaming message.
//
// ## What "naive" means (and what it does NOT mean)
//
// Naive = STRUCTURAL byte-level concat. N consecutive segments in a
// group become ONE merged segment whose:
//
//   - Full = the OLDEST source segment's full (the restore base; its
//     snapshot anchor S_0 covers the group's full restore range).
//   - Incrementals = each source segment's incrementals, in lineage
//     order (source[0]'s incrementals, then source[1]'s, ...). Each
//     incremental's chunk files are MOVED verbatim into the merged
//     segment's directory; bytes are NOT decompressed/recompressed/
//     re-encrypted (that's #16's event-level dedup territory).
//   - StartPosition = oldest source's StartPosition.
//   - EndPosition   = newest source's EndPosition.
//   - CreatedAt     = oldest source's CreatedAt.
//   - Codec         = the (uniform) source codec.
//   - ChainEncryption (carried on the full manifest) = oldest source's
//     full's ChainEncryption — left UNCHANGED. We never re-encrypt or
//     re-key on compact.
//
// What we DO NOT do (deferred to #16, the event-level compactor):
//
//   - No event-level dedup, no same-row collapsing, no per-tx
//     compaction. A row inserted then updated then deleted across
//     three merged segments stays as three separate events.
//   - No chunk recompression (a gzip chunk stays gzip; a zstd chunk
//     stays zstd — and that's why mixed-codec groups REFUSE LOUDLY,
//     see [errMergeGroupCodecMismatch]).
//   - No re-encryption / re-keying — the loud-failure tenet's
//     analogue here: if any source segments in a merge group have
//     different `ChainEncryption` bindings (KEKMode / KEKRef / Mode /
//     Argon2id salt), refuse with a clear recovery hint.
//
// ## Atomic safety (the crash-recovery story)
//
// The catalog swap is the linearization commit:
//
//  1. Stage every merged group's chunks + manifests under a
//     `.compact-staging-<groupID>/` sub-directory of the lineage root.
//     A crash anywhere in this phase leaves the original segments
//     intact; the staging dir is unsalvageable garbage that the next
//     compact run (or a manual clean) removes.
//  2. Replay the staged contents into the merged segment's final
//     `seg-merged-<groupID>/` sub-dir. (BackupStore has no rename
//     primitive; we do this with copy-then-delete inside the
//     lineage-root store — see [renameStagingToFinal].)
//  3. Atomic catalog swap: write the new `lineage.json` referencing
//     the merged segment in place of its sources, in ONE Put call.
//     THIS IS THE COMMIT BOUNDARY. Pre-swap → "compact never
//     happened" (sources still authoritative); post-swap → "compact
//     happened" (merged segment is authoritative).
//  4. Delete the now-orphaned source segments' files. A crash here
//     leaves them as a recoverable garbage set the next compact run
//     would also clean.
//
// Mid-compact crash recovery (next CompactChain invocation):
//   - Any `.compact-staging-*` dir under the lineage root is
//     unsalvageable; delete on resume / next-run preflight.
//   - The lineage.json on disk is the authority. If it references the
//     merged segment, the sources' leftover files (if any) are
//     orphans to GC; if it still references the sources, the
//     in-flight merged segment is the orphan.

// CompactOpts configures [CompactChain]. MergeWindow is required (a
// duration of zero or negative is refused).
type CompactOpts struct {
	// MergeWindow is the maximum gap between consecutive segments'
	// CreatedAt to be considered part of the same merge group. Within
	// a group every consecutive pair satisfies `cur.CreatedAt -
	// prev.CreatedAt <= MergeWindow`. Required.
	MergeWindow time.Duration

	// DryRun reports the would-merge plan without touching storage or
	// rewriting lineage.json.
	DryRun bool

	// SmartCompaction enables ADR-0064 §14e event-level collapsing
	// over each merge group's staged change-chunks. When false (the
	// v1 default), naive byte-level concat applies and chunks are
	// moved verbatim. See ADR-0064.
	SmartCompaction bool

	// PKStrategy controls how smart compaction identifies "the same
	// row" across CDC events. Empty resolves to [PKStrategyPK]. Has
	// no effect when SmartCompaction is false.
	PKStrategy PKStrategy

	// Now overrides the wall-clock source. Used by tests to make the
	// merged-segment CappedAt + UpdatedAt deterministic.
	Now func() time.Time

	// newSegmentID overrides the merged-segment-ID minter. Tests use
	// this for deterministic asserts; production leaves nil (the
	// minter is a random hex string under [generateMergedSegmentID]).
	newSegmentID func() string

	// Signer, when non-nil, re-signs every manifest + the lineage after
	// a compact of a SIGNED chain (ADR-0154 Q4). Merging renumbers link
	// positions and rewrites the merged manifest's content, so the whole
	// survivor set is re-signed. When the chain is signed and Signer is
	// nil, compact REFUSES rather than emit an unsigned merged successor.
	Signer *lineage.Signer
}

// CompactResult summarises a [CompactChain] run.
type CompactResult struct {
	// GroupsConsidered is the count of in-window groups identified
	// across the lineage's retained segments (size-1 groups are
	// counted but skipped — they are no-ops). Always >= GroupsMerged.
	GroupsConsidered int

	// GroupsMerged is the count of size-≥-2 groups actually compacted.
	GroupsMerged int

	// SegmentsRemoved is the sum across all merged groups of (source
	// count - 1). Each group of N sources collapses into 1 merged
	// segment.
	SegmentsRemoved int

	// BytesBefore / BytesAfter are the summed chunk-byte totals before
	// and after compact. Under naive compact BytesBefore == BytesAfter
	// (bytes are moved, never rewritten). Under smart compact (ADR-0064)
	// BytesAfter < BytesBefore because the rewritten change-chunks
	// drop the collapsed-out events' bytes.
	BytesBefore int64
	BytesAfter  int64

	// EventsBefore / EventsAfter are ADR-0064 §9 per-row event tallies
	// summed across every merged group. Both zero under naive compact
	// (it doesn't decode events). Under smart compact:
	//   EventsBefore = INSERT/UPDATE/DELETE/TRUNCATE count in the
	//                  source chunks
	//   EventsAfter  = same count after the policy-table collapse
	//   EventsCollapsed = EventsBefore - EventsAfter
	EventsBefore    int64
	EventsAfter     int64
	EventsCollapsed int64

	// RowsCollapsed is the count of distinct (schema, table,
	// PK-tuple) keys whose accumulator had >1 event (collapse
	// candidates). Zero under naive compact.
	RowsCollapsed int64

	// TablesWithoutPK is the sorted list of "schema.table"
	// references that smart compact skipped because the table has
	// no declared PK; their events passed through verbatim under
	// the naive fall-through path (ADR-0064 §4). Empty under naive
	// compact AND under smart compact when every touched table has
	// a PK.
	TablesWithoutPK []string

	// Plan is the per-group breakdown — populated under DryRun (the
	// reporting-only path) AND under real compact (so callers can log
	// what actually happened). One entry per CONSIDERED group AFTER the
	// ADR-0087 coverage-gap subdivision (a CreatedAt-window group split
	// at a rotation-boundary gap yields multiple entries); size-1 groups
	// carry MergedSegmentID == "" and no merged dir.
	Plan []CompactPlanGroup
}

// CompactPlanGroup is one merge group's plan-or-result entry.
type CompactPlanGroup struct {
	// SourceSegmentIDs lists the source segments' [lineage.Segment.SegmentID]
	// values in lineage (oldest-first) order.
	SourceSegmentIDs []string

	// MergedSegmentID is the new merged segment's ID. Empty for
	// skipped size-1 groups.
	MergedSegmentID string

	// MergedSegmentDir is the final sub-directory the merged segment
	// lives under, mirroring the rotation FSM's `seg-*/` shape.
	MergedSegmentDir string

	// WindowSpan is the duration between the group's oldest and
	// newest source segments' CreatedAt. Always <= operator
	// MergeWindow * (size-1); pairwise gaps each fall under
	// MergeWindow.
	WindowSpan time.Duration

	// BytesEstimate is the sum of source chunk bytes the merge moves.
	// Always EQUALS the merged segment's BytesAfter on naive compact.
	BytesEstimate int64

	// EventsBefore / EventsAfter / EventsCollapsed / RowsCollapsed
	// are ADR-0064 §9 per-group event tallies. Zero under naive
	// compact and under size-1 (skipped) groups. See CompactResult
	// for field semantics.
	EventsBefore    int64
	EventsAfter     int64
	EventsCollapsed int64
	RowsCollapsed   int64

	// TablesWithoutPK lists "schema.table" refs smart compact skipped
	// in this group. Empty for naive compact and for groups where
	// every touched table has a PK.
	TablesWithoutPK []string
}

// compactStagingDirPrefix is the on-disk marker for a mid-compact
// staging directory at the lineage root. Any dir under the root
// matching `<compactStagingDirPrefix><groupID>/` is unsalvageable
// post-crash garbage — the catalog never references it until the
// linearization swap, and the swap happens AFTER staging completes.
const compactStagingDirPrefix = ".compact-staging-"

// mergedSegmentDirPrefix is the on-disk sub-dir name a successfully-
// merged segment lives under. Mirrors the rotation FSM's
// [lineage.RotationSegmentDirPrefix] shape so a post-compact lineage looks like
// a naturally-rotated multi-segment lineage to every downstream reader.
const mergedSegmentDirPrefix = "seg-merged-"

// CompactedCapReason is the [lineage.Segment.CapReason] applied to a
// merged segment. Distinguishes it from a naturally-rotated segment
// (which uses [rotationReasonAge] / [rotationReasonChainLength]) for
// operator inspection.
const CompactedCapReason = "compacted"

// errMergeGroupCodecMismatch is the loud-failure refusal when the
// sources in a merge group have differing recorded codecs. ADR-0046
// §5: each segment's codec is recorded, never inferred — so a
// multi-codec merge group would either need to re-encode (the silent
// data-handling-change loud-failure tenet rejects) or fail boundary
// validation at restore. The operator's recovery is to split the
// window so each group is codec-uniform, or to re-encode the chain
// explicitly first.
var errMergeGroupCodecMismatch = errors.New("backup compact: merge group straddles different segment codecs; refusing to merge byte-level chunks across codecs — split the merge window OR re-encode the chain first")

// segMeta carries per-source-segment derived data the compact pipeline
// computes once and reuses across grouping + execution + catalog
// rebuilding. Indexed against the `eligible` slice (segments from
// RestorableFromSegment forward).
type segMeta struct {
	idx       int // index in `eligible`
	catIdx    int // index in cat.Segments
	createdAt time.Time
	fullMani  *irbackup.Manifest
	byteTotal int64
}

// groupRange is a half-open [start, end) span within the `metas` slice
// describing one merge group's source segments. Used by both the
// CreatedAt-window grouping pass and the ADR-0087 coverage-gap
// subdivision pass.
type groupRange struct{ start, end int }

// plannedGroup is one pre-flighted merge group ready for the staging
// phase. dir == "" marks a size-1 (no-op) group.
type plannedGroup struct {
	span                  []segMeta
	plan                  CompactPlanGroup
	finalIncrementalPaths []string // populated by executeMergeGroup, recorded in lineage
}

// CompactChain executes a naive segment-compaction pass against the
// lineage in store. See package doc + ADR-0046 §14d + ADR-0087. Returns
// the summary on success; the loud pre-flight refusals (encryption-
// keyset boundary, codec mismatch) are wrapped errors. A coverage-gap
// boundary (ADR-0087) is NOT a refusal: the group is subdivided so the
// stamp-less rotation-born segment stays in its own merge group (WARN-
// named), and the run still succeeds.
//
//nolint:funlen // ratchet: pre-existing 234-line accretion; split when next touched (hold-the-line note in .golangci.yml)
func CompactChain(ctx context.Context, store irbackup.Store, opts CompactOpts) (*CompactResult, error) {
	if opts.MergeWindow <= 0 {
		return nil, errors.New("backup compact: --merge-window is required (positive duration)")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	mintID := opts.newSegmentID
	if mintID == nil {
		mintID = generateMergedSegmentID
	}

	// Load FOR UPDATE (ADR-0161): the catalog swap at the end of this run
	// is a CAS on the chain write-generation observed here, so a backup /
	// prune / second compact landing during the (potentially long) merge
	// window conflicts loudly instead of being clobbered.
	cat, ok, err := lineage.LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("backup compact: load lineage catalog: %w", err)
	}
	if !ok {
		return nil, errors.New("backup compact: lineage.json not found; run `sluice backup verify --rebuild-catalog` first")
	}
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return nil, fmt.Errorf("backup compact: lineage restorable_from_segment=%d out of range — corrupt lineage", cat.RestorableFromSegment)
	}

	eligible := cat.Segments[cat.RestorableFromSegment:]
	if len(eligible) < 2 {
		slog.InfoContext(ctx, "backup compact: nothing to compact (fewer than 2 retained segments)")
		return &CompactResult{}, nil
	}

	// ADR-0154 Q4: never emit an unsigned merged successor to a signed
	// chain. Refuse loudly when the chain is signed and no signing key
	// was supplied; otherwise re-sign the merged result below.
	signed, err := lineage.ChainIsSigned(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("backup compact: probe signed chain: %w", err)
	}
	if err := refuseUnsignableMaintenance("backup compact", signed, opts.DryRun, opts.Signer); err != nil {
		return nil, err
	}

	// Load per-segment metadata once.
	metas := make([]segMeta, 0, len(eligible))
	for i := range eligible {
		seg := &eligible[i]
		ss := seg.Store(store)
		fm, err := lineage.ReadManifestAt(ctx, ss, seg.FullManifestPath)
		if err != nil {
			return nil, fmt.Errorf("backup compact: read segment %d full %q: %w", i, seg.FullManifestPath, err)
		}
		bt, err := segmentByteTotal(ctx, ss, seg, fm)
		if err != nil {
			return nil, fmt.Errorf("backup compact: sum segment %d bytes: %w", i, err)
		}
		metas = append(metas, segMeta{
			idx:       i,
			catIdx:    cat.RestorableFromSegment + i,
			createdAt: fm.CreatedAt,
			fullMani:  fm,
			byteTotal: bt,
		})
	}

	// Pairwise greedy grouping by CreatedAt distance. Cut a new group
	// whenever the gap to the prior element exceeds MergeWindow.
	// Size-1 groups are no-ops (still reported in the Plan).
	var groups []groupRange
	{
		g := groupRange{start: 0, end: 1}
		for i := 1; i < len(metas); i++ {
			gap := metas[i].createdAt.Sub(metas[i-1].createdAt)
			if gap <= opts.MergeWindow {
				g.end = i + 1
				continue
			}
			groups = append(groups, g)
			g = groupRange{start: i, end: i + 1}
		}
		groups = append(groups, g)
	}

	// ADR-0087: subdivide each CreatedAt-window group at every
	// coverage-gap boundary (prev.EndPosition != cur's incremental
	// coverage start). A rotation-born segment that never committed an
	// incremental in its creating session (source idle at stream stop,
	// or a crash/end at the rotation boundary) carries no
	// IncrementalCoverageStart stamp, so it falls back to its full's
	// snapshot anchor S — a few WAL bytes past the prior segment's
	// EndPosition (P_N). The (P_N, S] window then lives ONLY in this
	// segment's full snapshot, which a merge would drop. Rather than
	// REFUSE the whole compact run (the pre-ADR-0087 behaviour, which
	// blamed lineage corruption on a chain this binary's own rotation
	// produced), we split the group so each such segment stays in its
	// own merge group — losing nothing, keeping the chain fully
	// restorable, and still merging every contiguous run around it.
	// Runs BEFORE the naive/smart branch so both modes (and --dry-run)
	// see the same subdivided plan + WARNs.
	groups = subdivideAtCoverageGaps(ctx, eligible, metas, groups)

	res := &CompactResult{}
	planned := make([]plannedGroup, 0, len(groups))
	for _, g := range groups {
		span := metas[g.start:g.end]
		entry := CompactPlanGroup{
			SourceSegmentIDs: make([]string, 0, len(span)),
		}
		for _, s := range span {
			entry.SourceSegmentIDs = append(entry.SourceSegmentIDs, eligible[s.idx].SegmentID)
			entry.BytesEstimate += s.byteTotal
		}
		if len(span) >= 2 {
			entry.WindowSpan = span[len(span)-1].createdAt.Sub(span[0].createdAt)
		}
		res.GroupsConsidered++
		res.BytesBefore += entry.BytesEstimate

		if len(span) < 2 {
			res.BytesAfter += entry.BytesEstimate
			res.Plan = append(res.Plan, entry)
			planned = append(planned, plannedGroup{span: append([]segMeta(nil), span...), plan: entry})
			continue
		}

		// Loud-failure pre-flight: every source segment in the group
		// must agree on (a) codec, (b) ChainEncryption binding, and
		// (c) boundary contiguity (no position gap between consecutive
		// sources).
		if err := assertGroupCodecUniform(eligible, span); err != nil {
			return nil, err
		}
		if err := assertGroupEncryptionKeysetUniform(eligible, span); err != nil {
			return nil, err
		}
		if err := assertGroupBoundaryContiguous(eligible, span); err != nil {
			return nil, err
		}

		segID := mintID()
		entry.MergedSegmentID = segID
		entry.MergedSegmentDir = mergedSegmentDirFor(segID)
		res.BytesAfter += entry.BytesEstimate
		res.GroupsMerged++
		res.SegmentsRemoved += len(span) - 1
		res.Plan = append(res.Plan, entry)
		planned = append(planned, plannedGroup{span: append([]segMeta(nil), span...), plan: entry})
	}

	if opts.DryRun {
		slog.InfoContext(
			ctx, "backup compact: dry-run plan",
			slog.Int("groups_considered", res.GroupsConsidered),
			slog.Int("groups_to_merge", res.GroupsMerged),
			slog.Int("segments_to_remove", res.SegmentsRemoved),
		)
		return res, nil
	}

	if res.GroupsMerged == 0 {
		slog.InfoContext(ctx, "backup compact: no size-≥-2 merge groups found within --merge-window; nothing to do")
		return res, nil
	}

	// First-pass cleanup: any leftover `.compact-staging-*` dirs from
	// an earlier crashed run are unsalvageable; delete on resume so
	// they don't pile up. Loud-but-non-fatal: a stale staging dir is
	// garbage, not a correctness hazard.
	if err := cleanupStagingDirs(ctx, store); err != nil {
		slog.WarnContext(
			ctx, "backup compact: stale staging-dir cleanup failed; continuing",
			slog.String("err", err.Error()),
		)
	}

	smartPK := resolvePKStrategy(opts.PKStrategy)
	tablesWithoutPK := make(map[string]struct{})
	for i := range planned {
		pg := &planned[i]
		if pg.plan.MergedSegmentID == "" {
			continue
		}
		if err := executeMergeGroup(ctx, store, eligible, pg); err != nil {
			return nil, fmt.Errorf("backup compact: merge group %s: %w", pg.plan.MergedSegmentID, err)
		}
		if !opts.SmartCompaction {
			continue
		}
		// Smart compaction (ADR-0064 §14e) — rewrite the merged
		// segment's change-chunks in place to collapse same-row
		// event chains. Runs AFTER the staging→final rename so the
		// chunks live at their final paths; pre-catalog-swap so a
		// crash here leaves the merged segment authoritative-but-
		// pre-compact (the catalog still references the sources).
		mergedStore := lineage.NewPrefixedStore(store, pg.plan.MergedSegmentDir)
		codec := eligible[pg.span[0].idx].CodecOrDefault()
		// Encryption CEK: smart compaction needs to decrypt + re-
		// encrypt under the segment's keyset. v1 keeps parity with
		// the naive path's "no re-encryption" stance by REFUSING
		// when the segment is encrypted — the operator must
		// disable smart compaction (--smart-compaction-off) for
		// encrypted chains until cross-keyset event-level recompaction
		// is designed (a follow-on chunk). Plaintext chains (the
		// common case for local-FS DR archives) flow through.
		if span0Enc := pg.span[0].fullMani.ChainEncryption; span0Enc != nil {
			return nil, fmt.Errorf("backup compact: --smart-compaction is not yet supported on encrypted chains (merge group %s is bound to keyset %s); re-run with --smart-compaction-off to use naive concat",
				pg.plan.MergedSegmentID, encryptionFingerprint(span0Enc))
		}
		groupRes, err := applySmartCompactionToStagedGroup(ctx, mergedStore, pg, codec, nil, smartPK)
		if err != nil {
			return nil, fmt.Errorf("backup compact: smart-compact merge group %s: %w", pg.plan.MergedSegmentID, err)
		}
		// Update the per-group plan entry + the top-level result.
		// res.Plan was already appended to during planning; locate
		// the entry by MergedSegmentID and update it.
		for pi := range res.Plan {
			if res.Plan[pi].MergedSegmentID != pg.plan.MergedSegmentID {
				continue
			}
			res.Plan[pi].EventsBefore = groupRes.eventsBefore
			res.Plan[pi].EventsAfter = groupRes.eventsAfter
			res.Plan[pi].EventsCollapsed = groupRes.eventsBefore - groupRes.eventsAfter
			res.Plan[pi].RowsCollapsed = groupRes.rowsCollapsed
			res.Plan[pi].TablesWithoutPK = groupRes.tablesWithoutPKList()
			break
		}
		res.EventsBefore += groupRes.eventsBefore
		res.EventsAfter += groupRes.eventsAfter
		res.RowsCollapsed += groupRes.rowsCollapsed
		// Re-derive BytesAfter for this group: the chunks have been
		// rewritten with possibly-fewer events, so the merged
		// segment's actual byte total is groupRes.bytesAfter (chunk
		// data only; manifest bytes are negligible and the naive
		// BytesEstimate was chunk-byte sums).
		res.BytesAfter += groupRes.bytesAfter - pg.plan.BytesEstimate
		for k := range groupRes.tablesWithoutPK {
			tablesWithoutPK[k] = struct{}{}
		}
	}
	res.EventsCollapsed = res.EventsBefore - res.EventsAfter
	if len(tablesWithoutPK) > 0 {
		res.TablesWithoutPK = make([]string, 0, len(tablesWithoutPK))
		for k := range tablesWithoutPK {
			res.TablesWithoutPK = append(res.TablesWithoutPK, k)
		}
		sort.Strings(res.TablesWithoutPK)
	}

	// Catalog swap (the linearization commit). Build the post-compact
	// lineage by walking cat.Segments and either passing each segment
	// through OR (when it's the OLDEST source of a planned merge
	// group) emitting the merged segment + skipping the rest.
	cat.Segments = buildPostCompactSegments(cat, planned, now())
	cat.RestorableFromSegment = 0
	cat.UpdatedAt = now().UTC()
	if err := lineage.WriteLineageCatalog(ctx, store, cat); err != nil {
		return nil, fmt.Errorf("backup compact: rewrite lineage catalog: %w", err)
	}

	// ADR-0154: re-sign the merged survivor set at its new positions +
	// the lineage. Runs BEFORE the orphan-delete sweep so the signed
	// artifacts reflect the authoritative post-compact structure.
	if err := resignIfSigned(ctx, store, signed, opts.Signer); err != nil {
		return nil, fmt.Errorf("backup compact: re-sign merged chain: %w", err)
	}

	// Post-commit delete pass: the merged segment is now authoritative;
	// every source's leftover files are orphans. Failures here are
	// non-fatal by design (the catalog swap above already committed,
	// so the chain is correct either way) — but a failed sweep leaks
	// backup-store disk forever, so it must leave a breadcrumb for the
	// operator rather than vanish.
	for i := range planned {
		pg := &planned[i]
		if pg.plan.MergedSegmentID == "" {
			continue
		}
		for _, s := range pg.span {
			seg := &eligible[s.idx]
			target := seg.Dir
			var sweepErr error
			if seg.Dir == "" {
				target = "(root segment artifacts)"
				sweepErr = sweepRootSegmentArtifacts(ctx, store, seg)
			} else {
				sweepErr = sweepSegmentSubdir(ctx, store, seg.Dir)
			}
			if sweepErr != nil {
				slog.WarnContext(
					ctx, "backup compact: orphan sweep failed — superseded segment files remain in the backup store",
					slog.String("segment_dir", target),
					slog.String("merged_into", pg.plan.MergedSegmentID),
					slog.String("error", sweepErr.Error()),
				)
			}
		}
	}

	logArgs := []any{
		slog.Int("groups_merged", res.GroupsMerged),
		slog.Int("segments_removed", res.SegmentsRemoved),
		slog.Int64("bytes_before", res.BytesBefore),
		slog.Int64("bytes_after", res.BytesAfter),
	}
	if opts.SmartCompaction {
		logArgs = append(
			logArgs,
			slog.Bool("smart_compaction", true),
			slog.Int64("events_before", res.EventsBefore),
			slog.Int64("events_after", res.EventsAfter),
			slog.Int64("events_collapsed", res.EventsCollapsed),
			slog.Int64("rows_collapsed", res.RowsCollapsed),
			slog.Int("tables_without_pk", len(res.TablesWithoutPK)),
		)
	}
	slog.InfoContext(ctx, "backup compact: lineage compacted", logArgs...)
	return res, nil
}

// segmentByteTotal sums every chunk's byte length across the segment's
// full + every incremental.
func segmentByteTotal(ctx context.Context, ss irbackup.Store, seg *lineage.Segment, fullMani *irbackup.Manifest) (int64, error) {
	var total int64
	for _, t := range fullMani.Tables {
		for _, ch := range t.Chunks {
			n, err := readByteSize(ctx, ss, ch.File)
			if err != nil {
				return 0, fmt.Errorf("full chunk %q: %w", ch.File, err)
			}
			total += n
		}
	}
	for _, ip := range seg.Incrementals {
		im, err := lineage.ReadManifestAt(ctx, ss, ip)
		if err != nil {
			return 0, fmt.Errorf("read incremental %q: %w", ip, err)
		}
		for _, ch := range im.ChangeChunks {
			n, err := readByteSize(ctx, ss, ch.File)
			if err != nil {
				return 0, fmt.Errorf("incremental chunk %q: %w", ch.File, err)
			}
			total += n
		}
	}
	return total, nil
}

// readByteSize returns the length in bytes of path in store via a
// streaming Get + io.Copy into io.Discard. Naive but correct;
// BackupStore has no Stat. Compact's reporting path is not a hot path
// (operator-explicit op).
func readByteSize(ctx context.Context, store irbackup.Store, path string) (int64, error) {
	rc, err := store.Get(ctx, path)
	if err != nil {
		return 0, fmt.Errorf("get %q: %w", path, err)
	}
	defer func() { _ = rc.Close() }()
	n, err := io.Copy(io.Discard, rc)
	if err != nil {
		return 0, fmt.Errorf("read %q: %w", path, err)
	}
	return n, nil
}

// assertGroupCodecUniform refuses LOUDLY when source segments in a
// merge group have different recorded codecs.
func assertGroupCodecUniform(eligible []lineage.Segment, span []segMeta) error {
	codec := eligible[span[0].idx].CodecOrDefault()
	for i := 1; i < len(span); i++ {
		c := eligible[span[i].idx].CodecOrDefault()
		if c != codec {
			return fmt.Errorf(
				"%w: segment %s codec=%q vs segment %s codec=%q",
				errMergeGroupCodecMismatch,
				eligible[span[0].idx].SegmentID, codec,
				eligible[span[i].idx].SegmentID, c,
			)
		}
	}
	return nil
}

// assertGroupEncryptionKeysetUniform refuses LOUDLY when the source
// segments' fulls bind to DIFFERENT encryption keysets. The keyset is
// the (KEKMode, KEKRef, Mode, Argon2id-salt) tuple recorded on each
// segment full's [irbackup.ChainEncryption]. Mismatched keysets would
// produce a merged segment whose chunks are wrapped under multiple
// CEKs sharing no common KEK — restore would have no way to pick the
// right one per chunk under sluice's per-chain (now per-segment)
// encryption model. Refuse with an actionable recovery hint per task
// spec.
func assertGroupEncryptionKeysetUniform(eligible []lineage.Segment, span []segMeta) error {
	first := encryptionFingerprint(span[0].fullMani.ChainEncryption)
	firstSeg := eligible[span[0].idx].SegmentID
	for i := 1; i < len(span); i++ {
		fp := encryptionFingerprint(span[i].fullMani.ChainEncryption)
		if fp != first {
			return fmt.Errorf(
				"backup compact: merge window straddles encryption keyset boundaries: segment %s bound to keyset %s, segment %s bound to keyset %s; refusing to merge — split the merge window OR re-key the chain first",
				firstSeg, first,
				eligible[span[i].idx].SegmentID, fp,
			)
		}
	}
	return nil
}

// BoundaryHasCoverageGap reports whether there is a position gap
// between two consecutive sources: the prior segment's EndPosition does
// not equal the next segment's earliest incremental coverage. The
// comparison is against [lineage.Segment.incrementalCoverageStartOrStart]
// (ADR-0067), NOT StartPosition: a rotation-opened segment that DID
// commit an incremental in its creating session keeps the (P_N, S]
// overlap in its incrementals and records IncrementalCoverageStart =
// P_N == the prior segment's EndPosition, so that boundary is
// contiguous. A rotation-born segment that never committed an
// incremental (Bug 139 — source idle at stream stop, or a crash/end at
// the rotation boundary) has no stamp and falls back to StartPosition
// (S), a few WAL bytes past P_N — a gap. We compare via raw engine +
// token equality (positions across a rotation handoff share an engine,
// so JSON-string equality is a sufficient, conservative discriminator).
func BoundaryHasCoverageGap(prev, cur *lineage.Segment) bool {
	curStart := cur.IncrementalCoverageStartOrStart()
	return prev.EndPosition.Token != curStart.Token ||
		prev.EndPosition.Engine != curStart.Engine
}

// assertGroupBoundaryContiguous is a DEFENSIVE internal invariant run in
// the per-group preflight: every consecutive pair within a planned merge
// group must be boundary-contiguous. ADR-0087 made this UNREACHABLE in
// the normal path — [subdivideAtCoverageGaps] already split every
// coverage-gap boundary into a separate group before preflight, so a
// surviving size-≥-2 group is contiguous by construction. If this fires
// it is therefore a SUBDIVISION BUG (the split pass missed a gap),
// caught loudly here before any byte-level merge drops the (P_N, S]
// window — never silently lose DR data.
func assertGroupBoundaryContiguous(eligible []lineage.Segment, span []segMeta) error {
	for i := 1; i < len(span); i++ {
		prev := &eligible[span[i-1].idx]
		cur := &eligible[span[i].idx]
		if BoundaryHasCoverageGap(prev, cur) {
			return fmt.Errorf(
				"backup compact: internal invariant violated — a planned merge group has a position gap between segment %s (end=%+v) and segment %s (incremental coverage starts at %+v) that the ADR-0087 coverage-gap subdivision should have split. This is a sluice bug; refusing to merge (a byte-level merge would drop the events in that range, which live only in the later segment's full snapshot — DR data)",
				prev.SegmentID, prev.EndPosition,
				cur.SegmentID, cur.IncrementalCoverageStartOrStart(),
			)
		}
	}
	return nil
}

// subdivideAtCoverageGaps (ADR-0087) splits each CreatedAt-window group
// at every coverage-gap boundary so a byte-level merge never drops the
// (P_N, S] window that lives only in a stamp-less rotation-born
// segment's full snapshot. Each split boundary emits ONE operator-
// accurate WARN. Size-1 subgroups are the existing size-1 no-op shape.
// Pure (no storage I/O); safe to call on the --dry-run path so plans
// carry the same subdivision + WARNs as a real run.
func subdivideAtCoverageGaps(ctx context.Context, eligible []lineage.Segment, metas []segMeta, groups []groupRange) []groupRange {
	out := make([]groupRange, 0, len(groups))
	for _, g := range groups {
		start := g.start
		for i := g.start + 1; i < g.end; i++ {
			prev := &eligible[metas[i-1].idx]
			cur := &eligible[metas[i].idx]
			if !BoundaryHasCoverageGap(prev, cur) {
				continue
			}
			warnCoverageGapSplit(ctx, prev, cur)
			out = append(out, groupRange{start: start, end: i})
			start = i
		}
		out = append(out, groupRange{start: start, end: g.end})
	}
	return out
}

// warnCoverageGapSplit emits the single operator-accurate WARN for one
// ADR-0087 subdivision boundary, naming both segments + the position
// delta and explaining (in operator terms) WHY the boundary cannot
// merge and that NO data is lost.
func warnCoverageGapSplit(ctx context.Context, prev, cur *lineage.Segment) {
	curStart := cur.IncrementalCoverageStartOrStart()
	curRotationBorn := cur.Dir != "" || cur.CapReason != "" || cur.Open()
	slog.WarnContext(
		ctx, "backup compact: merge group split at a rotation-boundary coverage gap — "+
			"the later segment was born by a rotation and never committed an incremental in its "+
			"creating session (source idle at stream stop, or the stream ended/crashed at the "+
			"rotation boundary), so the window between the two segments exists ONLY in the later "+
			"segment's full snapshot, which merging would drop. The segments stay in separate merge "+
			"groups; NO data is lost and the chain remains fully restorable.",
		slog.String("prev_segment_id", prev.SegmentID),
		slog.String("cur_segment_id", cur.SegmentID),
		slog.String("prev_end_position", prev.EndPosition.Token),
		slog.String("cur_coverage_start_position", curStart.Token),
		slog.Bool("cur_zero_incrementals", len(cur.Incrementals) == 0),
		slog.Bool("cur_rotation_born", curRotationBorn),
	)
}

// encryptionFingerprint returns a stable string identifier for a
// segment's encryption binding. nil ChainEncryption → "plaintext".
// Populated → "<KEKMode>|<KEKRef>|<Mode>|argon2id-salt-hex" so two
// segments whose fulls share every binding field collapse to the same
// fingerprint and every keyset-divergent pair is distinguishable.
func encryptionFingerprint(enc *irbackup.ChainEncryption) string {
	if enc == nil {
		return "plaintext"
	}
	saltHex := ""
	if enc.Argon2id != nil {
		saltHex = hex.EncodeToString(enc.Argon2id.Salt)
	}
	return fmt.Sprintf("%s|%s|%s|%s", enc.KEKMode, enc.KEKRef, enc.Mode, saltHex)
}

// mergedSegmentDirFor returns the sub-directory name a merged segment
// should live under within the lineage root.
func mergedSegmentDirFor(segmentID string) string {
	return mergedSegmentDirPrefix + segmentID
}

// stagingDirFor returns the staging-phase dir name for a merged
// segment ID.
func stagingDirFor(segmentID string) string {
	return compactStagingDirPrefix + segmentID
}

// generateMergedSegmentID mints a fresh segment ID for a merged
// segment. Hex-encoded 16-byte random for uniqueness.
func generateMergedSegmentID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Should not fail on any supported platform; fall back to a
		// time-based ID so we never return empty.
		return fmt.Sprintf("merge-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// executeMergeGroup does the staged file-move dance for one group:
// stage → swap-to-final-dir. The catalog write (the linearization
// commit) is the caller's responsibility, AFTER every group's
// staging+swap completes.
//
// Staging-phase work:
//  1. Copy the OLDEST source's full manifest + every chunk it
//     references into the staging dir under `manifest.json` + its
//     same-relative-path chunks (the merged-segment-full's identity).
//  2. For each source in the group (oldest-first), copy every
//     incremental manifest + its change chunks into the staging dir,
//     re-numbering manifest filenames to preserve lineage-order under
//     the merged segment's local `manifests/` namespace.
//  3. Rename staging → final dir (copy-then-delete via the store, no
//     real rename primitive at the BackupStore layer).
//
// On success pg.finalIncrementalPaths is populated with the merged
// segment's incremental paths in lineage order; the catalog rebuild
// reads them.
func executeMergeGroup(
	ctx context.Context,
	store irbackup.Store,
	eligible []lineage.Segment,
	pg *plannedGroup,
) error {
	stagingDir := stagingDirFor(pg.plan.MergedSegmentID)
	finalDir := pg.plan.MergedSegmentDir
	stagingStore := lineage.NewPrefixedStore(store, stagingDir)

	// 1. The oldest source's full becomes the merged segment's full.
	oldest := &eligible[pg.span[0].idx]
	oldestStore := oldest.Store(store)
	oldestFull := pg.span[0].fullMani

	if err := copyFile(ctx, oldestStore, stagingStore, oldest.FullManifestPath, lineage.ManifestFileName); err != nil {
		return fmt.Errorf("stage merged full manifest: %w", err)
	}
	for _, t := range oldestFull.Tables {
		for _, ch := range t.Chunks {
			if err := copyFile(ctx, oldestStore, stagingStore, ch.File, ch.File); err != nil {
				return fmt.Errorf("stage merged full chunk %q: %w", ch.File, err)
			}
		}
	}

	// 2. Every source's incrementals + change-chunks, in lineage
	//    order. Manifest paths are re-numbered so the merged segment's
	//    incrementals sort by `manifests/incr-NNNNN-…` in chain
	//    order. Chunk files keep their original `chunks/_changes/…`
	//    paths (each source's chunks are independently named; the
	//    chance of name collision across sources is negligible and
	//    would be a content-equivalent collision in any case because
	//    the path encodes the chunk's source CreatedAt).
	incrCount := 0
	finalIncrPaths := make([]string, 0)
	// ADR-0067 parent re-stitch: a merged segment drops every source
	// segment's full except the oldest, so the FIRST incremental of each
	// dropped-full segment must re-chain off the previous link (its
	// original parent full is gone). Walk the merged incrementals in
	// lineage order, pointing each one's ParentBackupID at the prior
	// link. irbackup.ComputeBackupID ignores ParentBackupID (it hashes
	// created_at/engine/kind/end_position), so the BackupID is unchanged
	// and there is NO cascade — the boundary positions already proved
	// linearity; this only fixes the linkage metadata the restore-walk's
	// parent-link check enforces. (Pre-ADR-0067 this was never exercised:
	// the only multi-segment merges reachable were position-aligned seed
	// lineages whose incrementals carried no ParentBackupID, and live
	// rotation chains refused at the position-gap pre-flight.)
	prevLinkID := lineage.ManifestBackupID(oldestFull)
	for _, s := range pg.span {
		seg := &eligible[s.idx]
		segStore := seg.Store(store)
		for _, ip := range seg.Incrementals {
			im, err := lineage.ReadManifestAt(ctx, segStore, ip)
			if err != nil {
				return fmt.Errorf("read source incremental %q: %w", ip, err)
			}
			for _, ch := range im.ChangeChunks {
				if err := copyFile(ctx, segStore, stagingStore, ch.File, ch.File); err != nil {
					return fmt.Errorf("stage incremental chunk %q: %w", ch.File, err)
				}
			}
			im.ParentBackupID = prevLinkID
			prevLinkID = lineage.ManifestBackupID(im)
			newPath := fmt.Sprintf("%sincr-%05d-%s.json",
				lineage.IncrementalManifestPrefix, incrCount, lineage.ManifestBackupID(im))
			b, err := json.MarshalIndent(im, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal staged incremental manifest: %w", err)
			}
			if err := stagingStore.Put(ctx, newPath, bytes.NewReader(b)); err != nil {
				return fmt.Errorf("stage incremental manifest %q: %w", newPath, err)
			}
			finalIncrPaths = append(finalIncrPaths, newPath)
			incrCount++
		}
	}

	// 3. Rename staging → final.
	if err := renameStagingToFinal(ctx, store, stagingDir, finalDir); err != nil {
		return fmt.Errorf("rename staging→final: %w", err)
	}
	pg.finalIncrementalPaths = finalIncrPaths
	return nil
}

// copyFile reads src.Path → dst.Path through the store-level Get/Put
// primitives. Idempotent: a re-run after a partial copy overwrites the
// destination cleanly (BackupStore.Put is overwrite semantics).
func copyFile(ctx context.Context, src, dst irbackup.Store, srcPath, dstPath string) error {
	rc, err := src.Get(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("get %q: %w", srcPath, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read %q: %w", srcPath, err)
	}
	if err := dst.Put(ctx, dstPath, bytes.NewReader(body)); err != nil {
		return fmt.Errorf("put %q: %w", dstPath, err)
	}
	return nil
}

// renameStagingToFinal moves every file from stagingDir → finalDir
// under store. The conceptual "atomic rename" step in the staged
// commit; the BackupStore interface has no rename, so we list-then-
// copy-then-delete. The rename is NOT the commit boundary (that's the
// lineage.json swap upstream); a crash mid-rename leaves final
// partially populated and the catalog still pointing at the sources,
// so the next compact run's stale-staging-dir cleanup + catalog-
// driven sweep cleans both.
func renameStagingToFinal(ctx context.Context, store irbackup.Store, stagingDir, finalDir string) error {
	paths, err := store.List(ctx, stagingDir+"/")
	if err != nil {
		return fmt.Errorf("list staging %q: %w", stagingDir, err)
	}
	for _, p := range paths {
		rel := strings.TrimPrefix(p, stagingDir+"/")
		dstPath := finalDir + "/" + rel
		if err := copyFile(ctx, store, store, p, dstPath); err != nil {
			return fmt.Errorf("rename copy %q→%q: %w", p, dstPath, err)
		}
	}
	for _, p := range paths {
		_ = store.Delete(ctx, p)
	}
	return nil
}

// cleanupStagingDirs sweeps any leftover `.compact-staging-*` files
// from prior crashed runs.
func cleanupStagingDirs(ctx context.Context, store irbackup.Store) error {
	paths, err := store.List(ctx, compactStagingDirPrefix)
	if err != nil {
		return fmt.Errorf("list staging dirs: %w", err)
	}
	for _, p := range paths {
		if err := store.Delete(ctx, p); err != nil {
			return fmt.Errorf("delete stale staging %q: %w", p, err)
		}
	}
	return nil
}

// sweepRootSegmentArtifacts deletes the conventional-layout files at
// the lineage root that belong to a merged source. After compact the
// merged segment lives under its own sub-dir, so the root manifest +
// root incrementals + root chunks are orphans.
//
// Best-effort within the sweep: every delete is ATTEMPTED, and each
// failure is collected (naming its path) into the joined return so
// the caller's WARN carries a diagnostic trail — a persistently
// failing delete must never vanish silently while leaking backup-
// store disk. Missing files are not failures (both LocalStore and
// BlobStore treat delete-of-absent as nil), so a re-run after a
// partial sweep stays clean.
func sweepRootSegmentArtifacts(ctx context.Context, store irbackup.Store, seg *lineage.Segment) error {
	var errs []error
	if err := store.Delete(ctx, lineage.ManifestFileName); err != nil {
		errs = append(errs, fmt.Errorf("delete %q: %w", lineage.ManifestFileName, err))
	}
	for _, ip := range seg.Incrementals {
		if err := store.Delete(ctx, ip); err != nil {
			errs = append(errs, fmt.Errorf("delete %q: %w", ip, err))
		}
	}
	paths, err := store.List(ctx, "chunks/")
	if err != nil {
		errs = append(errs, fmt.Errorf(`list "chunks/": %w`, err))
	}
	for _, p := range paths {
		if err := store.Delete(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("delete %q: %w", p, err))
		}
	}
	return errors.Join(errs...)
}

// sweepSegmentSubdir deletes every file under a (no-longer-referenced)
// segment sub-directory. Used to GC merged sources' original dirs
// after the catalog swap committed them out of authority. Per-file
// delete failures are collected and joined (see
// [sweepRootSegmentArtifacts] for the best-effort contract).
func sweepSegmentSubdir(ctx context.Context, store irbackup.Store, dir string) error {
	paths, err := store.List(ctx, dir+"/")
	if err != nil {
		return err
	}
	var errs []error
	for _, p := range paths {
		if err := store.Delete(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("delete %q: %w", p, err))
		}
	}
	return errors.Join(errs...)
}

// buildPostCompactSegments returns the new lineage segment list after
// applying every planned merge. Walks cat.Segments in order; for each
// segment, either passes it through OR (if it's the OLDEST source of
// a planned merge group) emits the merged segment and SKIPS the
// remaining sources in the group.
func buildPostCompactSegments(cat *lineage.Catalog, planned []plannedGroup, now time.Time) []lineage.Segment {
	type ref struct {
		isOldest bool
		group    int
	}
	mark := make(map[int]ref, len(cat.Segments))
	for gi, pg := range planned {
		if pg.plan.MergedSegmentID == "" {
			continue
		}
		for i, s := range pg.span {
			mark[s.catIdx] = ref{isOldest: i == 0, group: gi}
		}
	}

	out := make([]lineage.Segment, 0, len(cat.Segments))
	for i := range cat.Segments {
		r, inGroup := mark[i]
		if !inGroup {
			out = append(out, cat.Segments[i])
			continue
		}
		if !r.isOldest {
			continue
		}
		out = append(out, mergedSegmentFromGroup(cat, &planned[r.group], now))
	}
	return out
}

// mergedSegmentFromGroup builds the [lineage.Segment] entry that
// REPLACES a planned group's sources in the post-compact lineage.
func mergedSegmentFromGroup(cat *lineage.Catalog, pg *plannedGroup, now time.Time) lineage.Segment {
	oldest := &cat.Segments[pg.span[0].catIdx]
	newest := &cat.Segments[pg.span[len(pg.span)-1].catIdx]

	// CappedAt: the merged segment is closed (post-rotation, fully
	// historical). Prefer the newest source's CappedAt when present;
	// otherwise stamp `now()` so a not-yet-capped newest (the open
	// segment scenario the eligibility floor should already filter,
	// but defended here as belt-and-suspenders) still records a cap.
	cappedAt := now.UTC()
	if newest.CappedAt != nil && !newest.CappedAt.IsZero() {
		cappedAt = *newest.CappedAt
	}

	// VerbatimExtensionColumns: a merged segment's full IS the oldest
	// source's full, so the marker carries over verbatim.
	var verbatim []string
	if len(oldest.VerbatimExtensionColumns) > 0 {
		verbatim = append([]string(nil), oldest.VerbatimExtensionColumns...)
	}

	return lineage.Segment{
		SegmentID:        pg.plan.MergedSegmentID,
		Dir:              pg.plan.MergedSegmentDir,
		FullManifestPath: lineage.ManifestFileName,
		Incrementals:     append([]string(nil), pg.finalIncrementalPaths...),
		StartPosition:    oldest.StartPosition,
		EndPosition:      newest.EndPosition,
		// ADR-0067: the merged segment's full IS the oldest source's full
		// (anchor = oldest.StartPosition) and its first incremental IS the
		// oldest source's first incremental — so its earliest incremental
		// coverage is the oldest source's. Carry IncrementalCoverageStart
		// verbatim so (a) the merged segment's own full->first-incremental
		// boundary validates (the kept (P_N, S] overlap), and (b) it stays
		// contiguous with any segment preceding the merge group. Empty
		// (oldest never-rotated) stays empty -> resolves to StartPosition.
		IncrementalCoverageStart: oldest.IncrementalCoverageStart,
		CappedAt:                 &cappedAt,
		CapReason:                CompactedCapReason,
		Codec:                    oldest.CodecOrDefault(),
		VerbatimExtensionColumns: verbatim,
	}
}
