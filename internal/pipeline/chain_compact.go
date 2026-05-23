// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

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
	"strings"
	"time"

	"github.com/orware/sluice/internal/ir"
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

	// Now overrides the wall-clock source. Used by tests to make the
	// merged-segment CappedAt + UpdatedAt deterministic.
	Now func() time.Time

	// newSegmentID overrides the merged-segment-ID minter. Tests use
	// this for deterministic asserts; production leaves nil (the
	// minter is a random hex string under [generateMergedSegmentID]).
	newSegmentID func() string
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
	// and after compact. The arithmetic identity is BytesBefore ==
	// BytesAfter on naive compact (we move bytes, never rewrite them);
	// the field pair is exposed so a future event-level compactor can
	// report a real savings figure under the same result shape.
	BytesBefore int64
	BytesAfter  int64

	// Plan is the per-group breakdown — populated under DryRun (the
	// reporting-only path) AND under real compact (so callers can log
	// what actually happened). One entry per CONSIDERED group; size-1
	// groups carry MergedSegmentID == "" and no merged dir.
	Plan []CompactPlanGroup
}

// CompactPlanGroup is one merge group's plan-or-result entry.
type CompactPlanGroup struct {
	// SourceSegmentIDs lists the source segments' [LineageSegment.SegmentID]
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
}

// compactStagingDirPrefix is the on-disk marker for a mid-compact
// staging directory at the lineage root. Any dir under the root
// matching `<compactStagingDirPrefix><groupID>/` is unsalvageable
// post-crash garbage — the catalog never references it until the
// linearization swap, and the swap happens AFTER staging completes.
const compactStagingDirPrefix = ".compact-staging-"

// mergedSegmentDirPrefix is the on-disk sub-dir name a successfully-
// merged segment lives under. Mirrors the rotation FSM's
// [rotationSegmentDirPrefix] shape so a post-compact lineage looks like
// a naturally-rotated multi-segment lineage to every downstream reader.
const mergedSegmentDirPrefix = "seg-merged-"

// compactedCapReason is the [LineageSegment.CapReason] applied to a
// merged segment. Distinguishes it from a naturally-rotated segment
// (which uses [rotationReasonAge] / [rotationReasonChainLength]) for
// operator inspection.
const compactedCapReason = "compacted"

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
	fullMani  *ir.Manifest
	byteTotal int64
}

// plannedGroup is one pre-flighted merge group ready for the staging
// phase. dir == "" marks a size-1 (no-op) group.
type plannedGroup struct {
	span                  []segMeta
	plan                  CompactPlanGroup
	finalIncrementalPaths []string // populated by executeMergeGroup, recorded in lineage
}

// CompactChain executes a naive segment-compaction pass against the
// lineage in store. See package doc + ADR-0046 §14d. Returns the
// summary on success; any pre-flight refusal (encryption-keyset
// boundary, codec mismatch, gap-between-segments) is a wrapped error.
func CompactChain(ctx context.Context, store ir.BackupStore, opts CompactOpts) (*CompactResult, error) {
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

	cat, ok, err := loadLineageCatalog(ctx, store)
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

	// Load per-segment metadata once.
	metas := make([]segMeta, 0, len(eligible))
	for i := range eligible {
		seg := &eligible[i]
		ss := seg.store(store)
		fm, err := readManifestAt(ctx, ss, seg.FullManifestPath)
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
	type groupRange struct{ start, end int } // half-open within metas
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

	for i := range planned {
		pg := &planned[i]
		if pg.plan.MergedSegmentID == "" {
			continue
		}
		if err := executeMergeGroup(ctx, store, eligible, pg); err != nil {
			return nil, fmt.Errorf("backup compact: merge group %s: %w", pg.plan.MergedSegmentID, err)
		}
	}

	// Catalog swap (the linearization commit). Build the post-compact
	// lineage by walking cat.Segments and either passing each segment
	// through OR (when it's the OLDEST source of a planned merge
	// group) emitting the merged segment + skipping the rest.
	cat.Segments = buildPostCompactSegments(cat, planned, now())
	cat.RestorableFromSegment = 0
	cat.UpdatedAt = now().UTC()
	if err := writeLineageCatalog(ctx, store, cat); err != nil {
		return nil, fmt.Errorf("backup compact: rewrite lineage catalog: %w", err)
	}

	// Post-commit delete pass: the merged segment is now authoritative;
	// every source's leftover files are orphans.
	for i := range planned {
		pg := &planned[i]
		if pg.plan.MergedSegmentID == "" {
			continue
		}
		for _, s := range pg.span {
			seg := &eligible[s.idx]
			if seg.Dir == "" {
				_ = sweepRootSegmentArtifacts(ctx, store, seg)
				continue
			}
			_ = sweepSegmentSubdir(ctx, store, seg.Dir)
		}
	}

	slog.InfoContext(
		ctx, "backup compact: lineage compacted",
		slog.Int("groups_merged", res.GroupsMerged),
		slog.Int("segments_removed", res.SegmentsRemoved),
		slog.Int64("bytes_before", res.BytesBefore),
		slog.Int64("bytes_after", res.BytesAfter),
	)
	return res, nil
}

// segmentByteTotal sums every chunk's byte length across the segment's
// full + every incremental.
func segmentByteTotal(ctx context.Context, ss ir.BackupStore, seg *LineageSegment, fullMani *ir.Manifest) (int64, error) {
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
		im, err := readManifestAt(ctx, ss, ip)
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
func readByteSize(ctx context.Context, store ir.BackupStore, path string) (int64, error) {
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
func assertGroupCodecUniform(eligible []LineageSegment, span []segMeta) error {
	codec := eligible[span[0].idx].codecOrDefault()
	for i := 1; i < len(span); i++ {
		c := eligible[span[i].idx].codecOrDefault()
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
// segment full's [ir.ChainEncryption]. Mismatched keysets would
// produce a merged segment whose chunks are wrapped under multiple
// CEKs sharing no common KEK — restore would have no way to pick the
// right one per chunk under sluice's per-chain (now per-segment)
// encryption model. Refuse with an actionable recovery hint per task
// spec.
func assertGroupEncryptionKeysetUniform(eligible []LineageSegment, span []segMeta) error {
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

// assertGroupBoundaryContiguous refuses LOUDLY when there's a position
// gap between consecutive sources in a merge group:
// `seg[i].EndPosition != seg[i+1].StartPosition` under the engine-
// tagged token comparison the lineage records. The rotation FSM
// guarantees `<=`, never strict less, in the normal case — equality
// is the common case (same TxCommit boundary handed off). A strict
// inequality (gap) would mean events between seg[i].End and
// seg[i+1].Start live ONLY in seg[i+1]'s full snapshot, which naive
// compact is about to drop — refusing here avoids silent event loss.
//
// We compare via raw engine + token equality (positions on either
// side of a rotation handoff share an engine, so JSON-string equality
// is a sufficient and conservative discriminator). Operators see a
// clear refusal that names the boundary; #16's event-level compactor
// can replay smarter.
func assertGroupBoundaryContiguous(eligible []LineageSegment, span []segMeta) error {
	for i := 1; i < len(span); i++ {
		prev := &eligible[span[i-1].idx]
		cur := &eligible[span[i].idx]
		if prev.EndPosition.Token != cur.StartPosition.Token ||
			prev.EndPosition.Engine != cur.StartPosition.Engine {
			return fmt.Errorf(
				"backup compact: merge group has a position gap between segment %s (end=%+v) and segment %s (start=%+v); naive compact would drop events live only in the later segment's full snapshot — split the merge window so the gap is on a group boundary",
				prev.SegmentID, prev.EndPosition,
				cur.SegmentID, cur.StartPosition,
			)
		}
	}
	return nil
}

// encryptionFingerprint returns a stable string identifier for a
// segment's encryption binding. nil ChainEncryption → "plaintext".
// Populated → "<KEKMode>|<KEKRef>|<Mode>|argon2id-salt-hex" so two
// segments whose fulls share every binding field collapse to the same
// fingerprint and every keyset-divergent pair is distinguishable.
func encryptionFingerprint(enc *ir.ChainEncryption) string {
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
	store ir.BackupStore,
	eligible []LineageSegment,
	pg *plannedGroup,
) error {
	stagingDir := stagingDirFor(pg.plan.MergedSegmentID)
	finalDir := pg.plan.MergedSegmentDir
	stagingStore := newPrefixedStore(store, stagingDir)

	// 1. The oldest source's full becomes the merged segment's full.
	oldest := &eligible[pg.span[0].idx]
	oldestStore := oldest.store(store)
	oldestFull := pg.span[0].fullMani

	if err := copyFile(ctx, oldestStore, stagingStore, oldest.FullManifestPath, ManifestFileName); err != nil {
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
	for _, s := range pg.span {
		seg := &eligible[s.idx]
		segStore := seg.store(store)
		for _, ip := range seg.Incrementals {
			im, err := readManifestAt(ctx, segStore, ip)
			if err != nil {
				return fmt.Errorf("read source incremental %q: %w", ip, err)
			}
			for _, ch := range im.ChangeChunks {
				if err := copyFile(ctx, segStore, stagingStore, ch.File, ch.File); err != nil {
					return fmt.Errorf("stage incremental chunk %q: %w", ch.File, err)
				}
			}
			newPath := fmt.Sprintf("%sincr-%05d-%s.json",
				incrementalManifestPrefix, incrCount, manifestBackupID(im))
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

// copyFile reads src.path → dst.path through the store-level Get/Put
// primitives. Idempotent: a re-run after a partial copy overwrites the
// destination cleanly (BackupStore.Put is overwrite semantics).
func copyFile(ctx context.Context, src, dst ir.BackupStore, srcPath, dstPath string) error {
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
func renameStagingToFinal(ctx context.Context, store ir.BackupStore, stagingDir, finalDir string) error {
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
func cleanupStagingDirs(ctx context.Context, store ir.BackupStore) error {
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
func sweepRootSegmentArtifacts(ctx context.Context, store ir.BackupStore, seg *LineageSegment) error {
	_ = store.Delete(ctx, ManifestFileName)
	for _, ip := range seg.Incrementals {
		_ = store.Delete(ctx, ip)
	}
	if paths, err := store.List(ctx, "chunks/"); err == nil {
		for _, p := range paths {
			_ = store.Delete(ctx, p)
		}
	}
	return nil
}

// sweepSegmentSubdir deletes every file under a (no-longer-referenced)
// segment sub-directory. Used to GC merged sources' original dirs
// after the catalog swap committed them out of authority.
func sweepSegmentSubdir(ctx context.Context, store ir.BackupStore, dir string) error {
	paths, err := store.List(ctx, dir+"/")
	if err != nil {
		return err
	}
	for _, p := range paths {
		_ = store.Delete(ctx, p)
	}
	return nil
}

// buildPostCompactSegments returns the new lineage segment list after
// applying every planned merge. Walks cat.Segments in order; for each
// segment, either passes it through OR (if it's the OLDEST source of
// a planned merge group) emits the merged segment and SKIPS the
// remaining sources in the group.
func buildPostCompactSegments(cat *LineageCatalog, planned []plannedGroup, now time.Time) []LineageSegment {
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

	out := make([]LineageSegment, 0, len(cat.Segments))
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

// mergedSegmentFromGroup builds the [LineageSegment] entry that
// REPLACES a planned group's sources in the post-compact lineage.
func mergedSegmentFromGroup(cat *LineageCatalog, pg *plannedGroup, now time.Time) LineageSegment {
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

	return LineageSegment{
		SegmentID:                pg.plan.MergedSegmentID,
		Dir:                      pg.plan.MergedSegmentDir,
		FullManifestPath:         ManifestFileName,
		Incrementals:             append([]string(nil), pg.finalIncrementalPaths...),
		StartPosition:            oldest.StartPosition,
		EndPosition:              newest.EndPosition,
		CappedAt:                 &cappedAt,
		CapReason:                compactedCapReason,
		Codec:                    oldest.codecOrDefault(),
		VerbatimExtensionColumns: verbatim,
	}
}
