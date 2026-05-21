// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// # lineage.json — native bounded-segment lineage catalog (ADR-0046)
//
// lineage.json lives at the lineage root and is the first-class
// on-disk description of a backup's structure. It REPLACES the
// pre-ADR-0046 chain.json (clean break — zero on-disk chains, no
// migration shim; see ADR-0046 §1 for the full rationale).
//
// A **lineage** is an ordered list of **segments**. A segment is one
// `backup full` anchor plus the incrementals chained off it, capped
// by policy (age / chain-length). "Rotation" is not an exceptional
// event grafted onto an unbounded chain — advancing the lineage is
// `appendSegment`. The grafted model's RotatedAt / SucceededBy /
// RotationReason / Tombstoned bimodality is GONE.
//
// **A never-rotated backup is a one-segment lineage** with the
// segment's Dir == "" (chunks/manifests at the conventional root
// paths). That shape takes the same single-segment restore path as a
// pre-ADR single chain — a strict generalization, not a heavier common
// path. (The segment's codec is whatever --compression selected;
// v0.67.0+ defaults to zstd, not gzip — see [DefaultCodec].)
//
// lineage.json is an accelerator for segment-shape queries the same
// way chain.json was for chain-shape queries, but it is ALSO the
// authoritative record of each segment's compression codec (recorded,
// never sniffed — ADR-0046 §5) and segment boundary positions. The
// underlying manifest files remain authoritative for schema / chunk
// lists; a missing / stale lineage.json triggers the directory-walk
// fallback for the common one-segment shape (a multi-segment lineage
// with sub-dir segments cannot be reconstructed by a bare walk, so a
// corrupt lineage.json on a rotated backup is a loud refusal — DR
// data, never a silent partial).
//
// See docs/adr/adr-0046-inline-backup-chain-rotation.md.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// LineageCatalogFileName is the filename of the lineage catalog within
// a backup store. Lives at the lineage root, sibling to the segment-0
// full's [ManifestFileName].
const LineageCatalogFileName = "lineage.json"

// lineageCatalogFormatVersion is the integer version of the
// lineage.json schema. Bumped only on incompatible field renames;
// additive fields don't require a bump.
const lineageCatalogFormatVersion = 1

// LineageCatalog is the deserialised content of lineage.json. The
// Segments slice is ordered: Segments[0] is the lineage root (the
// first full + its incrementals), each subsequent segment opened by an
// inline rotation. Segments[i].EndPosition <= Segments[i+1].StartPosition
// by construction (the rotation FSM's S>=P_N hard-fail assertion); the
// restore lineage-walk validates it with the SAME monotonicity check
// it uses for intra-segment incremental boundaries.
type LineageCatalog struct {
	FormatVersion int       `json:"format_version"`
	LineageID     string    `json:"lineage_id,omitempty"`
	SluiceVersion string    `json:"sluice_version,omitempty"`
	SourceEngine  string    `json:"source_engine,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	// Segments is the ordered segment list. One entry for a
	// never-rotated backup; N for a backup rotated N-1 times.
	Segments []LineageSegment `json:"segments"`

	// RestorableFromSegment is the prune floor: the index of the
	// oldest segment still fully restorable. Prune (ADR-0046 §4)
	// advances it as it drops leading whole segments. Restore refuses
	// to start before this segment. Zero on an unpruned lineage.
	RestorableFromSegment int `json:"restorable_from_segment"`
}

// LineageSegment is one segment within a lineage: a `backup full`
// anchor plus the incrementals chained off it, capped by policy.
//
// Wire shape matches ADR-0046 §1. CappedAt / CapReason are absent
// (zero / empty) on the open (last) segment and set on every prior
// segment by the rotation FSM's atomic COMMIT write.
type LineageSegment struct {
	// SegmentID is a stable identifier for the segment (the full's
	// BackupID). Used for log lines and operator inspection.
	SegmentID string `json:"segment_id"`

	// Dir is the segment's sub-directory relative to the lineage root,
	// consumed via [newPrefixedStore]. EMPTY for segment 0 of a
	// never-rotated (or the root segment of a rotated) lineage — that
	// segment lives at the conventional root paths so a one-segment
	// lineage is byte-identical to a pre-ADR single chain. Rotation-
	// opened segments get `seg-<unix-millis>/`.
	Dir string `json:"dir,omitempty"`

	// FullManifestPath is the segment's full manifest path RELATIVE TO
	// THE SEGMENT'S Dir (i.e. as the per-segment prefixed store sees
	// it). Always [ManifestFileName] in v0.67.0 (the full lands at the
	// segment root); kept explicit so a future layout change doesn't
	// need a format bump.
	FullManifestPath string `json:"full_manifest_path"`

	// Incrementals is the ordered list of incremental manifest paths
	// for this segment, RELATIVE TO Dir, in chain order.
	Incrementals []string `json:"incrementals"`

	// StartPosition is the segment's full's snapshot anchor (S). For
	// segment 0 it's the root full's EndPosition; for a rotation-
	// opened segment it's the snapshot anchor S captured on the same
	// CDC handle, hard-asserted S >= prior segment's EndPosition.
	StartPosition ir.Position `json:"start_position"`

	// EndPosition is the segment's last committed incremental's
	// EndPosition (P_N). Equals StartPosition for a segment with no
	// incrementals yet (a freshly-opened segment).
	EndPosition ir.Position `json:"end_position"`

	// CappedAt is the instant the rotation FSM's COMMIT closed this
	// segment. nil/zero on the open (last) segment.
	CappedAt *time.Time `json:"capped_at,omitempty"`

	// CapReason records why the segment was capped:
	// [rotationReasonAge] or [rotationReasonChainLength]. Empty on the
	// open segment.
	CapReason string `json:"cap_reason,omitempty"`

	// Codec is the compression codec every chunk in this segment was
	// written with (ADR-0046 §5). Recorded here, NEVER inferred from
	// the chunk bytes on restore. Empty resolves to [DefaultCodec]
	// (zstd, v0.67.0+); a v0.67.0+ backup always records it explicitly.
	Codec Codec `json:"codec,omitempty"`

	// VerbatimExtensionColumns is the ADR-0047 backup capability
	// marker: the "schema.table.column" references in this segment's
	// schema whose IR type is [ir.VerbatimType] (an uncatalogued PG
	// extension type carried verbatim). A non-empty list means the
	// segment is **PG-restore-only** — the restore-target engine is
	// unknown at backup time, so this is RECORDED here (the
	// record-never-sniff idiom, exactly like the per-segment Codec)
	// and enforced LOUDLY at restore preflight against the *actual*
	// target engine (see [refuseVerbatimRestoreToNonPG]). Empty /
	// absent on every pre-ADR-0047 and every non-verbatim backup —
	// older sluice ignores the unknown field (additive; no format
	// bump), and a legacy/never-rotated backup is unaffected.
	VerbatimExtensionColumns []string `json:"verbatim_extension_columns,omitempty"`
}

// hasVerbatimExtensionColumns reports whether the segment carries the
// ADR-0047 PG-restore-only marker.
func (s *LineageSegment) hasVerbatimExtensionColumns() bool {
	return len(s.VerbatimExtensionColumns) > 0
}

// codecOrDefault returns the segment's recorded codec, resolving an
// empty value to [DefaultCodec] (zstd).
func (s *LineageSegment) codecOrDefault() Codec { return resolveCodec(s.Codec) }

// open reports whether this is the open (last, uncapped) segment.
func (s *LineageSegment) open() bool { return s.CappedAt == nil || s.CappedAt.IsZero() }

// store returns the per-segment [ir.BackupStore] view: the lineage
// store narrowed to the segment's Dir (a no-op wrap when Dir == "",
// the common one-segment / root-segment shape).
func (s *LineageSegment) store(root ir.BackupStore) ir.BackupStore {
	return newPrefixedStore(root, s.Dir)
}

// loadLineageCatalog reads lineage.json from store. Returns (catalog,
// true, nil) when present and parseable; (nil, false, nil) when
// absent; (nil, false, err) for I/O / parse / version failures. The
// "absent" case is the legacy one-segment fall-through callers handle
// by synthesising a single root segment over the conventional layout.
func loadLineageCatalog(ctx context.Context, store ir.BackupStore) (*LineageCatalog, bool, error) {
	exists, err := store.Exists(ctx, LineageCatalogFileName)
	if err != nil {
		return nil, false, fmt.Errorf("inspect %q: %w", LineageCatalogFileName, err)
	}
	if !exists {
		return nil, false, nil
	}
	rc, err := store.Get(ctx, LineageCatalogFileName)
	if err != nil {
		return nil, false, fmt.Errorf("get %q: %w", LineageCatalogFileName, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %w", LineageCatalogFileName, err)
	}
	var cat LineageCatalog
	if err := json.Unmarshal(body, &cat); err != nil {
		return nil, false, fmt.Errorf("decode %q: %w", LineageCatalogFileName, err)
	}
	if cat.FormatVersion > lineageCatalogFormatVersion {
		return nil, false, fmt.Errorf("%s format version %d is newer than this build supports (%d); upgrade sluice",
			LineageCatalogFileName, cat.FormatVersion, lineageCatalogFormatVersion)
	}
	if len(cat.Segments) == 0 {
		return nil, false, fmt.Errorf("%s contains zero segments; corrupt lineage (DR data — refusing to silently continue)",
			LineageCatalogFileName)
	}
	return &cat, true, nil
}

// writeLineageCatalog serialises cat as indented JSON and writes it to
// store via a single-object Put. **This single Put is the rotation
// FSM's atomic linearization point** (ADR-0046 §2 COMMIT): it is
// always issued AFTER the next segment's full is durable, so there is
// no window in which the lineage is non-authoritative. Atomic at the
// storage layer from any reader's perspective (object stores: a Put is
// all-or-nothing; local FS: write-tmp + rename inside LocalStore).
func writeLineageCatalog(ctx context.Context, store ir.BackupStore, cat *LineageCatalog) error {
	cat.FormatVersion = lineageCatalogFormatVersion
	b, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal lineage catalog: %w", err)
	}
	return store.Put(ctx, LineageCatalogFileName, bytes.NewReader(b))
}

// segmentRecord pairs a parsed manifest with the path it loaded from
// AND the segment it belongs to. The chain-walk + restore code carries
// the segment so it can resolve the per-segment store + codec without
// re-deriving them. Embeds [manifestRecord] so existing helpers
// (manifestBackupID etc.) work unchanged.
type segmentRecord struct {
	manifestRecord
	segment *LineageSegment
}

// resolveLineage returns the lineage for store. When lineage.json is
// present it's authoritative. When UNREADABLE (parse/version/IO
// error), [loadLineageCatalog] already surfaced a loud error. When
// ABSENT, a single synthetic root segment (Dir == "", codec
// [DefaultCodec]) is constructed over the conventional layout — the
// pre-ADR single-chain shape, a one-segment lineage by strict
// generalization — BUT only if the on-disk shape is genuinely
// single-segment. If rotation-opened segment sub-dirs (`seg-*`) exist
// while lineage.json is absent, the backup is a rotated multi-segment
// lineage that cannot be reconstructed from a bare walk: that is a
// LOUD refusal, never a silent root-only partial (Bug 66 — the absent
// case does NOT auto-surface from loadLineageCatalog the way the
// unreadable case does, so resolveLineage must guard it here).
func resolveLineage(ctx context.Context, store ir.BackupStore) (*LineageCatalog, error) {
	cat, ok, err := loadLineageCatalog(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("lineage catalog: %w", err)
	}
	if ok {
		return cat, nil
	}
	// lineage.json ABSENT. Before falling back to the legacy
	// single-segment synthesis, refuse loudly if the on-disk shape is
	// actually a rotated MULTI-segment backup (Bug 66): rotation opens
	// `seg-<unix-millis>/` sub-dirs, and a multi-segment lineage cannot
	// be reconstructed from a bare root walk — silently restoring only
	// the root segment would drop every rotation-opened segment with
	// exit 0 (DR data: loud-fail, never a silent partial — the same
	// contract the unreadable-lineage.json path already honors). A
	// genuine never-rotated / pre-ADR backup has no `seg-*` dirs and
	// still synthesizes + restores below (strict generalization).
	segEvidence, err := store.List(ctx, rotationSegmentDirPrefix)
	if err != nil {
		return nil, fmt.Errorf("inspect rotation segments (%s*): %w", rotationSegmentDirPrefix, err)
	}
	if len(segEvidence) > 0 {
		return nil, fmt.Errorf(
			"backup has rotation-opened segment directories (%s*) but lineage.json is missing: "+
				"a multi-segment lineage cannot be reconstructed from a bare directory walk; "+
				"refusing to restore only the root segment (DR data — never a silent partial). "+
				"Restore from a copy whose lineage.json is intact (it is the authoritative structural "+
				"record for a rotated backup; `backup verify --rebuild-catalog` only rebuilds the "+
				"legacy one-segment shape)",
			rotationSegmentDirPrefix,
		)
	}
	// Absent and genuinely single-segment: synthesise the legacy
	// lineage. The manifest list is discovered by a directory walk (the
	// pre-v0.47.0 fall-through, preserved for the one-segment shape).
	root := &LineageSegment{
		Dir:              "",
		FullManifestPath: ManifestFileName,
		Codec:            DefaultCodec,
	}
	exists, err := store.Exists(ctx, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("inspect %q: %w", ManifestFileName, err)
	}
	if !exists {
		return nil, errors.New("no manifests found in store (no lineage.json, no manifest.json) — take a `sluice backup full` first")
	}
	fullM, err := readManifestAt(ctx, store, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", ManifestFileName, err)
	}
	root.SegmentID = manifestBackupID(fullM)
	root.StartPosition = fullM.EndPosition
	root.EndPosition = fullM.EndPosition
	incs, err := store.List(ctx, incrementalManifestPrefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", incrementalManifestPrefix, err)
	}
	sort.Strings(incs)
	for _, p := range incs {
		if isIncrementalManifestPath(p) {
			root.Incrementals = append(root.Incrementals, p)
		}
	}
	return &LineageCatalog{
		FormatVersion: lineageCatalogFormatVersion,
		SourceEngine:  fullM.SourceEngine,
		SluiceVersion: fullM.SluiceVersion,
		Segments:      []LineageSegment{*root},
	}, nil
}

// listAllSegmentManifests returns every manifest across every segment
// of the lineage in lineage order (segment 0 full, segment 0
// incrementals, segment 1 full, ...), each tagged with its segment so
// callers know which per-segment store + codec to use. Replaces the
// pre-ADR [listAllManifests] catalog/walk dispatch — the lineage is
// the single dispatch point now.
func listAllSegmentManifests(ctx context.Context, store ir.BackupStore) ([]segmentRecord, error) {
	cat, err := resolveLineage(ctx, store)
	if err != nil {
		return nil, err
	}
	var out []segmentRecord
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		if err := validateRecordedCodec(seg.Codec); err != nil {
			return nil, err
		}
		ss := seg.store(store)
		fm, err := readManifestAt(ctx, ss, seg.FullManifestPath)
		if err != nil {
			return nil, fmt.Errorf("segment %d (%s) full %q: %w", i, seg.SegmentID, seg.FullManifestPath, err)
		}
		out = append(out, segmentRecord{
			manifestRecord: manifestRecord{path: seg.FullManifestPath, manifest: fm},
			segment:        seg,
		})
		for _, ip := range seg.Incrementals {
			im, err := readManifestAt(ctx, ss, ip)
			if err != nil {
				return nil, fmt.Errorf("segment %d (%s) incremental %q: %w", i, seg.SegmentID, ip, err)
			}
			out = append(out, segmentRecord{
				manifestRecord: manifestRecord{path: ip, manifest: im},
				segment:        seg,
			})
		}
	}
	return out, nil
}

// updateLineageForManifestBestEffort keeps lineage.json in sync after
// a non-rotation manifest write (full backup completion, a normal
// rollover's incremental). It appends/updates the manifest within the
// OPEN segment and advances that segment's EndPosition. Rotation's
// segment-append + cap is a SEPARATE atomic write (see the rotation
// FSM); this helper never touches CappedAt / prior segments.
//
// Best-effort by design (mirrors the pre-ADR chain.json posture for
// the non-rotation path): the manifest file is the source of truth for
// the one-segment shape, so a transient lineage.json hiccup is
// WARN-logged and recovered on the next write. The codec is recorded
// from the supplied value so the open segment's Codec is pinned on
// first write and never changes mid-segment.
func updateLineageForManifestBestEffort(
	ctx context.Context,
	store ir.BackupStore,
	manifest *ir.Manifest,
	manifestPath string,
	codec Codec,
) {
	if err := updateLineageForManifest(ctx, store, manifest, manifestPath, codec); err != nil {
		slog.WarnContext(
			ctx, "lineage catalog update failed; lineage.json may be stale until next write",
			slog.String("manifest_path", manifestPath),
			slog.String("err", err.Error()),
		)
	}
}

func updateLineageForManifest(
	ctx context.Context,
	store ir.BackupStore,
	manifest *ir.Manifest,
	manifestPath string,
	codec Codec,
) error {
	if manifest == nil {
		return errors.New("lineage catalog: nil manifest")
	}
	cat, ok, err := loadLineageCatalog(ctx, store)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if !ok {
		// First lineage.json write for this backup. Seed a single root
		// segment over the conventional layout (Dir == "").
		cat = &LineageCatalog{
			FormatVersion: lineageCatalogFormatVersion,
			SourceEngine:  manifest.SourceEngine,
			SluiceVersion: manifest.SluiceVersion,
			CreatedAt:     now,
			Segments: []LineageSegment{{
				SegmentID:        manifestBackupID(manifest),
				Dir:              "",
				FullManifestPath: ManifestFileName,
				Codec:            resolveCodec(codec),
			}},
		}
	}
	seg := &cat.Segments[len(cat.Segments)-1] // the open segment
	switch canonicalKind(manifest.Kind) {
	case ir.BackupKindFull:
		seg.FullManifestPath = manifestPath
		seg.SegmentID = manifestBackupID(manifest)
		seg.StartPosition = manifest.EndPosition
		seg.EndPosition = manifest.EndPosition
		if seg.Codec == "" {
			seg.Codec = resolveCodec(codec)
		}
		// ADR-0047 backup capability marker. The full carries the
		// authoritative schema; record the verbatim-typed columns so
		// the restore-time engine gate can refuse a non-PG target
		// loudly. Recorded, never sniffed at restore (ADR-0046 idiom);
		// stays nil for every non-verbatim backup so the field is
		// absent in the common case (and legacy readers ignore it).
		seg.VerbatimExtensionColumns = verbatimExtensionColumnsIn(manifest.Schema)
	case ir.BackupKindIncremental:
		// Dedup on path (per-chunk checkpoint re-writes the same path).
		found := false
		for _, ip := range seg.Incrementals {
			if ip == manifestPath {
				found = true
				break
			}
		}
		if !found {
			seg.Incrementals = append(seg.Incrementals, manifestPath)
		}
		seg.EndPosition = manifest.EndPosition
	}
	if cat.SourceEngine == "" {
		cat.SourceEngine = manifest.SourceEngine
	}
	if cat.SluiceVersion == "" {
		cat.SluiceVersion = manifest.SluiceVersion
	}
	if cat.CreatedAt.IsZero() {
		cat.CreatedAt = now
	}
	cat.UpdatedAt = now
	return writeLineageCatalog(ctx, store, cat)
}

// openSegmentStore returns the open (last) segment's store view + the
// codec recorded for it. For an absent lineage.json (the legacy
// one-segment shape, or a freshly-written full not yet lineage-
// catalogued) it returns the root store + the supplied write codec
// default. Used by the incremental + stream orchestrators: every
// non-rotation incremental lands in the OPEN segment, under its Dir,
// with its codec — the rotation FSM is the only thing that appends a
// new segment.
//
// writeCodec is the operator's --compression choice; it's used only
// when the lineage doesn't yet pin a codec for the open segment (first
// incremental into a never-catalogued backup). When the lineage exists
// the recorded codec wins (codec is recorded, never re-chosen
// mid-segment — a segment is single-codec by construction).
func openSegmentStore(ctx context.Context, store ir.BackupStore, writeCodec Codec) (ir.BackupStore, Codec, error) {
	cat, ok, err := loadLineageCatalog(ctx, store)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return store, resolveCodec(writeCodec), nil
	}
	seg := &cat.Segments[len(cat.Segments)-1]
	if err := validateRecordedCodec(seg.Codec); err != nil {
		return nil, "", err
	}
	c := seg.Codec
	if c == "" {
		c = resolveCodec(writeCodec)
	}
	return seg.store(store), resolveCodec(c), nil
}

// canonicalKind normalises empty Kind to BackupKindFull. Mirror of
// the ir.canonicalKind helper; duplicated here so the catalog code
// doesn't pull in unexported ir helpers.
func canonicalKind(kind string) string {
	if kind == "" {
		return ir.BackupKindFull
	}
	return kind
}

// RebuildLineageCatalogAt walks the conventional one-segment layout
// (manifest.json + manifests/incr-*.json) and writes a fresh
// single-segment lineage.json. Operator-facing entry point for
// `sluice backup verify --rebuild-catalog`. Only the one-segment
// (Dir == "") shape is rebuildable from a bare walk — a multi-segment
// rotated lineage's sub-dir structure is not recoverable without the
// catalog, by design (the catalog IS the structural record for a
// rotated backup). Returns the segment + manifest count.
func RebuildLineageCatalogAt(ctx context.Context, store ir.BackupStore) (segments, manifests int, err error) {
	recs, err := listAllManifestsViaWalk(ctx, store)
	if err != nil {
		return 0, 0, err
	}
	if len(recs) == 0 {
		return 0, 0, errors.New("rebuild: no manifests found at the conventional layout")
	}
	now := time.Now().UTC()
	root := LineageSegment{
		Dir:              "",
		FullManifestPath: ManifestFileName,
		Codec:            DefaultCodec,
	}
	for _, r := range recs {
		switch canonicalKind(r.manifest.Kind) {
		case ir.BackupKindFull:
			root.SegmentID = manifestBackupID(r.manifest)
			root.StartPosition = r.manifest.EndPosition
			if root.EndPosition.Engine == "" && root.EndPosition.Token == "" {
				root.EndPosition = r.manifest.EndPosition
			}
		case ir.BackupKindIncremental:
			root.Incrementals = append(root.Incrementals, r.path)
			root.EndPosition = r.manifest.EndPosition
		}
	}
	cat := &LineageCatalog{
		FormatVersion: lineageCatalogFormatVersion,
		SourceEngine:  recs[0].manifest.SourceEngine,
		SluiceVersion: recs[0].manifest.SluiceVersion,
		CreatedAt:     now,
		UpdatedAt:     now,
		Segments:      []LineageSegment{root},
	}
	if err := writeLineageCatalog(ctx, store, cat); err != nil {
		return 0, 0, err
	}
	return 1, len(recs), nil
}
