// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

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
// v0.67.0+ defaults to zstd, not gzip — see [blobcodec.DefaultCodec].)
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
	"slices"
	"sort"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// LineageCatalogFileName is the filename of the lineage catalog within
// a backup store. Lives at the lineage root, sibling to the segment-0
// full's [ManifestFileName].
const LineageCatalogFileName = "lineage.json"

// lineageCatalogFormatVersion is the integer version of the
// lineage.json schema. Bumped only on incompatible field renames;
// additive fields don't require a bump.
const lineageCatalogFormatVersion = 1

// Catalog is the deserialised content of lineage.json. The
// Segments slice is ordered: Segments[0] is the lineage root (the
// first full + its incrementals), each subsequent segment opened by an
// inline rotation. Segments[i].EndPosition <= Segments[i+1].StartPosition
// by construction (the rotation FSM's S>=P_N hard-fail assertion); the
// restore lineage-walk validates it with the SAME monotonicity check
// it uses for intra-segment incremental boundaries.
type Catalog struct {
	FormatVersion int       `json:"format_version"`
	LineageID     string    `json:"lineage_id,omitempty"`
	SluiceVersion string    `json:"sluice_version,omitempty"`
	SourceEngine  string    `json:"source_engine,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	// Segments is the ordered segment list. One entry for a
	// never-rotated backup; N for a backup rotated N-1 times.
	Segments []Segment `json:"segments"`

	// RestorableFromSegment is the prune floor: the index of the
	// oldest segment still fully restorable. Prune (ADR-0046 §4)
	// advances it as it drops leading whole segments. Restore refuses
	// to start before this segment. Zero on an unpruned lineage.
	RestorableFromSegment int `json:"restorable_from_segment"`

	// guardGen / guardObserved carry the ADR-0161 concurrent-writer
	// guard's observation from [LoadLineageCatalogForUpdate] to
	// [WriteLineageCatalog]: the chain write-generation listed BEFORE
	// this catalog was read, arming the write's compare-and-swap.
	// Unexported by design — never serialized, and a catalog that was
	// not loaded for update (read paths, test fixtures) writes
	// unguarded, today's behavior.
	guardGen      uint64
	guardObserved bool
}

// Segment is one segment within a lineage: a `backup full`
// anchor plus the incrementals chained off it, capped by policy.
//
// Wire shape matches ADR-0046 §1. CappedAt / CapReason are absent
// (zero / empty) on the open (last) segment and set on every prior
// segment by the rotation FSM's atomic COMMIT write.
type Segment struct {
	// SegmentID is a stable identifier for the segment (the full's
	// BackupID). Used for log lines and operator inspection.
	SegmentID string `json:"segment_id"`

	// Dir is the segment's sub-directory relative to the lineage root,
	// consumed via [NewPrefixedStore]. EMPTY for segment 0 of a
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

	// IncrementalCoverageStart is the position of the FIRST event this
	// segment's incrementals cover (ADR-0067). For a rotation-opened
	// segment it is the PRIOR segment's EndPosition (P_N): rotation keeps
	// the (P_N, S] overlap in the new segment's incrementals instead of
	// dropping it, so the lineage is born-contiguous and compactable
	// (the overlap re-applies idempotently on restore, ADR-0010, the same
	// snapshot->CDC handoff dedup used at the initial full->stream
	// transition). It may therefore PRECEDE StartPosition (the segment's
	// full anchor S). StartPosition keeps its meaning (full anchor /
	// restore base); only the two CONTIGUITY checks (compaction §14d +
	// restore's segment-to-segment boundary) key off this field.
	//
	// Empty/absent resolves to StartPosition via
	// [Segment.IncrementalCoverageStartOrStart] — the pre-ADR-0067
	// behavior (never-rotated segments, segment 0, and every backup
	// written before this field existed; additive, no format bump).
	//
	// ADR-0087 (Bug 139): a rotation-born segment whose creating session
	// stopped or crashed at the boundary BEFORE committing its first
	// incremental stays empty here (no incremental ever proved the
	// overlap) — which previously left it un-compactable across the prior
	// boundary forever. Two things now heal that: compact subdivides at
	// the gap instead of refusing the run, and the next stream/incremental
	// resume of such a segment ([rotationBoundaryResumeStart]) replays from
	// the prior segment's EndPosition (P_N), so the first post-resume
	// commit stamps this field = P_N and the segment becomes contiguous.
	IncrementalCoverageStart ir.Position `json:"incremental_coverage_start,omitempty"`

	// CappedAt is the instant the rotation FSM's COMMIT closed this
	// segment. nil/zero on the open (last) segment.
	CappedAt *time.Time `json:"capped_at,omitempty"`

	// CapReason records why the segment was capped:
	// [rotationReasonAge] or [rotationReasonChainLength]. Empty on the
	// open segment.
	CapReason string `json:"cap_reason,omitempty"`

	// Codec is the compression codec every chunk in this segment was
	// written with (ADR-0046 §5). Recorded here, NEVER inferred from
	// the chunk bytes on restore. Empty resolves to [blobcodec.DefaultCodec]
	// (zstd, v0.67.0+); a v0.67.0+ backup always records it explicitly.
	Codec blobcodec.Codec `json:"codec,omitempty"`

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
func (s *Segment) HasVerbatimExtensionColumns() bool {
	return len(s.VerbatimExtensionColumns) > 0
}

// codecOrDefault returns the segment's recorded codec, resolving an
// empty value to [blobcodec.DefaultCodec] (zstd).
func (s *Segment) CodecOrDefault() blobcodec.Codec { return blobcodec.ResolveCodec(s.Codec) }

// incrementalCoverageStartOrStart returns the segment's earliest
// incremental-coverage position (ADR-0067). When
// [Segment.IncrementalCoverageStart] is set (a rotation-opened
// segment, where it equals the prior segment's EndPosition P_N) that
// value wins; otherwise it resolves to StartPosition (never-rotated
// segments, segment 0, and pre-ADR-0067 backups — the historical
// behavior where a segment's incrementals began at its full anchor).
// The two contiguity checks (compaction §14d + restore segment-to-
// segment boundary) compare against THIS, not StartPosition, so a
// rotated lineage reads as gapless.
func (s *Segment) IncrementalCoverageStartOrStart() ir.Position {
	if s.IncrementalCoverageStart.Engine == "" && s.IncrementalCoverageStart.Token == "" {
		return s.StartPosition
	}
	return s.IncrementalCoverageStart
}

// open reports whether this is the open (last, uncapped) segment.
func (s *Segment) Open() bool { return s.CappedAt == nil || s.CappedAt.IsZero() }

// store returns the per-segment [irbackup.Store] view: the lineage
// store narrowed to the segment's Dir (a no-op wrap when Dir == "",
// the common one-segment / root-segment shape).
func (s *Segment) Store(root irbackup.Store) irbackup.Store {
	return NewPrefixedStore(root, s.Dir)
}

// LoadLineageCatalog reads lineage.json from store. Returns (catalog,
// true, nil) when present and parseable; (nil, false, nil) when
// absent; (nil, false, err) for I/O / parse / version failures. The
// "absent" case is the legacy one-segment fall-through callers handle
// by synthesising a single root segment over the conventional layout.
func LoadLineageCatalog(ctx context.Context, store irbackup.Store) (*Catalog, bool, error) {
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
	var cat Catalog
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

// WriteLineageCatalog serialises cat as indented JSON and writes it to
// store via a single-object Put. **This single Put is the rotation
// FSM's atomic linearization point** (ADR-0046 §2 COMMIT): it is
// always issued AFTER the next segment's full is durable, so there is
// no window in which the lineage is non-authoritative. Atomic at the
// storage layer from any reader's perspective (object stores: a Put is
// all-or-nothing; local FS: write-tmp + rename inside LocalStore).
//
// ADR-0161: a catalog loaded via [LoadLineageCatalogForUpdate] carries
// the chain write-generation observed before its read; on a store with
// the [irbackup.ConditionalPutter] capability this write first CLAIMS
// the next generation, refusing loudly (coded
// [sluicecode.CodeBackupChainConflict]) when another writer advanced
// the chain in between — the catalog is then NOT written. An unstamped
// catalog (read-path loads, test fixtures) writes unguarded.
func WriteLineageCatalog(ctx context.Context, store irbackup.Store, cat *Catalog) error {
	cat.FormatVersion = lineageCatalogFormatVersion
	b, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal lineage catalog: %w", err)
	}
	claimed := false
	if cp, ok := store.(irbackup.ConditionalPutter); ok && cat.guardObserved {
		won, err := claimChainGen(ctx, cp, cat.guardGen+1, cat.SluiceVersion)
		if err != nil {
			return err // the coded concurrent-writer refusal; catalog untouched
		}
		if won {
			// Advance the stamp as soon as the slot is OURS (not after the
			// Put): a second write of this same loaded catalog — including
			// a retry after a failed Put — claims the slot AFTER its own
			// rather than self-conflicting on a generation it consumed.
			cat.guardGen++
			claimed = true
		}
	}
	if err := store.Put(ctx, LineageCatalogFileName, bytes.NewReader(b)); err != nil {
		return err
	}
	if claimed {
		gcChainGenMarkers(ctx, store, cat.guardGen)
	}
	return nil
}

// SegmentRecord pairs a parsed manifest with the path it loaded from
// AND the segment it belongs to. The chain-walk + restore code carries
// the segment so it can resolve the per-segment store + codec without
// re-deriving them. Embeds [ManifestRecord] so existing helpers
// (ManifestBackupID etc.) work unchanged.
type SegmentRecord struct {
	ManifestRecord
	Segment *Segment
}

// ResolveLineage returns the lineage for store. When lineage.json is
// present it's authoritative. When UNREADABLE (parse/version/IO
// error), [LoadLineageCatalog] already surfaced a loud error. When
// ABSENT, a single synthetic root segment (Dir == "", codec sniffed
// from chunk magic — [blobcodec.DefaultCodec] only as the assumed
// fallback, audit N-14) is constructed over the conventional layout — the
// pre-ADR single-chain shape, a one-segment lineage by strict
// generalization — BUT only if the on-disk shape is genuinely
// single-segment. If rotation-opened segment sub-dirs (`seg-*`) exist
// while lineage.json is absent, the backup is a rotated multi-segment
// lineage that cannot be reconstructed from a bare walk: that is a
// LOUD refusal, never a silent root-only partial (Bug 66 — the absent
// case does NOT auto-surface from LoadLineageCatalog the way the
// unreadable case does, so ResolveLineage must guard it here).
func ResolveLineage(ctx context.Context, store irbackup.Store) (*Catalog, error) {
	cat, ok, err := LoadLineageCatalog(ctx, store)
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
	segEvidence, err := store.List(ctx, RotationSegmentDirPrefix)
	if err != nil {
		return nil, fmt.Errorf("inspect rotation segments (%s*): %w", RotationSegmentDirPrefix, err)
	}
	if len(segEvidence) > 0 {
		return nil, fmt.Errorf(
			"backup has rotation-opened segment directories (%s*) but lineage.json is missing: "+
				"a multi-segment lineage cannot be reconstructed from a bare directory walk; "+
				"refusing to restore only the root segment (DR data — never a silent partial). "+
				"Restore from a copy whose lineage.json is intact (it is the authoritative structural "+
				"record for a rotated backup; `backup verify --rebuild-catalog` only rebuilds the "+
				"legacy one-segment shape)",
			RotationSegmentDirPrefix,
		)
	}
	// Absent and genuinely single-segment: synthesise the legacy
	// lineage. The manifest list is discovered by a directory walk (the
	// pre-v0.47.0 fall-through, preserved for the one-segment shape).
	root := &Segment{
		Dir:              "",
		FullManifestPath: ManifestFileName,
	}
	exists, err := store.Exists(ctx, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("inspect %q: %w", ManifestFileName, err)
	}
	if !exists {
		return nil, errors.New("no manifests found in store (no lineage.json, no manifest.json) — take a `sluice backup full` first")
	}
	fullM, err := ReadManifestAt(ctx, store, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", ManifestFileName, err)
	}
	root.SegmentID = ManifestBackupID(fullM)
	root.StartPosition = fullM.EndPosition
	root.EndPosition = fullM.EndPosition
	incs, err := store.List(ctx, IncrementalManifestPrefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", IncrementalManifestPrefix, err)
	}
	sort.Strings(incs)
	for _, p := range incs {
		if isIncrementalManifestPath(p) {
			root.Incrementals = append(root.Incrementals, p)
		}
	}
	// The synthetic root's codec is SNIFFED from chunk magic bytes, not
	// assumed (audit N-14): a gzip/none-era chain whose lineage.json was
	// lost previously got DefaultCodec stamped here and every consumer
	// (restore / verify / chain-walk) then hit a bare zstd decode error.
	// Best-effort posture — this result is in-memory (re-derived per
	// call, never written), so on any probe failure the write default is
	// assumed with a WARN naming the assumption; a wrong assumption
	// still fails LOUDLY at the first chunk decode, and `backup verify
	// --rebuild-catalog` (which sniffs strictly, with key material for
	// encrypted chains) records the truth durably.
	root.Codec = blobcodec.DefaultCodec
	sniffed, found, sniffErr := sniffLegacyRootCodec(ctx, store, fullM, root.Incrementals)
	switch {
	case sniffErr != nil:
		slog.WarnContext(
			ctx, "lineage: cannot sniff the codec for the synthetic root segment; assuming the write default — if chunk decodes fail, run `sluice backup verify --rebuild-catalog` (with --encrypt + key material for an encrypted chain) to sniff and record the true codec",
			slog.String("assumed_codec", string(blobcodec.DefaultCodec)),
			slog.String("reason", sniffErr.Error()),
		)
	case !found:
		slog.WarnContext(
			ctx, "lineage: chain references no chunks; assuming the write default codec for the synthetic root segment",
			slog.String("assumed_codec", string(blobcodec.DefaultCodec)),
		)
	default:
		root.Codec = sniffed
	}
	return &Catalog{
		FormatVersion: lineageCatalogFormatVersion,
		SourceEngine:  fullM.SourceEngine,
		SluiceVersion: fullM.SluiceVersion,
		Segments:      []Segment{*root},
	}, nil
}

// ListAllSegmentManifests returns every manifest across every segment
// of the lineage in lineage order (segment 0 full, segment 0
// incrementals, segment 1 full, ...), each tagged with its segment so
// callers know which per-segment store + codec to use. Replaces the
// pre-ADR [listAllManifests] catalog/walk dispatch — the lineage is
// the single dispatch point now.
func ListAllSegmentManifests(ctx context.Context, store irbackup.Store) ([]SegmentRecord, error) {
	cat, err := ResolveLineage(ctx, store)
	if err != nil {
		return nil, err
	}
	var out []SegmentRecord
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		if err := blobcodec.ValidateRecordedCodec(seg.Codec); err != nil {
			return nil, err
		}
		ss := seg.Store(store)
		fm, err := ReadManifestAt(ctx, ss, seg.FullManifestPath)
		if err != nil {
			return nil, fmt.Errorf("segment %d (%s) full %q: %w", i, seg.SegmentID, seg.FullManifestPath, err)
		}
		out = append(out, SegmentRecord{
			ManifestRecord: ManifestRecord{Path: seg.FullManifestPath, Manifest: fm},
			Segment:        seg,
		})
		for _, ip := range seg.Incrementals {
			im, err := ReadManifestAt(ctx, ss, ip)
			if err != nil {
				return nil, fmt.Errorf("segment %d (%s) incremental %q: %w", i, seg.SegmentID, ip, err)
			}
			out = append(out, SegmentRecord{
				ManifestRecord: ManifestRecord{Path: ip, Manifest: im},
				Segment:        seg,
			})
		}
	}
	return out, nil
}

// UpdateLineageForManifestBestEffort keeps lineage.json in sync after
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
//
// ONE error class is never swallowed: the ADR-0161 concurrent-writer
// conflict (coded [sluicecode.CodeBackupChainConflict]) is returned to
// the caller. A conflict is not a transient store hiccup — it is
// evidence a SECOND writer is interleaving this chain (a duplicate
// cron, a racing compact/prune), and the next write would not heal
// that; the operator must be told loudly.
func UpdateLineageForManifestBestEffort(
	ctx context.Context,
	store irbackup.Store,
	manifest *irbackup.Manifest,
	manifestPath string,
	codec blobcodec.Codec,
) error {
	err := UpdateLineageForManifest(ctx, store, manifest, manifestPath, codec)
	if err == nil {
		return nil
	}
	if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeBackupChainConflict {
		return err
	}
	slog.WarnContext(
		ctx, "lineage catalog update failed; lineage.json may be stale until next write",
		slog.String("manifest_path", manifestPath),
		slog.String("err", err.Error()),
	)
	return nil
}

// UpdateLineageForManifest is the fail-loud counterpart of
// [UpdateLineageForManifestBestEffort]: it appends/updates the manifest
// within the lineage's OPEN segment and advances that segment's
// EndPosition (seeding a single root segment on the first write), and
// returns any catalog-write error rather than swallowing it. Rotation's
// segment-append + cap is a SEPARATE atomic write (the rotation FSM);
// this helper never touches CappedAt / prior segments.
func UpdateLineageForManifest(
	ctx context.Context,
	store irbackup.Store,
	manifest *irbackup.Manifest,
	manifestPath string,
	codec blobcodec.Codec,
) error {
	if manifest == nil {
		return errors.New("lineage catalog: nil manifest")
	}
	cat, ok, guard, err := loadCatalogForUpdate(ctx, store)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if !ok {
		// First lineage.json write for this backup. Seed a single root
		// segment over the conventional layout (Dir == ""), stamped with
		// the ADR-0161 observation so even the seeding write is a CAS —
		// two writers racing to seed one chain conflict loudly.
		cat = &Catalog{
			FormatVersion: lineageCatalogFormatVersion,
			SourceEngine:  manifest.SourceEngine,
			SluiceVersion: manifest.SluiceVersion,
			CreatedAt:     now,
			Segments: []Segment{{
				SegmentID:        ManifestBackupID(manifest),
				Dir:              "",
				FullManifestPath: ManifestFileName,
				Codec:            blobcodec.ResolveCodec(codec),
			}},
		}
		stampChainGuard(cat, guard)
	}
	seg := &cat.Segments[len(cat.Segments)-1] // the open segment
	switch CanonicalKind(manifest.Kind) {
	case irbackup.BackupKindFull:
		seg.FullManifestPath = manifestPath
		seg.SegmentID = ManifestBackupID(manifest)
		seg.StartPosition = manifest.EndPosition
		seg.EndPosition = manifest.EndPosition
		if seg.Codec == "" {
			seg.Codec = blobcodec.ResolveCodec(codec)
		}
		// ADR-0047 backup capability marker. The full carries the
		// authoritative schema; record the verbatim-typed columns so
		// the restore-time engine gate can refuse a non-PG target
		// loudly. Recorded, never sniffed at restore (ADR-0046 idiom);
		// stays nil for every non-verbatim backup so the field is
		// absent in the common case (and legacy readers ignore it).
		seg.VerbatimExtensionColumns = VerbatimExtensionColumnsIn(manifest.Schema)
	case irbackup.BackupKindIncremental:
		// ADR-0067: the FIRST incremental's StartPosition defines the
		// segment's earliest incremental coverage. Record it ONLY when it
		// differs from the full anchor (StartPosition) — i.e. a rotation
		// that kept the (P_N, S] overlap, where the first incremental
		// starts at P_N < S. Derived from the ACTUAL first incremental
		// (not recorded at rotation COMMIT) so it stays honest across a
		// crash-recovery that resumes at S rather than P_N: there the
		// first incremental starts at S, equals StartPosition, and the
		// field correctly stays unset (a normal, non-overlap segment).
		// The root segment's first incremental also starts at the full
		// anchor, so the field stays unset there too.
		if len(seg.Incrementals) == 0 && manifest.StartPosition != seg.StartPosition {
			seg.IncrementalCoverageStart = manifest.StartPosition
		}
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
	return WriteLineageCatalog(ctx, store, cat)
}

// OpenSegmentStore returns the open (last) segment's store view + the
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
func OpenSegmentStore(ctx context.Context, store irbackup.Store, writeCodec blobcodec.Codec) (irbackup.Store, blobcodec.Codec, error) {
	cat, ok, err := LoadLineageCatalog(ctx, store)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return store, blobcodec.ResolveCodec(writeCodec), nil
	}
	seg := &cat.Segments[len(cat.Segments)-1]
	if err := blobcodec.ValidateRecordedCodec(seg.Codec); err != nil {
		return nil, "", err
	}
	c := seg.Codec
	if c == "" {
		c = blobcodec.ResolveCodec(writeCodec)
	}
	return seg.Store(store), blobcodec.ResolveCodec(c), nil
}

// CanonicalKind normalises empty Kind to BackupKindFull. Mirror of
// the ir.CanonicalKind helper; duplicated here so the catalog code
// doesn't pull in unexported ir helpers.
func CanonicalKind(kind string) string {
	if kind == "" {
		return irbackup.BackupKindFull
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
//
// The rebuilt segment's codec is SNIFFED from chunk magic bytes and
// VERIFIED across the chain ([SniffChainCodec] — audit N-14; the
// pre-fix rebuild stamped [blobcodec.DefaultCodec] unconditionally, so
// a gzip/none chain got a rebuilt record that LIED and restore failed
// with a bare zstd decode error whose remediation hint pointed back at
// this very tool). Because this path writes the DURABLE record restore
// will trust, every ambiguity refuses loudly: mixed probes, unreadable
// chunks, unknown magic, and encrypted chunks without env (the codec is
// sealed inside the encryption envelope — supply the chain's read
// envelope, built from the operator's --encrypt flags; nil is fine for
// plaintext chains). Only a chain with NO chunks at all falls back to
// [blobcodec.DefaultCodec], with a WARN naming the assumption.
func RebuildLineageCatalogAt(ctx context.Context, store irbackup.Store, env crypto.EnvelopeEncryption) (segments, manifests int, err error) {
	// ADR-0161: observe the chain write-generation BEFORE the walk, so
	// the rebuild's catalog write below is a CAS — a live writer landing
	// mid-rebuild conflicts loudly instead of being clobbered by (or
	// clobbering) the rebuilt record.
	genN, genObserved, err := observeChainGen(ctx, store)
	if err != nil {
		return 0, 0, fmt.Errorf("rebuild: %w", err)
	}
	recs, err := ListAllManifestsViaWalk(ctx, store)
	if err != nil {
		return 0, 0, err
	}
	if len(recs) == 0 {
		return 0, 0, errors.New("rebuild: no manifests found at the conventional layout")
	}
	codec, found, err := SniffChainCodec(ctx, store, recs, env)
	switch {
	case errors.Is(err, ErrCodecSniffEncrypted):
		return 0, 0, fmt.Errorf(
			"rebuild: %w — re-run `sluice backup verify --rebuild-catalog` with --encrypt and the chain's "+
				"passphrase / KMS reference so the codec can be read from a decrypted chunk; recording a guessed "+
				"codec is the exact wrong-heal this rebuild used to perform (audit N-14)", err,
		)
	case err != nil:
		return 0, 0, fmt.Errorf("rebuild: sniff segment codec: %w", err)
	case !found:
		codec = blobcodec.DefaultCodec
		slog.WarnContext(
			ctx, "rebuild: chain references no chunks; recording the write-default codec on the rebuilt segment (assumption — nothing to probe)",
			slog.String("assumed_codec", string(codec)),
		)
	default:
		slog.InfoContext(
			ctx, "rebuild: segment codec sniffed from chunk magic bytes",
			slog.String("codec", string(codec)),
		)
	}
	now := time.Now().UTC()
	root := Segment{
		Dir:              "",
		FullManifestPath: ManifestFileName,
		Codec:            codec,
	}
	for _, r := range recs {
		switch CanonicalKind(r.Manifest.Kind) {
		case irbackup.BackupKindFull:
			root.SegmentID = ManifestBackupID(r.Manifest)
			root.StartPosition = r.Manifest.EndPosition
			if root.EndPosition.Engine == "" && root.EndPosition.Token == "" {
				root.EndPosition = r.Manifest.EndPosition
			}
		case irbackup.BackupKindIncremental:
			root.Incrementals = append(root.Incrementals, r.Path)
			root.EndPosition = r.Manifest.EndPosition
		}
	}
	cat := &Catalog{
		FormatVersion: lineageCatalogFormatVersion,
		SourceEngine:  recs[0].Manifest.SourceEngine,
		SluiceVersion: recs[0].Manifest.SluiceVersion,
		CreatedAt:     now,
		UpdatedAt:     now,
		Segments:      []Segment{root},
	}
	stampChainGuard(cat, chainGuardStamp{gen: genN, observed: genObserved})
	if err := WriteLineageCatalog(ctx, store, cat); err != nil {
		return 0, 0, err
	}
	return 1, len(recs), nil
}

// ReconcileOpenSegmentCatalog heals the OPEN (last) segment's recorded
// Incrementals list against the on-disk manifest chain, called on stream
// resume before the parent is resolved.
//
// Why this exists: an incremental's manifest is written durably FIRST,
// then its lineage.json entry is appended best-effort (so a transient
// catalog-write failure never fails the stream). A crash — or a ctx
// cancel — between those two steps leaves the incremental ON DISK but
// ABSENT from the catalog's Incrementals list. On resume the stream
// re-stitches off the on-disk tail correctly (the data chain stays
// intact and the next incremental's ParentBackupID points at the
// orphan), but the catalog keeps the gap: its first recorded
// incremental now parents off the orphan instead of the segment full.
// Restore's strict per-link chain walk then refuses the segment as
// "branching/mis-stitched lineage" — a loud, un-restorable backup from
// what is actually a complete on-disk chain. (Reproduced as the
// ADR-0046 crash-injection matrix's post-drain/post-snapshot flake: the
// rotation-opened segment's first (P_N, S] overlap incremental was the
// orphan.)
//
// The heal rebuilds the open segment's Incrementals from the on-disk
// chain ORDER (parent links — not the filename sort, which can tie on
// same-millisecond rollovers), re-deriving IncrementalCoverageStart and
// EndPosition from the true first/last links. It is conservative and
// idempotent: a no-op when the catalog already matches disk, and it
// REFUSES TO GUESS when the on-disk set isn't a single clean linear
// chain off the full (a parentless incremental, a branch, or an
// unreachable manifest) — those are left for restore's strict check to
// surface rather than masked by a heuristic repair.
func ReconcileOpenSegmentCatalog(ctx context.Context, rootStore, segStore irbackup.Store) error {
	cat, ok, err := LoadLineageCatalogForUpdate(ctx, rootStore)
	if err != nil || !ok || len(cat.Segments) == 0 {
		return err // nothing catalogued yet — fresh start, nothing to heal
	}
	seg := &cat.Segments[len(cat.Segments)-1]

	recs, err := ListAllManifestsViaWalk(ctx, segStore)
	if err != nil {
		return err
	}

	// Index incrementals by parent BackupID and locate the full.
	childByParent := make(map[string]ManifestRecord, len(recs))
	var fullID string
	for _, r := range recs {
		switch CanonicalKind(r.Manifest.Kind) {
		case irbackup.BackupKindFull:
			fullID = ManifestBackupID(r.Manifest)
		case irbackup.BackupKindIncremental:
			pid := r.Manifest.ParentBackupID
			if pid == "" {
				return nil // can't chain deterministically — stay strict
			}
			if _, dup := childByParent[pid]; dup {
				return nil // branch: two children share a parent — don't guess
			}
			childByParent[pid] = r
		}
	}
	if fullID == "" {
		return nil // no full walked (unexpected) — don't touch the catalog
	}

	// Walk the linear chain from the full, consuming each link.
	ordered := make([]ManifestRecord, 0, len(childByParent))
	for cur := fullID; ; {
		child, found := childByParent[cur]
		if !found {
			break
		}
		ordered = append(ordered, child)
		delete(childByParent, cur)
		cur = ManifestBackupID(child.Manifest)
	}
	if len(childByParent) != 0 {
		return nil // an incremental wasn't reachable from the full — stay strict
	}

	paths := make([]string, len(ordered))
	for i, r := range ordered {
		paths[i] = r.Path
	}
	if slices.Equal(seg.Incrementals, paths) {
		return nil // catalog already matches disk — idempotent no-op
	}

	seg.Incrementals = paths
	if len(ordered) > 0 {
		first := ordered[0].Manifest
		// ADR-0067: a rotation-opened segment's first incremental starts
		// at the (P_N, S] overlap, which precedes the full's anchor; record
		// that coverage start. A never-rotated segment's first incremental
		// starts at the full's end (== seg.StartPosition) — no overlap.
		if first.StartPosition != seg.StartPosition {
			seg.IncrementalCoverageStart = first.StartPosition
		}
		seg.EndPosition = ordered[len(ordered)-1].Manifest.EndPosition
	}
	cat.UpdatedAt = time.Now().UTC()
	slog.WarnContext(
		ctx, "lineage: healed open-segment catalog from the on-disk chain "+
			"(an incremental was orphaned from lineage.json by a crash/cancel "+
			"during its best-effort catalog update; re-catalogued in chain order)",
		slog.String("seg_dir", seg.Dir),
		slog.Int("on_disk_incrementals", len(paths)),
	)
	return WriteLineageCatalog(ctx, rootStore, cat)
}
