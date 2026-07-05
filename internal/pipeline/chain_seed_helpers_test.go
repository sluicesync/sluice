// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// Lineage-segment seeding helpers duplicated from the carved-out backup
// package's chain_compact_test.go so the root-resident stream_resume_heal_test
// (which pins the root rotationBoundaryResumeStart behaviour) keeps its
// fixtures without importing the backup test tree.

type segmentSeedOpts struct {
	// codecsPerSegment, when len > 0, overrides the per-segment codec.
	codecsPerSegment []blobcodec.Codec
	// encPerSegment, when len > 0, attaches ChainEncryption to each
	// segment's full.
	encPerSegment []*irbackup.ChainEncryption
	// gapBetweenBoundaries, when true, deliberately advances each
	// segment's StartPosition past the prior's EndPosition (creating
	// the contiguity-violation shape).
	gapBetweenBoundaries bool
	// gapAtSegment marks the SPECIFIC non-root segment indices that take
	// the gap shape (full anchor AND incrementals advanced past P_N, no
	// stamp) — models a mid-chain rotation-born segment that received
	// incrementals starting at S but was never stamped (the bd0c5e94 rig
	// shape). Unlike gapBetweenBoundaries (every boundary), this gaps only
	// the listed segments.
	gapAtSegment map[int]bool
	// rotatedOverlap, when true, produces the ADR-0067 rotation shape for
	// each non-root segment: the full is anchored at S = prior.End + 50
	// (a gap, like gapBetweenBoundaries), BUT the segment's incrementals
	// KEEP the (P_N, S] overlap — they start at P_N (== prior.End) — and
	// the segment records IncrementalCoverageStart = P_N. Such a lineage
	// is contiguous (prior.End == cur.IncrementalCoverageStart) and so
	// compactable, unlike the gapBetweenBoundaries shape.
	rotatedOverlap bool

	// zeroIncrStampless, when non-nil, marks segment indices that take
	// the exact Bug-139 shape: rotation-born (full anchored at S =
	// prior.End + 50), ZERO incrementals, and NO IncrementalCoverageStart
	// stamp — so the segment resolves to its full anchor S, a gap past the
	// prior segment's P_N. Such a segment's EndPosition stays at S (== its
	// StartPosition). Only valid on non-root segments (i > 0).
	zeroIncrStampless map[int]bool

	// openLastSegment, when true, leaves the LAST segment uncapped (the
	// lineage's open segment). Used with zeroIncrStampless to model the
	// trailing idle-stop OPEN segment.
	openLastSegment bool
}

func seedSegmentsWithGapsOpts(t *testing.T, store irbackup.Store, base time.Time, gaps []time.Duration, opts segmentSeedOpts) {
	t.Helper()
	n := len(gaps) + 1
	cumulative := time.Duration(0)
	prevEndLSN := uint64(100)
	cappedAt := time.Time{} // overwritten per loop

	// First, build segments and write their conventional files. Then
	// hand-author the lineage.json describing the multi-segment shape.
	cat := &lineage.Catalog{
		FormatVersion: 1,
		SourceEngine:  "postgres",
		CreatedAt:     base,
		UpdatedAt:     base,
	}

	for i := 0; i < n; i++ {
		segCreatedAt := base.Add(cumulative)
		dir := ""
		if i > 0 {
			dir = fmt.Sprintf("seg-%d", i)
		}
		codec := blobcodec.CodecGzip
		if i < len(opts.codecsPerSegment) {
			codec = opts.codecsPerSegment[i]
		}
		var chainEnc *irbackup.ChainEncryption
		if i < len(opts.encPerSegment) {
			chainEnc = opts.encPerSegment[i]
		}

		// Segment's full anchor + incremental-coverage start. Default:
		// both at the prior segment's EndPosition (contiguous, no
		// overlap). gapBetweenBoundaries advances BOTH past P_N (an
		// uncompactable gap — the (P_N, S] window lives only in the
		// full). rotatedOverlap (ADR-0067) anchors the full at S = P_N+50
		// but KEEPS the incrementals starting at P_N (the overlap).
		pN := prevEndLSN
		fullAnchorLSN := pN
		incrStartLSN := pN
		stampless := i > 0 && opts.zeroIncrStampless[i]
		switch {
		case i > 0 && (opts.gapBetweenBoundaries || opts.gapAtSegment[i]):
			fullAnchorLSN, incrStartLSN = pN+50, pN+50
		case i > 0 && opts.rotatedOverlap:
			fullAnchorLSN, incrStartLSN = pN+50, pN
		case stampless:
			// Bug-139 shape: rotation-born full anchored at S = P_N+50,
			// but the segment never received an incremental in its
			// creating session — so no stamp and no incrementals.
			fullAnchorLSN, incrStartLSN = pN+50, pN+50
		}
		// Full is a snapshot at fullAnchorLSN: Start == End == fullAnchorLSN.
		full := compactSeedFullManifest(t, segCreatedAt, fullAnchorLSN, chainEnc)
		segStore := lineage.NewPrefixedStore(store, dir)
		if err := lineage.WriteManifestAt(context.Background(), segStore, lineage.ManifestFileName, full); err != nil {
			t.Fatalf("seed full %d: %v", i, err)
		}
		// Two incrementals stepping LSN forward by 50 each — unless this is
		// the stamp-less zero-incremental Bug-139 segment, which has none.
		incrPaths := []string{}
		curLSN := incrStartLSN
		if !stampless {
			for j := 1; j <= 2; j++ {
				incrCreatedAt := segCreatedAt.Add(time.Duration(j) * 10 * time.Minute)
				nextLSN := curLSN + 50
				ip := fmt.Sprintf("manifests/incr-%05d-seg%d-%d.json", j, i, j)
				im := compactSeedIncrementalManifest(t, incrCreatedAt, curLSN, nextLSN)
				// Seed a tiny change chunk file the manifest references,
				// so segmentByteTotal has something to read.
				chunkPath := fmt.Sprintf("chunks/_changes/seg%d-incr%d.jsonl.gz", i, j)
				body := []byte(fmt.Sprintf("seg%d-incr%d-body", i, j))
				if err := segStore.Put(context.Background(), chunkPath, bytes.NewReader(body)); err != nil {
					t.Fatalf("seed change chunk: %v", err)
				}
				im.ChangeChunks = []*irbackup.ChunkInfo{{File: chunkPath, RowCount: 1, SHA256: ""}}
				if err := lineage.WriteManifestAt(context.Background(), segStore, ip, im); err != nil {
					t.Fatalf("seed incr (%d,%d): %v", i, j, err)
				}
				incrPaths = append(incrPaths, ip)
				curLSN = nextLSN
			}
		}
		prevEndLSN = curLSN

		segEntry := lineage.Segment{
			SegmentID:        full.BackupID,
			Dir:              dir,
			FullManifestPath: lineage.ManifestFileName,
			Incrementals:     incrPaths,
			StartPosition:    full.EndPosition,
			EndPosition:      ir.Position{Engine: "postgres", Token: lsnToken(curLSN)},
			Codec:            codec,
		}
		if i > 0 && opts.rotatedOverlap {
			// ADR-0067: incrementals kept the (P_N, S] overlap; record
			// P_N as the earliest incremental coverage so the lineage is
			// contiguous with the prior segment (prior.End == this).
			segEntry.IncrementalCoverageStart = ir.Position{Engine: "postgres", Token: lsnToken(pN)}
		}
		// Cap every segment except the last (the lineage's open
		// segment). For compact tests we mostly want closed segments
		// — but a never-rotated last segment is fine; the eligibility
		// floor allows compact across any retained range. openLastSegment
		// leaves the trailing segment uncapped (the idle-stop OPEN shape).
		if i < n-1 || (i == n-1 && !opts.openLastSegment && stampless) {
			cappedAt = segCreatedAt.Add(time.Hour)
			if i < len(gaps) {
				cappedAt = segCreatedAt.Add(gaps[i] - time.Minute)
			}
			segEntry.CappedAt = &cappedAt
			segEntry.CapReason = rotationReasonAge
		}
		cat.Segments = append(cat.Segments, segEntry)

		if i < len(gaps) {
			cumulative += gaps[i]
		}
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("write lineage catalog: %v", err)
	}
}

func compactSeedFullManifest(t *testing.T, createdAt time.Time, lsn uint64, enc *irbackup.ChainEncryption) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     createdAt,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: lsnToken(lsn)},
		PartialState:  irbackup.BackupStateComplete,
	}
	if enc != nil {
		m.ChainEncryption = enc
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

func compactSeedIncrementalManifest(t *testing.T, createdAt time.Time, startLSN, endLSN uint64) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     createdAt,
		Kind:          irbackup.BackupKindIncremental,
		StartPosition: ir.Position{Engine: "postgres", Token: lsnToken(startLSN)},
		EndPosition:   ir.Position{Engine: "postgres", Token: lsnToken(endLSN)},
		PartialState:  irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

func lsnToken(lsn uint64) string {
	return fmt.Sprintf(`{"slot":"s","lsn":"0/%X"}`, lsn)
}
