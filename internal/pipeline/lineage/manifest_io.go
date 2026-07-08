// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// ManifestFileName is the filename of the manifest within a backup
// directory. Convention; restore looks here first.
const ManifestFileName = "manifest.json"

// IncrementalManifestPrefix is the path prefix incremental manifests
// live under. The full's manifest stays at the legacy
// [ManifestFileName] = "manifest.json" path; incrementals live under
// `manifests/incr-…json` so the chain-restore walker can list them
// without a per-name pattern match.
const IncrementalManifestPrefix = "manifests/"

// incrementalManifestFilenamePrefix is the per-file prefix every
// incremental manifest's basename starts with. The chain-walker filters
// `List(manifests/)` results on this prefix so non-manifest state
// files that share the directory (e.g. Phase 4's
// `manifests/stream_state.json`) are not mistaken for chain entries.
const incrementalManifestFilenamePrefix = "incr-"

// isIncrementalManifestPath reports whether path is shaped like a
// chain-restore-eligible incremental manifest entry — i.e. it sits
// directly under [IncrementalManifestPrefix], its basename begins with
// [incrementalManifestFilenamePrefix], and it ends in `.json`. Used
// by chain-walker manifest discovery so additions to the
// `manifests/` directory (such as `stream_state.json`) don't get
// treated as chain entries.
func isIncrementalManifestPath(path string) bool {
	if !strings.HasPrefix(path, IncrementalManifestPrefix) {
		return false
	}
	rest := path[len(IncrementalManifestPrefix):]
	if strings.ContainsRune(rest, '/') {
		// Something nested under manifests/ — not a chain entry shape.
		return false
	}
	if !strings.HasPrefix(rest, incrementalManifestFilenamePrefix) {
		return false
	}
	return strings.HasSuffix(rest, ".json")
}

// WriteManifestAt is [WriteManifest] generalised to a caller-supplied
// path. The full-backup writer's [WriteManifest] hard-codes
// [ManifestFileName]; the incremental writer needs an arbitrary path.
func WriteManifestAt(ctx context.Context, store irbackup.Store, path string, manifest *irbackup.Manifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, path, bytes.NewReader(b))
}

// unmarshalManifest decodes a manifest body. Pulled out so the
// chain-walk path and the legacy single-manifest path share one
// implementation.
func unmarshalManifest(body []byte) (*irbackup.Manifest, error) {
	var m irbackup.Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &m, nil
}

// ManifestRecord pairs a parsed manifest with the path it was loaded
// from. Used by chain-walk and parent-resolve logic.
type ManifestRecord struct {
	Path     string
	Manifest *irbackup.Manifest
}

// ListAllManifestsViaWalk is the [store.List] + per-manifest
// [ReadManifestAt] implementation over the conventional one-segment
// layout (manifest.json + manifests/incr-*.json). ADR-0046: the
// lineage catalog ([ListAllSegmentManifests]) is the cross-segment
// dispatch point now; this walk is used for a SINGLE segment's store
// (the open-segment parent resolve in incremental/stream, and the
// one-segment legacy / rebuild paths). It does NOT cross segment
// sub-dirs by design.
func ListAllManifestsViaWalk(ctx context.Context, store irbackup.Store) ([]ManifestRecord, error) {
	var out []ManifestRecord

	// Full's manifest at the legacy path.
	if exists, err := store.Exists(ctx, ManifestFileName); err != nil {
		return nil, fmt.Errorf("inspect %q: %w", ManifestFileName, err)
	} else if exists {
		m, err := ReadManifestAt(ctx, store, ManifestFileName)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", ManifestFileName, err)
		}
		out = append(out, ManifestRecord{Path: ManifestFileName, Manifest: m})
	}

	// Incremental manifests under `manifests/`. Filter the listing on
	// shape so non-manifest state files in the same directory (e.g.
	// Phase 4's `manifests/stream_state.json`) aren't mistaken for
	// chain entries.
	paths, err := store.List(ctx, IncrementalManifestPrefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", IncrementalManifestPrefix, err)
	}
	sort.Strings(paths) // lexically sortable by construction
	for _, p := range paths {
		if !isIncrementalManifestPath(p) {
			continue
		}
		m, err := ReadManifestAt(ctx, store, p)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", p, err)
		}
		out = append(out, ManifestRecord{Path: p, Manifest: m})
	}
	return out, nil
}

// ReadManifestAt is [ReadManifest] generalised to a caller-supplied
// path. Same format-version gating and ADR-0086 sidecar replay as the
// legacy helper — chain walkers reading a crashed full's in-progress
// manifest (the ADR-0085 adoption surface) must see the reconstructed
// truth too. The sidecar path recorded on the manifest is relative to
// the same store root the manifest was read from.
func ReadManifestAt(ctx context.Context, store irbackup.Store, path string) (*irbackup.Manifest, error) {
	rc, err := store.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", path, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	m, err := unmarshalManifest(body)
	if err != nil {
		return nil, err
	}
	if m.FormatVersion > irbackup.BackupFormatVersion {
		return nil, fmt.Errorf("manifest %q format version %d is newer than this build supports (%d); upgrade sluice",
			path, m.FormatVersion, irbackup.BackupFormatVersion)
	}
	if err := replayManifestProgress(ctx, store, m); err != nil {
		return nil, err
	}
	return m, nil
}

// WriteManifest serialises manifest as indented JSON and writes it to
// the conventional [ManifestFileName] path.
func WriteManifest(ctx context.Context, store irbackup.Store, manifest *irbackup.Manifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, ManifestFileName, bytes.NewReader(b))
}

// ReadManifestIfPresent returns the prior manifest if one exists in
// store, or (nil, nil) when no manifest is on disk. Distinct from
// [ReadManifest] which surfaces a NotFound as an error: resume code
// needs to distinguish "no prior backup" (fresh start) from "prior
// manifest is unreadable" (operator-actionable failure).
func ReadManifestIfPresent(ctx context.Context, store irbackup.Store) (*irbackup.Manifest, error) {
	exists, err := store.Exists(ctx, ManifestFileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return ReadManifest(ctx, store)
}

// ReadRootManifest returns the lineage root's manifest if present, or
// (nil, nil) when absent. A thin manifest-IO wrapper over
// [ReadManifestIfPresent]; the operator-facing entry the CLI's backup
// inspection commands read the root full's header through.
func ReadRootManifest(ctx context.Context, store irbackup.Store) (*irbackup.Manifest, error) {
	return ReadManifestIfPresent(ctx, store)
}

// ReadManifest loads and decodes the manifest from store. Used by
// both restore and `sluice backup verify`. An in-progress manifest in
// the ADR-0086 sidecar layout is reconstructed here (base + sidecar
// replay), so EVERY reader downstream — resume, restore, verify, the
// broker's chain-root preflight — sees one truth without knowing the
// layout exists.
func ReadManifest(ctx context.Context, store irbackup.Store) (*irbackup.Manifest, error) {
	rc, err := store.Get(ctx, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read manifest body: %w", err)
	}
	var m irbackup.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if m.FormatVersion > irbackup.BackupFormatVersion {
		return nil, fmt.Errorf("backup: manifest format version %d is newer than this build supports (%d); upgrade sluice",
			m.FormatVersion, irbackup.BackupFormatVersion)
	}
	if err := replayManifestProgress(ctx, store, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// replayManifestProgress reconstructs an in-progress sidecar-layout
// manifest (ADR-0086): the base manifest under-reports progress by
// design, and the truth is base + replay of the sidecar's
// matching-attempt events. A no-op for every other manifest shape
// (finalized, legacy in-progress, incremental/stream) — those carry no
// sidecar reference.
//
// A missing sidecar is the crash-before-first-checkpoint window: the
// base is authoritative. A torn final line and stale previous-attempt
// lines are expected crash debris — tolerated, but logged loudly so
// operators see them named. Anything else malformed fails loudly
// (corruption must never silently shrink the reconstructed progress).
//
// The decoded manifest is then NORMALIZED to the self-contained shape
// (sidecar reference cleared, schema-appropriate format version
// restored): replay is not idempotent — re-applying chunk events onto
// an already-reconstructed manifest would duplicate chunks — so the
// in-memory representation must never be replayable again, nor
// persistable in a shape that references a sidecar it already
// absorbed. The on-disk base keeps the v3 stamp + reference; only the
// decoded view is normalized.
func replayManifestProgress(ctx context.Context, store irbackup.Store, m *irbackup.Manifest) error {
	if m.PartialState != irbackup.BackupStateInProgress || m.ProgressSidecar == nil {
		return nil
	}
	defer func() {
		m.ProgressSidecar = nil
		// Restore the schema-appropriate final version — but only when
		// the in-progress stamp was the plain sidecar tier. A recorded
		// version ABOVE the sidecar tier is the writer's finalVersion
		// showing through (the commit stamp is max(sidecar, final)):
		// today that is 4 (standalone sequences) or 5 (encrypted
		// chunk-binding, ADR-0152), and it must survive normalization —
		// recomputing from the schema would strip the v5 stamp and send
		// every reader down the legacy nil-AAD decrypt path for chunks
		// that were written BOUND.
		if m.FormatVersion <= irbackup.FormatVersionProgressSidecar {
			m.FormatVersion = irbackup.FormatVersionFor(m.Schema)
		}
	}()
	sidecar := m.ProgressSidecar.File
	exists, err := store.Exists(ctx, sidecar)
	if err != nil {
		return fmt.Errorf("inspect progress sidecar %q: %w", sidecar, err)
	}
	if !exists {
		slog.DebugContext(
			ctx, "backup: in-progress manifest has no progress sidecar yet (crash before the first checkpoint); base manifest is authoritative",
			slog.String("sidecar", sidecar),
		)
		return nil
	}
	rc, err := store.Get(ctx, sidecar)
	if err != nil {
		return fmt.Errorf("read progress sidecar %q: %w", sidecar, err)
	}
	defer func() { _ = rc.Close() }()
	stats, err := irbackup.ReplayProgress(m, rc)
	if err != nil {
		return fmt.Errorf("replay progress sidecar %q: %w", sidecar, err)
	}
	if stats.TornTail {
		slog.WarnContext(
			ctx, "backup: progress sidecar ends in a torn line (crash mid-append); the event it carried is lost and its table will re-stream on resume",
			slog.String("sidecar", sidecar),
		)
	}
	if stats.StaleLines > 0 {
		slog.WarnContext(
			ctx, "backup: progress sidecar carries lines from a previous attempt; skipped (attempt-id mismatch — debris from a crash between a base-manifest write and the sidecar reset)",
			slog.String("sidecar", sidecar),
			slog.Int("stale_lines", stats.StaleLines),
		)
	}
	slog.DebugContext(
		ctx, "backup: reconstructed in-progress manifest from progress sidecar",
		slog.String("sidecar", sidecar),
		slog.Int("chunks_applied", stats.ChunksApplied),
		slog.Int("tables_completed", stats.TablesCompleted),
	)
	return nil
}
