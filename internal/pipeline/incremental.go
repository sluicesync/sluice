// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Incremental backup orchestrator. Phase 3.1 of the logical-backup
// feature (`docs/dev/design-logical-backups-phase-3.md`): take a
// previous backup's terminal CDC position, stream every event after
// it for a bounded window, write each event to a chunk file, and emit
// a manifest that links to the parent.
//
// Shape mirrors [Backup]:
//
//   - Construct the value with engine + DSN + parent reference + window.
//   - Call [IncrementalBackup.Run] with a context.
//   - Errors are wrapped with phase names so a failed run pinpoints
//     where it failed.
//
// Schema deltas: rather than parsing DDL events out of the CDC stream
// (engine-specific, fiddly), v1 captures the schema at the start and
// end of the window and diffs them. The diff produces
// [ir.SchemaDeltaEntry] entries that record AddTable / DropTable /
// AlterTable shapes with full before/after table values. Restore-side
// applies these via existing schema-writer surfaces. This is a
// deliberate v1 simplification — DDL emitted mid-window without
// observable schema effect at the boundaries (e.g. ADD COLUMN then
// DROP COLUMN before window ends) is folded into a no-op delta, which
// is the right semantics for chain restore.
//
// Window closure: time-bound (`Window`) or change-count-bound
// (`MaxChanges`). First-fired wins. The default `Window=5m` strikes a
// balance between "enough WAL/binlog to bridge a typical operator
// outage" and "small enough to be a tractable restore unit"; operators
// tune via the CLI.

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

// DefaultIncrementalWindow is the default value of
// [IncrementalBackup.Window] when the field is left zero. 5 minutes
// matches the design doc's "smaller chains restore faster, larger
// chains amortise per-window overhead" trade-off.
const DefaultIncrementalWindow = 5 * time.Minute

// DefaultIncrementalChunkChanges is the per-chunk change count when
// [IncrementalBackup.ChunkChanges] is left zero. Same value as
// [DefaultBackupChunkRows] for symmetry — the change-chunk format is
// row-shaped enough that the same bound makes sense.
const DefaultIncrementalChunkChanges = 100_000

// changeChunksPrefix is the path prefix change chunks live under.
// Distinct from `chunks/` (rows live there) so a `List(chunks/)` call
// for the legacy row-chunk path doesn't accidentally enumerate
// incremental change chunks.
const changeChunksPrefix = "chunks/_changes/"

// incrementalManifestPrefix is the path prefix incremental manifests
// live under. The full's manifest stays at the legacy
// [ManifestFileName] = "manifest.json" path; incrementals live under
// `manifests/incr-…json` so the chain-restore walker can list them
// without a per-name pattern match.
const incrementalManifestPrefix = "manifests/"

// IncrementalBackup runs a single Phase 3.1 incremental backup
// against Source / SourceDSN, taking the parent backup's terminal
// CDC position from a manifest already written to Store, streaming
// CDC events for a bounded window, and emitting a new manifest +
// chunk files into the same store.
//
// IncrementalBackup does not retain state between Run calls.
// Concurrent calls on the same value are not supported.
type IncrementalBackup struct {
	// Source is the engine the source DSN belongs to. Must declare
	// CDC support (Capabilities().CDC != ir.CDCNone). Required.
	Source ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// Store is the [ir.BackupStore] the parent manifest lives in and
	// the new incremental manifest + chunks are written to. Required.
	Store ir.BackupStore

	// ParentRef identifies the parent backup the incremental chains
	// off. Either a [BackupID] (e.g. "abc123def4567890") or the empty
	// string to chain off the most recent manifest in Store. Required
	// for clean chains; an empty value with no manifests in the store
	// errors with "no parent manifest found".
	ParentRef string

	// SlotName, on engines with a slot concept (Postgres), overrides
	// the engine's default replication-slot name. Engines without
	// slots (MySQL: binlog stream is the slot) ignore the field.
	SlotName string

	// Window bounds the wall-clock duration the orchestrator streams
	// CDC events for. Zero falls back to [DefaultIncrementalWindow].
	// First of Window or MaxChanges to fire closes the window.
	Window time.Duration

	// MaxChanges bounds the total number of [ir.Change] events the
	// orchestrator captures. Zero disables the cap (Window-only). The
	// cap is approximate — a TxBegin/Commit pair that straddles the
	// boundary is allowed to complete so the chain doesn't end
	// mid-transaction.
	MaxChanges int

	// ChunkChanges is the per-chunk change-event count. Zero falls
	// back to [DefaultIncrementalChunkChanges]. The writer rolls over
	// to a new chunk file whenever the current one hits this count.
	ChunkChanges int

	// SluiceVersion is the build identifier of the running binary,
	// recorded in the manifest. Optional.
	SluiceVersion string

	// Now, when set, overrides the wall-clock-time source for
	// [ir.Manifest.CreatedAt]. Used by tests to pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time

	// clockNow is the time source used internally for window-closure
	// timing. Defaults to time.Now; tests can override via NowFn for
	// deterministic window expiry.
	clockNow func() time.Time
}

// Run executes the incremental backup. Returns nil on success.
//
// On success: Store gains exactly one new manifest under
// `manifests/incr-…json` and one or more change-chunk files under
// `chunks/_changes/`. The new manifest carries Kind=incremental,
// ParentBackupID, StartPosition, EndPosition, SchemaHash, and
// (when DDL ran during the window) SchemaDelta entries.
func (b *IncrementalBackup) Run(ctx context.Context) error {
	if err := b.validate(); err != nil {
		return err
	}

	// 1. Resolve the parent manifest. The parent's EndPosition (or, on
	//    a parent-is-full first incremental, the parent's recorded
	//    snapshot position) becomes our StartPosition.
	parent, parentPath, err := b.resolveParent(ctx)
	if err != nil {
		return fmt.Errorf("incremental: resolve parent: %w", err)
	}
	startPos := parent.EndPosition
	if startPos.Engine == "" && startPos.Token == "" {
		// v0.16.x fulls didn't record an EndPosition. Phase 3.1 still
		// supports them by streaming "from now" — ie capturing
		// changes after the incremental opens the slot, on the
		// understanding that the resulting chain is approximate (any
		// changes between the full's snapshot point and now would be
		// missed). Operators get a clear log line so the gap is
		// visible. Future Phase 3.3 work to backfill EndPosition into
		// fulls will close this gap.
		slog.WarnContext(ctx, "incremental: parent manifest has no EndPosition; chain will start from CDC's current position (parent is a v0.16.x full or pre-Phase-3 manifest)",
			slog.String("parent_path", parentPath),
		)
	}

	// 2. The "before" baseline for SchemaDelta is the parent
	//    manifest's recorded schema — that's the source's shape at
	//    the parent's terminal CDC position, which is exactly the
	//    shape against which the incremental's window's events apply.
	//    Reading the source again here would catch the post-ALTER
	//    shape (DDL on the source between the parent and now landed
	//    before this read), making the diff useless. SchemaHash is
	//    computed from the same baseline.
	beforeSchema := parent.Schema
	beforeHash, err := ir.ComputeSchemaHash(beforeSchema)
	if err != nil {
		return fmt.Errorf("incremental: hash source schema (start): %w", err)
	}

	// 3. Open CDC reader at parent's EndPosition.
	cdc, err := b.openCDCReader(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("incremental: open cdc reader: %w", err))
	}
	defer closeIf(cdc)

	changesCh, err := cdc.StreamChanges(ctx, startPos)
	if err != nil {
		// The engine returns a wrapped ir.ErrPositionInvalid when the
		// source's WAL / binlog has been pruned past startPos. Surface
		// that loudly with a clear "your --since parent is too old;
		// take a fresh full" line.
		if errors.Is(err, ir.ErrPositionInvalid) {
			return fmt.Errorf("incremental: source has pruned past parent's terminal position; take a fresh full backup or shorten the chain interval. Underlying: %w", err)
		}
		return wrapWithHint(PhaseCDC, fmt.Errorf("incremental: start cdc stream: %w", err))
	}

	// 4. Stream changes for the window, writing chunks as we go.
	clockNow := b.clockNow
	if clockNow == nil {
		clockNow = time.Now
	}
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	windowDur := b.Window
	if windowDur <= 0 {
		windowDur = DefaultIncrementalWindow
	}
	chunkSize := b.ChunkChanges
	if chunkSize <= 0 {
		chunkSize = DefaultIncrementalChunkChanges
	}

	manifest := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		SluiceVersion:  b.SluiceVersion,
		CreatedAt:      now().UTC(),
		SourceEngine:   b.Source.Name(),
		Schema:         beforeSchema,
		Tables:         nil, // incrementals don't carry table-level row chunks
		PartialState:   ir.BackupStateInProgress,
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: parent.BackupID,
		StartPosition:  startPos,
		SchemaHash:     beforeHash,
	}
	// If the parent has no BackupID (legacy v0.16.x), compute one
	// retroactively so chain-walk has a stable link. The retroactive
	// ID is identical to what `incremental` would compute for the
	// same content, so a future re-write of the parent manifest
	// (e.g. with the v0.17.0 backup-full path) doesn't break the
	// chain.
	if manifest.ParentBackupID == "" {
		manifest.ParentBackupID = ir.ComputeBackupID(parent)
	}

	deadline := clockNow().Add(windowDur)
	endPos, totalChanges, captureErr := b.captureWindow(ctx, cdc, changesCh, manifest, chunkSize, deadline, b.MaxChanges, clockNow)
	if captureErr != nil {
		return wrapWithHint(PhaseCDC, fmt.Errorf("incremental: capture window: %w", captureErr))
	}
	manifest.EndPosition = endPos

	// 5. Read source schema at window end and diff against the start
	//    snapshot to populate SchemaDelta. The window may produce
	//    zero deltas (the common case — most incrementals carry no
	//    DDL); the Diff helper returns an empty slice in that case.
	afterSchema, err := b.readSourceSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("incremental: read source schema (end): %w", err))
	}
	manifest.SchemaDelta = diffSchemas(beforeSchema, afterSchema)
	if len(manifest.SchemaDelta) > 0 {
		// The end-state schema is more useful for restore-side
		// targeting than the start-state. Swap it in so the manifest's
		// recorded Schema reflects the post-window source shape.
		manifest.Schema = afterSchema
		afterHash, err := ir.ComputeSchemaHash(afterSchema)
		if err != nil {
			return fmt.Errorf("incremental: hash source schema (end): %w", err)
		}
		// Phase 3.1 records the post-window schema hash so the chain
		// walker can detect a schema change between adjacent
		// incrementals (their start-of-window hash should match the
		// previous incremental's end-of-window hash).
		manifest.SchemaHash = afterHash
	}

	// 6. Compute BackupID and finalise.
	manifest.BackupID = ir.ComputeBackupID(manifest)
	manifest.PartialState = ir.BackupStateComplete

	manifestPath := buildIncrementalManifestPath(manifest)
	if err := writeManifestAt(ctx, b.Store, manifestPath, manifest); err != nil {
		return fmt.Errorf("incremental: write manifest: %w", err)
	}

	slog.InfoContext(ctx, "incremental backup complete",
		slog.String("backup_id", manifest.BackupID),
		slog.String("parent_backup_id", manifest.ParentBackupID),
		slog.Int("changes", int(totalChanges)),
		slog.Int("chunks", len(manifest.ChangeChunks)),
		slog.Int("schema_deltas", len(manifest.SchemaDelta)),
		slog.String("manifest_path", manifestPath),
	)
	return nil
}

// validate sanity-checks required fields.
func (b *IncrementalBackup) validate() error {
	switch {
	case b.Source == nil:
		return errors.New("incremental: Source engine is nil")
	case b.SourceDSN == "":
		return errors.New("incremental: SourceDSN is empty")
	case b.Store == nil:
		return errors.New("incremental: Store is nil")
	}
	if b.Source.Capabilities().CDC == ir.CDCNone {
		return fmt.Errorf("incremental: source engine %q does not declare CDC support", b.Source.Name())
	}
	return nil
}

// resolveParent finds the parent manifest in the store.
//
//   - If b.ParentRef is non-empty, look for a manifest whose BackupID
//     matches. The legacy `manifest.json` path is checked first
//     (matches if the full's computed BackupID matches), then every
//     `manifests/incr-…json`.
//   - If b.ParentRef is empty, pick the most recent manifest in the
//     store (highest CreatedAt).
//
// Returns the parent manifest, the relative path it was loaded from,
// and an error.
func (b *IncrementalBackup) resolveParent(ctx context.Context) (*ir.Manifest, string, error) {
	manifests, err := listAllManifests(ctx, b.Store)
	if err != nil {
		return nil, "", err
	}
	if len(manifests) == 0 {
		return nil, "", errors.New("no parent manifest found in store; take a `sluice backup full` first")
	}
	if b.ParentRef != "" {
		for _, m := range manifests {
			id := m.manifest.BackupID
			if id == "" {
				id = ir.ComputeBackupID(m.manifest)
			}
			if id == b.ParentRef {
				return m.manifest, m.path, nil
			}
		}
		return nil, "", fmt.Errorf("parent backup %q not found in store; available: %s",
			b.ParentRef, manifestSummary(manifests))
	}
	// Pick the most recent manifest.
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].manifest.CreatedAt.After(manifests[j].manifest.CreatedAt)
	})
	return manifests[0].manifest, manifests[0].path, nil
}

// readSourceSchema opens a fresh schema reader and reads the source
// schema. Used at the start and end of the incremental's window for
// SchemaDelta computation.
func (b *IncrementalBackup) readSourceSchema(ctx context.Context) (*ir.Schema, error) {
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("open schema reader: %w", err)
	}
	defer closeIf(sr)
	return sr.ReadSchema(ctx)
}

// openCDCReader opens the engine's CDC reader, honouring SlotName via
// the optional [ir.CDCReaderWithSlotOpener] surface when supplied.
func (b *IncrementalBackup) openCDCReader(ctx context.Context) (ir.CDCReader, error) {
	if b.SlotName != "" {
		if opener, ok := b.Source.(ir.CDCReaderWithSlotOpener); ok {
			return opener.OpenCDCReaderWithSlot(ctx, b.SourceDSN, b.SlotName)
		}
		// Engine doesn't support custom slot names — log and fall through.
		slog.InfoContext(ctx, "incremental: --slot-name supplied but engine has no slot concept; ignoring",
			slog.String("engine", b.Source.Name()),
			slog.String("slot_name", b.SlotName),
		)
	}
	return b.Source.OpenCDCReader(ctx, b.SourceDSN)
}

// captureWindow drains changes from changesCh for the configured
// window, writing them into change chunks staged on manifest.
// Returns the position of the last applied change (the window's
// EndPosition), the total change count, and any fatal error.
//
// Window closure: deadline reached (clockNow >= deadline) OR
// totalChanges >= maxChanges (when maxChanges > 0). The orchestrator
// is permissive about straddle: a TxBegin already received but the
// matching TxCommit not yet observed extends the window by up to one
// transaction so the chain doesn't end mid-tx.
//
// cdc is passed in so an early channel-close (the CDC reader's pump
// terminating with an error) surfaces the underlying error via
// `cdc.Err()` rather than silently exiting the window with zero
// captured changes.
func (b *IncrementalBackup) captureWindow(
	ctx context.Context,
	cdc ir.CDCReader,
	changesCh <-chan ir.Change,
	manifest *ir.Manifest,
	chunkSize int,
	deadline time.Time,
	maxChanges int,
	clockNow func() time.Time,
) (ir.Position, int64, error) {
	var (
		writer        *changeChunkWriter
		buf           *bytes.Buffer
		chunkIdx      int
		totalChanges  int64
		lastPos       ir.Position
		inTransaction bool
	)

	// runNamespace is the per-incremental directory segment chunks land
	// under. Without it, a second incremental into the same store would
	// reuse `chunks/_changes/changes-0.jsonl.gz` and overwrite the
	// first's chunk on disk while the manifests still recorded the
	// original SHA-256 — a chain-restore + verify hard failure (Bug 35
	// from the v0.17.0 cycle). The namespace is derived from
	// manifest.CreatedAt because BackupID isn't computable yet (it
	// depends on EndPosition, which is only known once the window
	// closes); CreatedAt is fixed when the manifest is constructed and
	// uniquely identifies a Run() invocation in practice.
	runNamespace := changeChunkRunNamespace(manifest)

	flush := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close chunk: %w", err)
		}
		path := changeChunkPath(runNamespace, chunkIdx)
		hash := writer.Hash()
		if err := b.Store.Put(ctx, path, buf); err != nil {
			return fmt.Errorf("store put %q: %w", path, err)
		}
		manifest.ChangeChunks = append(manifest.ChangeChunks, &ir.ChunkInfo{
			File:     path,
			RowCount: writer.ChangeCount(),
			SHA256:   hash,
		})
		writer = nil
		buf = nil
		chunkIdx++
		return nil
	}

	openWriter := func() error {
		buf = &bytes.Buffer{}
		w, err := newChangeChunkWriter(buf)
		if err != nil {
			return fmt.Errorf("open chunk: %w", err)
		}
		writer = w
		return nil
	}

	// timer fires when the wall-clock deadline expires. We check it
	// between drains so the window is never extended past
	// deadline+one-transaction. Compute the timeout via the injected
	// clock so tests can pin "now".
	timer := time.NewTimer(deadline.Sub(clockNow()))
	defer timer.Stop()

	deadlinePassed := false
	for {
		select {
		case <-ctx.Done():
			return lastPos, totalChanges, ctx.Err()
		case <-timer.C:
			deadlinePassed = true
			// Check immediately whether we can close cleanly. If we're
			// not in a transaction, close now; otherwise wait for the
			// next TxCommit.
			if !inTransaction {
				if err := flush(); err != nil {
					return lastPos, totalChanges, err
				}
				return lastPos, totalChanges, nil
			}
		case change, ok := <-changesCh:
			if !ok {
				// Channel closed. If the CDC reader recorded an error,
				// surface it (loud-failure tenet); otherwise treat as
				// orderly window end so the manifest still records what
				// we got.
				if errReader, ok := cdc.(interface{ Err() error }); ok {
					if e := errReader.Err(); e != nil {
						return lastPos, totalChanges, fmt.Errorf("cdc reader: %w", e)
					}
				}
				if err := flush(); err != nil {
					return lastPos, totalChanges, err
				}
				return lastPos, totalChanges, nil
			}
			// Track transaction boundary so we can extend the window
			// to the next TxCommit when the deadline straddles a tx.
			switch change.(type) {
			case ir.TxBegin:
				inTransaction = true
			case ir.TxCommit:
				inTransaction = false
			}
			if writer == nil {
				if err := openWriter(); err != nil {
					return lastPos, totalChanges, err
				}
			}
			if err := writer.WriteChange(change); err != nil {
				return lastPos, totalChanges, err
			}
			totalChanges++
			// Position-bearing changes update lastPos.
			pos := change.Pos()
			if pos.Engine != "" || pos.Token != "" {
				lastPos = pos
			}
			// Roll the chunk when it hits the row cap.
			if writer.ChangeCount() >= int64(chunkSize) {
				if err := flush(); err != nil {
					return lastPos, totalChanges, err
				}
			}
			// MaxChanges (approximate): close on a tx boundary at-or-after
			// the cap.
			if maxChanges > 0 && totalChanges >= int64(maxChanges) && !inTransaction {
				if err := flush(); err != nil {
					return lastPos, totalChanges, err
				}
				return lastPos, totalChanges, nil
			}
			// Deadline-already-passed and we just observed a TxCommit:
			// close now.
			if deadlinePassed && !inTransaction {
				if err := flush(); err != nil {
					return lastPos, totalChanges, err
				}
				return lastPos, totalChanges, nil
			}
		}
	}
}

// changeChunkPath returns the conventional path of change chunk
// index for a given run-namespace segment. Lives under
// [changeChunksPrefix]/<runNamespace>/ so two incrementals into the
// same store don't collide on the file basename. See Bug 35 in
// `sluice-testing/BUG-CATALOG.md`.
//
// The legacy un-namespaced shape (`chunks/_changes/changes-N.jsonl.gz`)
// is no longer written. v0.17.0-vintage backup directories with the
// flat layout still restore correctly because the chain-restore path
// reads `chunk.File` verbatim from the manifest — the manifest's
// recorded path is the source of truth for reads regardless of which
// shape produced it.
func changeChunkPath(runNamespace string, idx int) string {
	return fmt.Sprintf("%s%s/changes-%d.jsonl.gz", changeChunksPrefix, runNamespace, idx)
}

// changeChunkRunNamespace returns the per-Run() namespace segment for
// change-chunk paths. Derived from the manifest's CreatedAt rendered
// as a 13-digit zero-padded UnixMilli — same encoding
// [buildIncrementalManifestPath] uses for the manifest filename, so
// operators inspecting the directory see the same lexically-sortable
// prefix on the manifest and its chunks.
//
// CreatedAt is preferred over BackupID because BackupID isn't
// computable until EndPosition is known (i.e. after the window
// closes), but chunks need a stable namespace before the first write.
// Two Run() invocations colliding on UnixMilli is implausible: a Run
// constructs a manifest, then opens a CDC stream, then writes chunks —
// not two such pipelines fit in one millisecond on real hardware.
func changeChunkRunNamespace(m *ir.Manifest) string {
	return fmt.Sprintf("%013d", m.CreatedAt.UTC().UnixMilli())
}

// buildIncrementalManifestPath returns the conventional relative
// path an incremental manifest is written to. The path encodes the
// CreatedAt unix-millis (lexically sortable across a chain) plus the
// short BackupID for disambiguation when two incrementals are taken
// in the same millisecond on the same source (rare but possible
// under load).
func buildIncrementalManifestPath(m *ir.Manifest) string {
	short := m.BackupID
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("%sincr-%013d-%s.json",
		incrementalManifestPrefix,
		m.CreatedAt.UTC().UnixMilli(),
		short,
	)
}

// writeManifestAt is [writeManifest] generalised to a caller-supplied
// path. The full-backup writer's [writeManifest] hard-codes
// [ManifestFileName]; the incremental writer needs an arbitrary path.
func writeManifestAt(ctx context.Context, store ir.BackupStore, path string, manifest *ir.Manifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, path, bytes.NewReader(b))
}

// unmarshalManifest decodes a manifest body. Pulled out so the
// chain-walk path and the legacy single-manifest path share one
// implementation.
func unmarshalManifest(body []byte) (*ir.Manifest, error) {
	var m ir.Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &m, nil
}

// manifestRecord pairs a parsed manifest with the path it was loaded
// from. Used by chain-walk and parent-resolve logic.
type manifestRecord struct {
	path     string
	manifest *ir.Manifest
}

// listAllManifests returns every manifest in store (the full's
// [ManifestFileName] and every `manifests/incr-…json`), with parent
// resolution / chain ordering left to the caller.
func listAllManifests(ctx context.Context, store ir.BackupStore) ([]manifestRecord, error) {
	var out []manifestRecord

	// Full's manifest at the legacy path.
	if exists, err := store.Exists(ctx, ManifestFileName); err != nil {
		return nil, fmt.Errorf("inspect %q: %w", ManifestFileName, err)
	} else if exists {
		m, err := readManifestAt(ctx, store, ManifestFileName)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", ManifestFileName, err)
		}
		out = append(out, manifestRecord{path: ManifestFileName, manifest: m})
	}

	// Incremental manifests under `manifests/`.
	paths, err := store.List(ctx, incrementalManifestPrefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", incrementalManifestPrefix, err)
	}
	sort.Strings(paths) // lexically sortable by construction
	for _, p := range paths {
		m, err := readManifestAt(ctx, store, p)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", p, err)
		}
		out = append(out, manifestRecord{path: p, manifest: m})
	}
	return out, nil
}

// readManifestAt is [readManifest] generalised to a caller-supplied
// path. Same format-version gating as the legacy helper.
func readManifestAt(ctx context.Context, store ir.BackupStore, path string) (*ir.Manifest, error) {
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
	if m.FormatVersion > ir.BackupFormatVersion {
		return nil, fmt.Errorf("manifest %q format version %d is newer than this build supports (%d); upgrade sluice",
			path, m.FormatVersion, ir.BackupFormatVersion)
	}
	return m, nil
}

// manifestSummary returns a human-readable list of manifest IDs for
// error messages.
func manifestSummary(records []manifestRecord) string {
	parts := make([]string, 0, len(records))
	for _, r := range records {
		id := r.manifest.BackupID
		if id == "" {
			id = ir.ComputeBackupID(r.manifest) + " (computed)"
		}
		parts = append(parts, fmt.Sprintf("%s (%s, %s)", id, r.manifest.Kind, r.path))
	}
	return joinComma(parts)
}

// joinComma joins parts with ", " — local helper to avoid pulling in
// strings just for one call.
func joinComma(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += ", " + p
	}
	return out
}
