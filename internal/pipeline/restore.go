// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Restore orchestrator. The inverse of [Backup]: read manifest, apply
// the schema (with cross-engine retargeting if needed), bulk-copy
// every chunk's rows into the target with per-chunk SHA-256
// verification, then create indexes / constraints / views.
//
// The restore-phase order mirrors [Migrator]:
//
//   1. CreateTablesWithoutConstraints
//   2. Bulk-copy rows from chunks (per-chunk SHA-256 verified)
//   3. SyncIdentitySequences
//   4. CreateIndexes
//   5. CreateConstraints
//   6. CreateViews
//
// Cross-engine restore is supported via [translate.RetargetForEngine]:
// a PG-source backup restoring into MySQL gets PG-native types
// rewritten to their MySQL-storage equivalents (UUID → CHAR(36), etc.)
// before the schema is applied. Same shape `sluice schema diff` uses.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// Restore runs a single Phase 1 full restore from Store into Target /
// TargetDSN. Construct the value, then call Run.
type Restore struct {
	// Target is the engine the target DSN belongs to. Required.
	// May differ from the backup's source engine — the orchestrator
	// runs `translate.RetargetForEngine` to bridge type differences.
	Target ir.Engine

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// Store is the [ir.BackupStore] to read manifest + chunks from.
	// Required.
	Store ir.BackupStore

	// Filter selects which tables from the manifest participate.
	// Empty (zero value) restores every table.
	Filter TableFilter

	// MaxBufferBytes is the soft byte cap on per-batch buffered
	// memory in the row writer. Same semantics as [Migrator.MaxBufferBytes].
	// Zero means "no cap".
	MaxBufferBytes int64

	// SkipChainDispatch, when true, suppresses the chain-detection
	// branch in [Restore.Run]. Used internally by [ChainRestore] so
	// that re-entering Restore for the full-application step doesn't
	// loop back into another chain restore. End-users leave this
	// false; the public single-manifest restore path detects chain
	// shape and dispatches.
	SkipChainDispatch bool

	// DataOnly, when true, restores ONLY the manifest's row data via an
	// idempotent upsert bulk-copy and skips CreateTables / CreateIndexes
	// / CreateConstraints / CreateViews. Set by [ChainRestore] for
	// every segment full AFTER the first in a multi-segment lineage
	// (ADR-0046 §3): segment 0 establishes the schema + indexes; a
	// later rotation segment's full is a fresh snapshot of the SAME
	// (possibly DDL-evolved) schema, so re-running the non-idempotent
	// index/constraint phases would error ("relation already exists"),
	// while the rows still need refreshing. Idempotent because the
	// later segment's snapshot is at S >= the prior segment's end, so
	// an upsert converges on the correct state. End-users leave this
	// false (a single-manifest restore always builds the schema).
	DataOnly bool

	// TargetSchema is the per-source target-schema namespace override
	// (ADR-0031), mirroring [Migrator.TargetSchema] /
	// [Streamer.TargetSchema] / [Previewer.TargetSchema]. When set,
	// the target schema writer / row writer / change applier route
	// user-data DDL + INSERTs into the named schema. Engines that
	// don't expose [ir.SchemaSetter] surface a flat-namespace refusal
	// at validate time (today: MySQL, since schemas == databases on
	// MySQL — operators use a different --target DSN database
	// instead). Empty preserves the DSN-derived default schema (the
	// pre-v0.56.0 shape). v0.56.0 / GitHub UX-gap closure flagged by
	// the v0.55.0 cycle subagent.
	TargetSchema string

	// Envelope, when non-nil, is the [crypto.EnvelopeEncryption] used
	// to unwrap CEKs from encrypted manifests. Required when the
	// chain's full manifest carries [ir.ChainEncryption]. A nil
	// Envelope against an encrypted chain produces a clear refusal
	// at chain-walk time naming the missing key — no partial data
	// lands on the target.
	Envelope crypto.EnvelopeEncryption

	// chainCEK caches the chain-level CEK after first unwrap so
	// per-chain mode pays the unwrap cost (Argon2id, KMS Decrypt)
	// exactly once per Run. Internal — set by Run on the first
	// encrypted-chunk read.
	chainCEK []byte

	// segCodec is the codec recorded for the segment being restored on
	// the single-manifest path (the root segment for a public restore;
	// the specific segment ChainRestore is applying when it re-enters
	// with SkipChainDispatch). Recorded, never sniffed (ADR-0046 §5).
	// Set by Run / by ChainRestore.applyFull.
	segCodec Codec
}

// lineageNeedsWalk reports whether the store's lineage requires the
// lineage-walk restore path: more than one segment, or a single
// segment carrying incrementals. A one-segment-no-incrementals lineage
// (== a pre-ADR single full) returns false so the single-manifest path
// handles it with byte-identical behaviour.
func (r *Restore) lineageNeedsWalk(ctx context.Context) (bool, error) {
	cat, err := resolveLineage(ctx, r.Store)
	if err != nil {
		return false, err
	}
	if len(cat.Segments) > 1 {
		return true, nil
	}
	return len(cat.Segments[0].Incrementals) > 0, nil
}

// rootSegmentCodec returns the codec recorded for segment 0 (the root
// segment). Absent lineage → gzip (pre-ADR default). The codec is
// recorded, NEVER inferred from chunk bytes.
func (r *Restore) rootSegmentCodec(ctx context.Context) (Codec, error) {
	cat, err := resolveLineage(ctx, r.Store)
	if err != nil {
		return "", err
	}
	seg := &cat.Segments[0]
	if err := validateRecordedCodec(seg.Codec); err != nil {
		return "", err
	}
	return seg.codecOrDefault(), nil
}

// Run executes the restore. Returns nil on success; a wrapped error
// pointing at the failed phase otherwise.
//
// Phase 3 (v0.17.0+): when the store contains incremental manifests
// in addition to the full, Run delegates to [ChainRestore] which
// walks the chain in order. The single-manifest path remains
// unchanged for backups produced by `sluice backup full` alone.
func (r *Restore) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	// 0. Detect lineage shape (ADR-0046). A multi-segment lineage, OR
	//    a one-segment lineage with incrementals, dispatches to the
	//    lineage-walk restore. A one-segment-no-incrementals lineage
	//    (== a pre-ADR single full) takes the single-manifest path
	//    below — byte-identical behaviour to before.
	//
	// SkipChainDispatch is set internally by ChainRestore when it
	// re-enters the single-manifest path to apply ONE segment's full
	// alone; without it the two would mutually recurse.
	if !r.SkipChainDispatch {
		multi, err := r.lineageNeedsWalk(ctx)
		if err != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("restore: detect lineage: %w", err))
		}
		if multi {
			chain := &ChainRestore{
				Target:         r.Target,
				TargetDSN:      r.TargetDSN,
				Store:          r.Store,
				Filter:         r.Filter,
				MaxBufferBytes: r.MaxBufferBytes,
				Envelope:       r.Envelope,
				TargetSchema:   r.TargetSchema,
			}
			return chain.Run(ctx)
		}
	}

	// 1. Read manifest. Single-manifest path: this is the root
	//    segment's full at the conventional ManifestFileName.
	manifest, err := readManifest(ctx, r.Store)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: %w", err))
	}
	// The root segment's recorded codec governs the full's chunks.
	// Recorded, never sniffed (ADR-0046). Absent lineage → gzip.
	if r.segCodec == "" {
		r.segCodec, err = r.rootSegmentCodec(ctx)
		if err != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("restore: %w", err))
		}
	}
	slog.InfoContext(
		ctx, "restore: loaded manifest",
		slog.Int("format_version", manifest.FormatVersion),
		slog.String("source_engine", manifest.SourceEngine),
		slog.String("target_engine", r.Target.Name()),
		slog.Int("tables", len(manifest.Tables)),
		slog.Time("created_at", manifest.CreatedAt),
	)

	if manifest.Schema == nil {
		return errors.New("restore: manifest carries no schema")
	}

	// 1.4. ADR-0047 verbatim-extension restore-time engine gate. A
	//      backup carrying verbatim (uncatalogued) PG extension-typed
	//      columns is PG-restore-only; restoring it to a non-PG target
	//      is a LOUD refusal before any data moves (never a silent
	//      drop/mangle). The single-manifest path gates on the
	//      manifest schema directly — the same schema the lineage
	//      marker is derived from, so the two restore paths agree.
	if err := refuseVerbatimManifestRestoreToNonPG(manifest.Schema, r.Target); err != nil {
		return wrapWithHint(PhaseConnect, err)
	}

	// 1.45. Cross-engine supportability gate (Bug 134). The chain
	//       restore path has gated PG-native constructs (EXCLUDE
	//       constraints, extension opclasses, PostGIS metadata, …)
	//       since Phase 5, but this single-manifest branch never
	//       called the gate — so a full-only PG backup restored to a
	//       MySQL-family target exited 0 with an EXCLUDE constraint
	//       SILENTLY downgraded to a plain non-unique KEY (semantic-
	//       invariant loss; every row still arrives, which is exactly
	//       why nothing else caught it). Same recorded-SourceEngine vs
	//       target-name dispatch as chain_restore.go step 2, gated
	//       BEFORE the retarget so the refusal sees the source-true
	//       schema, and BEFORE the table filter for path-consistency
	//       with the chain (which also gates its root's full schema).
	if manifest.SourceEngine != "" && manifest.SourceEngine != r.Target.Name() {
		if err := checkCrossEngineSupportable(
			manifest.Schema,
			manifest.SourceEngine, r.Target.Name(),
			fmt.Sprintf("restore: full %s", manifestBackupID(manifest)),
		); err != nil {
			return err
		}
		slog.InfoContext(
			ctx, "restore: cross-engine mode",
			slog.String("source_engine", manifest.SourceEngine),
			slog.String("target_engine", r.Target.Name()),
		)
	}

	// 1.5. Encryption pre-flight. If the chain root manifest carries
	// [ir.ChainEncryption], the operator MUST have supplied an
	// envelope that can unwrap the chain's CEK. A missing envelope
	// against an encrypted chain refuses up-front so no partial data
	// lands on the target.
	if err := r.preflightEncryption(manifest); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: %w", err))
	}

	// 2. Filter tables — both at the schema level (so unwanted
	//    tables never get created) and at the manifest-table level
	//    (so unwanted chunks never get streamed).
	if err := applyTableFilter(ctx, manifest.Schema, r.Filter); err != nil {
		return err
	}
	manifest.Tables = filterManifestTables(manifest.Tables, r.Filter)

	// 3. Cross-engine retarget (identity for same-engine).
	schema := translate.RetargetForEngine(manifest.Schema, manifest.SourceEngine, r.Target.Name())

	// 4. Open target writers.
	if err := validateTargetSchema(r.Target, r.TargetSchema); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: %w", err))
	}
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: open target schema writer: %w", err))
	}
	applyTargetSchema(sw, r.TargetSchema)
	defer closeIf(sw)

	rw, err := r.Target.OpenRowWriter(ctx, r.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: open target row writer: %w", err))
	}
	applyMaxBufferBytes(rw, r.MaxBufferBytes)
	applyTargetSchema(rw, r.TargetSchema)
	defer closeIf(rw)

	// 5. Phase 1: tables. Skipped in DataOnly mode (a later
	//    rotation-segment full — schema already established by
	//    segment 0; CreateTables IF NOT EXISTS would be a no-op
	//    anyway, but we skip the whole schema surface for clarity).
	if !r.DataOnly {
		if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("restore: create tables: %w", err))
		}
		slog.InfoContext(ctx, "restore: tables created", slog.Int("count", len(schema.Tables)))
	}

	// 6. Phase 2: bulk-copy from chunks. DataOnly uses an idempotent
	//    upsert so re-applying a later segment's snapshot over the
	//    prior segment's restored state converges (no PK-collision).
	tablesByName := indexManifestTables(manifest.Tables)
	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		entry, ok := tablesByName[key]
		if !ok {
			slog.InfoContext(ctx, "restore: table not in manifest; skipping bulk-copy",
				slog.String("table", table.Name))
			continue
		}
		if err := r.restoreTable(ctx, rw, table, entry); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("restore: table %q: %w", table.Name, err))
		}
	}

	if r.DataOnly {
		slog.InfoContext(ctx, "restore: data-only segment refresh complete",
			slog.Int("tables", len(schema.Tables)))
		return nil
	}

	// 7. Phase 3: identity-sequence sync.
	if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("restore: sync identity sequences: %w", err))
	}

	// 8. Phase 4: indexes.
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		return wrapWithHint(PhaseIndexes, fmt.Errorf("restore: create indexes: %w", err))
	}

	// 9. Phase 5: constraints.
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseConstraints, fmt.Errorf("restore: create constraints: %w", err))
	}

	// 10. Phase 6: views.
	if err := runViewsPhase(ctx, schema, sw); err != nil {
		return wrapWithHint(PhaseViews, err)
	}

	slog.InfoContext(ctx, "restore complete", slog.Int("tables", len(schema.Tables)))
	return nil
}

// validate sanity-checks required fields.
func (r *Restore) validate() error {
	switch {
	case r.Target == nil:
		return errors.New("restore: Target engine is nil")
	case r.TargetDSN == "":
		return errors.New("restore: TargetDSN is empty")
	case r.Store == nil:
		return errors.New("restore: Store is nil")
	}
	return nil
}

// restoreTable bulk-copies every chunk's rows into the target via the
// row writer, verifying each chunk's SHA-256 along the way. The
// orchestrator opens one [chunkReader] per chunk; rows from all
// chunks of a table flow through a single channel into a single
// [ir.RowWriter.WriteRows] call so the writer's batching / commit
// logic doesn't reset per chunk.
//
// Per-chunk SHA-256 verification is the load-bearing layer-1 check
// of the proto-ADR's "100% confidence" story. A mismatch is a hard
// failure — silent corruption is not acceptable.
func (r *Restore) restoreTable(
	ctx context.Context,
	rw ir.RowWriter,
	table *ir.Table,
	entry *ir.TableManifest,
) error {
	if len(entry.Chunks) == 0 {
		slog.InfoContext(ctx, "restore: empty table; no chunks to apply",
			slog.String("table", table.Name))
		return nil
	}

	// Bug 40b fix (v0.20.1): wrap the producer goroutine in a derived
	// context so a writer-side failure can cancel the producer.
	// Pre-fix: the producer pushed rows into an unbuffered channel
	// that the writer stopped draining the moment WriteRows returned
	// an error; the producer then blocked forever on `out <- row`,
	// and `<-errCh` deadlocked. Net effect: a target-side
	// "column does not exist" turned into a silent hang with idle
	// PG connections in ClientRead state and operators having to
	// Stop-Process to recover. The streamCtx + streamCancel pattern
	// below mirrors the one [ChainRestore.applyIncremental] uses for
	// change-chunk streaming.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	rowCh := make(chan ir.Row)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		var rowsApplied int64
		for chunkIdx, chunk := range entry.Chunks {
			chunkRows, err := r.streamChunkRows(streamCtx, chunk, rowCh)
			if err != nil {
				errCh <- fmt.Errorf("chunk %d (%s): %w", chunkIdx, chunk.File, err)
				return
			}
			rowsApplied += chunkRows
			slog.DebugContext(
				ctx, "restore: chunk verified and streamed",
				slog.String("table", table.Name),
				slog.Int("chunk", chunkIdx),
				slog.Int64("rows", chunkRows),
			)
		}
		if entry.RowCount > 0 && rowsApplied != entry.RowCount {
			errCh <- fmt.Errorf("layer-2 row-count mismatch on table %q: manifest says %d, streamed %d",
				table.Name, entry.RowCount, rowsApplied)
			return
		}
		errCh <- nil
	}()

	// DataOnly (later rotation-segment full): use the idempotent
	// upsert writer when the engine exposes it so re-applying the
	// snapshot over the prior segment's restored rows converges
	// (ON CONFLICT / ON DUPLICATE KEY UPDATE). Engines without the
	// surface, or no-PK tables, fall back to plain WriteRows — the
	// caller's lineage invariant (S_n >= prior end) means the rows are
	// at-or-ahead, so a plain insert only collides on a PK the upsert
	// would have updated to the same value; the idempotent path is the
	// correct one and shipping engines (PG, MySQL) implement it.
	writeFn := rw.WriteRows
	if r.DataOnly {
		if iw, ok := rw.(ir.IdempotentRowWriter); ok {
			writeFn = iw.WriteRowsIdempotent
		}
	}
	if err := writeFn(ctx, table, rowCh); err != nil {
		// Bug 40b: cancel the producer's context so a goroutine
		// blocked on `rowCh <- row` unblocks via the streamChunkRows
		// `<-ctx.Done()` arm. Without this, `<-errCh` below would
		// deadlock — the silent-hang shape from Bug 40.
		slog.ErrorContext(
			ctx, "restore: write rows failed; cancelling chunk producer",
			slog.String("table", table.Name),
			slog.String("err", err.Error()),
		)
		streamCancel()
		<-errCh
		return fmt.Errorf("write rows for table %q: %w", table.Name, err)
	}
	if err := <-errCh; err != nil {
		return err
	}
	slog.InfoContext(
		ctx, "restore: table complete",
		slog.String("table", table.Name),
		slog.Int64("rows", entry.RowCount),
		slog.Int("chunks", len(entry.Chunks)),
	)
	return nil
}

// streamChunkRows opens chunk in the store, decodes every row, sends
// each into rowCh, and verifies the SHA-256 on close. Returns the
// row count read from this chunk, which the caller compares against
// the manifest entry's RowCount for layer-2 verification.
func (r *Restore) streamChunkRows(
	ctx context.Context,
	chunk *ir.ChunkInfo,
	rowCh chan<- ir.Row,
) (int64, error) {
	src, err := r.Store.Get(ctx, chunk.File)
	if err != nil {
		return 0, fmt.Errorf("open chunk: %w", err)
	}
	cek, err := r.chunkCEK(chunk)
	if err != nil {
		_ = src.Close()
		return 0, fmt.Errorf("resolve chunk cek: %w", err)
	}
	cr, err := newChunkReader(src, chunk.SHA256, cek, r.segCodec)
	if err != nil {
		return 0, fmt.Errorf("open chunk reader: %w", err)
	}

	var rows int64
	for {
		row, err := cr.ReadRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = cr.Close()
			return rows, fmt.Errorf("read row: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = cr.Close()
			return rows, ctx.Err()
		case rowCh <- row:
			rows++
		}
	}
	if err := cr.Close(); err != nil {
		// SHA-256 mismatch surfaces here as a wrapped
		// ErrChunkHashMismatch; loud-failure tenet means we surface
		// it directly rather than continuing.
		return rows, err
	}
	if chunk.RowCount > 0 && rows != chunk.RowCount {
		return rows, fmt.Errorf("layer-2 chunk row-count mismatch on %s: manifest says %d, decoded %d",
			chunk.File, chunk.RowCount, rows)
	}
	return rows, nil
}

// filterManifestTables filters the manifest's table list against the
// supplied filter, mirroring the schema-side filtering. Empty filter
// returns the input unchanged.
func filterManifestTables(in []*ir.TableManifest, filter TableFilter) []*ir.TableManifest {
	if filter.IsEmpty() {
		return in
	}
	out := in[:0]
	for _, t := range in {
		if t == nil {
			continue
		}
		if filter.Allows(t.Name) {
			out = append(out, t)
		}
	}
	return out
}

// indexManifestTables returns a "schema.name" → entry map. Used by
// [Restore.Run] to look up each schema-table's manifest entry in O(1).
func indexManifestTables(tables []*ir.TableManifest) map[string]*ir.TableManifest {
	out := make(map[string]*ir.TableManifest, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		out[manifestTableKey(t.Schema, t.Name)] = t
	}
	return out
}

func manifestTableKey(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// preflightEncryption inspects the manifest for [ir.ChainEncryption]
// and, when present, validates that an envelope is supplied and that
// it can unwrap the chain's CEK. Caches the chain-level CEK on
// r.chainCEK for per-chain mode so subsequent chunk reads pay no
// further unwrap cost.
//
// On a plaintext chain this is a no-op; on an encrypted chain with no
// envelope, it returns an operator-actionable error naming the chain's
// KEKMode and (where relevant) KEKRef so the operator knows what they
// need to supply.
func (r *Restore) preflightEncryption(manifest *ir.Manifest) error {
	if manifest == nil || manifest.ChainEncryption == nil {
		return nil
	}
	enc := manifest.ChainEncryption
	if r.Envelope == nil {
		return fmt.Errorf("encrypted chain (algorithm=%q kek_mode=%q kek_ref=%q) requires --encrypt + a passphrase / KMS reference; no key was supplied",
			enc.Algorithm, enc.KEKMode, enc.KEKRef)
	}
	if enc.KEKMode != "" && r.Envelope.Mode() != enc.KEKMode {
		return fmt.Errorf("encryption envelope mode %q does not match chain's recorded kek_mode %q",
			r.Envelope.Mode(), enc.KEKMode)
	}
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		if len(enc.WrappedCEK) == 0 {
			return errors.New("encrypted chain in per-chain mode but ChainEncryption.WrappedCEK is empty (manifest corrupted?)")
		}
		cek, err := r.Envelope.UnwrapCEK(enc.WrappedCEK)
		if err != nil {
			return fmt.Errorf("unwrap chain cek (wrong passphrase / KMS key?): %w", err)
		}
		r.chainCEK = cek
	}
	return nil
}

// chunkCEK returns the per-chunk CEK for chunk based on the chunk's
// recorded [ir.ChunkEncryption]. Per-chain mode returns r.chainCEK;
// per-chunk mode unwraps the chunk's own [ir.ChunkEncryption.WrappedCEK]
// via the envelope.
//
// Returns nil for plaintext chunks (Encryption == nil) — caller passes
// nil cek to newChunkReader for the legacy plaintext read path.
func (r *Restore) chunkCEK(chunk *ir.ChunkInfo) ([]byte, error) {
	if chunk.Encryption == nil {
		return nil, nil
	}
	// Per-chunk wrap takes precedence: if the chunk carries its own
	// wrapped CEK, unwrap it.
	if len(chunk.Encryption.WrappedCEK) > 0 {
		if r.Envelope == nil {
			return nil, errors.New("per-chunk encrypted chunk encountered without envelope")
		}
		cek, err := r.Envelope.UnwrapCEK(chunk.Encryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("unwrap chunk cek: %w", err)
		}
		return cek, nil
	}
	// Per-chain mode: use the cached chain CEK (preflight already
	// unwrapped it).
	if r.chainCEK == nil {
		return nil, errors.New("encrypted chunk encountered but chain CEK is unset (preflight skipped?)")
	}
	return r.chainCEK, nil
}

// VerifyOptions controls [VerifyBackup]'s verification surface. The
// zero value performs only the byte-level SHA-256 check (the historical
// behavior, preserved for backward compatibility).
type VerifyOptions struct {
	// Envelope, when non-nil, enables decrypt-probe verification on
	// every encrypted chunk. Closes Bug 117 (v0.94.1): pre-fix,
	// per-chunk-mode chains accepted a passphrase rotation
	// mid-chain silently — `backup verify` was SHA-only, the
	// later chunks' SHAs still matched (the bytes on disk DID
	// hash correctly), and the divergence only surfaced at restore
	// time as a partial-fail with no rollback. Post-fix, the
	// envelope is used to unwrap each per-chunk WrappedCEK; an
	// unwrap failure (wrong passphrase / wrong KMS key for that
	// chunk) is reported as a verify failure with a clear
	// chunk-naming error so the operator can identify the rotation
	// point before attempting restore.
	//
	// Per-chain mode is also probed (unwrap the chain-level CEK
	// once up-front) so a fully-wrong passphrase fails fast with a
	// single clear error rather than 0 verify failures + a
	// restore-time surprise.
	//
	// Must match the chain root's recorded KEKMode when set; a
	// mismatch returns an irrecoverable error from VerifyBackup.
	Envelope crypto.EnvelopeEncryption
}

// VerifyBackup walks every chunk referenced by the manifest in store,
// rehashes each chunk's bytes, and reports any mismatches. Used by
// `sluice backup verify` for "is my backup still intact?" cron probes
// against archived backups — no decoding of rows, just byte-level
// hash comparison against the manifest.
//
// Returns the total number of chunks, the count of failed chunks, and
// an error. A non-nil error pinpoints an irrecoverable problem
// (manifest unreadable, ctx cancelled); per-chunk hash failures are
// reported via slog at error level AND counted in the failed return,
// so the caller can report "N of M chunks failed verification"
// without needing the full list.
//
// Phase 3 (v0.17.0+): when the store contains a backup chain (a full
// plus one or more incremental manifests), VerifyBackup walks every
// manifest's chunks — both the row chunks of the full and the
// change chunks of each incremental.
//
// VerifyBackup is the historical SHA-only entrypoint preserved for
// backward compatibility (all existing tests rely on this signature).
// Operators wanting the Bug 117 decrypt probe pass a non-nil
// VerifyOptions.Envelope via [VerifyBackupWith].
func VerifyBackup(ctx context.Context, store ir.BackupStore) (total, failed int, err error) {
	return VerifyBackupWith(ctx, store, VerifyOptions{})
}

// VerifyBackupWith is the options-bearing form of [VerifyBackup]. When
// opts.Envelope is non-nil, every encrypted chunk's WrappedCEK is
// unwrapped against the supplied envelope so a passphrase rotation
// mid-chain (Bug 117) surfaces at verify-time instead of partial-failing
// the restore.
func VerifyBackupWith(ctx context.Context, store ir.BackupStore, opts VerifyOptions) (total, failed int, err error) {
	records, err := listAllSegmentManifests(ctx, store)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: %w", err)
	}
	if len(records) == 0 {
		return 0, 0, errors.New("verify: no manifests found in store")
	}
	// Bug 117 closure: when an envelope is supplied AND the chain
	// root records ChainEncryption, validate the chain-level
	// envelope eagerly so the operator gets a single clear "wrong
	// passphrase / wrong KMS key" error up front. For per-chain
	// mode this is also the only decrypt probe per verify run; for
	// per-chunk mode it confirms the operator's envelope is
	// well-formed before per-chunk probes run in the chunk loop.
	if opts.Envelope != nil {
		if rootEnc := records[0].manifest.ChainEncryption; rootEnc != nil {
			if rootEnc.KEKMode != "" && opts.Envelope.Mode() != rootEnc.KEKMode {
				return 0, 0, fmt.Errorf(
					"verify: envelope mode %q does not match chain's recorded kek_mode %q",
					opts.Envelope.Mode(), rootEnc.KEKMode,
				)
			}
			if len(rootEnc.WrappedCEK) > 0 {
				if _, uerr := opts.Envelope.UnwrapCEK(rootEnc.WrappedCEK); uerr != nil {
					return 0, 0, fmt.Errorf(
						"verify: unwrap chain cek (wrong passphrase / KMS key?): %w", uerr,
					)
				}
			}
		}
	}
	for _, rec := range records {
		manifest := rec.manifest
		// Chunk files are addressed relative to the segment's store
		// (Dir-prefixed). verify only rehashes bytes — codec is
		// irrelevant for a byte-level SHA check.
		segStore := rec.segment.store(store)
		// Row chunks (full backups).
		for _, table := range manifest.Tables {
			for _, chunk := range table.Chunks {
				total++
				if err := verifyChunk(ctx, segStore, chunk); err != nil {
					failed++
					slog.ErrorContext(
						ctx, "verify: chunk failed",
						slog.String("manifest", rec.path),
						slog.String("table", table.Name),
						slog.String("file", chunk.File),
						slog.String("error", err.Error()),
					)
					continue
				}
				if perr := probeChunkDecrypt(opts.Envelope, chunk); perr != nil {
					failed++
					slog.ErrorContext(
						ctx, "verify: chunk decrypt probe failed",
						slog.String("manifest", rec.path),
						slog.String("table", table.Name),
						slog.String("file", chunk.File),
						slog.String("error", perr.Error()),
					)
					continue
				}
				slog.DebugContext(
					ctx, "verify: chunk OK",
					slog.String("manifest", rec.path),
					slog.String("table", table.Name),
					slog.String("file", chunk.File),
				)
			}
		}
		// Change chunks (incremental backups).
		for _, chunk := range manifest.ChangeChunks {
			total++
			if err := verifyChunk(ctx, segStore, chunk); err != nil {
				failed++
				slog.ErrorContext(
					ctx, "verify: change chunk failed",
					slog.String("manifest", rec.path),
					slog.String("file", chunk.File),
					slog.String("error", err.Error()),
				)
				continue
			}
			if perr := probeChunkDecrypt(opts.Envelope, chunk); perr != nil {
				failed++
				slog.ErrorContext(
					ctx, "verify: change chunk decrypt probe failed",
					slog.String("manifest", rec.path),
					slog.String("file", chunk.File),
					slog.String("error", perr.Error()),
				)
				continue
			}
			slog.DebugContext(
				ctx, "verify: change chunk OK",
				slog.String("manifest", rec.path),
				slog.String("file", chunk.File),
			)
		}
	}
	return total, failed, nil
}

// probeChunkDecrypt attempts to unwrap a per-chunk WrappedCEK using the
// supplied envelope. No-op when the envelope is nil (no decrypt probe
// requested), the chunk is plaintext (no Encryption metadata), or the
// chunk is per-chain-mode (empty WrappedCEK; the chain root's probe
// already covered it). Returns a wrapping error on unwrap failure so
// the caller can surface "wrong passphrase for THIS chunk" — the
// Bug 117 signal.
func probeChunkDecrypt(env crypto.EnvelopeEncryption, chunk *ir.ChunkInfo) error {
	if env == nil || chunk == nil || chunk.Encryption == nil {
		return nil
	}
	if len(chunk.Encryption.WrappedCEK) == 0 {
		return nil
	}
	if _, err := env.UnwrapCEK(chunk.Encryption.WrappedCEK); err != nil {
		return fmt.Errorf("unwrap chunk cek (passphrase rotated mid-chain?): %w", err)
	}
	return nil
}

// verifyChunk fetches a chunk and recomputes its SHA-256, returning
// nil on match or a wrapped [ErrChunkHashMismatch] on mismatch.
func verifyChunk(ctx context.Context, store ir.BackupStore, chunk *ir.ChunkInfo) error {
	src, err := store.Get(ctx, chunk.File)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = src.Close() }()
	got, err := hashChunkBytes(ctx, src)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	if got != chunk.SHA256 {
		return fmt.Errorf("%w: expected %s, got %s", ErrChunkHashMismatch, chunk.SHA256, got)
	}
	return nil
}
