// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// ParquetExport implements `sluice backup export-as-parquet`
// (ADR-0163, roadmap item 63): a one-shot, read-only transcode of an
// EXISTING backup's row chunks into one Parquet file per table, plus a
// parquet_index.json export manifest. Exit-only by design — sluice
// never reads its Parquet output back; restore keeps the JSON-Lines
// path. The export is a new EXIT over the existing store/manifest
// contract, never a new capture path: chunks are fetched, SHA-256
// verified, decrypted, and header-validated through exactly the same
// blobcodec/lineage machinery restore uses, so every integrity
// property restore enforces (chunk hash, GCM AAD binding, plaintext-
// splice refusal, signed-chain verification, layer-2 row counts)
// holds for the export too.
//
// Shape decisions (ADR-0163):
//
//   - One Parquet file per table (`<schema>.<table>.parquet`), row
//     groups aligned 1:1 with the source chunks, zstd-compressed;
//     each file's footer metadata records the source chunk list with
//     SHA-256s so operators can cross-reference `backup verify`.
//   - The export represents ONE snapshot: a segment full's manifest
//     (the latest by default; `--backup-id` selects an earlier
//     segment's full — "chain to a point" at snapshot granularity).
//     Incremental change-windows after that full are NOT folded in —
//     the exporter WARNs loudly with the count so the boundary is
//     never silent; operators needing point-in-time row state restore
//     the chain and re-export.

import (
	"bytes"
	"context"
	stdcrypto "crypto"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/pipeline/parquetexport"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ParquetIndexFileName is the export manifest written at the output
// root. It doubles as the completion marker (written last) and the
// overwrite sentinel (a fresh export refuses to clobber a prior one
// without ForceOverwrite).
const ParquetIndexFileName = "parquet_index.json"

// exportRowBatchSize is how many encoded rows accumulate before one
// parquet writer Write call. Bounded per-batch memory; row groups are
// cut per source CHUNK (Flush), not per batch.
const exportRowBatchSize = 512

// ParquetExport runs a single export. Construct the value, then call
// Run.
type ParquetExport struct {
	// Store is the backup chain to read (required). Never written.
	Store irbackup.Store

	// Output is the destination store the Parquet files + index land
	// in (required).
	Output irbackup.Store

	// Filter selects which tables participate. Empty exports every
	// table.
	Filter migcore.TableFilter

	// BackupID, when non-empty, selects the segment FULL manifest to
	// export by its BackupID ("chain to a point" at snapshot
	// granularity). Empty exports the latest segment's full. Naming
	// an incremental — or an unknown id — is a loud refusal listing
	// the exportable fulls.
	BackupID string

	// ForceOverwrite discards a prior export at the destination. By
	// default a destination already carrying parquet_index.json is
	// refused.
	ForceOverwrite bool

	// Envelope, VerifyKey, RequireSignature mirror [Restore]: the
	// decrypt envelope for encrypted chains, the asymmetric verify
	// key, and the strict-signature policy. The export honors them
	// exactly like restore does.
	Envelope         crypto.EnvelopeEncryption
	VerifyKey        stdcrypto.PublicKey
	RequireSignature bool

	// SluiceVersion is stamped into parquet_index.json (informational).
	SluiceVersion string

	// chainCEK / chainEncrypted mirror [Restore]'s chunk-decrypt
	// state: the cached per-chain CEK and whether the selected
	// manifest records chain encryption (a plaintext chunk is then a
	// splice, refused).
	chainCEK       []byte
	chainEncrypted bool
}

// parquetIndex is the JSON shape of [ParquetIndexFileName].
type parquetIndex struct {
	FormatVersion   int                  `json:"format_version"`
	SluiceVersion   string               `json:"sluice_version,omitempty"`
	BackupID        string               `json:"backup_id"`
	SourceEngine    string               `json:"source_engine"`
	BackupCreatedAt time.Time            `json:"backup_created_at"`
	ExportedAt      time.Time            `json:"exported_at"`
	EndPosition     ir.Position          `json:"end_position,omitempty"`
	Tables          []*parquetIndexTable `json:"tables"`
}

// parquetIndexTable is one exported table's index entry.
type parquetIndexTable struct {
	Schema       string                `json:"schema,omitempty"`
	Name         string                `json:"name"`
	File         string                `json:"file"`
	Rows         int64                 `json:"rows"`
	RowGroups    int                   `json:"row_groups"`
	SourceChunks []*irbackup.ChunkInfo `json:"source_chunks,omitempty"`
	TypeNotes    []string              `json:"type_notes,omitempty"`
}

// Run executes the export. Returns nil on success; a wrapped (and,
// for refusal classes, coded) error otherwise. Nothing is written to
// the source store, ever.
func (e *ParquetExport) Run(ctx context.Context) error {
	if err := e.validate(); err != nil {
		return err
	}

	// 1. Walk the lineage (validates each segment's recorded codec) and
	//    structurally validate every manifest before touching chunks —
	//    the same Bug-182 posture verify/restore hold.
	records, err := lineage.ListAllSegmentManifests(ctx, e.Store)
	if err != nil {
		return fmt.Errorf("export-as-parquet: %w", err)
	}
	if len(records) == 0 {
		return errors.New("export-as-parquet: no manifests found in the backup store")
	}
	for _, rec := range records {
		if verr := validateManifestStructure(rec.Manifest); verr != nil {
			return sluicecode.Wrap(sluicecode.CodeBackupSignatureInvalid,
				"the backup manifest is structurally invalid (tampered or corrupt) — export from a known-good chain",
				fmt.Errorf("export-as-parquet: manifest %q: %w", rec.Path, verr))
		}
	}

	// 2. ADR-0154 signature policy over the WHOLE chain at its walked
	//    positions — identical to chain restore, so a signed chain's
	//    tamper/rollback refusals gate the export too.
	if err := verifyChainSignatures(ctx, e.Store, records, verifyMaterial{env: e.Envelope, verifyPub: e.VerifyKey}, e.RequireSignature); err != nil {
		return fmt.Errorf("export-as-parquet: %w", err)
	}

	// 3. Select the snapshot to export: the latest full, or --backup-id.
	rec, trailingIncrementals, err := selectExportFull(records, e.BackupID)
	if err != nil {
		return err
	}
	manifest := rec.Manifest
	if manifest.Schema == nil {
		return errors.New("export-as-parquet: selected manifest carries no schema")
	}
	if trailingIncrementals > 0 {
		slog.WarnContext(
			ctx, "export-as-parquet: the chain carries incremental windows AFTER the exported snapshot; their changes are NOT in this export — the Parquet files represent the source exactly at the full's snapshot position (restore the chain and re-export for point-in-time state)",
			slog.String("backup_id", lineage.ManifestBackupID(manifest)),
			slog.Int("incrementals_not_included", trailingIncrementals),
		)
	}

	// 4. Encryption preflight on the selected full (each segment full
	//    is its chain header) — mirrors Restore.preflightEncryption,
	//    including the supplied-key-vs-plaintext-claim refusal.
	if err := e.preflightEncryption(manifest); err != nil {
		return fmt.Errorf("export-as-parquet: %w", err)
	}

	// 5. Refuse to clobber a prior export.
	if !e.ForceOverwrite {
		exists, err := e.Output.Exists(ctx, ParquetIndexFileName)
		if err != nil {
			return fmt.Errorf("export-as-parquet: inspect output: %w", err)
		}
		if exists {
			return fmt.Errorf("export-as-parquet: the output already contains %s from a prior export; pass --force-overwrite to replace it", ParquetIndexFileName)
		}
	}

	// 6. Table filter (schema + manifest sides, mirroring restore).
	if err := migcore.ApplyTableFilter(ctx, manifest.Schema, e.Filter); err != nil {
		return err
	}
	manifest.Tables = filterManifestTables(manifest.Tables, e.Filter)

	segStore := rec.Segment.Store(e.Store)
	codec := rec.Segment.CodecOrDefault()
	chunkColumns := sourceChunkColumns(manifest.Schema)
	tablesByName := indexManifestTables(manifest.Tables)

	slog.InfoContext(
		ctx, "export-as-parquet: starting",
		slog.String("backup_id", lineage.ManifestBackupID(manifest)),
		slog.String("source_engine", manifest.SourceEngine),
		slog.Int("tables", len(manifest.Schema.Tables)),
		slog.Bool("encrypted", manifest.ChainEncryption != nil),
	)

	index := &parquetIndex{
		FormatVersion:   1,
		SluiceVersion:   e.SluiceVersion,
		BackupID:        lineage.ManifestBackupID(manifest),
		SourceEngine:    manifest.SourceEngine,
		BackupCreatedAt: manifest.CreatedAt,
		ExportedAt:      time.Now().UTC(),
		EndPosition:     manifest.EndPosition,
	}
	for _, table := range manifest.Schema.Tables {
		entry, ok := tablesByName[manifestTableKey(table.Schema, table.Name)]
		if !ok {
			slog.InfoContext(ctx, "export-as-parquet: table not in manifest; skipping",
				slog.String("table", table.Name))
			continue
		}
		tentry, err := e.exportTable(ctx, segStore, codec, manifest, table, entry, chunkColumns)
		if err != nil {
			return err
		}
		index.Tables = append(index.Tables, tentry)
	}

	// 7. The index is written LAST: its presence marks a completed
	//    export (and is the overwrite sentinel above).
	b, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("export-as-parquet: marshal %s: %w", ParquetIndexFileName, err)
	}
	if err := putBytes(ctx, e.Output, ParquetIndexFileName, b); err != nil {
		return fmt.Errorf("export-as-parquet: write %s: %w", ParquetIndexFileName, err)
	}

	var totalRows int64
	for _, t := range index.Tables {
		totalRows += t.Rows
	}
	slog.InfoContext(
		ctx, "export-as-parquet: complete",
		slog.Int("tables", len(index.Tables)),
		slog.Int64("rows", totalRows),
	)
	return nil
}

// validate sanity-checks required fields.
func (e *ParquetExport) validate() error {
	switch {
	case e.Store == nil:
		return errors.New("export-as-parquet: Store is nil")
	case e.Output == nil:
		return errors.New("export-as-parquet: Output is nil")
	}
	return nil
}

// selectExportFull picks the full manifest the export represents: the
// LAST full in lineage order by default, or the one whose BackupID
// matches backupID. It also counts the incremental windows recorded
// AFTER the selected full (the WARN input). Refusals name what IS
// exportable so the operator's next command is obvious.
func selectExportFull(records []lineage.SegmentRecord, backupID string) (sel lineage.SegmentRecord, trailingIncrementals int, err error) {
	fullIdx := -1
	var fullIDs []string
	for i := range records {
		m := records[i].Manifest
		if lineage.CanonicalKind(m.Kind) != irbackup.BackupKindFull {
			continue
		}
		id := lineage.ManifestBackupID(m)
		fullIDs = append(fullIDs, id)
		if backupID == "" || id == backupID {
			fullIdx = i
			if backupID != "" {
				break
			}
		}
	}
	if fullIdx == -1 {
		if backupID == "" {
			return lineage.SegmentRecord{}, 0, errors.New("export-as-parquet: the backup store contains no full snapshot manifest")
		}
		return lineage.SegmentRecord{}, 0, fmt.Errorf("export-as-parquet: --backup-id %q does not name a full snapshot in this chain (exportable fulls: %v); incremental change-windows are not exportable — export the full they chain from",
			backupID, fullIDs)
	}
	for i := fullIdx + 1; i < len(records); i++ {
		if lineage.CanonicalKind(records[i].Manifest.Kind) == irbackup.BackupKindIncremental {
			trailingIncrementals++
		}
	}
	return records[fullIdx], trailingIncrementals, nil
}

// exportTable transcodes one table's chunks into one Parquet file at
// the output store and returns its index entry.
func (e *ParquetExport) exportTable(
	ctx context.Context,
	segStore irbackup.Store,
	codec blobcodec.Codec,
	manifest *irbackup.Manifest,
	table *ir.Table,
	entry *irbackup.TableManifest,
	chunkColumns map[string][]string,
) (*parquetIndexTable, error) {
	// Bug-183 posture (mirrors restore): a chunkless table recording
	// rows is a tampered/emptied manifest — refuse rather than export
	// a populated table as empty.
	if len(entry.Chunks) == 0 && entry.RowCount > 0 {
		return nil, sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"export from an untampered copy, or sign the chain so a tampered manifest is caught at verify time",
			fmt.Errorf("export-as-parquet: table %q records %d rows but its manifest carries no chunks — refusing to export a populated table as empty",
				table.Name, entry.RowCount))
	}

	tc, err := parquetexport.NewTableCodec(table)
	if err != nil {
		return nil, sluicecode.Wrap(sluicecode.CodeExportUnrepresentable,
			"exclude the table (--exclude-table) or query its JSON-Lines chunks directly (see the DuckDB cookbook recipe)", err)
	}
	for _, note := range tc.Notes {
		slog.WarnContext(ctx, "export-as-parquet: documented type-mapping downgrade",
			slog.String("table", table.Name), slog.String("note", note))
	}

	fileName := parquetFileName(table.Schema, table.Name)
	tmp, err := os.CreateTemp("", "sluice-parquet-*")
	if err != nil {
		return nil, fmt.Errorf("export-as-parquet: create scratch file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	opts := []parquet.WriterOption{
		tc.Schema,
		parquet.Compression(&parquet.Zstd),
		parquet.KeyValueMetadata("sluice:format", "1"),
		parquet.KeyValueMetadata("sluice:backup_id", lineage.ManifestBackupID(manifest)),
		parquet.KeyValueMetadata("sluice:source_engine", manifest.SourceEngine),
		parquet.KeyValueMetadata("sluice:backup_created_at", manifest.CreatedAt.UTC().Format(time.RFC3339Nano)),
		parquet.KeyValueMetadata("sluice:schema", table.Schema),
		parquet.KeyValueMetadata("sluice:table", table.Name),
	}
	if chunksJSON, merr := json.Marshal(entry.Chunks); merr == nil {
		// The chunk-provenance block: file/sha256/row_count per source
		// chunk, cross-referenceable against `backup verify`.
		opts = append(opts, parquet.KeyValueMetadata("sluice:source_chunks", string(chunksJSON)))
	} else {
		return nil, fmt.Errorf("export-as-parquet: marshal source-chunk metadata for %q: %w", table.Name, merr)
	}
	for k, v := range tc.Metadata {
		opts = append(opts, parquet.KeyValueMetadata(k, v))
	}
	w := parquet.NewGenericWriter[map[string]any](tmp, opts...)

	var rows int64
	for chunkIdx, chunk := range entry.Chunks {
		chunkRows, err := e.exportChunk(ctx, segStore, codec, manifest, table, chunk, chunkColumns, tc, w)
		if err != nil {
			return nil, fmt.Errorf("export-as-parquet: table %q chunk %d (%s): %w", table.Name, chunkIdx, chunk.File, err)
		}
		rows += chunkRows
		// Row groups align 1:1 with source chunks (ADR-0163): the
		// operator-visible chunk concept survives into the Parquet
		// file, and the footer's source_chunks list indexes them.
		if err := w.Flush(); err != nil {
			return nil, fmt.Errorf("export-as-parquet: table %q: flush row group %d: %w", table.Name, chunkIdx, err)
		}
	}

	// Layer-2 table-level check, mirroring restore: the ACTUAL decoded
	// sum vs the manifest's recorded RowCount, both directions.
	switch {
	case entry.RowCount > 0 && rows != entry.RowCount:
		return nil, fmt.Errorf("export-as-parquet: layer-2 row-count mismatch on table %q: manifest says %d, decoded %d",
			table.Name, entry.RowCount, rows)
	case entry.RowCount == 0 && rows != 0:
		return nil, sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"export from an untampered copy, or sign the chain so a zeroed row-count is caught at verify time",
			fmt.Errorf("export-as-parquet: layer-2 row-count anomaly on table %q: manifest records 0 rows but its chunks decoded %d (zeroed RowCount)",
				table.Name, rows))
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("export-as-parquet: table %q: close parquet writer: %w", table.Name, err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("export-as-parquet: table %q: rewind scratch file: %w", table.Name, err)
	}
	if err := e.Output.Put(ctx, fileName, tmp); err != nil {
		return nil, fmt.Errorf("export-as-parquet: table %q: write %s: %w", table.Name, fileName, err)
	}
	slog.InfoContext(
		ctx, "export-as-parquet: table exported",
		slog.String("table", table.Name),
		slog.String("file", fileName),
		slog.Int64("rows", rows),
		slog.Int("row_groups", len(entry.Chunks)),
	)
	return &parquetIndexTable{
		Schema:       table.Schema,
		Name:         table.Name,
		File:         fileName,
		Rows:         rows,
		RowGroups:    len(entry.Chunks),
		SourceChunks: entry.Chunks,
		TypeNotes:    tc.Notes,
	}, nil
}

// exportChunk streams one source chunk through the table codec into
// the parquet writer, with the full restore-grade integrity ladder:
// SHA-256-verified fetch, CEK resolution (with the plaintext-splice
// refusal), GCM AAD binding from the manifest's RECORDED version,
// chunk-header column-set validation, and the per-chunk layer-2 row
// count.
func (e *ParquetExport) exportChunk(
	ctx context.Context,
	segStore irbackup.Store,
	codec blobcodec.Codec,
	manifest *irbackup.Manifest,
	table *ir.Table,
	chunk *irbackup.ChunkInfo,
	chunkColumns map[string][]string,
	tc *parquetexport.TableCodec,
	w *parquet.GenericWriter[map[string]any],
) (int64, error) {
	src, err := blobcodec.FetchChunkVerified(ctx, segStore, chunk.File, chunk.SHA256)
	if err != nil {
		return 0, lineage.CodeChunkHashError(fmt.Errorf("open chunk: %w", err))
	}
	cek, err := e.chunkCEK(chunk)
	if err != nil {
		_ = src.Close()
		return 0, fmt.Errorf("resolve chunk cek: %w", err)
	}
	cr, err := blobcodec.NewChunkReader(src, chunk.SHA256, cek, codec, irbackup.ChunkAADFor(manifest, chunk, table.Schema, table.Name))
	if err != nil {
		return 0, lineage.CodeChunkAuthError(fmt.Errorf("open chunk reader: %w", err))
	}
	// Header ↔ schema cross-check (ADR-0152): a chunk written against a
	// different schema version would silently mis-key rows.
	key := manifestTableKey(table.Schema, table.Name)
	expected, ok := chunkColumns[key]
	if !ok {
		_ = cr.Close()
		return 0, fmt.Errorf("internal: no source column set for table %q", key)
	}
	if missing, extra := diffColumnSets(expected, cr.Header().Columns); len(missing) > 0 || len(extra) > 0 {
		_ = cr.Close()
		return 0, fmt.Errorf("chunk does not match table %q's schema: header is missing columns %v and carries unexpected columns %v — the chunk was written against a different schema version than this manifest records; refusing before any row is exported",
			key, missing, extra)
	}

	var rows int64
	batch := make([]map[string]any, 0, exportRowBatchSize)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, werr := w.Write(batch); werr != nil {
			return fmt.Errorf("write parquet rows: %w", werr)
		}
		batch = batch[:0]
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = cr.Close()
			return rows, err
		}
		row, err := cr.ReadRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = cr.Close()
			return rows, fmt.Errorf("read row: %w", err)
		}
		encoded, err := tc.EncodeRow(row)
		if err != nil {
			_ = cr.Close()
			return rows, sluicecode.Wrap(sluicecode.CodeExportUnrepresentable,
				"the value cannot be represented faithfully in Parquet; exclude the table (--exclude-table) or query its JSON-Lines chunks directly (see the DuckDB cookbook recipe)",
				fmt.Errorf("row %d: %w", rows, err))
		}
		batch = append(batch, encoded)
		rows++
		if len(batch) == exportRowBatchSize {
			if err := flushBatch(); err != nil {
				_ = cr.Close()
				return rows, err
			}
		}
	}
	if err := flushBatch(); err != nil {
		_ = cr.Close()
		return rows, err
	}
	if err := cr.Close(); err != nil {
		return rows, lineage.CodeChunkHashError(err)
	}
	// Per-chunk layer-2 checks, byte-identical semantics to restore's
	// streamChunkRows (incl. the zeroed-RowCount tamper refusal).
	switch {
	case chunk.RowCount > 0 && rows != chunk.RowCount:
		return rows, fmt.Errorf("layer-2 chunk row-count mismatch: manifest says %d, decoded %d", chunk.RowCount, rows)
	case chunk.RowCount == 0 && rows > 0:
		return rows, sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"export from an untampered copy, or sign the chain so a zeroed chunk row-count is caught at verify time",
			fmt.Errorf("layer-2 chunk row-count anomaly: manifest records 0 rows but the chunk decoded %d (zeroed chunk RowCount)", rows))
	}
	return rows, nil
}

// preflightEncryption mirrors [Restore.preflightEncryption] for the
// export's read-only chunk path: refuse a supplied key against a
// plaintext-claiming backup (the SEC-MIRROR downgrade), demand a key
// for an encrypted chain, check the envelope mode, and unwrap/cache
// the chain CEK (per-chain mode) via the ADR-0152 bound chokepoint.
func (e *ParquetExport) preflightEncryption(manifest *irbackup.Manifest) error {
	if manifest == nil || manifest.ChainEncryption == nil {
		if e.Envelope != nil {
			return sluicecode.Wrap(sluicecode.CodeBackupChunkAuthFailed,
				"remove --encrypt if this backup is genuinely unencrypted; if it should be encrypted, its chain-encryption marker was stripped (tampered/downgraded) — sign backups (--sign + --require-signature) to make this tamper-evident",
				errors.New("an encryption key was supplied but this backup is not encrypted (no chain-encryption metadata) — refusing to export a plaintext-claiming backup under a key"))
		}
		return nil
	}
	e.chainEncrypted = true
	enc := manifest.ChainEncryption
	if e.Envelope == nil {
		return fmt.Errorf("encrypted chain (algorithm=%q kek_mode=%q kek_ref=%q) requires --encrypt + a passphrase / KMS reference; no key was supplied",
			enc.Algorithm, enc.KEKMode, enc.KEKRef)
	}
	if enc.KEKMode != "" && e.Envelope.Mode() != enc.KEKMode {
		return fmt.Errorf("encryption envelope mode %q does not match chain's recorded kek_mode %q",
			e.Envelope.Mode(), enc.KEKMode)
	}
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		if len(enc.WrappedCEK) == 0 {
			return errors.New("encrypted chain in per-chain mode but ChainEncryption.WrappedCEK is empty (manifest corrupted?)")
		}
		cek, err := lineage.UnwrapChainCEK(e.Envelope, enc.WrappedCEK, manifest)
		if err != nil {
			return fmt.Errorf("unwrap chain cek (wrong passphrase / KMS key?): %w", err)
		}
		e.chainCEK = cek
		return nil
	}
	// Per-chunk mode: no chain-level CEK; retarget the envelope's key
	// version before the per-chunk unwraps.
	lineage.RebindEnvelopeKEK(e.Envelope, manifest)
	return nil
}

// chunkCEK mirrors [Restore.chunkCEK]: per-chunk wrap wins, per-chain
// falls back to the cached CEK, plaintext chunks in an encrypted chain
// are a splice (refused), plaintext chunks in a plaintext chain return
// nil.
func (e *ParquetExport) chunkCEK(chunk *irbackup.ChunkInfo) ([]byte, error) {
	if chunk.Encryption == nil {
		if e.chainEncrypted {
			return nil, lineage.PlaintextChunkSplicedError(chunk.File)
		}
		return nil, nil
	}
	if len(chunk.Encryption.WrappedCEK) > 0 {
		if e.Envelope == nil {
			return nil, errors.New("per-chunk encrypted chunk encountered without envelope")
		}
		cek, err := e.Envelope.UnwrapCEK(chunk.Encryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("unwrap chunk cek: %w", err)
		}
		return cek, nil
	}
	if e.chainCEK == nil {
		return nil, errors.New("encrypted chunk encountered but chain CEK is unset (preflight skipped?)")
	}
	return e.chainCEK, nil
}

// parquetFileName is the per-table output path: schema-qualified for
// namespaced engines, bare elsewhere — mirroring manifestTableKey.
func parquetFileName(schema, name string) string {
	return manifestTableKey(schema, name) + ".parquet"
}

// putBytes writes an in-memory blob to the output store.
func putBytes(ctx context.Context, store irbackup.Store, path string, b []byte) error {
	return store.Put(ctx, path, bytes.NewReader(b))
}
