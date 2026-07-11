// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Incremental backup orchestrator. Phase 3.1 of the logical-backup
// feature (`docs/dev/design/logical-backups-phase-3.md`): take a
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
// [irbackup.SchemaDeltaEntry] entries that record AddTable / DropTable /
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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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

	// Store is the [irbackup.Store] the parent manifest lives in and
	// the new incremental manifest + chunks are written to. Required.
	Store irbackup.Store

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

	// Encryption, when non-nil, encrypts every change chunk written
	// during this incremental's window. See [lineage.BackupEncryption]. The
	// chain root (the parent full's manifest) governs the chain's
	// encryption shape; the orchestrator validates that this run's
	// encryption matches the parent at start so a mixed-mode chain
	// can't be created.
	Encryption *lineage.BackupEncryption

	// Sign, when true (or forced true because the parent chain is
	// already signed), writes a detached signature over this
	// incremental's manifest + re-signs the lineage catalog (ADR-0154
	// Phase 1). A signed chain's invariant is "every manifest is signed",
	// so extending a signed (v6) chain MUST sign even without the flag —
	// the orchestrator refuses loudly if it cannot (no passphrase-mode
	// envelope), rather than emit an unsigned successor.
	Sign bool

	// Ed25519Signer, when non-nil, signs this incremental with an
	// asymmetric Ed25519 keypair (ADR-0154 Phase 2, `--sign-key`) instead
	// of the HMAC-off-KEK default. Required to EXTEND an Ed25519-signed
	// chain (the chain's scheme must not be mixed); works on plaintext +
	// encrypted (incl. KMS) chains. Mutually exclusive with Sign.
	Ed25519Signer *lineage.Signer

	// Codec is the operator's --compression choice. Consulted only
	// when the open segment's codec isn't already pinned in
	// lineage.json (a segment is single-codec by construction; the
	// recorded codec wins once set). Empty resolves to gzip (pre-ADR
	// default).
	Codec blobcodec.Codec

	// segCodec is the codec resolved for the open segment at Run
	// start; threaded into the change-chunk writer. Set by Run.
	segCodec blobcodec.Codec

	// segStore is the open-segment store view (b.Store narrowed to
	// the open segment's Dir; a no-op wrap for the common one-segment
	// shape). Manifest + chunk writes go here; the lineage update goes
	// to the root b.Store. Set by Run.
	segStore irbackup.Store

	// Now, when set, overrides the wall-clock-time source for
	// [irbackup.Manifest.CreatedAt]. Used by tests to pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time

	// clockNow is the time source used internally for window-closure
	// timing. Defaults to time.Now; tests can override via NowFn for
	// deterministic window expiry.
	clockNow func() time.Time

	// scope is the parent-derived table-name predicate threaded into
	// [readSourceSchema] so the end-position schema-read on engines
	// that implement [ir.TableScoper] (PostgreSQL today) restricts
	// itself to the chain's original table set. Set once at Run start
	// from the parent manifest's recorded schema. Nil means
	// "unscoped" — preserves the historical behaviour for chains
	// whose parent has no recorded table list. Bug 110 closure.
	scope func(tableName string) bool
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

	// 0. Resolve the open segment: every incremental lands in the
	//    lineage's open segment, under its Dir, with its recorded
	//    codec (ADR-0046). For a never-rotated backup that's the root
	//    (Dir == "") — byte-identical to the pre-ADR single chain.
	segStore, segCodec, err := lineage.OpenSegmentStore(ctx, b.Store, b.Codec)
	if err != nil {
		return fmt.Errorf("incremental: resolve open segment: %w", err)
	}
	b.segStore = segStore
	b.segCodec = segCodec

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
		slog.WarnContext(
			ctx, "incremental: parent manifest has no EndPosition; chain will start from CDC's current position (parent is a v0.16.x full or pre-Phase-3 manifest)",
			slog.String("parent_path", parentPath),
		)
	}

	// 1.1. ADR-0087 rotation-boundary resume heal (Bug 139) — symmetric
	//    with `backup stream`'s resume path. A one-shot `backup incremental`
	//    extending a chain whose open segment is rotation-born and has zero
	//    committed incrementals (the prior stream/incremental session stopped
	//    or crashed at the rotation boundary) must resume from the prior
	//    segment's EndPosition (P_N), not the open segment's full anchor S, so
	//    the first incremental starts at P_N, stamps IncrementalCoverageStart
	//    = P_N, and the lineage is born-contiguous + compactable. See
	//    [rotationBoundaryResumeStart].
	if healed, priorEnd, ok := rotationBoundaryResumeStart(ctx, b.Store, startPos); ok {
		slog.InfoContext(
			ctx, "incremental: resuming a rotation-born segment from the prior segment's EndPosition (P_N) "+
				"to re-establish ADR-0067 overlap coverage — the creating session stopped/crashed before "+
				"committing this segment's first incremental (Bug 139)",
			slog.String("parent_path", parentPath),
			slog.String("prior_end_pN", priorEnd.Token),
			slog.String("full_anchor_S", startPos.Token),
		)
		startPos = healed
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
	beforeHash, err := irbackup.ComputeSchemaHash(beforeSchema)
	if err != nil {
		return fmt.Errorf("incremental: hash source schema (start): %w", err)
	}

	// Build the parent-derived table-name predicate so the
	// end-position schema-read in step 5 restricts itself to the
	// chain's original table set. This closes Bug 110: pre-fix the
	// schema-read iterated every table in the source and a single
	// unrelated table carrying a verbatim-eligible column type
	// (xml / money / interval / etc.) failed the whole incremental.
	// Engines that don't implement [ir.TableScoper] (today: MySQL)
	// silently fall through to the unscoped read in
	// [readSourceSchema]. A parent with no recorded tables (corrupt
	// manifest / pre-v0.94 fall-back) leaves b.scope nil — also
	// equivalent to the historical behaviour.
	if beforeSchema != nil && len(beforeSchema.Tables) > 0 {
		allowed := make(map[string]struct{}, len(beforeSchema.Tables))
		for _, t := range beforeSchema.Tables {
			if t != nil {
				allowed[t.Name] = struct{}{}
			}
		}
		b.scope = func(tableName string) bool {
			_, ok := allowed[tableName]
			return ok
		}
	}

	// 2.5. Chain-resume preflight (engines with server-side consumer
	// state, i.e. Postgres slots): verify the slot can actually serve
	// startPos BEFORE opening the stream. Without it, a slot created
	// (or advanced by another consumer) after the parent backup makes
	// the walsender silently fast-forward past the WAL in between —
	// the incremental SUCCEEDS while the chain silently misses those
	// writes. The preflight converts that into a loud refusal naming
	// `backup full --chain-slot` as the fix (see [migcore.PreflightChainResume]).
	if err := migcore.PreflightChainResume(ctx, b.Source, b.SourceDSN, startPos); err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("incremental: chain preflight: %w", err))
	}

	// 3. Open CDC reader at parent's EndPosition.
	cdc, err := b.openCDCReader(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("incremental: open cdc reader: %w", err))
	}
	defer migcore.CloseIf(cdc)

	// Chain-consumer ack mode: without an applier there is no LSN
	// tracker, and the reader's no-tracker keepalive fallback acks the
	// STREAMED position — which can run ahead of what this run durably
	// commits (events parsed by the pump but past the window close are
	// discarded). An ack past the recorded EndPosition releases WAL
	// the chain has not captured, silently gapping the next link. Hold
	// the ack at the stream's start; the committed end is released
	// after the manifest write below.
	holdChainAck(cdc)

	changesCh, err := cdc.StreamChanges(ctx, startPos)
	if err != nil {
		// The engine returns a wrapped ir.ErrPositionInvalid when the
		// source's WAL / binlog has been pruned past startPos. Surface
		// that loudly with a clear "your --since parent is too old;
		// take a fresh full" line.
		if errors.Is(err, ir.ErrPositionInvalid) {
			return fmt.Errorf("incremental: source cannot serve the parent's terminal position (WAL/binlog pruned past it, or the source identity changed); take a fresh full backup — `backup full --chain-slot` provisions retention so this cannot recur — or shorten the chain interval. Underlying: %w", err)
		}
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("incremental: start cdc stream: %w", err))
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

	manifest := &irbackup.Manifest{
		// Bug 116 closure: stamp the smallest format version safe for
		// this incremental's schema. Same proportional rule as fulls.
		FormatVersion:  irbackup.FormatVersionFor(beforeSchema),
		SluiceVersion:  b.SluiceVersion,
		CreatedAt:      now().UTC(),
		SourceEngine:   b.Source.Name(),
		Schema:         beforeSchema,
		Tables:         nil, // incrementals don't carry table-level row chunks
		PartialState:   irbackup.BackupStateInProgress,
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: parent.BackupID,
		StartPosition:  startPos,
		SchemaHash:     beforeHash,
		// Bug 184: record whether this engine's CDC positions commit AFTER
		// their rows (VStream), so restore knows a schema anchor at
		// EndPosition cannot prove the window's data was applied.
		CDCPositionCommitsAfterRows: b.Source.Capabilities().CDCPositionCommitsAfterRows,
	}
	// If the parent has no BackupID (legacy v0.16.x), compute one
	// retroactively so chain-walk has a stable link. The retroactive
	// ID is identical to what `incremental` would compute for the
	// same content, so a future re-write of the parent manifest
	// (e.g. with the v0.17.0 backup-full path) doesn't break the
	// chain.
	if manifest.ParentBackupID == "" {
		manifest.ParentBackupID = irbackup.ComputeBackupID(parent)
	}

	// Phase 6.1: align this incremental's encryption with the chain
	// root. The parent full's [irbackup.ChainEncryption] dictates the chain's
	// shape; mismatched runs (encrypt mid-chain or vice versa) are
	// refused at chain-restore time anyway, so reject early here.
	chainCEK, err := b.alignEncryption(ctx, parent)
	if err != nil {
		return fmt.Errorf("incremental: encryption: %w", err)
	}
	// ADR-0152: an encrypted incremental's chunks are freshly written
	// this run, so they always carry the chunk-position binding and the
	// manifest is stamped accordingly — even when it extends an OLDER
	// (pre-v5) chain, whose root keeps its own version and unbound
	// shape (each link's chunks are gated by its OWN recorded version).
	// Must precede captureWindow so [irbackup.ChunkAAD] gates on.
	if b.Encryption != nil {
		manifest.FormatVersion = max(manifest.FormatVersion, irbackup.FormatVersionEncryptedChunkBinding)
	}
	// ADR-0154: extend a signed chain as signed (stamps v6 + preflights
	// the signer before the window opens). See [resolveSigning].
	signing, err := b.resolveSigning(ctx, manifest)
	if err != nil {
		return err
	}

	deadline := clockNow().Add(windowDur)
	endPos, totalChanges, captureErr := b.captureWindow(ctx, cdc, changesCh, manifest, chunkSize, deadline, b.MaxChanges, clockNow, chainCEK)
	if captureErr != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("incremental: capture window: %w", captureErr))
	}
	manifest.EndPosition = endPos
	if err := assertDataWindowEndPositionInvariant(manifest); err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, err)
	}

	// 5. Read source schema at window end and diff against the start
	//    snapshot to populate SchemaDelta. The window may produce
	//    zero deltas (the common case — most incrementals carry no
	//    DDL); the Diff helper returns an empty slice in that case.
	afterSchema, err := b.readSourceSchema(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("incremental: read source schema (end): %w", err))
	}
	manifest.SchemaDelta = migcore.DiffSchemas(beforeSchema, afterSchema)
	if len(manifest.SchemaDelta) > 0 {
		// The end-state schema is more useful for restore-side
		// targeting than the start-state. Swap it in so the manifest's
		// recorded Schema reflects the post-window source shape.
		manifest.Schema = afterSchema
		afterHash, err := irbackup.ComputeSchemaHash(afterSchema)
		if err != nil {
			return fmt.Errorf("incremental: hash source schema (end): %w", err)
		}
		// Phase 3.1 records the post-window schema hash so the chain
		// walker can detect a schema change between adjacent
		// incrementals (their start-of-window hash should match the
		// previous incremental's end-of-window hash).
		manifest.SchemaHash = afterHash
	} else {
		// item 51: even a no-DDL window must carry the END-of-window
		// standalone-sequence positions - positions advance with DML,
		// so the delta gate above never re-stamps for them, and the
		// chain-tail re-prime would otherwise read positions the
		// window's own changes already consumed. ComputeSchemaHash
		// canonicalizes positions away, so the swap is hash-invisible
		// for POSITION-only drift — but sequence OPTIONS changed inside
		// a no-DDL window DO shift the fingerprint, and chain-restore
		// now verifies recorded-vs-recomputed (ADR-0152), so the hash
		// is re-stamped over the swapped schema. Recorded hash ==
		// hash(recorded schema) is the invariant; the adjacent-link
		// continuity reading stays intact because the next link's
		// before-hash is computed from THIS recorded schema. (Pre-
		// ADR-0152 manifests skipped the re-stamp; the chain-restore
		// verifier carries a named WARN carve-out for that shape.)
		manifest.Schema = schemaWithRefreshedSequences(manifest.Schema, afterSchema)
		refreshedHash, err := irbackup.ComputeSchemaHash(manifest.Schema)
		if err != nil {
			return fmt.Errorf("incremental: hash refreshed schema: %w", err)
		}
		manifest.SchemaHash = refreshedHash
	}

	// 6. Compute BackupID and finalise.
	manifest.BackupID = irbackup.ComputeBackupID(manifest)
	manifest.PartialState = irbackup.BackupStateComplete

	manifestPath := buildIncrementalManifestPath(manifest)
	if err := lineage.WriteManifestAt(ctx, b.segStore, manifestPath, manifest); err != nil {
		return fmt.Errorf("incremental: write manifest: %w", err)
	}
	// The window is durable — let the slot release WAL up to its end.
	// (Effective on the reader's next keepalive; for a one-shot
	// incremental the release usually lands via the NEXT run's start
	// ack instead, which is equivalent. The long-lived `backup stream`
	// path relies on this to bound WAL retention per rollover.)
	releaseChainAckTo(ctx, cdc, manifest.EndPosition)
	// ADR-0046: append this incremental to the open segment in
	// lineage.json (best-effort for the non-rotation path; the
	// manifest file is authoritative for the one-segment shape).
	lineage.UpdateLineageForManifestBestEffort(ctx, b.Store, manifest, manifestPath, b.segCodec)

	// ADR-0154: sign this incremental's manifest + re-sign the lineage
	// catalog (the enumeration just grew). The manifest signature lives
	// next to the manifest in the segment store; the lineage signature at
	// the lineage root. Fail-loud — a signed chain with an unsigned tail
	// refuses at restore.
	if signing {
		if err := b.signIncrementalArtifacts(ctx, manifest, manifestPath); err != nil {
			return fmt.Errorf("incremental: sign artifacts: %w", err)
		}
	}

	slog.InfoContext(
		ctx, "incremental backup complete",
		slog.String("backup_id", manifest.BackupID),
		slog.String("parent_backup_id", manifest.ParentBackupID),
		slog.Int("changes", int(totalChanges)),
		slog.Int("chunks", len(manifest.ChangeChunks)),
		slog.Int("schema_deltas", len(manifest.SchemaDelta)),
		slog.Int("schema_history", len(manifest.SchemaHistory)),
		slog.String("manifest_path", manifestPath),
	)
	return nil
}

// resolveSigning decides whether this incremental must be signed and, if
// so, preflights the signer + stamps the signed FormatVersion on manifest
// BEFORE the window opens (ADR-0154 Q4). Signing is forced when the chain
// is already signed — detected by the presence of lineage.json.sig, the
// real signal (a bare v6 FormatVersion stamp with no signature is not a
// signed chain). Refuses loudly on a plaintext chain or a non-signable
// (KMS) envelope rather than emit an unsigned successor.
func (b *IncrementalBackup) resolveSigning(ctx context.Context, manifest *irbackup.Manifest) (bool, error) {
	chainSigned, err := lineage.ChainIsSigned(ctx, b.Store)
	if err != nil {
		return false, fmt.Errorf("incremental: probe signed chain: %w", err)
	}
	if !b.Sign && b.Ed25519Signer == nil && !chainSigned {
		return false, nil
	}
	if _, err := b.incrementalWriteSigner(ctx, chainSigned); err != nil {
		return false, err
	}
	manifest.FormatVersion = max(manifest.FormatVersion, irbackup.FormatVersionSignedManifest)
	return true, nil
}

// incrementalWriteSigner resolves the signer for this incremental,
// enforcing that it matches the EXISTING chain's signature scheme when
// extending a signed chain — a chain must never mix HMAC and Ed25519
// links (mixing would make the "all links same scheme" verify probe pick
// the wrong verifier). Ed25519 (`--sign-key`) is allowed on plaintext,
// encrypted, and KMS chains; HMAC-off-KEK (`--sign`) needs a local
// passphrase-derived KEK.
func (b *IncrementalBackup) incrementalWriteSigner(ctx context.Context, chainSigned bool) (*lineage.Signer, error) {
	var chainScheme string
	if chainSigned {
		s, ok, err := lineage.ChainSignatureScheme(ctx, b.Store)
		if err != nil {
			return nil, fmt.Errorf("incremental: probe chain scheme: %w", err)
		}
		if ok {
			chainScheme = s
		}
	}
	if b.Ed25519Signer != nil {
		// b.Ed25519Signer carries any --sign-key signer (Ed25519 OR KMS);
		// compare its FAMILY against the chain's (a composite kms token
		// carries the algorithm, so match on family, never the whole token).
		suppliedFamily := b.Ed25519Signer.Scheme
		if chainScheme != "" && irbackup.SchemeFamily(chainScheme) != suppliedFamily {
			return nil, fmt.Errorf("incremental: --sign-key (%s) cannot extend a %q-signed chain — extend it with the scheme it was signed under; refusing to mix signature schemes in one chain", suppliedFamily, chainScheme)
		}
		return b.Ed25519Signer, nil
	}
	// HMAC-off-KEK path (`--sign`, or extending an HMAC-signed chain). An
	// asymmetric chain (ed25519 / kms) cannot be extended without --sign-key.
	if fam := irbackup.SchemeFamily(chainScheme); fam == irbackup.SignatureSchemeEd25519 || fam == irbackup.SignatureSchemeKMS {
		return nil, fmt.Errorf("incremental: this chain is %q-signed (ADR-0154) — extend it with `backup incremental --sign-key <key|kms://...>` matching that scheme; refusing to emit an unsigned/mis-signed successor to a signed chain", chainScheme)
	}
	if b.Encryption == nil {
		return nil, errors.New("incremental: cannot HMAC-sign a plaintext chain (ADR-0154 HMAC-off-KEK needs a KEK); use --sign-key (Ed25519) for a plaintext chain, or the parent is unsigned/plaintext")
	}
	signer, ok, err := lineage.NewSigner(b.Encryption.Envelope)
	if err != nil {
		return nil, fmt.Errorf("incremental: signing: %w", err)
	}
	if !ok {
		return nil, errors.New("incremental: extending a signed chain needs a passphrase-derived signing key, but the envelope cannot key an HMAC off its KEK (use --sign-key for Ed25519; KMS Sign is ADR-0154 Phase 3); refusing to emit an unsigned successor to a signed chain")
	}
	return signer, nil
}

// signIncrementalArtifacts signs this incremental's manifest at its
// lineage position and re-signs the lineage catalog (ADR-0154). It first
// ensures the lineage update is durable (fail-loud, idempotent) — the
// best-effort update above is inadequate for a signed chain, whose
// freshness anchor is the signed link enumeration.
func (b *IncrementalBackup) signIncrementalArtifacts(ctx context.Context, manifest *irbackup.Manifest, manifestPath string) error {
	// The chain is signed by the time we sign (resolveSigning committed to
	// it); probe the scheme to pick the same signer resolveSigning
	// validated. Extending an unsigned parent with --sign has chainSigned
	// false at this point, so pass the current probe.
	chainSigned, err := lineage.ChainIsSigned(ctx, b.Store)
	if err != nil {
		return fmt.Errorf("incremental: probe signed chain: %w", err)
	}
	signer, err := b.incrementalWriteSigner(ctx, chainSigned)
	if err != nil {
		return err
	}
	// Ensure the catalog durably includes this incremental before we sign
	// its enumeration (idempotent: dedups on manifest path).
	if err := lineage.UpdateLineageForManifest(ctx, b.Store, manifest, manifestPath, b.segCodec); err != nil {
		return fmt.Errorf("ensure lineage catalog durable: %w", err)
	}
	cat, ok, err := lineage.LoadLineageCatalog(ctx, b.Store)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("lineage catalog absent after update; cannot assign a signing sequence")
	}
	// The just-appended incremental is the newest link in flat order.
	seq := lineage.ManifestCount(cat) - 1
	if seq < 0 {
		seq = 0
	}
	if err := lineage.WriteManifestSig(ctx, b.segStore, manifestPath, manifest, seq, signer); err != nil {
		return fmt.Errorf("write manifest signature: %w", err)
	}
	if err := lineage.WriteLineageSig(ctx, b.Store, cat, signer); err != nil {
		return fmt.Errorf("write lineage signature: %w", err)
	}
	slog.InfoContext(ctx, "incremental: wrote detached signatures (ADR-0154)",
		slog.String("scheme", signer.Scheme),
		slog.String("key_id", signer.KeyID),
		slog.Int("sequence", seq))
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
func (b *IncrementalBackup) resolveParent(ctx context.Context) (*irbackup.Manifest, string, error) {
	// An incremental chains off a manifest in the OPEN segment. Walk
	// the open segment's manifests (b.segStore is already narrowed to
	// its Dir).
	manifests, err := lineage.ListAllManifestsViaWalk(ctx, b.segStore)
	if err != nil {
		return nil, "", err
	}
	if len(manifests) == 0 {
		return nil, "", errors.New("no parent manifest found in store; take a `sluice backup full` first")
	}
	if b.ParentRef != "" {
		for _, m := range manifests {
			id := m.Manifest.BackupID
			if id == "" {
				id = irbackup.ComputeBackupID(m.Manifest)
			}
			if id == b.ParentRef {
				if err := refuseInProgressParent(m.Manifest, m.Path); err != nil {
					return nil, "", err
				}
				return m.Manifest, m.Path, nil
			}
		}
		return nil, "", fmt.Errorf("parent backup %q not found in store; available: %s",
			b.ParentRef, manifestSummary(manifests))
	}
	// Resume off the chain TAIL (open segment append order), NOT
	// max CreatedAt. See [chainTailManifest].
	tail := chainTailManifest(ctx, b.Store, manifests)
	if err := refuseInProgressParent(tail.Manifest, tail.Path); err != nil {
		return nil, "", err
	}
	return tail.Manifest, tail.Path, nil
}

// refuseInProgressParent refuses to extend a chain off a manifest whose
// PartialState is still "in_progress" — a crashed (or still running)
// `backup full`. Required by task #42 (ADR-0085): in-progress full
// manifests now carry their chain anchor from the first write, so
// without this guard an incremental would silently chain at the anchor
// of a full whose row chunks are incomplete — restore would be missing
// tables while exiting 0. (Incremental/stream manifests are only ever
// persisted in their complete state, so in practice this fires only on
// a crashed full at the chain root.)
func refuseInProgressParent(m *irbackup.Manifest, path string) error {
	if m == nil || m.PartialState != irbackup.BackupStateInProgress {
		return nil
	}
	return fmt.Errorf(
		"parent backup manifest %q records an interrupted run (partial_state=in_progress); a chain cannot extend from an incomplete link. "+
			"Finish it first — re-running the same `backup full` command resumes an interrupted full — or start a fresh chain with `backup full --force-overwrite`",
		path,
	)
}

// chainTailManifest returns the chain tail to resume an incremental
// off of, given the open segment's store, the walk result over that
// store ([lineage.ListAllManifestsViaWalk]: the segment full first when
// present, then the incremental manifests in lexically-sorted path
// order), and the catalog.
//
// The AUTHORITATIVE chain order is the open segment's lineage.json
// `Incrementals` slice — it is written in append (commit) order by
// [lineage.UpdateLineageForManifest], so its last entry is the chain tail
// (ADR-0046: lineage.json is the structural record). The tail
// manifest is the one at that path. When the lineage is absent (the
// legacy never-catalogued one-segment shape) or has no incrementals
// yet, fall back to the walk: the last record (last incremental, or
// the full if the segment has none).
//
// This replaced a max(CreatedAt) selection that branched the lineage
// on a CreatedAt tie: irbackup.Manifest.CreatedAt is wall-clock with
// platform-dependent resolution, not unique nor strictly monotonic
// with chain order, so two back-to-back rollovers landing in the
// same millisecond made a post-restart resolveParent resume off the
// second-to-last link. lineage.BuildLineageChain's parent-link validator
// (correctly) refused the resulting branched lineage at restore
// time. Resolving the parent from the lineage's recorded append
// order keeps the restart's first incremental stitched exactly where
// an uninterrupted run would have put it. Path order is NOT a safe
// substitute: the incremental filename's unix-millis can tie and the
// short-ID disambiguator is content-derived (effectively unordered),
// so two same-millisecond incrementals can sort either way.
//
// rootStore is the lineage root (lineage.json lives there, NOT inside
// a segment sub-dir); recs and the segment Incrementals paths are
// both relative to the open segment's Dir, so they match by path.
// recs must be non-empty (callers check len == 0 first).
func chainTailManifest(ctx context.Context, rootStore irbackup.Store, recs []lineage.ManifestRecord) lineage.ManifestRecord {
	cat, ok, err := lineage.LoadLineageCatalog(ctx, rootStore)
	if err == nil && ok && len(cat.Segments) > 0 {
		seg := &cat.Segments[len(cat.Segments)-1] // open segment
		if n := len(seg.Incrementals); n > 0 {
			tailPath := seg.Incrementals[n-1]
			for i := range recs {
				if recs[i].Path == tailPath {
					return recs[i]
				}
			}
		}
	}
	// No lineage / no incrementals recorded: the walk's last record is
	// the tail (full when the segment has no incrementals).
	return recs[len(recs)-1]
}

// readSourceSchema opens a fresh schema reader and reads the source
// schema. Used at the start and end of the incremental's window for
// SchemaDelta computation.
// readSourceSchema reads the source's schema and, on engines that
// implement [ir.TableScoper], restricts the read to the parent
// chain's table set. Pre-fix Bug 110: an unscoped read iterated
// every table in the source schema, so a single unrelated table
// carrying a verbatim-eligible column type (xml / money / interval
// / etc.) on a chain originally taken with `--include-table=X`
// failed the incremental at
// `read source schema (end): postgres: read columns: table "Y"
// column "Z": postgres: unsupported data_type` — a previously-
// working chain broke because an unrelated table was added to the
// source. The scope is derived from the parent manifest's recorded
// schema (the originally-included tables); engines that don't
// implement TableScoper (today: MySQL) fall through to the
// unscoped ReadSchema.
//
// b.scope is the engine-neutral predicate built once per Run from
// the parent manifest. When nil (a parent with no table list, e.g.
// pre-v0.94.0 manifests written before the scope was recorded),
// the historical unscoped behaviour is preserved.
func (b *IncrementalBackup) readSourceSchema(ctx context.Context) (*ir.Schema, error) {
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("open schema reader: %w", err)
	}
	defer migcore.CloseIf(sr)
	if b.scope != nil {
		if scoper, ok := sr.(ir.TableScoper); ok {
			scoper.SetTableScope(b.scope)
		}
	}
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
		slog.InfoContext(
			ctx, "incremental: --slot-name supplied but engine has no slot concept; ignoring",
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
	manifest *irbackup.Manifest,
	chunkSize int,
	deadline time.Time,
	maxChanges int,
	clockNow func() time.Time,
	chainCEK []byte,
) (ir.Position, int64, error) {
	var (
		writer        *blobcodec.ChangeChunkWriter
		buf           *bytes.Buffer
		chunkIdx      int
		totalChanges  int64
		lastPos       ir.Position
		inTransaction bool
		curWrappedCEK []byte
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

	// schemaHistorySeen dedupes schema-history entries by
	// [ir.SchemaVersionKey] across all chunks of this incremental's
	// window. The Chunk-B true-delta gate means the CDC reader should
	// only emit one SchemaSnapshot per genuine boundary, but defensive
	// dedup is cheap and worth the pin (locked design point #3 — the
	// engine's writeSchemaVersion is idempotent via the PK surrogate,
	// but de-duping at the manifest level keeps the persisted envelope
	// minimal).
	schemaHistorySeen := map[string]struct{}{}
	// drainSnapshots converts the writer's collected SchemaSnapshots to
	// SchemaHistoryEntry values and appends them to the manifest. The
	// StreamID is deliberately empty at backup time — the orchestrator
	// has no applier in the loop and therefore no source-side streamID
	// to record. Restore-time replay (applySchemaHistory in
	// chain_restore.go) substitutes ChainRestoreStreamID when the
	// entry's StreamID is empty.
	drainSnapshots := func(snaps []ir.SchemaSnapshot) error {
		for _, s := range snaps {
			if s.IR == nil {
				// A reader should never emit a nil-IR snapshot; if one
				// does, that's the loud-failure class (locked decision
				// #4b — never silently degrade schema history).
				return fmt.Errorf("schema snapshot for %s.%s at %+v has nil IR table",
					s.Schema, s.Table, s.Position)
			}
			key := ir.SchemaVersionKey("", s.Schema, s.Table, s.Position.Token)
			if _, dup := schemaHistorySeen[key]; dup {
				continue
			}
			schemaHistorySeen[key] = struct{}{}
			payload, err := ir.MarshalTable(s.IR)
			if err != nil {
				return fmt.Errorf("marshal schema-history table %s.%s: %w",
					s.Schema, s.Table, err)
			}
			manifest.SchemaHistory = append(manifest.SchemaHistory, &irbackup.SchemaHistoryEntry{
				// StreamID stays empty at backup time; restore-side
				// substitutes ChainRestoreStreamID. See drainSnapshots
				// doc above.
				StreamID:       "",
				Schema:         s.Schema,
				Table:          s.Table,
				AnchorPosition: s.Position,
				TableJSON:      payload,
			})
		}
		return nil
	}

	flush := func() error {
		if writer == nil {
			return nil
		}
		// ADR-0049 Chunk D: drain the writer's collected SchemaSnapshots
		// into the manifest BEFORE closing the writer. Snapshots ride
		// the manifest envelope, not the chunk's JSONL stream.
		if err := drainSnapshots(writer.Snapshots()); err != nil {
			return fmt.Errorf("drain schema snapshots: %w", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close chunk: %w", err)
		}
		path := changeChunkPath(runNamespace, chunkIdx)
		hash := writer.Hash()
		if err := b.segStore.Put(ctx, path, buf); err != nil {
			return fmt.Errorf("store put %q: %w", path, err)
		}
		ci := &irbackup.ChunkInfo{
			File:     path,
			RowCount: writer.ChangeCount(),
			SHA256:   hash,
		}
		if b.Encryption != nil {
			ci.Encryption = &irbackup.ChunkEncryption{
				Algorithm:  crypto.AlgorithmAESGCM,
				NonceLen:   crypto.NonceLen,
				AuthTagLen: crypto.AuthTagLen,
				WrappedCEK: curWrappedCEK,
			}
		}
		manifest.ChangeChunks = append(manifest.ChangeChunks, ci)
		writer = nil
		buf = nil
		curWrappedCEK = nil
		chunkIdx++
		return nil
	}

	openWriter := func() error {
		buf = &bytes.Buffer{}
		cek, wrapped, err := b.resolveChunkCEK(chainCEK)
		if err != nil {
			return fmt.Errorf("resolve chunk cek: %w", err)
		}
		curWrappedCEK = wrapped
		// ADR-0152: bind the chunk to the path + list ordinal flush
		// will record it at (chunkIdx only advances at flush, so open
		// and flush agree; the ordinal guards change-REPLAY order).
		path := changeChunkPath(runNamespace, chunkIdx)
		w, err := blobcodec.NewChangeChunkWriter(buf, cek, b.segCodec, irbackup.ChangeChunkAADForWrite(manifest, path, chunkIdx, cek))
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

// assertDataWindowEndPositionInvariant is the writer-side backstop for the
// Bug 184 / audit silent-loss-F2 soundness invariant. On an engine whose CDC
// positions do NOT commit after their rows (Postgres, MySQL-binlog — where a
// schema anchor strictly precedes the rows it introduces), a DATA-bearing
// incremental window's EndPosition must NEVER coincide with a schema-history
// anchor. The restore-side completeness net (chain_restore/broker) relies on
// exactly that to tell a genuine DDL-only window (0 chunks, anchor == EndPosition)
// apart from an emptied-data window; if a future reader change violated it, an
// emptied-data window on such an engine could masquerade as schema-only and
// silently drop data. This converts the implicit reader property into a checked
// one: fail the backup loudly rather than persist a manifest whose completeness
// check is unsound. (VStream engines legitimately co-locate a snapshot with its
// transaction's rows — which is WHY they set CDCPositionCommitsAfterRows and the
// restore net distrusts their anchors — so the invariant is asserted only when
// the flag is false.)
func assertDataWindowEndPositionInvariant(manifest *irbackup.Manifest) error {
	if manifest.CDCPositionCommitsAfterRows || len(manifest.ChangeChunks) == 0 {
		return nil
	}
	if manifest.SchemaHistoryAnchors(manifest.EndPosition) {
		return fmt.Errorf(
			"reader invariant violated: a data-bearing incremental window (%d change chunks) on a non-commit-after-rows engine recorded EndPosition %+v coinciding with a schema-history anchor — the restore-side completeness check would be unsound (an emptied-data window could masquerade as schema-only). This is a CDC-reader bug: on this engine a schema snapshot must be anchored strictly before the rows it introduces",
			len(manifest.ChangeChunks), manifest.EndPosition,
		)
	}
	return nil
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
func changeChunkRunNamespace(m *irbackup.Manifest) string {
	return fmt.Sprintf("%013d", m.CreatedAt.UTC().UnixMilli())
}

// buildIncrementalManifestPath returns the conventional relative
// path an incremental manifest is written to. The path encodes the
// CreatedAt unix-millis (lexically sortable across a chain) plus the
// short BackupID for disambiguation when two incrementals are taken
// in the same millisecond on the same source (rare but possible
// under load).
func buildIncrementalManifestPath(m *irbackup.Manifest) string {
	short := m.BackupID
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf(
		"%sincr-%013d-%s.json",
		lineage.IncrementalManifestPrefix,
		m.CreatedAt.UTC().UnixMilli(),
		short,
	)
}

// manifestSummary returns a human-readable list of manifest IDs for
// error messages.
func manifestSummary(records []lineage.ManifestRecord) string {
	parts := make([]string, 0, len(records))
	for _, r := range records {
		id := r.Manifest.BackupID
		if id == "" {
			id = irbackup.ComputeBackupID(r.Manifest) + " (computed)"
		}
		parts = append(parts, fmt.Sprintf("%s (%s, %s)", id, r.Manifest.Kind, r.Path))
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

// alignEncryption inspects the parent manifest's chain root for
// encryption metadata and validates that the incremental's
// configuration is consistent. Returns the chain-level CEK for
// per-chain mode, or nil for per-chunk / unencrypted.
//
// Refusal cases:
//
//   - Parent's chain is encrypted but b.Encryption is nil → refuse.
//   - Parent's chain is plaintext but b.Encryption is non-nil →
//     refuse (would create a mixed-mode chain).
//   - Parent's chain root carries [irbackup.ChainEncryption] but the
//     supplied envelope's Mode() doesn't match → refuse.
//
// Bug 43 (v0.22.1): when the parent's chain encryption records
// Argon2id params, the orchestrator rebuilds the supplied envelope
// against the chain's recorded salt before unwrapping the parent's
// WrappedCEK. CLI envelopes are minted with a fresh salt at startup
// (the salt is needed for cold-start), so without this rebind the
// unwrap fails with `aes-gcm open: cipher: message authentication
// failed`. Tests that pre-build envelopes with the chain's known salt
// don't supply RebuildForChain and pass through the cold-start path.
func (b *IncrementalBackup) alignEncryption(ctx context.Context, parent *irbackup.Manifest) ([]byte, error) {
	rootManifest, parentEnc, err := lineage.ChainRootEncryption(ctx, b.segStore, parent)
	if err != nil {
		// Audit N-6: a failed root-manifest read must NOT be conflated
		// with "parent chain is plaintext" — that branch decides whether
		// this segment's chunks are written encrypted or plaintext.
		return nil, fmt.Errorf("incremental: cannot determine parent chain encryption state (refusing to assume plaintext): %w", err)
	}
	switch {
	case parentEnc == nil && b.Encryption == nil:
		return nil, nil
	case parentEnc == nil && b.Encryption != nil:
		return nil, errors.New("incremental: parent chain is plaintext but --encrypt was supplied; cannot extend a plaintext chain with encrypted incrementals")
	case parentEnc != nil && b.Encryption == nil:
		return nil, fmt.Errorf("incremental: parent chain is encrypted (algorithm=%q kek_mode=%q kek_ref=%q) but no --encrypt + key was supplied",
			parentEnc.Algorithm, parentEnc.KEKMode, parentEnc.KEKRef)
	}
	// Both non-nil: rebind the envelope to the chain's salt (Bug 43)
	// before validating mode / unwrapping the chain CEK.
	if err := b.Encryption.RebindForChain(parentEnc.Argon2id); err != nil {
		return nil, fmt.Errorf("incremental: rebuild envelope for chain: %w", err)
	}
	if b.Encryption.Envelope == nil {
		return nil, errors.New("incremental: encryption envelope is nil")
	}
	if parentEnc.KEKMode != "" && b.Encryption.Envelope.Mode() != parentEnc.KEKMode {
		return nil, fmt.Errorf("incremental: envelope mode %q does not match chain's recorded kek_mode %q",
			b.Encryption.Envelope.Mode(), parentEnc.KEKMode)
	}
	mode := parentEnc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	// Bug 179: the chain's encryption mode is authoritative for EVERY
	// segment. --encrypt-mode sets the mode only for a fresh full; an
	// incremental extending an existing chain must use the chain's mode.
	// Refuse an explicit conflicting --encrypt-mode LOUDLY here — otherwise
	// the incremental builds and verifies but is un-restorable, because the
	// restore resolves a single chain mode/CEK from the root full while the
	// sibling resolveChunkCEK would write this segment's chunks under the
	// operator's (mismatched) mode. Inherit it when omitted so
	// resolveChunkCEK agrees with the chain.
	if b.Encryption.Mode != "" && b.Encryption.Mode != mode {
		return nil, fmt.Errorf("incremental: --encrypt-mode=%q conflicts with the chain's encryption mode %q; "+
			"an encrypted chain uses one mode for every segment (omit --encrypt-mode to inherit it, or start a fresh full backup)",
			b.Encryption.Mode, mode)
	}
	b.Encryption.Mode = mode
	if mode == crypto.EncryptModePerChain {
		if len(parentEnc.WrappedCEK) == 0 {
			return nil, errors.New("incremental: parent's chain encryption is per-chain but WrappedCEK is empty")
		}
		// ADR-0152 chokepoint: the chain-root manifest OWNS the wrap —
		// its recorded FormatVersion decides bound-vs-legacy, and the
		// Azure key-version retarget (audit N-9) rides along.
		cek, err := lineage.UnwrapChainCEK(b.Encryption.Envelope, parentEnc.WrappedCEK, rootManifest)
		if err != nil {
			return nil, fmt.Errorf("incremental: unwrap parent chain cek (wrong passphrase?): %w", err)
		}
		return cek, nil
	}
	// Per-chunk mode: retarget the envelope's key version before the
	// probe below / the per-chunk wraps this run performs.
	lineage.RebindEnvelopeKEK(b.Encryption.Envelope, rootManifest)
	// Per-chunk mode: no chain-level CEK to unwrap (each chunk wraps
	// its own CEK). Probe the operator's envelope against one of the
	// parent's existing chunk WrappedCEKs so a rotated passphrase
	// surfaces loudly at incremental start instead of silently
	// extending the chain with chunks wrapped under the new envelope
	// — Bug 117 ingestion-path closure. v0.94.1's VerifyBackupWith
	// closed the symmetric verify path; this closes the ingestion
	// side. If the parent carries no probe-able chunks (e.g. an empty
	// prior incremental window), the probe falls through silently and
	// any rotation surfaces later at restore — same behaviour as
	// pre-fix, no regression introduced.
	if probe := firstPerChunkProbe(parent); probe != nil {
		if err := lineage.ProbeChunkDecrypt(b.Encryption.Envelope, probe); err != nil {
			return nil, fmt.Errorf("incremental: %w", err)
		}
	}
	return nil, nil
}

// firstPerChunkProbe returns the first chunk in the parent manifest
// whose ChunkEncryption.WrappedCEK is non-empty — i.e. an existing
// per-chunk-mode chunk whose CEK can be probe-unwrapped against the
// operator's envelope. Searches Tables[].Chunks first (the full-manifest
// shape, where the chain root holds bulk-copy chunks) then ChangeChunks
// (the incremental-manifest shape). Returns nil when the parent carries
// no probe-able chunks; the caller then falls through without probing.
func firstPerChunkProbe(m *irbackup.Manifest) *irbackup.ChunkInfo {
	if m == nil {
		return nil
	}
	for _, t := range m.Tables {
		if t == nil {
			continue
		}
		for _, c := range t.Chunks {
			if c != nil && c.Encryption != nil && len(c.Encryption.WrappedCEK) > 0 {
				return c
			}
		}
	}
	for _, c := range m.ChangeChunks {
		if c != nil && c.Encryption != nil && len(c.Encryption.WrappedCEK) > 0 {
			return c
		}
	}
	return nil
}

// resolveChunkCEK returns the (cek, wrappedCEK) pair to use for the
// next change chunk. Mirrors [Backup.resolveChunkCEK]; per-chain mode
// reuses chainCEK with empty wrapped value, per-chunk generates a
// fresh CEK + wrap.
func (b *IncrementalBackup) resolveChunkCEK(chainCEK []byte) (cek, wrapped []byte, err error) {
	if b.Encryption == nil {
		return nil, nil, nil
	}
	mode := b.Encryption.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		return chainCEK, nil, nil
	}
	cek, err = crypto.GenerateCEK()
	if err != nil {
		return nil, nil, fmt.Errorf("generate chunk cek: %w", err)
	}
	wrapped, err = b.Encryption.Envelope.WrapCEK(cek)
	if err != nil {
		return nil, nil, fmt.Errorf("wrap chunk cek: %w", err)
	}
	return cek, wrapped, nil
}

// schemaWithRefreshedSequences returns recorded with its standalone
// sequences replaced by after's (item 51): the incremental / rollover
// manifest keeps the before-schema TABLE shape (the SchemaHash
// contract), while the sequence entries — whose positions advance
// with ordinary DML, invisible to the SchemaDelta gate — carry the
// end-of-window read the chain-tail re-prime consumes. Shallow copy;
// neither input is mutated. Returns recorded unchanged when there is
// nothing to swap.
func schemaWithRefreshedSequences(recorded, after *ir.Schema) *ir.Schema {
	if recorded == nil || after == nil {
		return recorded
	}
	if len(recorded.Sequences) == 0 && len(after.Sequences) == 0 {
		return recorded
	}
	cp := *recorded
	cp.Sequences = after.Sequences
	return &cp
}
