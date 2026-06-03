// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Backup-as-broker orchestrator. Phase 4.5 of the logical-backup
// feature (`docs/dev/design-logical-backups-phase-4-5.md`):
// `sluice sync from-backup` is the consumer-side companion to
// Phase 4's `sluice backup stream`. It polls the same chain root the
// stream writes to, detects new incrementals via ParentBackupID
// linkage to the last-applied state, and replays each one into a
// target via the existing [ir.ChangeApplier.ApplyBatch] path.
//
// Shape:
//
//   - Construct [SyncFromBackup] with target engine + DSN + chain
//     store + stream-id + poll interval.
//   - Call [SyncFromBackup.Run] with a context.
//   - The orchestrator drives a `for { tick(); replay(); commit(); }`
//     loop at PollInterval cadence (default 30s).
//   - Each tick: list manifests, filter to those NOT yet applied
//     (via the position recorded in `sluice_cdc_state`), apply each
//     in chain order — schema deltas first, then change chunks
//     through the engine's batched applier.
//   - The position written alongside the data is the broker's
//     synthetic position-shape: `Engine="backup-broker"`,
//     `Token={"chain_url":"...","last_applied_backup_id":"<id>"}`.
//     ADR-0007's transactional position-and-data atomicity makes a
//     broker crash mid-replay safe to re-apply (ADR-0010 idempotent
//     applier). Distinct from `sync start`'s positions (CDC LSN /
//     GTID); the broker's positions reference chain state.
//
// Cooperative stop:
//
//   - ctx cancellation: finishes the current in-flight incremental's
//     batch (the applier's existing channel-closed branch commits
//     the partial batch cleanly) and exits.
//   - Cross-machine stop request via `manifests/broker_state.json`'s
//     `stop_requested_at` field: same drain path. Polled between
//     ticks so the operator's stop is observed within ~PollInterval.
//   - Same-process stop via [RequestSyncFromBackupStop]: closes the
//     in-process channel (registered via [registerBrokerStopChan])
//     for instantaneous observation, no file I/O.
//
// First-start safeguards:
//
//   - Warm resume: `sluice_cdc_state` row for the supplied --stream-id
//     exists → pick up at last_applied_backup_id, replay forward.
//   - Cold start: no row + non-empty target → refuse with an
//     operator-actionable message naming the two recovery flags
//     (--reset-target-data or --at-chain-id=<ID>).
//   - --reset-target-data: drop target tables + run the chain
//     restore through to the chain's current tail, then transition
//     to live polling.
//   - --at-chain-id=<ID>: operator asserts the target is currently
//     at chain ID <ID>; broker writes a fresh sluice_cdc_state row
//     and transitions to live polling from that point.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// BackupBrokerPositionEngine is the sentinel value the broker writes
// into the `position_engine` column (encoded as the
// [ir.Position.Engine] field on per-change positions) so an operator
// inspecting `sluice_cdc_state` can tell at a glance which rows are
// driven by a chain broker rather than live CDC. The value is also
// what same-engine code-paths look for when deciding whether the
// persisted token is a chain reference vs a CDC position.
const BackupBrokerPositionEngine = "backup-broker"

// DefaultBrokerPollInterval is the wall-clock cadence each broker
// tick runs at when [SyncFromBackup.PollInterval] is left zero.
// 30 seconds matches the design doc's default — fast enough that
// continuous-stream → broker latency stays tolerable for staging and
// dev-refresh use cases, slow enough that a quiet chain doesn't
// produce constant `List(manifests/)` calls against blob storage.
const DefaultBrokerPollInterval = 30 * time.Second

// brokerStopPollInterval is the cadence the in-tick stop-signal poll
// runs at when waiting between ticks. Decoupled from PollInterval so
// an operator's `sluice sync from-backup stop` is observed promptly
// regardless of how long the current poll-interval has left to run.
// Mirrors [streamStopPollInterval]'s 1 s cadence.
const brokerStopPollInterval = 1 * time.Second

// SyncFromBackup runs a continuous-replay broker against Target /
// TargetDSN, consuming incrementals written to Store by an upstream
// `sluice backup stream`. Construct the value, then call Run. Run
// blocks until ctx is cancelled or a stop request is observed via
// `broker_state.json`.
//
// SyncFromBackup does not retain state between Run calls. Concurrent
// calls on the same value are not supported.
type SyncFromBackup struct {
	// Target is the engine the target DSN belongs to. Required.
	Target ir.Engine

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// Store is the [ir.BackupStore] the chain lives in. The broker
	// reads manifests + change chunks from it but never writes;
	// brokers are read-only consumers (acceptance criterion 10).
	// Required.
	Store ir.BackupStore

	// ChainURL is the operator-visible URL of the chain store.
	// Recorded in the position token and in log lines so monitoring
	// can correlate broker state with the source destination.
	// Optional — when empty the position token records only the
	// last-applied BackupID.
	ChainURL string

	// StreamID is the per-target identifier the broker uses to key
	// its row in `sluice_cdc_state`. Required for clean restart
	// semantics: a restart picks up at the persisted
	// last_applied_backup_id from this row.
	StreamID string

	// PollInterval bounds the wall-clock cadence each tick runs at.
	// Zero falls back to [DefaultBrokerPollInterval]. Tests use a
	// few-second value to make broker-catches-up assertions fast.
	PollInterval time.Duration

	// ApplyBatchSize is the upper bound on changes per target
	// transaction during incremental replay. Same shape as
	// [ChainRestore.ApplyBatchSize]. Zero falls back to
	// [DefaultChainRestoreBatchSize] (100).
	ApplyBatchSize int

	// MaxBufferBytes is the soft byte cap on per-batch buffered
	// memory. Same semantics as [Migrator.MaxBufferBytes].
	MaxBufferBytes int64

	// ResetTargetData, when true, runs a [ChainRestore] internally
	// before transitioning to live polling. Matches `migrate`'s
	// `--reset-target-data` shape: drops target tables, applies the
	// chain's full + every incremental up to the current tail, then
	// switches to incremental-by-incremental polling. Mutually
	// exclusive with [AtChainID].
	ResetTargetData bool

	// AtChainID, when non-empty, is the operator's assertion that
	// the target is currently at chain ID <AtChainID>. The broker
	// writes a fresh `sluice_cdc_state` row carrying this BackupID
	// and transitions to live polling from there. Mutually exclusive
	// with [ResetTargetData].
	AtChainID string

	// SluiceVersion is the build identifier of the running binary.
	// Recorded on log lines for diagnostics. Optional.
	SluiceVersion string

	// Envelope, when non-nil, is the [crypto.EnvelopeEncryption] used
	// to unwrap CEKs from encrypted manifests. Required when the chain
	// the broker is consuming carries [ir.ChainEncryption]. A nil
	// Envelope against an encrypted chain produces a clear refusal at
	// chain-walk time naming the missing key.
	Envelope crypto.EnvelopeEncryption

	// chainCEK caches the unwrapped per-chain CEK across ticks so
	// Argon2id (passphrase mode) runs once per broker process.
	chainCEK []byte

	// Now, when set, overrides the wall-clock-time source used for
	// `broker_state.json` timestamps. Tests pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time

	// brokerStatePath overrides the path of the liveness file. Tests
	// that exercise the file shape pin a deterministic path; in
	// production callers leave it empty and the default uses
	// [DefaultBrokerStateFilename].
	brokerStatePath string

	// pidHostFn returns the (pid, host) pair recorded on the liveness
	// file. Defaults to (os.Getpid, os.Hostname); tests inject a stub.
	pidHostFn func() (int, string)
}

// brokerPositionToken is the JSON shape encoded into
// [ir.Position.Token] for broker-driven rows in `sluice_cdc_state`.
// The shape is small + stable + JSON so an operator inspecting the
// control table by hand can read it; future fields (chain_format,
// schema_hash) are forward-compatible additions.
//
// Bug 39 fix (v0.20.1): the embedded `_engine` field is the broker's
// self-identifier — it round-trips through `sluice_cdc_state.source_position`
// even though the engine appliers' [ir.ChangeApplier.ReadPosition]
// implementations hard-code their own engine name into the returned
// [ir.Position.Engine]. Without this field, a broker restart against
// its own previously-written row was rejected as "non-broker writer"
// because Position.Engine came back as "postgres" / "mysql" instead
// of [BackupBrokerPositionEngine]. The envelope choice (over a DDL
// migration on `sluice_cdc_state`) is backward-compatible: legacy
// rows lacking `_engine` parse with the field empty and are correctly
// treated as non-broker.
type brokerPositionToken struct {
	Engine              string `json:"_engine,omitempty"`
	ChainURL            string `json:"chain_url,omitempty"`
	LastAppliedBackupID string `json:"last_applied_backup_id"`
}

// encodeBrokerPosition produces the [ir.Position] the broker writes
// alongside data writes during incremental replay. The Engine field
// is [BackupBrokerPositionEngine] so a future broker run can detect
// "this row was written by a broker, not a live CDC stream" without
// parsing the token. The same sentinel is also embedded in the JSON
// token (`_engine` field) so it survives the engine appliers'
// hard-coded engine-name discard on read (Bug 39 round-trip fix).
func encodeBrokerPosition(chainURL, backupID string) ir.Position {
	tok := brokerPositionToken{
		Engine:              BackupBrokerPositionEngine,
		ChainURL:            chainURL,
		LastAppliedBackupID: backupID,
	}
	body, _ := json.Marshal(tok)
	return ir.Position{
		Engine: BackupBrokerPositionEngine,
		Token:  string(body),
	}
}

// decodeBrokerPosition parses a position token written by
// [encodeBrokerPosition]. Returns (nil, error) when the token isn't
// JSON-shaped or doesn't carry the broker sentinel — a non-broker
// position written into the same row by some other code path.
// Callers handle that as "no broker state for this stream-id" and
// fall through to the cold-start branch.
//
// Bug 39 fix (v0.20.1): the discriminator is the embedded `_engine`
// field, NOT [ir.Position.Engine]. The latter is set by the engine
// applier's ReadPosition (always "postgres" / "mysql") and discards
// the broker's sentinel; the former survives the round-trip. Callers
// that want to ask "is this a broker row?" should call
// [isBrokerToken] rather than reading Position.Engine.
func decodeBrokerPosition(pos ir.Position) (*brokerPositionToken, error) {
	if pos.Token == "" {
		return nil, errors.New("broker: position token is empty")
	}
	var tok brokerPositionToken
	if err := json.Unmarshal([]byte(pos.Token), &tok); err != nil {
		return nil, fmt.Errorf("broker: decode position token: %w", err)
	}
	if tok.Engine != BackupBrokerPositionEngine {
		return nil, fmt.Errorf(
			"broker: token's _engine field is %q, not %q",
			tok.Engine, BackupBrokerPositionEngine,
		)
	}
	return &tok, nil
}

// isBrokerToken reports whether a persisted position token (read from
// `sluice_cdc_state.source_position` via [ir.ChangeApplier.ReadPosition])
// was written by a broker. It probes the token's embedded `_engine`
// field rather than [ir.Position.Engine], which the engine appliers
// stomp on with their own engine name. Returns false on JSON-decode
// failure (non-broker writers — live CDC — write opaque tokens that
// are typically NOT JSON envelopes; PG slots use a JSON envelope but
// without the `_engine` field).
func isBrokerToken(pos ir.Position) bool {
	if pos.Token == "" {
		return false
	}
	var tok brokerPositionToken
	if err := json.Unmarshal([]byte(pos.Token), &tok); err != nil {
		return false
	}
	return tok.Engine == BackupBrokerPositionEngine
}

// Run executes the long-running broker. Blocks until ctx is cancelled
// or a stop request is observed via `broker_state.json`. Returns nil
// on a clean exit; a wrapped error on any unrecoverable failure.
//
// On every successful incremental apply: the target gains the
// incremental's data + schema deltas, the broker's position row in
// `sluice_cdc_state` advances to that incremental's BackupID, and
// `broker_state.json` is updated with the apply timestamp.
func (b *SyncFromBackup) Run(ctx context.Context) error {
	if err := b.validate(); err != nil {
		return err
	}

	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	pollInterval := b.PollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultBrokerPollInterval
	}
	statePath := b.brokerStatePath
	if statePath == "" {
		statePath = DefaultBrokerStateFilename
	}
	pidHost := b.pidHostFn
	if pidHost == nil {
		pidHost = defaultPidHost
	}
	pid, host := pidHost()

	// 1. Open the applier and ensure its control table exists.
	applier, err := b.Target.OpenChangeApplier(ctx, b.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("broker: open target change applier: %w", err))
	}
	defer closeIf(applier)
	applyMaxBufferBytes(applier, b.MaxBufferBytes)
	if err := applier.EnsureControlTable(ctx); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("broker: ensure control table: %w", err))
	}

	// 2. Check existing position. Three branches downstream:
	//   - row exists + parses as broker token → warm resume.
	//   - row absent → cold start (refusal unless override flag).
	//   - row exists but isn't a broker token → conflict; refuse
	//     loudly (`stream_id` is being driven by a `sync start`,
	//     not a chain broker — overwriting would corrupt the live
	//     stream's resume state).
	persisted, found, err := applier.ReadPosition(ctx, b.StreamID)
	if err != nil {
		return wrapWithHint(PhaseCDC, fmt.Errorf("broker: read position: %w", err))
	}

	// Bug 39 fix (v0.20.1): identify broker-owned rows via the
	// embedded `_engine` JSON field, NOT [ir.Position.Engine]. The
	// engine appliers' ReadPosition discards the broker's sentinel
	// and returns its own engine name; the JSON envelope round-trips
	// the sentinel intact. See [isBrokerToken] for the discriminator.
	var lastAppliedID string
	switch {
	case found && isBrokerToken(persisted):
		tok, dErr := decodeBrokerPosition(persisted)
		if dErr != nil {
			return fmt.Errorf("broker: corrupt persisted position for stream %q: %w; clear the row manually or pass --reset-target-data",
				b.StreamID, dErr)
		}
		lastAppliedID = tok.LastAppliedBackupID
		slog.InfoContext(
			ctx, "broker: warm resume",
			slog.String("stream_id", b.StreamID),
			slog.String("last_applied_backup_id", lastAppliedID),
		)
	case found:
		return fmt.Errorf(
			"broker: stream %q is owned by a non-broker writer (position engine %q); "+
				"choose a different --stream-id or clear the conflicting row first",
			b.StreamID, persisted.Engine,
		)
	default:
		// Cold-start branch.
		startID, err := b.coldStart(ctx, applier)
		if err != nil {
			return err
		}
		lastAppliedID = startID
	}

	// 3. Initial state file write. The first heartbeat happens
	//    immediately so an operator running `sync from-backup stop`
	//    against a freshly-launched broker has something to clobber.
	state := &brokerState{
		PID:         pid,
		Host:        host,
		StreamID:    b.StreamID,
		StartedAt:   now().UTC(),
		LastApplyAt: now().UTC(),
	}
	if err := writeBrokerState(ctx, b.Store, statePath, state); err != nil {
		return fmt.Errorf("broker: write initial state: %w", err)
	}

	// 4. Register the in-process stop channel so a same-process
	//    RequestSyncFromBackupStop can signal us instantaneously.
	//    Cross-process operators take the file-poll path inside
	//    waitForNextTick; notifyBrokerStop is a no-op for them.
	stopCh, deregisterStopCh := registerBrokerStopChan(b.Store)
	defer deregisterStopCh()

	slog.InfoContext(
		ctx, "broker: started",
		slog.String("stream_id", b.StreamID),
		slog.String("chain_url", b.ChainURL),
		slog.String("target_engine", b.Target.Name()),
		slog.Duration("poll_interval", pollInterval),
		slog.String("last_applied_backup_id", lastAppliedID),
	)

	// Phase 5.4: cross-engine broker detection. When the chain's
	// source engine differs from the target's engine, the chain's
	// terminal EndPosition is engine-specific (PG: {slot,lsn}; MySQL:
	// GTID set) and cannot be translated to the target's CDC
	// position-shape. The broker still writes its own
	// `_engine="backup-broker"` envelope to `sluice_cdc_state` for
	// warm resume, but the chain-source-engine-flavored EndPosition
	// is intentionally omitted. Operators continuing CDC from a
	// cross-engine restored target run a fresh `sluice sync start`
	// against the source's native engine, or use --at-chain-id for
	// resumption assertions.
	chainSourceEngine := b.detectChainSourceEngine(ctx)
	if chainSourceEngine != "" && chainSourceEngine != b.Target.Name() {
		slog.InfoContext(
			ctx, "broker: cross-engine chain — chain's EndPosition not written to sluice_cdc_state; use --at-chain-id for cross-engine resumption assertions",
			slog.String("stream_id", b.StreamID),
			slog.String("chain_source_engine", chainSourceEngine),
			slog.String("target_engine", b.Target.Name()),
		)
	}

	// Phase 6.1: chain-encryption preflight. When the chain root
	// carries [ir.ChainEncryption], the broker must have an envelope
	// + the unwrapped chain CEK ready (per-chain mode) before the
	// first tick attempts to decrypt a change chunk. preflightChainEncryption
	// is a no-op for plaintext chains.
	if err := b.preflightChainEncryption(ctx); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("broker: %w", err))
	}

	// 5. Drive the tick loop. Each tick: list manifests, replay any
	//    new ones in chain order, advance lastAppliedID, sleep
	//    until next interval (or stop signal). The first iteration
	//    runs immediately so a freshly-launched broker against a
	//    chain with pending incrementals catches up without waiting
	//    a full PollInterval.
	for {
		// Stop-request check (cross-machine + in-process). A
		// ctx-cancel here also short-circuits via the same path.
		exit, sErr := b.checkStopSignals(ctx, statePath, stopCh)
		if sErr != nil {
			slog.WarnContext(
				ctx, "broker: failed to read broker_state for stop check; will retry on next tick",
				slog.String("err", sErr.Error()),
			)
		}
		if exit {
			slog.InfoContext(
				ctx, "broker: stop requested; exiting",
				slog.String("stream_id", b.StreamID),
				slog.String("last_applied_backup_id", lastAppliedID),
			)
			return nil
		}

		started := now()
		newApplied, totalBytes, applyErr := b.replayNewIncrementals(ctx, applier, lastAppliedID)
		elapsed := now().Sub(started)

		if applyErr != nil {
			if errors.Is(applyErr, context.Canceled) || errors.Is(applyErr, context.DeadlineExceeded) {
				slog.InfoContext(
					ctx, "broker: context cancelled; exiting",
					slog.String("stream_id", b.StreamID),
					slog.String("last_applied_backup_id", lastAppliedID),
				)
				return nil
			}
			return wrapWithHint(PhaseCDC, fmt.Errorf("broker: tick: %w", applyErr))
		}

		// Advance in-memory cursor only on successful applies. The
		// applier already wrote the persisted position
		// transactionally with the data; an outer crash here is
		// safe because the next Run reads the persisted position
		// back at startup.
		if newApplied != "" {
			lastAppliedID = newApplied
		}

		slog.InfoContext(
			ctx, "broker tick",
			slog.String("stream_id", b.StreamID),
			slog.String("last_applied_backup_id", lastAppliedID),
			slog.Int64("bytes_replayed", totalBytes),
			slog.Duration("elapsed", elapsed),
		)

		// Heartbeat update — preserves any operator-written
		// stop_requested_at via the merge helper.
		state.LastApplyAt = now().UTC()
		stopObserved, hbErr := writeBrokerStateMergeHeartbeat(ctx, b.Store, statePath, state)
		if hbErr != nil {
			slog.WarnContext(
				ctx, "broker: failed to update state file after tick",
				slog.String("err", hbErr.Error()),
			)
		}
		if stopObserved {
			slog.InfoContext(
				ctx, "broker: heartbeat merge observed concurrent stop_requested_at; exiting",
				slog.String("stream_id", b.StreamID),
				slog.String("last_applied_backup_id", lastAppliedID),
			)
			return nil
		}

		// Sleep until the next interval, observing both ctx cancel
		// and the in-process stop channel along the way.
		if exit := b.waitForNextTick(ctx, pollInterval, statePath, stopCh); exit {
			slog.InfoContext(
				ctx, "broker: stop requested during tick wait; exiting",
				slog.String("stream_id", b.StreamID),
				slog.String("last_applied_backup_id", lastAppliedID),
			)
			return nil
		}
	}
}

// detectChainSourceEngine returns the SourceEngine recorded in the
// chain's full-backup manifest, or "" when the chain can't be read
// (the broker's tick loop will surface its own error on the first
// pass; this helper is best-effort metadata for the cross-engine
// log line).
func (b *SyncFromBackup) detectChainSourceEngine(ctx context.Context) string {
	chain, err := buildBrokerChain(ctx, b.Store)
	if err != nil || len(chain) == 0 {
		return ""
	}
	return chain[0].manifest.SourceEngine
}

// validate sanity-checks required fields.
func (b *SyncFromBackup) validate() error {
	switch {
	case b.Target == nil:
		return errors.New("broker: Target engine is nil")
	case b.TargetDSN == "":
		return errors.New("broker: TargetDSN is empty")
	case b.Store == nil:
		return errors.New("broker: Store is nil")
	case b.StreamID == "":
		return errors.New("broker: StreamID is empty (a stable identifier is required for restart resume)")
	}
	if b.ResetTargetData && b.AtChainID != "" {
		return errors.New("broker: --reset-target-data and --at-chain-id are mutually exclusive")
	}
	return nil
}

// coldStart handles the no-existing-state branch. Three sub-shapes:
//
//   - --reset-target-data: drop tables + run ChainRestore + record
//     the chain's tail BackupID in the broker's position row.
//   - --at-chain-id=<ID>: operator-asserted resumption; record <ID>
//     as last_applied_backup_id without any data work.
//   - neither flag: refuse with an operator-actionable message.
//
// On success, returns the BackupID the broker should treat as the
// floor of "already applied" — i.e. the next tick's
// `replayNewIncrementals` will skip up to and including this ID.
func (b *SyncFromBackup) coldStart(ctx context.Context, applier ir.ChangeApplier) (string, error) {
	switch {
	case b.ResetTargetData:
		slog.InfoContext(
			ctx, "broker: cold start with --reset-target-data; running ChainRestore",
			slog.String("stream_id", b.StreamID),
		)
		return b.coldStartReset(ctx, applier)
	case b.AtChainID != "":
		slog.InfoContext(
			ctx, "broker: cold start with --at-chain-id assertion",
			slog.String("stream_id", b.StreamID),
			slog.String("at_chain_id", b.AtChainID),
		)
		return b.coldStartAtChainID(ctx, applier)
	}
	// Neither override flag set: operator-actionable refusal.
	return "", fmt.Errorf(
		"broker: no `sluice_cdc_state` row for stream %q on target; "+
			"either pass --reset-target-data to drop the target's data and replay the entire chain, "+
			"or pass --at-chain-id=<BACKUP-ID> after manually restoring the chain into the target. "+
			"Refusing to start without an override prevents silent target overwrites (mirrors `migrate --force-cold-start`)",
		b.StreamID,
	)
}

// coldStartReset runs an inline ChainRestore to land the chain's full
// + every incremental currently in the store, then records the
// chain's tail BackupID as the broker's resume floor.
//
// Bug 40 fix (v0.20.1): before invoking ChainRestore, drop every
// table named in the chain's terminal manifest schema. ChainRestore's
// schema-application path uses CREATE TABLE IF NOT EXISTS, which
// no-ops against pre-existing tables — so a target carrying a stale
// schema (e.g. previous cycle's `(id, email)` shape vs. the chain's
// `(id, email, created_at)` shape) would silently keep the old
// columns and the subsequent COPY would fail with "column does not
// exist". Mirror `migrate --reset-target-data`'s drop-loop pattern.
func (b *SyncFromBackup) coldStartReset(ctx context.Context, applier ir.ChangeApplier) (string, error) {
	chain, err := buildBrokerChain(ctx, b.Store)
	if err != nil {
		return "", wrapWithHint(PhaseConnect, fmt.Errorf("broker: build chain: %w", err))
	}
	if len(chain) == 0 {
		return "", errors.New("broker: chain is empty; cannot --reset-target-data with no full backup in store")
	}
	tailManifest := chain[len(chain)-1].manifest

	// Bug 40a fix: drop pre-existing target tables that match the
	// chain's terminal schema. ChainRestore's CREATE TABLE IF NOT
	// EXISTS would otherwise no-op against stale-schema tables and
	// trigger a "column does not exist" error in the subsequent COPY.
	if tailManifest.Schema != nil && len(tailManifest.Schema.Tables) > 0 {
		if err := b.dropExistingTargetTables(ctx, tailManifest.Schema); err != nil {
			return "", err
		}
	}

	rest := &ChainRestore{
		Target:         b.Target,
		TargetDSN:      b.TargetDSN,
		Store:          b.Store,
		MaxBufferBytes: b.MaxBufferBytes,
		ApplyBatchSize: b.ApplyBatchSize,
		Envelope:       b.Envelope,
	}
	if err := rest.Run(ctx); err != nil {
		return "", fmt.Errorf("broker: chain restore failed: %w", err)
	}
	tailID := manifestBackupID(tailManifest)
	if err := b.writePositionDirect(ctx, applier, tailID); err != nil {
		return "", fmt.Errorf("broker: record post-restore position: %w", err)
	}
	slog.InfoContext(
		ctx, "broker: cold start complete; transitioning to live polling",
		slog.String("stream_id", b.StreamID),
		slog.String("tail_backup_id", tailID),
		slog.Int("chain_length", len(chain)),
	)
	return tailID, nil
}

// dropExistingTargetTables drops every table named in schema on the
// target via the engine's [ir.TableDropper] / [ir.BulkTableDropper]
// surfaces. Reuses the same drop-loop pattern as
// `migrate --reset-target-data` (see [dropTables] in reset.go) so a
// target with a stale-schema table is wiped clean before
// ChainRestore's CREATE-IF-NOT-EXISTS path runs. Bug 40 fix.
func (b *SyncFromBackup) dropExistingTargetTables(ctx context.Context, schema *ir.Schema) error {
	rw, err := b.Target.OpenRowWriter(ctx, b.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect,
			fmt.Errorf("broker: --reset-target-data: open row writer: %w", err))
	}
	defer closeIf(rw)
	dropper, ok := rw.(ir.TableDropper)
	if !ok {
		return fmt.Errorf(
			"broker: --reset-target-data: target engine %q does not expose ir.TableDropper; "+
				"drop dest tables manually before re-running",
			b.Target.Name(),
		)
	}
	if err := dropTables(ctx, dropper, schema.Tables); err != nil {
		return err
	}
	if err := dropSchemaTypes(ctx, rw, schema); err != nil {
		return err
	}
	slog.InfoContext(
		ctx, "broker: --reset-target-data: target tables dropped before chain restore",
		slog.String("stream_id", b.StreamID),
		slog.Int("tables_dropped", len(schema.Tables)),
	)
	return nil
}

// coldStartAtChainID handles the operator-asserted resumption path.
// Validates that the asserted ID exists in the chain (typo
// protection) and writes the broker's position row.
func (b *SyncFromBackup) coldStartAtChainID(ctx context.Context, applier ir.ChangeApplier) (string, error) {
	chain, err := buildBrokerChain(ctx, b.Store)
	if err != nil {
		return "", wrapWithHint(PhaseConnect, fmt.Errorf("broker: build chain: %w", err))
	}
	found := false
	for _, link := range chain {
		if manifestBackupID(link.manifest) == b.AtChainID {
			found = true
			break
		}
	}
	if !found {
		ids := make([]string, 0, len(chain))
		for _, link := range chain {
			ids = append(ids, manifestBackupID(link.manifest))
		}
		return "", fmt.Errorf(
			"broker: --at-chain-id=%q not found in chain (available: %s)",
			b.AtChainID, strings.Join(ids, ", "),
		)
	}
	if err := b.writePositionDirect(ctx, applier, b.AtChainID); err != nil {
		return "", fmt.Errorf("broker: record at-chain-id position: %w", err)
	}
	return b.AtChainID, nil
}

// writePositionDirect inserts a position row for the broker via a
// synthetic single-event apply call. Used by the cold-start paths
// where there's no incremental data to replay but we still need the
// `sluice_cdc_state` row to exist before the tick loop starts.
//
// Implementation: the engine appliers' [ir.ChangeApplier.Apply] /
// [ir.BatchedChangeApplier.ApplyBatch] write the position alongside
// data inside a single transaction. To write JUST a position without
// a row event, we'd need a dedicated surface — which the IR doesn't
// expose. Workaround: call ApplyBatch with a channel carrying only a
// TxBegin + TxCommit pair. Both are no-op events for the applier
// (per ADR-0027 and the existing Postgres / MySQL implementations),
// so no data writes happen. But the applier's batched path opens a
// tx on the FIRST row event; an empty (begin/commit only) batch
// never opens a tx and therefore never writes a position. So we
// cannot use the existing applier interface for "position-only
// write".
//
// Instead, we invoke the engine's optional [ir.PositionWriter]
// surface. Postgres and MySQL implement it as of v0.20.0; the
// surface delegates to the same `writePositionTx` helper the apply
// path uses, so the contract is identical.
func (b *SyncFromBackup) writePositionDirect(ctx context.Context, applier ir.ChangeApplier, backupID string) error {
	pw, ok := applier.(ir.PositionWriter)
	if !ok {
		return fmt.Errorf(
			"broker: target engine %q does not expose ir.PositionWriter; cannot record cold-start position. "+
				"Wait until at least one incremental is available to apply, or use a target engine that supports the surface (postgres / mysql ≥ v0.20.0)",
			b.Target.Name(),
		)
	}
	pos := encodeBrokerPosition(b.ChainURL, backupID)
	if err := pw.WritePosition(ctx, b.StreamID, pos); err != nil {
		return fmt.Errorf("broker: write position: %w", err)
	}
	return nil
}

// replayNewIncrementals lists every manifest in the chain, identifies
// any incrementals NOT yet applied (relative to lastAppliedID), and
// applies them in chain order. Returns the BackupID of the last
// incremental applied (empty if no new ones), the total bytes
// replayed across all chunks, and any fatal error.
//
// "Not yet applied" semantics: an incremental is new if it appears
// in the chain AFTER lastAppliedID, where chain order is determined
// by the linked-list walk in [buildChain].
func (b *SyncFromBackup) replayNewIncrementals(
	ctx context.Context,
	applier ir.ChangeApplier,
	lastAppliedID string,
) (newApplied string, totalBytes int64, err error) {
	chain, err := buildBrokerChain(ctx, b.Store)
	if err != nil {
		return "", 0, fmt.Errorf("build chain: %w", err)
	}
	slog.DebugContext(
		ctx, "broker: replay tick chain snapshot",
		slog.String("stream_id", b.StreamID),
		slog.String("last_applied", lastAppliedID),
		slog.Int("chain_len", len(chain)),
	)
	if len(chain) == 0 {
		return "", 0, nil
	}

	// Find lastAppliedID's position in the chain. Everything after
	// it is the unapplied tail.
	startIdx := 0
	if lastAppliedID != "" {
		found := false
		for i, link := range chain {
			if manifestBackupID(link.manifest) == lastAppliedID {
				startIdx = i + 1
				found = true
				break
			}
		}
		if !found {
			return "", 0, fmt.Errorf(
				"broker: last_applied_backup_id %q not found in chain; "+
					"the chain may have been re-rooted on the source side. Operator action: "+
					"clear the broker's `sluice_cdc_state` row and re-run with --reset-target-data or --at-chain-id",
				lastAppliedID,
			)
		}
	}
	if startIdx >= len(chain) {
		// No new incrementals.
		return "", 0, nil
	}

	batchSize := b.ApplyBatchSize
	if batchSize <= 0 {
		batchSize = DefaultChainRestoreBatchSize
	}

	for i := startIdx; i < len(chain); i++ {
		// ctx-cancel between incrementals: surface so Run returns
		// cleanly without applying further incrementals. The
		// just-applied incremental's position is durable on the
		// target via the in-batch position write.
		if err := ctx.Err(); err != nil {
			return newApplied, totalBytes, err
		}
		link := &chain[i]
		// Skip the full's manifest (i==0) when no last-applied is
		// set; that case shouldn't happen on the warm-resume path
		// but is harmless to guard.
		if link.manifest.Kind == ir.BackupKindFull || link.manifest.Kind == "" {
			continue
		}
		bytesApplied, applyErr := b.applyIncremental(ctx, applier, link, batchSize)
		if applyErr != nil {
			return newApplied, totalBytes, fmt.Errorf("incremental %s: %w",
				manifestBackupID(link.manifest), applyErr)
		}
		newApplied = manifestBackupID(link.manifest)
		totalBytes += bytesApplied
		slog.InfoContext(
			ctx, "broker: incremental applied",
			slog.String("stream_id", b.StreamID),
			slog.String("backup_id", newApplied),
			slog.Int64("bytes", bytesApplied),
		)
	}
	return newApplied, totalBytes, nil
}

// applyIncremental replays one incremental's schema deltas + change
// chunks into the target. Mirrors [ChainRestore.applyIncremental] but
// rewrites each change's position to the broker's synthetic position-
// shape so the applier writes broker state alongside data.
//
// Returns the approximate bytes consumed from chunk files (the file
// count + per-chunk RowCount feed the broker's tick log line) and
// any fatal error.
func (b *SyncFromBackup) applyIncremental(
	ctx context.Context,
	applier ir.ChangeApplier,
	link *segmentRecord,
	batchSize int,
) (int64, error) {
	// 1. Schema deltas first.
	if len(link.manifest.SchemaDelta) > 0 {
		if err := b.applySchemaDeltas(ctx, link); err != nil {
			return 0, fmt.Errorf("apply schema deltas: %w", err)
		}
	}

	// 2. If the incremental has no chunks (schema-delta-only), we
	//    still need the broker's position to advance. Use the direct
	//    position-writer path; the schema-delta apply above is
	//    idempotent so a re-replay on broker crash is safe.
	backupID := manifestBackupID(link.manifest)
	if len(link.manifest.ChangeChunks) == 0 {
		if err := b.writePositionDirect(ctx, applier, backupID); err != nil {
			return 0, fmt.Errorf("write position for empty incremental: %w", err)
		}
		return 0, nil
	}

	// 3. Stream the change chunks through the applier with each
	//    change's position rewritten to the broker shape.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	changesCh := make(chan ir.Change)
	errCh := make(chan error, 1)
	pos := encodeBrokerPosition(b.ChainURL, backupID)
	go func() {
		defer close(changesCh)
		errCh <- b.streamIncrementalWithPosition(streamCtx, link, pos, changesCh)
	}()

	if batched, ok := applier.(ir.BatchedChangeApplier); ok {
		if err := batched.ApplyBatch(ctx, b.StreamID, changesCh, batchSize); err != nil {
			streamCancel()
			<-errCh
			return 0, fmt.Errorf("apply changes (batched): %w", err)
		}
	} else {
		if err := applier.Apply(ctx, b.StreamID, changesCh); err != nil {
			streamCancel()
			<-errCh
			return 0, fmt.Errorf("apply changes: %w", err)
		}
	}
	if err := <-errCh; err != nil {
		return 0, fmt.Errorf("stream chunks: %w", err)
	}

	// 4. Some appliers may not emit a position write when the batch
	//    contains only TxBegin/TxCommit boundary events (e.g. a
	//    chunk that's a single empty transaction). Defensive write
	//    here ensures the broker's position advances regardless.
	if err := b.writePositionDirect(ctx, applier, backupID); err != nil {
		return 0, fmt.Errorf("finalise position: %w", err)
	}

	// Approximate bytes = sum of chunk RowCount (rows isn't bytes
	// but is a proxy that doesn't require re-reading the chunk for
	// length; the tick log line is informational).
	var rows int64
	for _, c := range link.manifest.ChangeChunks {
		rows += c.RowCount
	}
	return rows, nil
}

// applySchemaDeltas applies the incremental's SchemaDelta entries via
// the engine's optional [ir.SchemaDeltaApplier] surface. Mirrors
// [ChainRestore.applySchemaDeltas]'s strategy: AddTable creates new
// tables (no rows yet), AlterTable emits ADD COLUMN for added
// columns, DropTable is a no-op for v1.
//
// Cross-engine (Phase 5): when the chain's source engine differs from
// the broker's target engine, the After-shape of each delta is routed
// through [translate.RetargetForEngine] before invoking the schema
// writer / [ir.SchemaDeltaApplier]. Mirrors the chain-restore path.
//
// Implementation duplicates the chain-restore logic intentionally
// rather than refactoring chain_restore.go (Tenet: don't refactor
// Phase 4 surfaces beyond what 4.5 requires).
func (b *SyncFromBackup) applySchemaDeltas(ctx context.Context, link *segmentRecord) error {
	if err := detectAmbiguousDeltas(link.manifest.SchemaDelta); err != nil {
		return fmt.Errorf(
			"unsupportable schema delta in incremental %s: %w. "+
				"Force a fresh full + new chain to recover",
			manifestBackupID(link.manifest), err,
		)
	}

	sourceEngine := link.manifest.SourceEngine
	targetEngine := b.Target.Name()
	if err := checkCrossEngineDeltaSupportable(
		link.manifest.SchemaDelta, sourceEngine, targetEngine,
		manifestBackupID(link.manifest),
	); err != nil {
		return err
	}

	sw, err := b.Target.OpenSchemaWriter(ctx, b.TargetDSN)
	if err != nil {
		return fmt.Errorf("open schema writer: %w", err)
	}
	defer closeIf(sw)

	deltaApplier, _ := sw.(ir.SchemaDeltaApplier)
	for _, d := range link.manifest.SchemaDelta {
		switch d.Kind {
		case ir.SchemaDeltaAddTable:
			if d.After == nil {
				continue
			}
			retargeted := translate.RetargetForEngine(
				&ir.Schema{Tables: []*ir.Table{d.After}},
				sourceEngine, targetEngine,
			)
			if err := sw.CreateTablesWithoutConstraints(ctx, retargeted); err != nil {
				return fmt.Errorf("create added table %s: %w", d.Table, err)
			}
			slog.InfoContext(
				ctx, "broker: schema delta — added table",
				slog.String("stream_id", b.StreamID),
				slog.String("table", d.Table),
			)
		case ir.SchemaDeltaAlterTable:
			if d.Before == nil || d.After == nil {
				continue
			}
			added := addedColumns(d.Before, d.After)
			if len(added) == 0 {
				continue
			}
			if deltaApplier == nil {
				slog.WarnContext(
					ctx, "broker: schema delta — altered table with added columns; engine has no SchemaDeltaApplier",
					slog.String("stream_id", b.StreamID),
					slog.String("table", d.Table),
					slog.Int("added_columns", len(added)),
				)
				continue
			}
			retargetedSchema := translate.RetargetForEngine(
				&ir.Schema{Tables: []*ir.Table{d.After}},
				sourceEngine, targetEngine,
			)
			retargetedTable := retargetedSchema.Tables[0]
			retargetedAdded := addedColumns(d.Before, retargetedTable)
			if err := deltaApplier.AlterAddColumn(ctx, retargetedTable, retargetedAdded); err != nil {
				return fmt.Errorf("alter add column on %s: %w", d.Table, err)
			}
			slog.InfoContext(
				ctx, "broker: schema delta — applied ADD COLUMN",
				slog.String("stream_id", b.StreamID),
				slog.String("table", d.Table),
				slog.Int("added_columns", len(added)),
			)
		case ir.SchemaDeltaDropTable:
			slog.WarnContext(
				ctx, "broker: schema delta — drop ignored (v1 does not auto-DROP)",
				slog.String("stream_id", b.StreamID),
				slog.String("table", d.Table),
			)
		default:
			return fmt.Errorf("unknown schema delta kind %q on table %q", d.Kind, d.Table)
		}
	}
	return nil
}

// streamIncrementalWithPosition reads each chunk and pushes the
// events onto out, rewriting every change's [ir.Position] to pos so
// the applier records the broker's chain-state token rather than the
// source's CDC token.
func (b *SyncFromBackup) streamIncrementalWithPosition(
	ctx context.Context,
	link *segmentRecord,
	pos ir.Position,
	out chan<- ir.Change,
) error {
	segStore := link.segment.store(b.Store)
	codec := link.segment.codecOrDefault()
	for chunkIdx, chunk := range link.manifest.ChangeChunks {
		if err := b.streamOneChunkWithPosition(ctx, segStore, codec, chunk, pos, out); err != nil {
			return fmt.Errorf("chunk %d (%s): %w", chunkIdx, chunk.File, err)
		}
	}
	return nil
}

// streamOneChunkWithPosition reads one chunk's events and pushes them
// onto out with each change's Position field rewritten to pos.
// segStore/codec come from the chunk's segment (recorded, not sniffed).
func (b *SyncFromBackup) streamOneChunkWithPosition(
	ctx context.Context,
	segStore ir.BackupStore,
	codec Codec,
	chunk *ir.ChunkInfo,
	pos ir.Position,
	out chan<- ir.Change,
) error {
	src, err := segStore.Get(ctx, chunk.File)
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	cek, err := b.chunkCEK(chunk)
	if err != nil {
		_ = src.Close()
		return fmt.Errorf("resolve chunk cek: %w", err)
	}
	cr, err := newChangeChunkReader(src, chunk.SHA256, cek, codec)
	if err != nil {
		return fmt.Errorf("open chunk reader: %w", err)
	}
	for {
		change, rErr := cr.ReadChange()
		if errors.Is(rErr, io.EOF) {
			break
		}
		if rErr != nil {
			_ = cr.Close()
			return fmt.Errorf("read change: %w", rErr)
		}
		select {
		case <-ctx.Done():
			_ = cr.Close()
			return ctx.Err()
		case out <- rewritePosition(change, pos):
		}
	}
	return cr.Close()
}

// rewritePosition returns a copy of c with its [ir.Position] field
// replaced by pos. Insert / Update / Delete / Truncate / TxBegin /
// TxCommit are all sealed-interface variants in the IR; we
// type-switch over the concrete shapes.
func rewritePosition(c ir.Change, pos ir.Position) ir.Change {
	switch v := c.(type) {
	case ir.Insert:
		v.Position = pos
		return v
	case ir.Update:
		v.Position = pos
		return v
	case ir.Delete:
		v.Position = pos
		return v
	case ir.Truncate:
		v.Position = pos
		return v
	case ir.TxBegin:
		v.Position = pos
		return v
	case ir.TxCommit:
		v.Position = pos
		return v
	}
	// Unknown shape — return as-is. Future IR additions land here
	// with their own position untouched; the broker's position
	// advance still happens via the post-replay direct write.
	return c
}

// checkStopSignals returns (true, nil) when either the in-process
// stop channel is closed OR the file's stop_requested_at field is
// set. Used at the top of each tick to exit promptly without doing
// the work of a tick that's about to be discarded.
func (b *SyncFromBackup) checkStopSignals(
	ctx context.Context,
	statePath string,
	stopCh <-chan struct{},
) (exit bool, err error) {
	select {
	case <-ctx.Done():
		return true, nil
	case <-stopCh:
		return true, nil
	default:
	}
	req, sErr := readBrokerStopRequested(ctx, b.Store, statePath)
	if sErr != nil {
		return false, sErr
	}
	return req != nil, nil
}

// waitForNextTick sleeps until the next tick should run, observing
// ctx-cancel, the in-process stop channel, and a sub-tick file-poll
// for cross-machine stop requests. Returns true when the broker
// should exit (any stop signal observed).
func (b *SyncFromBackup) waitForNextTick(
	ctx context.Context,
	pollInterval time.Duration,
	statePath string,
	stopCh <-chan struct{},
) bool {
	deadline := time.NewTimer(pollInterval)
	defer deadline.Stop()
	stopPoll := time.NewTicker(brokerStopPollInterval)
	defer stopPoll.Stop()

	for {
		select {
		case <-ctx.Done():
			return true
		case <-stopCh:
			return true
		case <-deadline.C:
			return false
		case <-stopPoll.C:
			req, err := readBrokerStopRequested(ctx, b.Store, statePath)
			if err != nil {
				slog.WarnContext(
					ctx, "broker: stop-poll read failed; will retry",
					slog.String("err", err.Error()),
				)
				continue
			}
			if req != nil {
				return true
			}
		}
	}
}

// preflightChainEncryption inspects the chain root's encryption
// metadata, validates that an envelope is supplied (when the chain is
// encrypted), and caches the chain-level CEK. Mirrors the
// [Restore.preflightEncryption] / [ChainRestore.preflightEncryption]
// shape, applied to the broker.
//
// Reads the legacy `manifest.json` directly because that's where the
// chain root lives in every backup chain shape. A cross-engine chain
// where the root is plaintext but an incremental is encrypted (or
// vice versa) is impossible per [IncrementalBackup.alignEncryption] /
// [BackupStream.alignEncryption], so this preflight covers the broker
// case fully.
func (b *SyncFromBackup) preflightChainEncryption(ctx context.Context) error {
	root, err := readManifestIfPresent(ctx, b.Store)
	if err != nil {
		return fmt.Errorf("read chain root manifest: %w", err)
	}
	if root == nil || root.ChainEncryption == nil {
		return nil
	}
	enc := root.ChainEncryption
	if b.Envelope == nil {
		return fmt.Errorf("chain is encrypted (algorithm=%q kek_mode=%q kek_ref=%q) but no --encrypt + key was supplied",
			enc.Algorithm, enc.KEKMode, enc.KEKRef)
	}
	if enc.KEKMode != "" && b.Envelope.Mode() != enc.KEKMode {
		return fmt.Errorf("envelope mode %q does not match chain's recorded kek_mode %q",
			b.Envelope.Mode(), enc.KEKMode)
	}
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		if len(enc.WrappedCEK) == 0 {
			return errors.New("encrypted chain in per-chain mode but ChainEncryption.WrappedCEK is empty")
		}
		cek, err := b.Envelope.UnwrapCEK(enc.WrappedCEK)
		if err != nil {
			return fmt.Errorf("unwrap chain cek (wrong passphrase?): %w", err)
		}
		b.chainCEK = cek
	}
	return nil
}

// chunkCEK resolves the per-change-chunk CEK using the broker's
// envelope + cached chain CEK. Mirrors [ChainRestore.changeChunkCEK].
func (b *SyncFromBackup) chunkCEK(chunk *ir.ChunkInfo) ([]byte, error) {
	if chunk.Encryption == nil {
		return nil, nil
	}
	if len(chunk.Encryption.WrappedCEK) > 0 {
		if b.Envelope == nil {
			return nil, errors.New("per-chunk encrypted change chunk encountered without envelope")
		}
		cek, err := b.Envelope.UnwrapCEK(chunk.Encryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("unwrap change chunk cek: %w", err)
		}
		return cek, nil
	}
	if b.chainCEK == nil {
		return nil, errors.New("encrypted change chunk encountered but chain CEK is unset")
	}
	return b.chainCEK, nil
}
