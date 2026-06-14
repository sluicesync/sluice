// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// # Bug-6 fix: shape applier values for JSON columns
//
// Before this fix, the applier's INSERT / UPDATE-SET / WHERE
// builders appended row values straight to the args slice and
// always emitted bare `?` placeholders. Both omissions hit JSON
// columns, producing two distinct production failures with one
// root cause — the applier didn't model the JSON column's wire
// shape on either side of the equation:
//
//   - Loud (PG → MySQL CDC, Vitess/PlanetScale targets only): the
//     new image is bound as []byte, which go-sql-driver/mysql
//     labels with `_binary` charset on the wire. Vitess rejects the
//     INSERT with "Cannot create a JSON value from a string with
//     CHARACTER SET 'binary'" and sluice exits. Vanilla MySQL is
//     more permissive and accepts the same bytes, which is why the
//     loud path was invisible to in-house testing for a long time.
//   - Silent (MySQL → MySQL CDC, vanilla MySQL included): the
//     applier emits `WHERE data = ?` against a JSON-typed column.
//     MySQL's equality operator does not implicitly cast the
//     parameter to JSON, so the predicate matches zero rows
//     regardless of whether the parameter is byte-identical to the
//     stored document. The applier explicitly tolerates "update
//     misses" for resume idempotency, so it silently advances the
//     position. The destination row stays stale forever.
//
// The fix is two-part:
//
//  1. Every applier-bound value is routed through prepareValue with
//     the column's declared IR type, so JSON []byte arrives as
//     string (the `_binary` charset prefix is then absent on the
//     wire). This kills the Vitess-specific loud path. The IR type,
//     not the value bytes, is the discriminator — a heuristic over
//     byte shape would be wrong for binary columns whose contents
//     happen to start with `{`.
//  2. WHERE predicates against JSON columns wrap the placeholder in
//     CAST(? AS JSON) so MySQL's equality operator does a JSON-vs-
//     JSON comparison instead of a JSON-vs-string-literal one. This
//     kills the silent path on vanilla MySQL too.
//
// To support both, the applier caches the destination column-type
// map per table and consults it on every Insert/Update/Delete.
// Cache miss is one round-trip; hit is a map lookup.
//
// As defence in depth, dispatch also emits a debug-level log when
// Update or Delete reports zero rows affected, so the previously
// silent divergence has at least one observable footprint in the
// log stream.

// ChangeApplier applies [ir.Change] events to a MySQL target, one
// source change per target transaction. It implements
// [ir.ChangeApplier].
//
// # Identity-key behaviour (read this before pointing it at a real
// # table)
//
// The applier upserts rows on Insert using the table's PRIMARY KEY
// as the conflict target — that's what makes resume after a partial
// apply safe (a re-applied Insert turns into a no-op UPDATE rather
// than a duplicate-key error). Two situations to be aware of:
//
//   - **Tables with a UNIQUE KEY but no PRIMARY KEY upsert
//     idempotently.** MySQL's ON DUPLICATE KEY UPDATE fires on a
//     conflict against ANY unique index, not just the PRIMARY KEY.
//     So even with no PK, the applier emits ON DUPLICATE KEY UPDATE
//     with a full-row SET-list (every column ← its new value); a
//     re-applied Insert that collides on the unique index becomes a
//     full-row upsert (a no-op for an identical row, a newer-image
//     overwrite otherwise) rather than a duplicate-key error. This
//     is what makes the ADR-0072 resumable cold-start COPY safe on
//     the Bug-125 table class (no PK + UNIQUE key, e.g. a
//     `connections` table): on resume, vtgate re-sends already-
//     copied rows past the last checkpoint and they absorb harmlessly
//     instead of 1062-ing the whole resume terminally.
//
//   - **Truly keyless tables (no PK AND no unique index) fall back to
//     effective plain INSERT.** The ON DUPLICATE KEY UPDATE clause is
//     still emitted but is inert (nothing to collide with), so a
//     re-applied Insert produces a duplicate row. Resume idempotency
//     on such tables is therefore best-effort, and continuous-sync on
//     them is not recommended — add a PRIMARY KEY (or at least a
//     UNIQUE key) to the source table first. The Bug-125 keyless
//     cold-start guard refuses these tables outright, so they don't
//     reach the resumable-COPY path in practice.
//
// # Lifecycle
//
// One applier per target connection pool. Apply is single-goroutine:
// it consumes the change channel sequentially to preserve source
// ordering. Concurrent calls on the same applier are not supported.
type ChangeApplier struct {
	db     *sql.DB
	schema string

	// multiDBRouting is the ADR-0074 Phase 1b per-change namespace
	// routing switch, set by [SetMultiDatabaseRouting]. When false (the
	// default — every single-database run, ALL engine pairs) the applier
	// writes into its bound `schema` and IGNORES each change's source
	// Schema for table qualification: BYTE-IDENTICAL to the pre-ADR-0074
	// behaviour. When true (a multi-database fan-out stream) it qualifies
	// the table ref with the change's source database ONLY when that
	// database is non-empty AND differs from `schema` — exactly the
	// cross-database case, mirroring the Phase-1a FK qualifier. See
	// [routedSchema] and the [ir.MultiDatabaseRouter] doc for why this is
	// an explicit opt-in rather than inferred from Change.Schema (the
	// back-compat guard against the cross-engine single-database case
	// where a PG source already populates Change.Schema).
	multiDBRouting bool

	// slotName is the active stream's resolved replication-slot name,
	// set by [SetSlotName] at Streamer startup. Threaded into every
	// [writePositionTx] call so the per-target sluice_cdc_state row's
	// slot_name column stays in sync with what the streamer is
	// actually consuming.
	//
	// MySQL has no native slot concept (the binlog stream is the
	// slot), so on same-engine MySQL → MySQL the streamer does not
	// supply a slot name and this stays empty. The field exists for
	// cross-engine PG → MySQL parity (OBS-1, v0.32.2): the PG
	// streamer's resolved slot name needs to round-trip through the
	// MySQL target's sluice_cdc_state row so a future
	// `sluice schema add-table --no-drain --slot-name=<name>`
	// against the same MySQL target can recover it via ListStreams.
	// Pre-v0.32.2, the same code path triggered MySQL Error 1054
	// ("Unknown column slot_name") because the MySQL control table
	// lacked the column entirely.
	slotName string

	// sourceFingerprint is the truncated SHA-256 hex of the streamer's
	// source DSN host+port+database tuple, set by
	// [SetSourceDSNFingerprint] at Streamer startup. Same
	// cross-engine parity rationale as slotName — see ADR-0031 and
	// OBS-1 (v0.32.2).
	sourceFingerprint string

	// targetSchema records the operator-supplied `--target-schema NAME`
	// on the per-target sluice_cdc_state row. MySQL targets do not
	// support `--target-schema` (the validate-time gate refuses it
	// upstream — MySQL has no schema-vs-database distinction), so on
	// the MySQL side this column always stays empty in practice. The
	// field exists for IR-side parity and to avoid silently dropping
	// the value if a future engine flavor relaxes the upstream gate.
	targetSchema string

	// execTimeout is the per-exec deadline applied via context.WithTimeout
	// around each tx.ExecContext / tx.QueryContext on the applier's
	// transaction. GitHub #23 Phase B fix: pre-v0.52.0 the apply path
	// had no per-exec timeout, so a destination connection that went
	// half-closed silently (no TCP FIN/RST) caused the driver's TLS
	// read to block indefinitely. The v0.42.0+ retry loop never fired
	// because the apply call never returned. With execTimeout > 0,
	// the deadline fires, the driver's watchCancel closes the
	// connection, [classifyApplierError] wraps the resulting
	// context.DeadlineExceeded as retriable, and the existing
	// runWithRetry loop activates cleanly.
	//
	// Set via [SetExecTimeout]. 0 disables the timeout (preserves
	// pre-v0.52.0 unbounded-block behavior).
	execTimeout time.Duration

	// pkCache maps "schema.table" → ordered list of PK column names.
	// Populated lazily via a single information_schema query the
	// first time a change for the table arrives. An empty slice
	// (length 0) means "table exists but has no PK" — in that case
	// Insert falls back to plain INSERT (see the package comment).
	pkCache map[string][]string

	// keylessCache maps "schema.table" → whether the table has NO
	// PRIMARY KEY and NO UNIQUE index (the ADR-0089 keyless guard's
	// "truly-keyless" verdict). Populated lazily via one
	// information_schema probe per table. warnedKeyless tracks tables
	// already WARNed about so the guard logs at most once per table.
	keylessCache  map[string]bool
	warnedKeyless map[string]bool

	// colTypeCache maps "schema.table" → column-name → *ir.Column. It
	// is the input to prepareValue for every value the applier
	// binds: see the file-header comment for the JSON-column bug
	// this exists to fix. The map carries the full Column descriptor
	// (not just the IR type) so prepareValue can consult fields like
	// [ir.Column.SourceColumnType] when disambiguating value shapes
	// — see Bug 47 / convertArrayLikeToJSON in row_writer.go.
	// Populated lazily on the first sight of a table via a single
	// information_schema query — same shape as pkCache. Cache miss
	// is one round-trip; hit is a map lookup.
	colTypeCache map[string]map[string]*ir.Column

	// maxBufferBytes is the soft byte-size cap on the in-flight
	// batch's buffered change values during ApplyBatch. Implements
	// [ir.MaxBufferBytesSetter] via [SetMaxBufferBytes]. Zero or
	// negative means "no byte cap"; the row-count cap remains the
	// only flush trigger. See ADR-0028.
	maxBufferBytes int64

	// redactor is the operator-configured PII redaction registry
	// (Phase 1.5, roadmap item 15a follow-on). When non-nil and
	// non-empty, every change's row data passes through
	// [redact.Registry.ApplyRow] before dispatch so PII columns get
	// the operator's strategy applied on CDC events the same way
	// Phase 1 already redacted bulk-copy rows. Set via
	// [SetRedactor]; nil/empty is the no-redactions hot path.
	redactor *redact.Registry

	// shardColumn / shardValue are the operator-configured
	// Shape-A discriminator (ADR-0048; CLI:
	// `--inject-shard-column NAME=VALUE`). When shardColumn != "",
	// the apply path stamps row[shardColumn]=shardValue onto every
	// row-bearing change (Insert.Row, Update.Before+After,
	// Delete.Before) before dispatch, the same way the bulk-copy
	// path stamps via the orchestrator-side
	// [internal/pipeline.shardStampRows]. Set via
	// [SetShardColumn] (the [ir.ShardColumnSetter] surface). Empty
	// shardColumn is the no-stamp hot path — pre-ADR-0048 single-
	// source streams pay zero cost.
	shardColumn string
	shardValue  any

	// streamID is the active stream's identifier, recorded by
	// [SetStreamID] at Streamer startup. Threaded through every
	// redactor.ApplyRow call so randomize:* strategies (PII Phase 2.c,
	// v0.59.0) derive a per-row replay-stable seed from streamID +
	// table + column + PK values.
	streamID string

	// batchSizeProvider is the optional AIMD controller's batch-size
	// surface (ADR-0052). When non-nil, ApplyBatch consults
	// provider.NextBatchSize() on every outer-loop iteration to
	// discover the controller's current target. Nil means "no
	// controller — use the static maxBatchSize from the caller."
	// Implements [ir.BatchSizeProviderSetter] via [SetBatchSizeProvider].
	batchSizeProvider ir.BatchSizeProvider

	// batchObserver is the optional AIMD controller's batch-outcome
	// surface (ADR-0052). When non-nil, applyOneBatch calls
	// observer.ObserveBatch(ctx, latency, rows, err) after every
	// commit (success path) or rollback (failure path) so the
	// controller can update its sliding-window p95 + retry-rate
	// accumulator. Nil means "no controller; no observation."
	// Implements [ir.BatchObserverSetter] via [SetBatchObserver].
	batchObserver ir.BatchObserver

	// activeSchema maps "schema.table" → the IR schema in effect at the
	// most-recently durably-persisted ADR-0049 boundary for that table.
	// O(1) amortised: populated on cold-start prime (resume from a
	// persisted position; one storage hit per primed table) and on
	// every successful SchemaSnapshot dispatch (cache update from
	// in-memory `ir.SchemaSnapshot.IR` — NO storage hit). Per-row
	// resolves are cache-only (Chunk D / future cross-engine
	// source-IR will read via [ChangeApplier.ActiveSchema]); the
	// storage-side `resolveSchemaVersion` is reached at prime time
	// only.
	//
	// Concurrency: applier-goroutine-owned. The applier serialises
	// every Apply / ApplyBatch call onto a single goroutine
	// (per-applier doc: "Concurrent calls on the same applier are not
	// supported"), and every cache write (Prime, post-commit
	// SchemaSnapshot) and every cache read ([ActiveSchema]) happens on
	// that same goroutine. No lock is required. If a future change
	// introduces a cross-goroutine reader (e.g. an out-of-band metrics
	// or admin probe), gate this field with sync.RWMutex; until then
	// the lock-free form is the correct shape.
	//
	// Cache-after-commit invariant (ADR-0049 Chunk C): the cache is
	// updated AT THE CALLER's post-commit observation point
	// (applyOne / applyOneBatch after `tx.Commit()` returns nil), NOT
	// inside dispatch. A failed dispatch / rolled-back tx must NOT
	// leave a cache entry that disagrees with persisted state.
	activeSchema map[string]activeSchemaVersion

	// resolveCallsForTest counts how many times the applier has
	// touched the schema-history storage to resolve a schema version
	// (i.e. the per-table loadRetainedSchemaVersions + resolve hit
	// inside [PrimeSchemaHistoryCache]). The post-commit cache update
	// path does NOT increment this counter — it never touches
	// storage. Read by the O(1)-amortised pin test
	// (TestApplier_SchemaCache_O1Amortised) to assert that a steady-
	// state stream of rows + boundaries has exactly #primed-tables
	// resolve hits, NOT O(rows) or O(boundaries).
	resolveCallsForTest atomic.Int64
}

// activeSchemaVersion is one entry in the ADR-0049 Chunk C applier
// active-version cache: the boundary anchor (for diagnostics +
// future Chunk D backup-envelope use) and the IR schema in effect
// at and after that anchor.
type activeSchemaVersion struct {
	Anchor ir.Position
	IR     *ir.Table
}

// SetMaxBufferBytes implements [ir.MaxBufferBytesSetter]. The
// streamer calls this after [Engine.OpenChangeApplier] returns when
// --max-buffer-bytes is set, before ApplyBatch runs. Zero or negative
// means "no byte cap"; the row-count cap remains the only flush
// trigger.
func (a *ChangeApplier) SetMaxBufferBytes(bytes int64) {
	a.maxBufferBytes = bytes
}

// SetSlotName records the active stream's resolved replication-slot
// name so subsequent position-writes populate the sluice_cdc_state
// row's slot_name column. Symmetric with the PG ChangeApplier's
// counterpart; v0.32.2 brought MySQL to schema parity (OBS-1) so a
// cross-engine PG → MySQL stream with `--slot-name <name>` can
// round-trip the slot identity through the target's control table.
//
// MySQL's own CDC streamer doesn't have a slot concept, so when the
// pipeline streamer runs against a MySQL source the structural
// SetSlotName call lands with an empty string (the streamer's
// `s.SlotName` is itself empty for MySQL sources). Empty input is a
// no-op via the COALESCE-tolerant shape in writePositionTx.
// Idempotent; the streamer may call this on every Run.
func (a *ChangeApplier) SetSlotName(slotName string) {
	a.slotName = slotName
}

// SetSourceDSNFingerprint implements [ir.SourceFingerprintRecorder].
// Records the source DSN fingerprint the streamer computed at startup
// so subsequent position-writes populate the sluice_cdc_state row's
// source_dsn_fingerprint column. Symmetric with PG (ADR-0031); MySQL
// schema parity arrived in v0.32.2 (OBS-1). Empty input is a no-op
// preservation via writePositionTx's COALESCE. Idempotent.
func (a *ChangeApplier) SetSourceDSNFingerprint(fingerprint string) {
	a.sourceFingerprint = fingerprint
}

// SetTargetSchema records the operator-supplied `--target-schema NAME`
// (ADR-0031, Bug 46) so subsequent position-writes populate the
// sluice_cdc_state row's target_schema column. MySQL's validate-time
// gate refuses `--target-schema` upstream (no schema-vs-database
// distinction), so on the MySQL side this method receives empty
// input in practice — the column stays NULL on the row. The setter
// exists for IR-side parity (OBS-1, v0.32.2): the streamer's
// structural-interface dispatch otherwise can't tell that the empty
// flag has been "applied" to the MySQL side, which would be
// confusing if a future engine flavor opens up target_schema for
// MySQL. Idempotent; empty input preserves the row's existing value
// via writePositionTx's COALESCE.
func (a *ChangeApplier) SetTargetSchema(name string) {
	a.targetSchema = name
}

// SetExecTimeout records the per-exec timeout the streamer should
// apply to every tx.ExecContext / tx.QueryContext call on the apply
// path. GitHub #23 Phase B fix (v0.52.0): bounds the time a hung
// destination connection can keep the apply goroutine blocked.
// Zero disables the timeout (preserves pre-v0.52.0 unbounded behavior).
//
// Implements the optional [applierExecTimeoutSetter] surface probed
// by [Streamer.openApplier]. Idempotent.
func (a *ChangeApplier) SetExecTimeout(d time.Duration) {
	a.execTimeout = d
}

// SetRedactor implements [ir.RedactorSetter] (PII Phase 1.5,
// roadmap item 15a follow-on). Stores the operator-configured
// redaction registry; the apply path invokes
// [redact.Registry.ApplyRow] on each change's row data before
// dispatch. The parameter type is `any` per the interface to
// avoid an ir → redact dependency cycle; we type-assert to
// *redact.Registry. nil registry or nil-asserting argument is the
// no-redactions default.
func (a *ChangeApplier) SetRedactor(registry any) {
	if registry == nil {
		a.redactor = nil
		return
	}
	r, _ := registry.(*redact.Registry)
	a.redactor = r
}

// SetStreamID implements [ir.StreamIDSetter] (PII Phase 2.c,
// v0.59.0). Records the active stream's identifier so each CDC
// event's redact.ApplyRow call can derive a replay-stable
// per-row seed for randomize:* strategies. Idempotent.
func (a *ChangeApplier) SetStreamID(streamID string) {
	a.streamID = streamID
}

// SetShardColumn implements [ir.ShardColumnSetter] (ADR-0048
// Shape A; DP-1 resolved 2026-05-21 to option (a) — the
// two-surface split). Records the operator-supplied discriminator
// column name + value so the apply path stamps it onto every
// row-bearing change (Insert.Row, Update.Before+After,
// Delete.Before) before dispatch, the same way bulk-copy's
// orchestrator-side wrap stamps the same column.
//
// Empty name clears the wiring (no-stamp default; the pre-
// ADR-0048 single-source hot path). Idempotent; the streamer may
// call this on every Run.
func (a *ChangeApplier) SetShardColumn(name string, value any) {
	a.shardColumn = name
	a.shardValue = value
}

// stampShardChange stamps the operator-supplied discriminator
// onto every row-bearing change before dispatch. Empty
// shardColumn is the no-op fast path. PII Phase 1.5's
// `redactChange` is the sibling pattern — both run pre-dispatch,
// both mutate the change's row map in place, both have the same
// {Insert.Row, Update.Before/After, Delete.Before} scope.
// Truncate / TxBegin / TxCommit / SchemaSnapshot carry no row
// data; pass-through.
//
// Stamping happens AFTER redactChange (redaction runs first per
// the existing call order in applyOne / applyOneBatch), so a
// redaction strategy that strips/overrides the discriminator
// column would land before the stamp restores it. The
// discriminator's IR column is sluice-injected (the operator's
// source schema cannot match a redaction rule against it), so in
// practice the order is observable only as a future-proofing
// guarantee.
func (a *ChangeApplier) stampShardChange(c ir.Change) {
	if a.shardColumn == "" {
		return
	}
	name := a.shardColumn
	val := a.shardValue
	switch v := c.(type) {
	case ir.Insert:
		if v.Row != nil {
			v.Row[name] = val
		}
	case ir.Update:
		if v.Before != nil {
			v.Before[name] = val
		}
		if v.After != nil {
			v.After[name] = val
		}
	case ir.Delete:
		if v.Before != nil {
			v.Before[name] = val
		}
	}
}

// SetMultiDatabaseRouting implements [ir.MultiDatabaseRouter] (ADR-0074
// Phase 1b). Enables per-change target-database routing for a
// multi-database fan-out CDC stream: when enabled, the applier qualifies
// an Insert/Update/Delete/Truncate with the change's source database
// (`db`.`table`) for the cross-database case (see [routedSchema]). When
// disabled (the default), the applier writes into its bound database and
// emits byte-identical single-database SQL. Idempotent; the streamer may
// call this on every Run.
func (a *ChangeApplier) SetMultiDatabaseRouting(enabled bool) {
	a.multiDBRouting = enabled
}

// SetBatchSizeProvider implements [ir.BatchSizeProviderSetter]
// (ADR-0052). Threads the AIMD controller onto the applier so each
// batch's row-cap reflects the controller's current decision. A nil
// provider clears the wiring (the static --apply-batch-size cap
// resumes). Idempotent.
func (a *ChangeApplier) SetBatchSizeProvider(p ir.BatchSizeProvider) {
	a.batchSizeProvider = p
}

// SetBatchObserver implements [ir.BatchObserverSetter] (ADR-0052).
// Threads the AIMD controller's observation surface onto the applier
// so each post-commit / post-rollback latency feeds the controller's
// sliding-window p95. A nil observer clears the wiring (no
// observation; controller decisions stagnate at whatever the last
// observation drove). Idempotent.
func (a *ChangeApplier) SetBatchObserver(o ir.BatchObserver) {
	a.batchObserver = o
}

// redactChange invokes the applier's redactor (if any) on the
// change's row data. Mutates the row in place. Returns a wrapped
// error on strategy refusal; ADR-0038's classifier sees the error
// as terminal-by-default.
//
// PII Phase 1.5: scope = Insert.Row, Update.Before, Update.After,
// Delete.Before. Truncate / TxBegin / TxCommit carry no row data;
// pass-through.
//
// PII Phase 2.c (v0.59.0): every ApplyRow call passes the table's
// PK column list + active streamID so randomize:* strategies
// derive a per-row replay-stable seed. The PK is fetched via the
// existing per-table pkCache (one information_schema round-trip
// per table on first sight).
func (a *ChangeApplier) redactChange(ctx context.Context, c ir.Change) error {
	if a.redactor.Empty() {
		return nil
	}
	switch v := c.(type) {
	case ir.Insert:
		pk, err := a.pkForRedact(ctx, v.Schema, v.Table)
		if err != nil {
			return err
		}
		return a.redactor.ApplyRow(v.Schema, v.Table, pk, v.Row, a.streamID)
	case ir.Update:
		pk, err := a.pkForRedact(ctx, v.Schema, v.Table)
		if err != nil {
			return err
		}
		if err := a.redactor.ApplyRow(v.Schema, v.Table, pk, v.Before, a.streamID); err != nil {
			return err
		}
		return a.redactor.ApplyRow(v.Schema, v.Table, pk, v.After, a.streamID)
	case ir.Delete:
		pk, err := a.pkForRedact(ctx, v.Schema, v.Table)
		if err != nil {
			return err
		}
		return a.redactor.ApplyRow(v.Schema, v.Table, pk, v.Before, a.streamID)
	}
	return nil
}

// pkForRedact returns the cached PK column list for the named
// table using the applier's connection pool. Wrapper around
// pkFor that opens a fresh tx — redactChange runs before the
// dispatch's tx is established, but schema metadata is stable
// across tx boundaries so this is safe.
//
// changeSchema is the change's SOURCE schema; the PK lookup must hit the
// TARGET namespace, so it is routed through [routedSchema] exactly like
// the dispatch path — this keeps the redact-side and dispatch-side
// pkCache entries keyed identically (single-database: both under the
// bound database; cross-database: both under the routed source database).
func (a *ChangeApplier) pkForRedact(ctx context.Context, changeSchema, table string) ([]string, error) {
	schema := a.routedSchema(changeSchema)
	qn := qualifiedName(schema, table)
	if cached, ok := a.pkCache[qn]; ok {
		return cached, nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql: applier: pkForRedact: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	pk, err := loadPrimaryKey(ctx, tx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("mysql: applier: pkForRedact: %w", err)
	}
	a.pkCache[qn] = pk
	return pk, nil
}

// txExec wraps tx.ExecContext with the applier's per-exec timeout
// (when set). On timeout expiry the driver's watchCancel closes the
// underlying connection; the resulting context.DeadlineExceeded is
// classified retriable by [classifyApplierError] so the runWithRetry
// loop activates.
func (a *ChangeApplier) txExec(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	if a.execTimeout <= 0 {
		return tx.ExecContext(ctx, query, args...)
	}
	execCtx, cancel := context.WithTimeout(ctx, a.execTimeout)
	defer cancel()
	return tx.ExecContext(execCtx, query, args...)
}

// execTimeoutCtx returns ctx wrapped with the applier's per-exec
// timeout (when set) plus the matching cancel func. Used at the
// writePositionTx call site, which is a package-level helper not
// reachable via [txExec]. Callers must `defer cancel()` (or call
// `cancel()` after the wrapped operation returns).
//
// Bug 56 (v0.52.1): the position-write path is the second TLS-read
// surface on the apply hot path; pre-v0.52.1 it was not wrapped, so
// a half-closed PlanetScale destination connection could still stall
// the apply goroutine indefinitely inside [writePositionTx]'s bare
// `tx.ExecContext`. Wrapping at the call site closes that gap.
func (a *ChangeApplier) execTimeoutCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.execTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, a.execTimeout)
}

// commitWithTimeout runs tx.Commit() under the per-exec watchdog
// (see [appliershared.RunWithDeadline] for the semantics and Bug 56 /
// v0.52.1 rationale). Thin wrapper that exists so callers don't have
// to thread the closure manually.
func (a *ChangeApplier) commitWithTimeout(tx *sql.Tx) error {
	return appliershared.RunWithDeadline(a.execTimeout, tx.Commit)
}

// Close releases the underlying connection pool.
func (a *ChangeApplier) Close() error {
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}

// EnsureControlTable creates the per-target sluice_cdc_state table
// and the additive ADR-0049 sluice_cdc_schema_history table if they
// don't exist. Idempotent. Must run before Apply on any fresh target;
// the Streamer drives this at startup. The schema-history ensure is
// strictly additive — it never touches sluice_cdc_state data.
func (a *ChangeApplier) EnsureControlTable(ctx context.Context) error {
	if err := ensureControlTable(ctx, a.db); err != nil {
		return err
	}
	if err := ensureSchemaHistoryTable(ctx, a.db); err != nil {
		return err
	}
	return ensureShardConsolidationLeaseTable(ctx, a.db)
}

// CompactSchemaHistoryBelow implements [ir.SchemaHistoryCompactor]
// (ADR-0049 Chunk D). Deletes sluice_cdc_schema_history rows whose
// anchor_position is STRICTLY OLDER than floor under this engine's
// [ir.PositionOrderer]. See compactSchemaHistoryBelow for the
// strict-older semantics + loud-floor preservation invariant.
func (a *ChangeApplier) CompactSchemaHistoryBelow(ctx context.Context, floor ir.Position) (int, error) {
	return compactSchemaHistoryBelow(ctx, a.db, Engine{}, floor)
}

// ReadPosition returns the last persisted source position for
// streamID, or ok=false when no row exists. The returned Position
// always has Engine = "mysql"; only the Token survives across
// runs (the engine reading is implicitly the engine that wrote).
func (a *ChangeApplier) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	token, ok, err := readPosition(ctx, a.db, streamID)
	if err != nil {
		return ir.Position{}, false, err
	}
	if !ok {
		return ir.Position{}, false, nil
	}
	// Mirror PG: returned Position.Engine is hard-coded to "mysql".
	// Broker-driven rows carry their engine sentinel inside the JSON
	// envelope (`_engine` field, see pipeline.isBrokerToken). Bug 39
	// (v0.20.1) is the load-bearing rationale for that envelope.
	return ir.Position{Engine: engineNameMySQL, Token: token}, true, nil
}

// ListStreams returns all rows in the per-target control table.
// Used by `sluice sync status` for operational visibility. Tolerant
// of the table being absent — operators querying status against a
// fresh target should see "no streams" rather than an error.
func (a *ChangeApplier) ListStreams(ctx context.Context) ([]ir.StreamStatus, error) {
	return listStreams(ctx, a.db, engineNameMySQL)
}

// RequestStop flips the stop flag on the named stream's row. The
// running [pipeline.Streamer] polls this column every few seconds
// and exits cleanly once it observes a non-NULL value. Idempotent —
// repeated calls land the same flag.
//
// Returns an error wrapping [errStreamNotFound] when no row exists
// for streamID; the CLI's `sync stop` branches on it to surface a
// friendly "no stream X on target" message.
func (a *ChangeApplier) RequestStop(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RequestStop: streamID is empty")
	}
	return requestStop(ctx, a.db, streamID)
}

// ReadStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. The pipeline's Streamer poll
// goroutine consults this method via a structural interface (the
// internal pipeline.stopFlagReader). Exported because Go's method-
// set rules require an exported method to satisfy an interface from
// another package — even when that interface is itself unexported.
func (a *ChangeApplier) ReadStopRequested(ctx context.Context, streamID string) (bool, error) {
	return readStopRequested(ctx, a.db, streamID)
}

// ClearStopRequested resets stop_requested_at to NULL for the named
// stream. The Streamer calls this at startup so a previous
// `sluice sync stop` doesn't leave a sticky signal that immediately
// exits the next `sluice sync start` (Bug 11 in v0.3.2 testing).
// Idempotent and tolerant of a missing row.
func (a *ChangeApplier) ClearStopRequested(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: ClearStopRequested: streamID is empty")
	}
	return clearStopRequested(ctx, a.db, streamID)
}

// ClearStream deletes the named stream's row from the per-target
// sluice_cdc_state table. Used by the `--reset-target-data` recovery
// path (ADR-0023). Implements [ir.StreamCleaner]. Idempotent and
// tolerant of a missing row or table.
func (a *ChangeApplier) ClearStream(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: ClearStream: streamID is empty")
	}
	return clearStream(ctx, a.db, streamID)
}

// ReadLiveAddedTables returns the comma-parsed live_added_tables
// column for streamID — the set of tables that have been live-added to
// this stream's scope by `sluice schema add-table --no-drain`
// (ADR-0034 MySQL Phase 2). The pipeline streamer's poll goroutine
// calls this on its tick cadence to keep its in-memory dispatch
// filter in sync.
//
// Empty slice covers all "no live-adds" surfaces: NULL column, missing
// row, missing column (legacy pre-v0.27.0 control table), missing
// table. The streamer treats every shape as "no live-adds; preserve
// the operator's original filter."
func (a *ChangeApplier) ReadLiveAddedTables(ctx context.Context, streamID string) ([]string, error) {
	if streamID == "" {
		return nil, errors.New("mysql: applier: ReadLiveAddedTables: streamID is empty")
	}
	return readLiveAddedTables(ctx, a.db, streamID)
}

// RecordLiveAddedTable appends tableName to the per-target row's
// live_added_tables column for streamID. ADR-0034. Called by the
// add-table --no-drain orchestrator on a successful live-add. The
// streamer's poll goroutine picks the change up on its next tick;
// from that point onwards, binlog events on the new table reach the
// applier.
//
// Idempotent: re-running with the same tableName does not double-
// record. Concurrent runs against different tables serialise via
// SELECT ... FOR UPDATE.
//
// Errors when the cdc-state row doesn't exist for streamID — the
// orchestrator's preflight has already verified this via ListStreams,
// but a clean error here surfaces the rare race where the row was
// deleted between preflight and write.
func (a *ChangeApplier) RecordLiveAddedTable(ctx context.Context, streamID, tableName string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RecordLiveAddedTable: streamID is empty")
	}
	return recordLiveAddedTable(ctx, a.db, streamID, tableName)
}

// WritePosition implements [ir.PositionWriter]: upserts the position
// row for streamID in `sluice_cdc_state` without any accompanying
// data write. Used by Phase 4.5's broker for cold-start initial-
// position writes and schema-delta-only incrementals (no change
// chunks → no Apply path to ride along with).
//
// Wraps the same writePositionTx helper the Apply path uses, so the
// row shape and idempotency contract are identical.
func (a *ChangeApplier) WritePosition(ctx context.Context, streamID string, pos ir.Position) error {
	if streamID == "" {
		return errors.New("mysql: applier: WritePosition: streamID is empty")
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: applier: WritePosition: begin tx: %w", err)
	}
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	err = writePositionTx(posCtx, tx, streamID, pos.Token, a.slotName, a.sourceFingerprint, a.targetSchema)
	posCancel()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := a.commitWithTimeout(tx); err != nil {
		return fmt.Errorf("mysql: applier: WritePosition: commit: %w", err)
	}
	return nil
}

// Apply consumes changes from the channel and applies each to the
// target in its own transaction. The position write happens inside
// the same transaction as the data write (per ADR-0007), so a
// crash between them rolls back both — progress and data can never
// diverge.
//
// Returns when the channel closes (clean shutdown), when ctx is
// cancelled, or when a target write fails.
//
// Per-apply DEBUG instrumentation (v0.53.0): the batched path emits
// `applier: batch latency` per completed batch; the v0.52.0 cycle's
// secondary finding was that default `--apply-batch-size=1` routes
// through this non-batched Apply which had no equivalent line, so
// operators running default settings had no DEBUG signal that apply
// was making progress. We emit `applier: apply latency` per
// successful change here for diagnostic symmetry. Same DEBUG level,
// so INFO operators never see it; cycle-test runs at DEBUG get the
// signal.
func (a *ChangeApplier) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	if streamID == "" {
		return errors.New("mysql: applier: streamID is empty (Streamer is responsible for resolving it)")
	}
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return nil
			}
			// Source-tx boundary events are no-ops on the per-change
			// path (ADR-0027): each row event already commits its
			// own target transaction, so a TxBegin / TxCommit
			// signal carries no extra information here. The
			// boundary semantics are only useful to the batched
			// applier, which observes them to align target tx
			// boundaries to source tx boundaries.
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit:
				continue
			}
			applyStart := time.Now()
			if err := a.applyOne(ctx, streamID, c); err != nil {
				return err
			}
			slog.DebugContext(
				ctx, "applier: apply latency",
				slog.String("stream_id", streamID),
				slog.Int("rows", 1),
				slog.Int64("millis", time.Since(applyStart).Milliseconds()),
			)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyOne dispatches a single change to its SQL form, runs the
// data write, and writes the position update — all in the same
// transaction.
//
// Errors are routed through [classifyApplierError] so the pipeline's
// retry policy (ADR-0038) can recognise transient Vitess / MySQL
// errors and back off rather than exit the stream.
func (a *ChangeApplier) applyOne(ctx context.Context, streamID string, c ir.Change) error {
	// PII Phase 1.5: redact CDC row data before dispatch when the
	// operator has configured rules. nil/empty redactor is a no-op
	// fast path; the apply hot path stays free when no redaction is
	// configured.
	if err := a.redactChange(ctx, c); err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: redact: %w", err))
	}
	// ADR-0048 Shape A: stamp the operator-supplied discriminator
	// (`--inject-shard-column NAME=VALUE`) onto every row-bearing
	// change before dispatch. Empty shardColumn is a no-op fast
	// path — single-source streams pay zero cost.
	a.stampShardChange(c)
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: begin tx: %w", err))
	}
	// ADR-0049 Chunk B1/B2: a SchemaSnapshot persists the boundary's
	// IR schema into sluice_cdc_schema_history. Locked decision #4a:
	// that write MUST be in the SAME target tx as the ADR-0007
	// position write below — a cross-tx crash that persists a
	// position whose schema version isn't durable causes a spurious
	// cold-start. dispatch handles the version write on `tx`; the
	// position write that follows rides the same `tx`, and a single
	// commit makes them atomic. A failure rolls back BOTH and
	// propagates (locked decision #4b: fatal/loud, never
	// logged-and-continued).
	if err := a.dispatch(ctx, tx, streamID, c); err != nil {
		_ = tx.Rollback()
		return classifyApplierError(err)
	}
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	err = writePositionTx(posCtx, tx, streamID, c.Pos().Token, a.slotName, a.sourceFingerprint, a.targetSchema)
	posCancel()
	if err != nil {
		_ = tx.Rollback()
		return classifyApplierError(err)
	}
	if err := a.commitWithTimeout(tx); err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: commit: %w", err))
	}
	// ADR-0049 Chunk C cache-after-commit invariant: a SchemaSnapshot
	// updates the active-version cache ONLY after its tx has
	// committed durably. A failed dispatch or commit short-circuits
	// above; the cache is never mutated on the rolled-back path.
	if snap, isSnap := c.(ir.SchemaSnapshot); isSnap {
		a.cacheActiveSchemaAfterCommit(snap)
	}
	return nil
}

// dispatch routes a single change to its SQL form on the open tx.
//
// streamID is the Apply / ApplyBatch caller's streamID arg, threaded
// through so the [ir.SchemaSnapshot] branch keys its
// sluice_cdc_schema_history write off the same value the
// [writePositionTx] caller uses on the same tx (ADR-0049 follow-up
// task #27). Pre-task-27 the snapshot branch read `a.streamID` (set
// only via the optional [ir.StreamIDSetter] interface) — every
// production caller does call SetStreamID before Apply, but a future
// non-migrate Apply path that didn't would silently key
// schema-history rows under `""` and surface as a loud
// [ir.ErrPositionInvalid] at resume time. Sourcing streamID from the
// Apply arg (the same path writePositionTx already uses) closes the
// footgun.
func (a *ChangeApplier) dispatch(ctx context.Context, tx *sql.Tx, streamID string, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		// ADR-0074 Phase 1b: routedSchema is the bound database in
		// single-database mode (byte-identical) and the change's source
		// database in a cross-database multi-database write. It is the
		// authoritative namespace for BOTH the information_schema lookup
		// (pkFor / colTypesFor) and the emitted table reference, so the
		// metadata cache and the write target stay consistent.
		schema := a.routedSchema(v.Schema)
		pk, err := a.pkFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", schema, v.Table, err)
		}
		colTypes, err := a.colTypesFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildInsertSQL(schema, v.Table, v.Row, pk, colTypes)
		if err != nil {
			return fmt.Errorf("mysql: applier: build insert for %s.%s: %w", schema, v.Table, err)
		}
		if _, err := a.txExec(ctx, tx, stmt, args...); err != nil {
			return fmt.Errorf("mysql: applier: insert into %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.Update:
		schema := a.routedSchema(v.Schema)
		colTypes, err := a.colTypesFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildUpdateSQL(schema, v.Table, v.Before, v.After, colTypes)
		if err != nil {
			return fmt.Errorf("mysql: applier: build update for %s.%s: %w", schema, v.Table, err)
		}
		// Update misses are tolerated (zero rows affected). On resume
		// we may replay an Update whose target row was already
		// updated — that's expected, not an error. Silent zero-rows-
		// affected can also signal Bug-6-style WHERE-predicate
		// breakage on JSON columns; we surface it at debug level so
		// the divergence has at least one observable footprint.
		res, err := a.txExec(ctx, tx, stmt, args...)
		if err != nil {
			return fmt.Errorf("mysql: applier: update %s.%s: %w", schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "update", schema, v.Table, res)
		return nil

	case ir.Delete:
		schema := a.routedSchema(v.Schema)
		colTypes, err := a.colTypesFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildDeleteSQL(schema, v.Table, v.Before, colTypes)
		if err != nil {
			return fmt.Errorf("mysql: applier: build delete for %s.%s: %w", schema, v.Table, err)
		}
		// Delete misses are tolerated for the same reason as Update.
		res, err := a.txExec(ctx, tx, stmt, args...)
		if err != nil {
			return fmt.Errorf("mysql: applier: delete from %s.%s: %w", schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "delete", schema, v.Table, res)
		return nil

	case ir.Truncate:
		// Bug 98 (v0.92.0): the source's pgoutput TruncateMessage may
		// carry CASCADE / RESTART IDENTITY option flags. MySQL TRUNCATE
		// has no CASCADE concept (InnoDB refuses TRUNCATE on FK-
		// referenced parents outright; there's no operator-side way to
		// express "cascade the delete to children" via TRUNCATE) and
		// no RESTART IDENTITY clause (TRUNCATE always resets
		// AUTO_INCREMENT in InnoDB). Log a WARN so the operator can
		// see the option-loss on cross-engine PG → MySQL CDC; emit
		// the plain TRUNCATE.
		if v.Cascade || v.RestartIdentity {
			slog.WarnContext(
				ctx,
				"mysql: applier: TRUNCATE option flag(s) ignored on MySQL target — MySQL TRUNCATE has no CASCADE / RESTART IDENTITY clause",
				slog.String("schema", v.Schema),
				slog.String("table", v.Table),
				slog.Bool("source_cascade", v.Cascade),
				slog.Bool("source_restart_identity", v.RestartIdentity),
			)
		}
		stmt := buildTruncateSQL(a.routedSchema(v.Schema), v.Table)
		if _, err := a.txExec(ctx, tx, stmt); err != nil {
			return fmt.Errorf("mysql: applier: truncate %s.%s: %w", a.routedSchema(v.Schema), v.Table, err)
		}
		return nil

	case ir.SchemaSnapshot:
		// ADR-0049 Chunk B: persist the boundary's IR schema into
		// sluice_cdc_schema_history on the SAME tx the caller
		// (applyOne / commitBatch) writes the ADR-0007 position on
		// (locked decision #4a). The streamID arg (threaded through
		// from Apply / ApplyBatch — ADR-0049 follow-up task #27)
		// keys the history row identically to the position row's
		// streamID on the same tx, so resolveSchemaVersion composes
		// cleanly. Pre-task-27 this read a.streamID (set via the
		// optional [ir.StreamIDSetter]); every CURRENT caller does
		// call SetStreamID before Apply, but sourcing from the arg
		// closes the latent footgun where any future non-migrate
		// Apply path that omits SetStreamID would silently key
		// rows under "" and surface as a loud
		// [ir.ErrPositionInvalid] at the next resume. A failure
		// here returns up through dispatch → the tx rolls back
		// (position write never lands) and the stream stops loudly
		// (locked decision #4b: fatal, never logged-and-continued —
		// a lost version silently degrades every future resume
		// across this boundary).
		if v.IR == nil {
			return errors.New("mysql: applier: schema snapshot has nil IR table")
		}
		if err := writeSchemaVersion(ctx, tx, streamID, v.Schema, v.Table, v.Position, v.IR); err != nil {
			return fmt.Errorf("mysql: applier: write schema version for %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil
	}
	return fmt.Errorf("mysql: applier: unknown change type %T", c)
}

// logZeroRowsAffected emits a debug-level log line when a target Exec
// reports zero rows affected. Resume idempotency depends on tolerating
// these (the comment in dispatch explains why), but a silent zero-
// rows-affected can also be the signature of a WHERE-predicate bug
// against a target row that exists but doesn't match — the silent
// failure mode of Bug 6. Logging it at debug level keeps the
// resume-idempotency contract intact while making the divergence
// visible to anyone investigating after the fact.
func logZeroRowsAffected(ctx context.Context, op, schema, table string, res sql.Result) {
	if res == nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		// RowsAffected is documented to return an error only when the
		// driver doesn't support it. go-sql-driver/mysql does, so we
		// shouldn't reach this branch — but we'd rather skip the log
		// than escalate a non-fatal driver quirk to a fatal error.
		return
	}
	if n == 0 {
		slog.DebugContext(
			ctx, "mysql: applier: zero rows affected",
			slog.String("op", op),
			slog.String("schema", schema),
			slog.String("table", table),
		)
	}
}

// pkFor returns the cached PK column list for the named table,
// loading it on the first sight of the table. An empty slice means
// "no PK" — Insert falls back to plain INSERT in that case.
// schema is the ALREADY-RESOLVED target database (the dispatch caller
// passes [routedSchema]'s result: the bound database in single-database
// mode, the change's source database in a cross-database multi-database
// write). It is used verbatim for both the cache key and the
// information_schema lookup — the routing decision lives entirely in the
// caller, so this helper must not re-apply applierSchema (that would
// override a cross-database lookup back to the bound database).
func (a *ChangeApplier) pkFor(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	qn := qualifiedName(schema, table)
	if cached, ok := a.pkCache[qn]; ok {
		return cached, nil
	}
	pk, err := loadPrimaryKey(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	a.pkCache[qn] = pk
	return pk, nil
}

// colTypesFor returns the cached column-name → IR type map for the
// named table, loading it on the first sight of the table. The map
// is consulted for every value the applier binds so prepareValue can
// shape JSON / Set / Geometry values for the driver — see the file-
// header comment for the JSON-column bug that makes this routing
// load-bearing.
//
// The reused machinery (loadTableSchema + translateType) is the same
// path the CDC reader takes to refresh its decoder cache after DDL,
// so any new IR type the schema reader learns is automatically
// available to the applier without further plumbing.
func (a *ChangeApplier) colTypesFor(ctx context.Context, _ *sql.Tx, schema, table string) (map[string]*ir.Column, error) {
	qn := qualifiedName(schema, table)
	if cached, ok := a.colTypeCache[qn]; ok {
		return cached, nil
	}
	// loadTableSchema queries information_schema directly; we use the
	// applier's *sql.DB rather than the open tx because the lookup is
	// effectively read-only metadata that is stable across the tx
	// boundary, and loadTableSchema's signature already takes a *sql.DB.
	// The pkFor helper uses the tx for symmetry with the data write,
	// but column-type metadata changes only on DDL, which sluice does
	// not interleave with row events on the applier side.
	// schema is the already-resolved target database (see pkFor) — used
	// verbatim for the lookup; the routing decision lives in the caller.
	tbl, err := loadTableSchema(ctx, a.db, schema, table)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*ir.Column, len(tbl.Columns))
	for _, col := range tbl.Columns {
		out[col.Name] = col
	}
	a.colTypeCache[qn] = out
	return out, nil
}

// routedSchema is the ADR-0074 Phase 1b namespace selector the dispatch
// path uses to qualify each change's table reference. It generalises
// [appliershared.Schema] to the multi-database fan-out case while
// preserving byte-identical single-database behaviour:
//
//   - Routing DISABLED (multiDBRouting == false; the default for every
//     single-database run, ALL engine pairs): returns
//     appliershared.Schema(a.schema, changeSchema) UNCHANGED — the bound
//     database when set, the change's schema only as a fallback. This is
//     the load-bearing back-compat guard. Note the cross-engine
//     single-database case (a PG source already populates Change.Schema,
//     differing from a MySQL target's bound database) lands here and
//     stays bound — qualifying on the differing namespace alone would
//     re-introduce the Phase-1a over-qualification regression.
//
//   - Routing ENABLED (multiDBRouting == true; a multi-database CDC
//     stream): qualifies with the change's source database ONLY when it
//     is non-empty AND differs from the bound `schema` — exactly the
//     cross-database case (`app_db` row applied while bound to a
//     control/different database). A change whose schema is empty or
//     equals the bound database returns the bound database, so an
//     in-bound-namespace change still emits the SAME bound SQL. This
//     mirrors emitAddForeignKey's "qualify across DIFFERING namespaces
//     only" rule.
func (a *ChangeApplier) routedSchema(changeSchema string) string {
	if a.multiDBRouting && changeSchema != "" && changeSchema != a.schema {
		return changeSchema
	}
	return appliershared.Schema(a.schema, changeSchema)
}

// loadPrimaryKey reads the PK columns for the named table from
// information_schema. Returns an empty slice (not nil) for tables
// with no PK; nil indicates a query error.
func loadPrimaryKey(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	const q = `
		SELECT column_name
		FROM   information_schema.statistics
		WHERE  table_schema = ?
		  AND  table_name   = ?
		  AND  index_name   = 'PRIMARY'
		ORDER  BY seq_in_index`

	rows, err := tx.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pk := make([]string, 0, 4)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pk = append(pk, col)
	}
	return pk, rows.Err()
}

// buildInsertSQL builds an INSERT statement using the row-alias
// UPSERT form (8.0.20+). With a non-empty PK the SET-list reassigns
// every non-PK column to the new row's value:
//
//	INSERT INTO `s`.`t` (`a`, `b`) VALUES (?, ?) AS new
//	ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
//
// With an empty PK list (tables without a PRIMARY KEY) it STILL emits
// ON DUPLICATE KEY UPDATE — with a full-row SET-list (every column ←
// its new value) — because MySQL fires that clause on a conflict
// against any unique index, not just the PK. This makes a no-PK table
// with a UNIQUE key idempotent on re-apply (the ADR-0072 Gap-2
// interlock); a truly keyless table never collides, so the clause is
// inert and behavior is effectively plain INSERT. See the
// ChangeApplier package doc for the full resume-idempotency contract.
//
// colTypes maps column names to their full IR descriptors and is the
// input to prepareValue. A missing entry (empty map, or column not
// present) is tolerated and the raw value is bound — the same
// pre-Bug-6 shape — so that callers without a populated cache
// (currently only unit tests pre-dating this fix) still produce
// valid SQL.
//
// The error return is the PG-applier signature convergence (repo-audit
// M2.1): today no MySQL value rule refuses, so it is always nil — it
// exists so a future error-producing value rule surfaces loudly on
// this path without another signature change. Same on the
// buildUpdateSQL / buildDeleteSQL / clause-builder siblings.
func buildInsertSQL(schema, table string, row ir.Row, pk []string, colTypes map[string]*ir.Column) (sqlStmt string, args []any, err error) {
	cols := appliershared.NonGeneratedRowKeys(row, colTypes)
	args = make([]any, 0, len(cols))
	colSQL := make([]string, len(cols))
	for i, c := range cols {
		colSQL[i] = quoteIdent(c)
		v, perr := prepareApplierValue(row[c], colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, v)
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	placeholders := strings.Repeat("?, ", len(cols))
	placeholders = strings.TrimSuffix(placeholders, ", ")

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableRef)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(colSQL, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(placeholders)
	sb.WriteByte(')')

	if len(pk) > 0 {
		// Row-alias UPSERT: every non-PK column gets reassigned to
		// the new row's value. PK columns are excluded from the
		// SET list because updating them on conflict would be a
		// no-op at best (PK columns equal by definition during the
		// conflict) and silently incorrect if the new and existing
		// rows have differing PK shapes.
		pkSet := make(map[string]struct{}, len(pk))
		for _, p := range pk {
			pkSet[p] = struct{}{}
		}
		nonPK := make([]string, 0, len(cols))
		for _, c := range cols {
			if _, isPK := pkSet[c]; !isPK {
				nonPK = append(nonPK, c)
			}
		}
		if len(nonPK) > 0 {
			sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
			parts := make([]string, len(nonPK))
			for i, c := range nonPK {
				parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
			}
			sb.WriteString(strings.Join(parts, ", "))
		} else {
			// Every column is a PK column — the row IS its own key.
			// On conflict there's nothing to update; emit
			// ON DUPLICATE KEY UPDATE with a no-op assignment so
			// the conflict is absorbed silently.
			sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
			sb.WriteString(quoteIdent(pk[0]))
			sb.WriteString(" = new.")
			sb.WriteString(quoteIdent(pk[0]))
		}
	} else {
		// No PRIMARY KEY. We still emit ON DUPLICATE KEY UPDATE so a
		// collision on ANY unique index is absorbed idempotently
		// rather than erroring with MySQL 1062 (duplicate-key). This
		// is the ADR-0072 Gap-2 interlock: the resumable cold-start
		// COPY routes the post-checkpoint catch-up rows through this
		// applier, and the checkpoint cadence (every 50k rows / 10s)
		// lags the COPY writer's flushes — so on resume vtgate
		// re-sends rows the target already holds (id > lastpk that
		// were flushed past the last checkpoint). On a no-PK table
		// with a UNIQUE key (the Bug-125 shape, e.g. `connections`)
		// those re-sends would otherwise collide on the unique index
		// and 1062 → terminal resume failure.
		//
		// ON DUPLICATE KEY UPDATE fires on a conflict against any
		// unique index, so the SET-list is what matters: set every
		// column to its new value (full-row upsert). That is correct
		// for both a re-emitted COPY row (overwrites with identical
		// data — a no-op) and a catch-up UPDATE that arrived as an
		// Insert (overwrites with the newer image).
		//
		// A TRULY keyless table (no PK AND no unique index) never
		// collides, so the ON DUPLICATE KEY UPDATE clause is inert
		// and behavior stays effectively plain-INSERT (the unchanged
		// best-effort path). Such tables are refused at cold-start by
		// the Bug-125 keyless guard, so they never reach resume.
		sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
		}
		sb.WriteString(strings.Join(parts, ", "))
	}
	return sb.String(), args, nil
}

// buildUpdateSQL builds an UPDATE statement. SET uses every column
// in After (including ones whose value didn't change — unchanged-
// column detection is a v1.5 optimization). WHERE uses every column
// in Before with NULL-aware predicate building.
func buildUpdateSQL(schema, table string, before, after ir.Row, colTypes map[string]*ir.Column) (sqlStmt string, args []any, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	setSQL, setArgs, err := buildSetClause(after, colTypes)
	if err != nil {
		return "", nil, err
	}
	whereSQL, whereArgs, err := buildWhereClause(before, colTypes)
	if err != nil {
		return "", nil, err
	}

	args = make([]any, 0, len(setArgs)+len(whereArgs))
	args = append(args, setArgs...)
	args = append(args, whereArgs...)
	return "UPDATE " + tableRef + " SET " + setSQL + " WHERE " + whereSQL, args, nil
}

// buildDeleteSQL builds a DELETE statement using the Before image
// as the WHERE predicate.
func buildDeleteSQL(schema, table string, before ir.Row, colTypes map[string]*ir.Column) (sqlStmt string, args []any, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	whereSQL, whereArgs, err := buildWhereClause(before, colTypes)
	if err != nil {
		return "", nil, err
	}
	return "DELETE FROM " + tableRef + " WHERE " + whereSQL, whereArgs, nil
}

// buildTruncateSQL builds a TRUNCATE TABLE statement.
func buildTruncateSQL(schema, table string) string {
	return "TRUNCATE TABLE " + quoteIdent(schema) + "." + quoteIdent(table)
}

// buildSetClause renders "col1 = ?, col2 = ?" for an UPDATE SET.
// NULL values bind through database/sql normally; no special form
// is needed in SET (unlike WHERE).
func buildSetClause(row ir.Row, colTypes map[string]*ir.Column) (clause string, args []any, err error) {
	cols := appliershared.NonGeneratedRowKeys(row, colTypes)
	parts := make([]string, len(cols))
	args = make([]any, 0, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c) + " = ?"
		v, perr := prepareApplierValue(row[c], colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, v)
	}
	return strings.Join(parts, ", "), args, nil
}

// buildWhereClause renders an AND-joined predicate with NULL-aware
// handling: nil row values produce "col IS NULL" (no parameter) so
// SQL's NULL semantics don't make the predicate unsatisfiable.
//
// JSON columns get a CAST(? AS JSON) on the right-hand side. The
// equality operator on a JSON-typed column compared to a plain
// string literal never matches in MySQL — the server doesn't
// implicitly cast the parameter to JSON, so `WHERE j = ?` returns
// zero rows even when the bound string is byte-equal to the stored
// document. CAST(? AS JSON) parses the parameter as JSON and the
// resulting JSON-vs-JSON comparison ignores formatting differences
// (whitespace, key order) the way operators expect. This is the
// SQL-side half of the Bug 6 silent-failure fix; the value-shaping
// half (prepareValue routing) is the other.
func buildWhereClause(row ir.Row, colTypes map[string]*ir.Column) (clause string, args []any, err error) {
	cols := appliershared.NonGeneratedRowKeys(row, colTypes)
	parts := make([]string, 0, len(cols))
	args = make([]any, 0, len(cols))
	for _, c := range cols {
		v := row[c]
		if v == nil {
			parts = append(parts, quoteIdent(c)+" IS NULL")
			continue
		}
		parts = append(parts, quoteIdent(c)+" = "+placeholderFor(colTypes, c))
		prepared, perr := prepareApplierValue(v, colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, prepared)
	}
	return strings.Join(parts, " AND "), args, nil
}

// placeholderFor returns the right-hand-side placeholder fragment
// for a column. JSON columns become CAST(? AS JSON) so MySQL's
// equality operator does a JSON-vs-JSON comparison rather than a
// JSON-vs-string-literal comparison (which silently never matches).
// Every other column type uses a bare ?.
func placeholderFor(colTypes map[string]*ir.Column, colName string) string {
	if colTypes == nil {
		return "?"
	}
	col, ok := colTypes[colName]
	if !ok || col == nil {
		return "?"
	}
	if _, isJSON := col.Type.(ir.JSON); isJSON {
		return "CAST(? AS JSON)"
	}
	return "?"
}

// prepareApplierValue is the applier's wrapper around prepareValue:
// it looks up the column's IR type and routes the value through the
// shared shaping helper from row_writer.go. When the column isn't in
// the map (cache cold or column unknown — defensive), it falls back
// to the raw value, mirroring the pre-Bug-6 behavior so the SQL is
// still valid in pathological setups.
//
// Routing through the shared helper rather than re-implementing the
// JSON []byte → string conversion here means new shaping rules added
// to prepareValue (for future IR types) are automatically picked up
// by the applier without touching this file.
//
// The error return is always nil today (MySQL's prepareValue is
// infallible) — it is the PG-applier signature convergence (repo-audit
// M2.1), reserved so a future refusing value rule propagates loudly
// through the builders without a signature change.
func prepareApplierValue(v any, colTypes map[string]*ir.Column, colName string) (any, error) {
	if colTypes == nil {
		return v, nil
	}
	col, ok := colTypes[colName]
	if !ok || col == nil {
		return v, nil
	}
	return prepareValue(v, col), nil
}

// (sortedKeys is shared with the schema reader — see schema_reader.go
// for the implementation. The applier uses it to render generated SQL
// in a deterministic column order.)
