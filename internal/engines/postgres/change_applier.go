// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// # Bug-6 fix: route applier args through prepareValue
//
// The MySQL side of this fix has the load-bearing motivation (see
// internal/engines/mysql/change_applier.go for the full write-up of
// the JSON-column wire-format bug — both the loud PG → MySQL and the
// silent MySQL → MySQL manifestations). The Postgres applier
// historically didn't surface the same failure mode because pgx
// inspects per-column type metadata before sending parameters, so
// JSON values arrived correctly even without the shaping path.
//
// However, the structural omission was symmetric: the PG applier
// also bypassed prepareValue, which means ir.Array values (canonical
// []any) and ir.Geometry values (raw WKB needing EWKB framing) would
// hit pgx in their IR shape rather than the driver-acceptable form
// the bulk-copy path uses. Mirroring the MySQL fix here keeps the
// two engines symmetric and inoculates the PG applier against the
// same class of bug for any future IR type whose shaping is
// non-trivial.
//
// As on the MySQL side, dispatch logs zero-rows-affected at debug
// level for Update / Delete so the silent-divergence failure mode
// has at least one observable footprint.

// ChangeApplier applies [ir.Change] events to a Postgres target,
// one source change per target transaction. It implements
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
//   - **Tables without any PK fall back to plain INSERT.** Postgres'
//     ON CONFLICT requires a unique-index target; without one, the
//     syntax is unusable. Plain INSERT means a re-applied Insert
//     produces a duplicate row. Resume idempotency on no-PK tables
//     is therefore best-effort, and continuous-sync on such tables
//     is not recommended. Add a PRIMARY KEY to the source table
//     before running sluice in continuous-sync mode.
//
//   - **Tables with a UNIQUE INDEX/CONSTRAINT but no PRIMARY KEY**
//     would be a candidate conflict target on PG (unlike MySQL),
//     but sluice doesn't special-case it. The applier behaves as
//     if there's no PK (plain INSERT path). If you need upsert
//     semantics here, declare the unique column as the PRIMARY KEY
//     on the source table.
//
// # Lifecycle
//
// One applier per target connection pool. Apply is single-goroutine:
// it consumes the change channel sequentially to preserve source
// ordering. Concurrent calls on the same applier are not supported.
type ChangeApplier struct {
	db *sql.DB
	// schema is the namespace user-data INSERT/UPDATE/DELETE / TRUNCATE
	// land in. Defaults to the DSN's `schema` query parameter (typically
	// `public`); operator-overridable at startup via [SetSchema] when
	// `--target-schema NAME` is supplied (ADR-0031). The override
	// does NOT move the per-target sluice_cdc_state control table —
	// see controlSchema.
	schema string

	// controlSchema is the namespace `sluice_cdc_state` lives in.
	// Pinned to the DSN-derived schema at construction time; never
	// moved by [SetSchema]. The split exists because multi-source
	// aggregation (ADR-0031) wants per-source user-data schemas
	// (`customer_svc.users`, `billing_svc.users`) but a single
	// shared control table per target so cross-stream `sync status`
	// keeps reading every recorded stream-id from one place.
	controlSchema string

	// slotName is the active stream's resolved replication-slot name,
	// set by [SetSlotName] at Streamer startup. Threaded into every
	// [writePositionTx] call so the per-target sluice_cdc_state row's
	// slot_name column stays in sync with what the streamer is
	// actually consuming. Recovered later by `sluice schema add-table
	// --no-drain` (ADR-0030) to read the right slot's
	// confirmed_flush_lsn for the live-add LSN-floor check. Empty
	// when the streamer hasn't called [SetSlotName] yet (e.g. broker
	// chain-handoff WritePosition before any sync start has run);
	// the row's existing slot_name is preserved on the conflict path.
	slotName string

	// sourceFingerprint is the truncated SHA-256 hex of the streamer's
	// source DSN host+port+database tuple, set by
	// [SetSourceDSNFingerprint] at Streamer startup. Threaded into
	// every [writePositionTx] call so the per-target sluice_cdc_state
	// row's source_dsn_fingerprint column stays in sync with what
	// the streamer is consuming. Used for stream-id collision
	// detection (ADR-0031): a future `sync start` against the same
	// target that supplies the same stream-id but a different source
	// DSN will see a fingerprint mismatch on its ListStreams probe
	// and refuse loudly. Empty preserves the row's existing value
	// (legacy / engine-not-supported / pre-streamer chain handoff).
	sourceFingerprint string

	// targetSchema is the operator-supplied `--target-schema NAME`
	// recorded by [SetTargetSchema] at Streamer startup
	// (ADR-0031, Bug 46). Threaded into every [writePositionTx] call
	// so the per-target sluice_cdc_state row's target_schema column
	// stays in sync with what the streamer is routing CDC events
	// to. `sluice schema add-table` reads this column back to resolve
	// the active stream's target-schema namespace and refuse a
	// mismatch loudly (the v0.25.0 silent-event-drop failure mode).
	// Empty preserves the row's existing value (streams started
	// without --target-schema, legacy rows, chain-handoff
	// WritePosition without streamer context).
	//
	// Distinct from a.schema: a.schema is the user-data namespace
	// the applier currently writes into; targetSchema records the
	// operator's intent so a mismatch can be detected. With the same
	// engine running in both modes (initial start + later add-table)
	// the values agree, but the recorded column is the canonical
	// resume signal.
	targetSchema string

	// pkCache maps "schema.table" → ordered list of PK column names.
	// Populated lazily via a single information_schema query the
	// first time a change for the table arrives. An empty slice
	// (length 0) means "table exists but has no PK" — in that case
	// Insert falls back to plain INSERT (see the package comment).
	pkCache map[string][]string

	// colTypeCache maps "schema.table" → column-name → IR type. It
	// is the input to prepareValue for every value the applier
	// binds; see the file-header comment for the JSON-column bug
	// the parallel MySQL fix exists to address. Populated lazily on
	// the first sight of a table — same shape as pkCache.
	colTypeCache map[string]map[string]ir.Type

	// generatedColCache maps "schema.table" → set of column names
	// that are GENERATED ALWAYS AS (...) STORED on the target. The
	// applier's SQL builders filter these out of every INSERT column
	// list, UPDATE SET, and UPDATE/DELETE WHERE predicate — PG
	// rejects non-DEFAULT values on generated columns (SQLSTATE 428C9
	// "cannot insert a non-DEFAULT value into column"). Mirrors the
	// bulk-load writer's existing filter; closes GitHub issue #12.
	// Populated alongside colTypeCache in colTypesFor; the parallel
	// map keeps the existing cache contract local-friendly while
	// adding the generated-column awareness the apply path needs.
	generatedColCache map[string]map[string]bool

	// lsnFeedback is the slot-ack-after-apply tracker (Bug 15,
	// ADR-0020). The applier reports the LSN of each successfully-
	// committed change here; the [CDCReader] reads from the same
	// tracker on its keepalive path so the slot's
	// confirmed_flush_lsn never advances past durably-applied
	// work. nil tracker means "no feedback wired" — the applier
	// runs as before, and the reader falls back to streamed-LSN
	// keepalives.
	lsnFeedback *lsnTracker

	// maxBufferBytes is the soft byte-size cap on the in-flight
	// batch's buffered change values during ApplyBatch. Implements
	// [ir.MaxBufferBytesSetter] via [SetMaxBufferBytes]. Zero or
	// negative means "no byte cap"; the row-count cap remains the
	// only flush trigger. See ADR-0028.
	maxBufferBytes int64

	// execTimeout is the per-statement deadline applied to every
	// tx.ExecContext call on the apply path. Set by [SetExecTimeout]
	// when the streamer observes the operator's --apply-exec-timeout
	// flag (GitHub issue #23 Phase B fix, v0.52.0). Zero or negative
	// means "no per-exec timeout" — the legacy v0.51.0 behaviour where
	// the apply call inherits only the streamer's parent context.
	//
	// When non-zero, each Exec is wrapped in context.WithTimeout. On
	// expiry the pgx driver's context-watcher closes the underlying
	// connection and returns context.DeadlineExceeded, which the
	// applier's [classifyApplierError] treats as retriable. The
	// existing runWithRetry loop then re-opens the applier and retries
	// the batch.
	//
	// Closes the silent-stall failure mode where a half-closed
	// destination connection blocked the apply goroutine indefinitely
	// inside crypto/tls.(*Conn).Read.
	execTimeout time.Duration

	// redactor is the operator-configured PII redaction registry
	// (Phase 1.5, roadmap item 15a follow-on). Symmetric with the
	// MySQL applier; see that engine for the design.
	redactor *redact.Registry

	// shardColumn / shardValue are the operator-configured
	// Shape-A discriminator (ADR-0048; CLI:
	// `--inject-shard-column NAME=VALUE`). When shardColumn != "",
	// the apply path stamps the value onto every row-bearing
	// change before dispatch — mirror of the MySQL applier; see
	// that engine for the full design.
	shardColumn string
	shardValue  any

	// streamID is the active stream's identifier, recorded by
	// [SetStreamID] at Streamer startup. Threaded through every
	// redactor.ApplyRow call so randomize:* strategies (PII Phase 2.c,
	// v0.59.0) derive a per-row replay-stable seed from streamID +
	// table + column + PK values. Empty for direct API users that
	// don't go through the streamer (chain-restore, broker, etc.);
	// randomize:* still works (the seed remains stable per
	// (table, column, PK) tuple) but operators wanting cross-stream
	// determinism should set the stream-id explicitly.
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

// SetExecTimeout sets the per-statement deadline applied to every
// tx.ExecContext call on the apply path. Implements the pipeline's
// applierExecTimeoutSetter optional interface (GitHub issue #23
// Phase B fix, v0.52.0). Zero or negative disables the timeout.
func (a *ChangeApplier) SetExecTimeout(d time.Duration) {
	a.execTimeout = d
}

// SetRedactor implements [ir.RedactorSetter]. PII Phase 1.5; same
// shape as the MySQL applier's SetRedactor — see that engine for
// the doc-comment + the type-assertion rationale.
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
// per-row seed for randomize:* strategies. Idempotent; the
// streamer may call this on every Run.
func (a *ChangeApplier) SetStreamID(streamID string) {
	a.streamID = streamID
}

// SetShardColumn implements [ir.ShardColumnSetter] (ADR-0048
// Shape A). Mirror of the MySQL applier; see that engine for the
// full design. Empty name clears the wiring (no-stamp default).
// Idempotent.
func (a *ChangeApplier) SetShardColumn(name string, value any) {
	a.shardColumn = name
	a.shardValue = value
}

// stampShardChange stamps the operator-supplied discriminator
// onto every row-bearing change before dispatch. Empty
// shardColumn is the no-op fast path. Mirror of the MySQL
// applier's stampShardChange; both engines must apply the same
// {Insert.Row, Update.Before/After, Delete.Before} scope to keep
// CDC events from per-shard streams identifiable on the
// consolidated target. Non-row changes (Truncate / TxBegin /
// TxCommit / SchemaSnapshot) pass through.
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

// redactChange mirrors the MySQL applier's redactChange. nil/empty
// redactor is the no-op fast path. PII Phase 1.5 row-data scope:
// Insert.Row, Update.Before/After, Delete.Before.
//
// PII Phase 2.c (v0.59.0): every ApplyRow call passes the table's
// PK column list + active streamID so randomize:* strategies
// derive a per-row replay-stable seed. The PK is fetched via the
// existing per-table pkCache (one info_schema round-trip per
// table on first sight). A nil tx falls back to the applier's
// *sql.DB connection — schema metadata is stable across the tx
// boundary so this is safe.
func (a *ChangeApplier) redactChange(ctx context.Context, c ir.Change) error {
	if a.redactor.Empty() {
		return nil
	}
	switch v := c.(type) {
	case ir.Insert:
		pk, err := a.pkForRedact(ctx, applierSchema(a.schema, v.Schema), v.Table)
		if err != nil {
			return err
		}
		return a.redactor.ApplyRow(v.Schema, v.Table, pk, v.Row, a.streamID)
	case ir.Update:
		pk, err := a.pkForRedact(ctx, applierSchema(a.schema, v.Schema), v.Table)
		if err != nil {
			return err
		}
		if err := a.redactor.ApplyRow(v.Schema, v.Table, pk, v.Before, a.streamID); err != nil {
			return err
		}
		return a.redactor.ApplyRow(v.Schema, v.Table, pk, v.After, a.streamID)
	case ir.Delete:
		pk, err := a.pkForRedact(ctx, applierSchema(a.schema, v.Schema), v.Table)
		if err != nil {
			return err
		}
		return a.redactor.ApplyRow(v.Schema, v.Table, pk, v.Before, a.streamID)
	}
	return nil
}

// pkForRedact returns the cached PK column list for the named
// table using the applier's connection pool. Wrapper that lets
// redactChange call pkFor without an open *sql.Tx — schema
// metadata is stable across tx boundaries, so a DB-level query
// is safe here and avoids the per-change tx-open cost when the
// applier hasn't seen the table yet.
//
// Returns nil PK when the table has no primary key; randomize:*
// strategies then refuse with a clear error (preflight should
// catch the no-PK case before CDC events arrive, but defense-
// in-depth applies here).
func (a *ChangeApplier) pkForRedact(ctx context.Context, schema, table string) ([]string, error) {
	qn := schema + "." + table
	if cached, ok := a.pkCache[qn]; ok {
		return cached, nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: pkForRedact: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	pk, err := loadPrimaryKey(ctx, tx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: pkForRedact: %w", err)
	}
	a.pkCache[qn] = pk
	return pk, nil
}

// forceSynchronousCommitOn emits `SET LOCAL synchronous_commit = on`
// as the first statement on every apply transaction. Hardens
// ADR-0007's "position + data in one tx" durability guarantee against
// the silent-loss failure mode where a PG role or database default of
// `synchronous_commit = off` (settable via `ALTER ROLE … SET
// synchronous_commit = off` or `ALTER DATABASE … SET …`) is inherited
// by the sluice apply session.
//
// Without this hardening, a `COMMIT` ACK from PG would return to
// sluice BEFORE the WAL is durably flushed to disk; a target-side
// crash between the ACK and the WAL flush would silently lose the
// position+data tx despite sluice having persisted forward, breaking
// the ADR-0007 atomicity contract. See PG Internals Ch 9.5
// (asynchronous commit semantics) and Ch 11.2 (role/db-level
// inheritance) for the underlying behaviour.
//
// `SET LOCAL` scope reverts automatically at tx end, so non-sluice
// sessions on the same role keep whatever value the operator
// configured for them. A session that already has
// `synchronous_commit = on` (the PG default) sees no behaviour
// change — the statement is an effective no-op there.
//
// Severity-A finding F7 from the 2026-05-22 PG-internals research
// run (durable findings doc:
// `sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md`).
func (a *ChangeApplier) forceSynchronousCommitOn(ctx context.Context, tx *sql.Tx) error {
	if _, err := a.txExec(ctx, tx, "SET LOCAL synchronous_commit = on"); err != nil {
		return fmt.Errorf("postgres: applier: force synchronous_commit=on: %w", err)
	}
	return nil
}

// txExec wraps tx.ExecContext with the applier's per-exec timeout
// (when set). On timeout expiry the pgx driver's ctx-watcher closes
// the underlying connection; the resulting context.DeadlineExceeded
// is classified retriable by [classifyApplierError] so the
// runWithRetry loop activates.
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
// a half-closed destination connection could still stall the apply
// goroutine indefinitely inside [writePositionTx]'s bare
// `tx.ExecContext`. Wrapping at the call site closes that gap.
func (a *ChangeApplier) execTimeoutCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.execTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, a.execTimeout)
}

// commitWithTimeout runs tx.Commit() under the per-exec watchdog
// (see [runWithDeadline] for the semantics and Bug 56 / v0.52.1
// rationale). Thin wrapper that exists so callers don't have to
// thread the closure manually.
func (a *ChangeApplier) commitWithTimeout(tx *sql.Tx) error {
	return runWithDeadline(a.execTimeout, tx.Commit)
}

// runWithDeadline runs f under a wall-clock deadline of `timeout`.
// Zero or negative timeout is a passthrough (f runs to completion
// inline). For positive timeouts, f runs in a goroutine and we race
// its return against a time.After: whichever wins, wins.
//
// On timeout we return [context.DeadlineExceeded] (classified
// retriable by [classifyApplierError]) so the runWithRetry loop
// reopens the applier on a fresh connection. The orphaned f goroutine
// cannot be cancelled — it will eventually return when the underlying
// state (typically a TCP socket the caller closes via Close()) errors
// out. One orphaned goroutine per timeout event is the bounded cost
// of closing the silent-stall failure mode.
//
// Used by [commitWithTimeout] because [database/sql.Tx.Commit] takes
// no context. Pulled out as a package-level function so it's testable
// without constructing a real *sql.Tx; the watchdog race semantics
// are non-trivial enough to deserve direct coverage.
//
// Bug 56 (v0.52.1): the apply path's third TLS-read surface (after
// dispatch's tx.ExecContext + writePositionTx) is the implicit commit
// flush. Pre-v0.52.1 it had no deadline; goroutine pprof on a v0.52.0
// stall showed goroutine 1 blocked at tx.Commit() for >10 min.
func runWithDeadline(timeout time.Duration, f func() error) error {
	if timeout <= 0 {
		return f()
	}
	done := make(chan error, 1)
	go func() { done <- f() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

// LSNTracker returns the applier's applied-LSN feedback channel.
// The [pipeline.Streamer] uses the structural interface
// `lsnTrackerProvider` to fetch this and hand it to the
// [CDCReader]'s `AttachLSNTracker`. Lazily allocated so callers
// that never wire the streamer (tests, direct API users) don't pay
// the cost.
func (a *ChangeApplier) LSNTracker() any {
	if a.lsnFeedback == nil {
		a.lsnFeedback = newLSNTracker()
	}
	return a.lsnFeedback
}

// reportAppliedToken extracts the LSN from a position token and
// reports it to the tracker. Token-parse errors are logged at debug
// level rather than propagated — losing one tracker update doesn't
// invalidate the batch we just successfully committed, and a malformed
// token is itself worth surfacing via a debug line for diagnosis.
//
// No-op when no tracker is wired (the legacy v0.4.0 shape) or when
// the token doesn't carry a valid LSN. Single-call cost is one JSON
// unmarshal + one LSN parse + one atomic CAS.
func (a *ChangeApplier) reportAppliedToken(ctx context.Context, token string) {
	if a.lsnFeedback == nil {
		return
	}
	lsn, err := lsnFromPositionToken(token)
	if err != nil {
		slog.DebugContext(ctx, "postgres: applier: applied-LSN report skipped (parse failure)",
			slog.String("err", err.Error()))
		return
	}
	if lsn == 0 {
		return
	}
	a.lsnFeedback.ReportApplied(lsn)
}

// _ keeps pglogrepl in the import list when the file is built
// without the lsn_tracker.go's symbols being referenced from this
// translation unit (defensive for future refactors that move the
// helper around).
var _ pglogrepl.LSN

// Close releases the underlying connection pool.
func (a *ChangeApplier) Close() error {
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}

// EnsureControlTable creates the per-target sluice_cdc_state table
// (in the applier's controlSchema — the DSN-derived namespace, NOT
// the operator-supplied --target-schema) if it doesn't exist.
// Idempotent.
//
// The split between data schema and control schema (ADR-0031) means
// `sluice_cdc_state` stays in `public` (or whatever the DSN selects)
// even when user data lands in `customer_svc.users` etc. One control
// table per target host serves multiple target-schema streams.
// The ADR-0049 sluice_cdc_schema_history table is created in the same
// controlSchema, additively — it never touches sluice_cdc_state data.
//
// Wrapped in [retryOnCatalogRace] to defend against the
// `pg_type_typname_nsp_index` / `pg_class_relname_nsp_index` SQLSTATE
// 23505 race that fires when N concurrent shard streams call this
// against a fresh target tightly (the ADR-0054 Phase 2e test surface
// — Task #29). All three inner ensure-table calls are idempotent
// CREATE TABLE IF NOT EXISTS, so retrying the whole bundle is safe
// even if an earlier inner call already succeeded.
func (a *ChangeApplier) EnsureControlTable(ctx context.Context) error {
	return retryOnCatalogRace(ctx, func() error {
		if err := ensureControlTable(ctx, a.db, a.controlSchema); err != nil {
			return err
		}
		if err := ensureSchemaHistoryTable(ctx, a.db, a.controlSchema); err != nil {
			return err
		}
		return ensureShardConsolidationLeaseTable(ctx, a.db, a.controlSchema)
	})
}

// retryOnCatalogRace wraps fn with a bounded retry on the narrow
// SQLSTATE 23505 race that PG raises on concurrent `CREATE TABLE IF
// NOT EXISTS` against a fresh schema. PG's IF NOT EXISTS check is on
// pg_class, but the table's row type is also allocated in pg_type —
// a concurrent CREATE TABLE for the same name has pre-allocated the
// pg_type row by the time the second one checks pg_class, and the
// `pg_type_typname_nsp_index` (or `pg_class_relname_nsp_index`) unique
// constraint then fires.
//
// Scope: the retry ONLY triggers for the specific pg_type / pg_class
// catalog-race shape (constraint-name match). Other 23505 cases
// (user-table unique violations, etc.) stay non-retriable per
// ADR-0038 — see [classifyError]. Three attempts with 50ms / 100ms /
// 200ms jitter-free backoff (the race resolves within milliseconds in
// practice; longer waits would mask non-race causes).
//
// Bug 84 cycle / ADR-0054 Phase 2e Task #29.
func retryOnCatalogRace(ctx context.Context, fn func() error) error {
	delays := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	var lastErr error
	for attempt := 0; attempt <= len(delays); attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isCatalogRaceError(err) {
			return err
		}
		if attempt == len(delays) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delays[attempt]):
		}
	}
	return lastErr
}

// isCatalogRaceError reports whether err is the narrow
// pg_type_typname_nsp_index / pg_class_relname_nsp_index SQLSTATE
// 23505 we want to retry on. False for every other 23505 (user-data
// uniqueness violations stay loud).
func isCatalogRaceError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	if pgErr.Code != "23505" {
		return false
	}
	return pgErr.ConstraintName == "pg_type_typname_nsp_index" ||
		pgErr.ConstraintName == "pg_class_relname_nsp_index"
}

// CompactSchemaHistoryBelow implements [ir.SchemaHistoryCompactor]
// (ADR-0049 Chunk D). Deletes sluice_cdc_schema_history rows in the
// applier's controlSchema whose anchor_position is STRICTLY OLDER than
// floor under this engine's [ir.PositionOrderer]. See
// compactSchemaHistoryBelow for the strict-older semantics + loud-floor
// preservation invariant.
func (a *ChangeApplier) CompactSchemaHistoryBelow(ctx context.Context, floor ir.Position) (int, error) {
	return compactSchemaHistoryBelow(ctx, a.db, Engine{}, a.controlSchema, floor)
}

// ReadPosition returns the last persisted source position for
// streamID, or ok=false when no row exists. The returned Position
// always has Engine = "postgres" — the persisted token does NOT carry
// engine identity (the `sluice_cdc_state` table has no
// `position_engine` column). For broker-driven rows the engine
// sentinel lives inside the JSON envelope (`_engine` field, see
// pipeline.isBrokerToken). Bug 39 (v0.20.1) is the load-bearing
// rationale for that envelope; the broker discriminates on the
// embedded sentinel, not on Position.Engine.
func (a *ChangeApplier) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	token, ok, err := readPosition(ctx, a.db, a.controlSchema, streamID)
	if err != nil {
		return ir.Position{}, false, err
	}
	if !ok {
		return ir.Position{}, false, nil
	}
	return ir.Position{Engine: engineNamePostgres, Token: token}, true, nil
}

// ListStreams returns all rows in the per-target control table.
// Used by `sluice sync status` for operational visibility. Tolerant
// of the table being absent — operators querying status against a
// fresh target should see "no streams" rather than an error.
func (a *ChangeApplier) ListStreams(ctx context.Context) ([]ir.StreamStatus, error) {
	return listStreams(ctx, a.db, a.controlSchema, engineNamePostgres)
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
		return errors.New("postgres: applier: RequestStop: streamID is empty")
	}
	return requestStop(ctx, a.db, a.controlSchema, streamID)
}

// ReadStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. The pipeline's Streamer poll
// goroutine consults this method via a structural interface (the
// internal pipeline.stopFlagReader). Exported because Go's method-
// set rules require an exported method to satisfy an interface from
// another package — even when that interface is itself unexported.
func (a *ChangeApplier) ReadStopRequested(ctx context.Context, streamID string) (bool, error) {
	return readStopRequested(ctx, a.db, a.controlSchema, streamID)
}

// ClearStopRequested resets stop_requested_at to NULL for the named
// stream. The Streamer calls this at startup so a previous
// `sluice sync stop` doesn't leave a sticky signal that immediately
// exits the next `sluice sync start` (Bug 11 in v0.3.2 testing).
// Idempotent and tolerant of a missing row.
func (a *ChangeApplier) ClearStopRequested(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: ClearStopRequested: streamID is empty")
	}
	return clearStopRequested(ctx, a.db, a.controlSchema, streamID)
}

// ClearStream deletes the named stream's row from the per-target
// sluice_cdc_state table. Used by the `--reset-target-data` recovery
// path (ADR-0023). Implements [ir.StreamCleaner]. Idempotent and
// tolerant of a missing row or table.
func (a *ChangeApplier) ClearStream(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: ClearStream: streamID is empty")
	}
	return clearStream(ctx, a.db, a.controlSchema, streamID)
}

// WritePosition implements [ir.PositionWriter]: upserts the
// position row for streamID in `sluice_cdc_state` without any
// accompanying data write. Used by Phase 4.5's broker for
// cold-start initial-position writes and schema-delta-only
// incrementals (no change chunks → no Apply path to ride along
// with).
//
// Wraps the same writePositionTx helper the Apply path uses, so
// the row shape and idempotency contract are identical. EncodeBroker
// callers pass [pipeline.BackupBrokerPositionEngine]-tagged positions
// here; the engine column is stored as part of the JSON token (see
// the v0.20.0 Phase 4.5 design), so the same writePositionTx call
// works without a schema change.
func (a *ChangeApplier) WritePosition(ctx context.Context, streamID string, pos ir.Position) error {
	if streamID == "" {
		return errors.New("postgres: applier: WritePosition: streamID is empty")
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: applier: WritePosition: begin tx: %w", err)
	}
	// F7: pin synchronous_commit on for the duration of this tx so a
	// role/db-level default of `off` can't silently break ADR-0007's
	// "position lands durably with the data" contract.
	if err := a.forceSynchronousCommitOn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	err = writePositionTx(posCtx, tx, a.controlSchema, streamID, pos.Token, a.slotName, a.sourceFingerprint, a.targetSchema)
	posCancel()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := a.commitWithTimeout(tx); err != nil {
		return fmt.Errorf("postgres: applier: WritePosition: commit: %w", err)
	}
	return nil
}

// SetSlotName records the active stream's resolved replication-slot
// name on the applier so subsequent position-writes (Apply +
// WritePosition + the batched-apply path) populate the
// sluice_cdc_state row's slot_name column. Phase 2 mid-stream live
// add-table (ADR-0030) reads the recorded slot back via [ListStreams]
// to look up the correct confirmed_flush_lsn before publication-add.
//
// Idempotent — the streamer may call this on every Run; the value
// stays current across warm resumes (the slot can change if the
// operator restarts with a different `--slot-name`). Empty input is
// allowed (no-op set) and reflects the engine-default slot
// (`sluice_slot`) — callers that need the resolved default should
// fill it in before calling.
func (a *ChangeApplier) SetSlotName(slotName string) {
	a.slotName = slotName
}

// SetSchema implements [ir.SchemaSetter]. Records the per-source
// target-schema namespace operator-supplied via `--target-schema NAME`
// (ADR-0031). User-data INSERT/UPDATE/DELETE / TRUNCATE land in the
// named schema; the per-target `sluice_cdc_state` table stays in the
// applier's `controlSchema` (pinned at construction time). Empty
// input is a no-op (preserves the DSN-derived default). Idempotent.
func (a *ChangeApplier) SetSchema(name string) {
	if name == "" {
		return
	}
	a.schema = name
}

// SetSourceDSNFingerprint implements [ir.SourceFingerprintRecorder].
// Records the source DSN fingerprint the streamer computed at startup
// (ADR-0031) so subsequent position-writes populate the
// sluice_cdc_state row's source_dsn_fingerprint column. Idempotent;
// the streamer may call this on every Run.
//
// Empty input is allowed (no-op set) and reflects the
// pre-fingerprinting case (legacy / engine-not-supported / direct
// WritePosition from the broker without a streamer). The COALESCE
// pattern in writePositionTx preserves any previously-recorded value
// in that case.
func (a *ChangeApplier) SetSourceDSNFingerprint(fingerprint string) {
	a.sourceFingerprint = fingerprint
}

// SetTargetSchema records the operator-supplied `--target-schema NAME`
// (ADR-0031, Bug 46) so subsequent position-writes populate the
// sluice_cdc_state row's target_schema column. Idempotent; the
// streamer may call this on every Run.
//
// Empty input is allowed (no-op preservation of an existing recorded
// value via the COALESCE in writePositionTx) and reflects the
// pre-flag case (legacy / streams started without --target-schema /
// direct WritePosition from the broker without a streamer). The
// COALESCE pattern in writePositionTx preserves any
// previously-recorded value in that case.
//
// Distinct from [SetSchema]: SetSchema mutates the user-data
// namespace the applier writes into (a.schema); SetTargetSchema
// records the operator's stated intent (a.targetSchema) so a
// subsequent `sluice schema add-table` can detect a mismatch
// between the operator-supplied flag and the active stream's
// recorded namespace.
func (a *ChangeApplier) SetTargetSchema(name string) {
	a.targetSchema = name
}

// Apply consumes changes from the channel and applies each to the
// target in its own transaction. The position write happens inside
// the same transaction as the data write (per ADR-0007); a crash
// between them rolls back both, so progress and data can never
// diverge.
//
// Returns when the channel closes (clean shutdown), when ctx is
// cancelled, or when a target write fails.
//
// Per-apply DEBUG instrumentation (v0.53.0): mirror of the MySQL
// applier — see that engine's `Apply` doc for the v0.52.0 cycle
// finding that default-batch-size operators had no DEBUG signal on
// the non-batched path. Per-change `applier: apply latency` line
// closes the gap.
func (a *ChangeApplier) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	if streamID == "" {
		return errors.New("postgres: applier: streamID is empty (Streamer is responsible for resolving it)")
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
// transaction. After a successful commit, the change's LSN is
// reported to the slot-ack feedback tracker (Bug 15, ADR-0020) so
// the [CDCReader]'s keepalive routine can advance
// confirmed_flush_lsn past this change.
func (a *ChangeApplier) applyOne(ctx context.Context, streamID string, c ir.Change) error {
	// PII Phase 1.5: redact CDC row data before dispatch.
	if err := a.redactChange(ctx, c); err != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: redact: %w", err))
	}
	// ADR-0048 Shape A: stamp the operator-supplied discriminator
	// (`--inject-shard-column NAME=VALUE`) onto every row-bearing
	// change before dispatch. Empty shardColumn is a no-op fast
	// path — single-source streams pay zero cost.
	a.stampShardChange(c)
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: begin tx: %w", err))
	}
	// F7: pin synchronous_commit on for the duration of this tx so a
	// role/db-level default of `off` can't silently break ADR-0007's
	// "position + data lands durably together" contract.
	if err := a.forceSynchronousCommitOn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return classifyApplierError(err)
	}
	if err := a.dispatch(ctx, tx, streamID, c); err != nil {
		_ = tx.Rollback()
		return classifyApplierError(err)
	}
	token := c.Pos().Token
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	err = writePositionTx(posCtx, tx, a.controlSchema, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
	posCancel()
	if err != nil {
		_ = tx.Rollback()
		return classifyApplierError(err)
	}
	if err := a.commitWithTimeout(tx); err != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: commit: %w", err))
	}
	// ADR-0049 Chunk C cache-after-commit invariant: a SchemaSnapshot
	// updates the active-version cache ONLY after its tx has
	// committed durably. A failed dispatch or commit short-circuits
	// above; the cache is never mutated on the rolled-back path.
	if snap, isSnap := c.(ir.SchemaSnapshot); isSnap {
		a.cacheActiveSchemaAfterCommit(snap)
	}
	a.reportAppliedToken(ctx, token)
	return nil
}

// errUnknownTable is the sentinel the colTypesFor lookup returns
// when a CDC event references a table that doesn't exist on the
// target. The applier skips such events with a warning rather than
// erroring out (Bug 13 defence-in-depth — the primary fix is
// scoping the publication to the source-side table list, but a
// drifted publication or a manually-altered scope shouldn't crash
// the whole stream).
var errUnknownTable = errors.New("postgres: applier: target table does not exist")

// dispatch routes a single change to its SQL form on the open tx.
//
// Events targeting a table that doesn't exist on the destination
// are skipped with a warning (defence-in-depth for Bug 13). The
// publication scope-by-table fix in [ensurePublication] keeps these
// out of the WAL stream in the normal case; this branch handles
// drift (a manually-altered publication, a stale schema cache,
// etc.) without taking the whole stream down.
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
		// ADR-0036 Phase A.3: applier-side capture probe. Logged at
		// DEBUG so it surfaces under the diagnose test's JSON debug
		// handler but stays silent in normal runs. Distinguishes M5c
		// (applier-side drop) from M5a/M5b (pgoutput filter or
		// streamer-snapshot-handoff race): if a missing row's body
		// appears here, the applier received the event and dropped it
		// (or failed downstream); if not, it never reached the
		// applier and the loss is upstream. See the diagnose test's
		// renderVerdictM5Attribution for the cross-reference.
		diagApplierInsertReceived(ctx, a.schema, v)
		schema := applierSchema(a.schema, v.Schema)
		pk, err := a.pkFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: pk lookup for %s.%s: %w", schema, v.Table, err)
		}
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if errors.Is(err, errUnknownTable) {
			logUnknownTable(ctx, "insert", schema, v.Table)
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		generated := a.generatedColsFor(schema, v.Table)
		stmt, args, err := buildInsertSQL(schema, v.Table, v.Row, pk, colTypes, generated)
		if err != nil {
			return fmt.Errorf("postgres: applier: build insert for %s.%s: %w", schema, v.Table, err)
		}
		if _, err := a.txExec(ctx, tx, stmt, args...); err != nil {
			return fmt.Errorf("postgres: applier: insert into %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.Update:
		schema := applierSchema(a.schema, v.Schema)
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if errors.Is(err, errUnknownTable) {
			logUnknownTable(ctx, "update", schema, v.Table)
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		generated := a.generatedColsFor(schema, v.Table)
		stmt, args, err := buildUpdateSQL(schema, v.Table, v.Before, v.After, colTypes, generated)
		if err != nil {
			return fmt.Errorf("postgres: applier: build update for %s.%s: %w", schema, v.Table, err)
		}
		// Update misses are tolerated (zero rows affected) for resume
		// idempotency; the same caveat as MySQL applies — see the
		// MySQL applier's dispatch comment for the rationale and the
		// debug-log defence-in-depth.
		res, err := a.txExec(ctx, tx, stmt, args...)
		if err != nil {
			return fmt.Errorf("postgres: applier: update %s.%s: %w", schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "update", schema, v.Table, res)
		return nil

	case ir.Delete:
		schema := applierSchema(a.schema, v.Schema)
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if errors.Is(err, errUnknownTable) {
			logUnknownTable(ctx, "delete", schema, v.Table)
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		generated := a.generatedColsFor(schema, v.Table)
		stmt, args, err := buildDeleteSQL(schema, v.Table, v.Before, colTypes, generated)
		if err != nil {
			return fmt.Errorf("postgres: applier: build delete for %s.%s: %w", schema, v.Table, err)
		}
		res, err := a.txExec(ctx, tx, stmt, args...)
		if err != nil {
			return fmt.Errorf("postgres: applier: delete from %s.%s: %w", schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "delete", schema, v.Table, res)
		return nil

	case ir.Truncate:
		schema := applierSchema(a.schema, v.Schema)
		stmt := buildTruncateSQL(schema, v.Table, v.Cascade, v.RestartIdentity)
		if _, err := a.txExec(ctx, tx, stmt); err != nil {
			// Truncate of a missing table fails with "relation does
			// not exist"; treat as a benign skip-with-warning so the
			// stream survives a stale-publication TRUNCATE event.
			if isMissingTableErr(err) {
				logUnknownTable(ctx, "truncate", schema, v.Table)
				return nil
			}
			return fmt.Errorf("postgres: applier: truncate %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.SchemaSnapshot:
		// ADR-0049 Chunk B3: persist the boundary's IR schema into
		// sluice_cdc_schema_history on the SAME tx the caller
		// (applyOne / commitBatch) writes the ADR-0007 position on
		// (locked decision #4a — controlSchema-qualified, the same
		// schema writePositionTx targets). The streamID arg (threaded
		// through from Apply / ApplyBatch — ADR-0049 follow-up task
		// #27) keys the history row identically to the position
		// row's streamID on the same tx, so resolveSchemaVersion
		// composes cleanly. Pre-task-27 this read a.streamID (set via
		// the optional [ir.StreamIDSetter]); every CURRENT caller does
		// call SetStreamID before Apply, but sourcing from the arg
		// closes the latent footgun where any future non-migrate
		// Apply path that omits SetStreamID would silently key rows
		// under "" and surface as a loud [ir.ErrPositionInvalid] at
		// the next resume. A failure returns up → the tx rolls back
		// (position write never lands) and the stream stops loudly
		// (locked decision #4b: fatal, never logged-and-continued).
		if v.IR == nil {
			return errors.New("postgres: applier: schema snapshot has nil IR table")
		}
		if err := writeSchemaVersion(ctx, tx, a.controlSchema, streamID, v.Schema, v.Table, v.Position, v.IR); err != nil {
			return fmt.Errorf("postgres: applier: write schema version for %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil
	}
	return fmt.Errorf("postgres: applier: unknown change type %T", c)
}

// diagApplierInsertReceived emits an ADR-0036 Phase A.3 DEBUG-level
// trace line every time an Insert event reaches the dispatch site.
// The diagnose test (`add_table_live_pg_diagnose_integration_test.go`)
// parses these to build an `applierByBody` map keyed off the events
// table's `body` column, then cross-references each missing row to
// classify the loss as M5c (applier received and dropped) vs
// M5a/M5b (pgoutput filtered or streamer-snapshot-handoff race —
// applier never saw it).
//
// Behaviour invariants:
//
//   - DEBUG-level — silent in normal runs; surfaces only when the
//     caller installs a debug-level slog handler (the diagnose test
//     does, via a JSON handler with `Level: slog.LevelDebug`).
//   - Logs the position token's parsed LSN so the cross-reference
//     can group entries by LSN if needed. Token-parse failures emit
//     `lsn=<unparseable>` rather than aborting.
//   - Extracts a string-typed `body` field when present so the
//     test's body-keyed lookup is trivial. Absent or non-string
//     bodies log `body=""` (the test treats "" as unknown for that
//     row's classification).
//
// Function-scoped so the dispatch site stays readable; the cost is
// one slog call per Insert when DEBUG is off (a fast no-op in
// log/slog).
func diagApplierInsertReceived(ctx context.Context, defaultSchema string, v ir.Insert) {
	schema := applierSchema(defaultSchema, v.Schema)
	body := ""
	if v.Row != nil {
		if raw, ok := v.Row["body"]; ok {
			if s, isString := raw.(string); isString {
				body = s
			}
		}
	}
	lsnStr := "<unparseable>"
	if lsn, err := lsnFromPositionToken(v.Position.Token); err == nil && lsn != 0 {
		lsnStr = lsn.String()
	}
	slog.DebugContext(
		ctx, "addtable.diag: applier insert received",
		slog.String("phase", "applier_insert_received"),
		slog.String("schema", schema),
		slog.String("relation", v.Table),
		slog.String("lsn", lsnStr),
		slog.String("body", body),
	)
}

// logUnknownTable surfaces the skip-with-warning footprint for
// events targeting a non-existent destination table. Operators
// should see exactly one of these per unknown table per applier
// lifetime (the column-type cache is populated on first miss with
// the sentinel and skipped thereafter).
func logUnknownTable(ctx context.Context, op, schema, table string) {
	slog.WarnContext(
		ctx, "postgres: applier: skipping CDC event for unknown target table",
		slog.String("op", op),
		slog.String("schema", schema),
		slog.String("table", table),
		slog.String("hint", "verify the publication is scoped to tables that exist on both source and target; re-run sluice migrate to add missing tables on the target"),
	)
}

// isMissingTableErr returns true when err carries the Postgres
// "relation does not exist" SQLSTATE 42P01. Used by the truncate
// dispatch to recognise a stale-publication TRUNCATE without
// taking a hard dependency on pgconn's error type.
func isMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	// SQLSTATE 42P01 = undefined_table. The text varies between
	// pgx versions but the substring is stable.
	msg := err.Error()
	return strings.Contains(msg, "42P01") || strings.Contains(msg, "does not exist")
}

// logZeroRowsAffected emits a debug-level log line when a target Exec
// reports zero rows affected. Mirrors the MySQL applier's helper of
// the same name; see that file for the full rationale (Bug-6 silent-
// divergence visibility without violating resume idempotency).
func logZeroRowsAffected(ctx context.Context, op, schema, table string, res sql.Result) {
	if res == nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		return
	}
	if n == 0 {
		slog.DebugContext(
			ctx, "postgres: applier: zero rows affected",
			slog.String("op", op),
			slog.String("schema", schema),
			slog.String("table", table),
		)
	}
}

// pkFor returns the cached PK column list for the named table,
// loading it on the first sight of the table. An empty slice means
// "no PK" — Insert falls back to plain INSERT in that case.
func (a *ChangeApplier) pkFor(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	qn := schema + "." + table
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

// generatedColsFor returns the set of generated-column names for
// the named table, populated alongside colTypesFor. Builders use it
// to filter generated columns out of INSERT column lists, UPDATE
// SET clauses, and UPDATE/DELETE WHERE predicates — PG rejects
// non-DEFAULT values on generated columns (SQLSTATE 428C9). Empty
// map for tables with no generated columns; nil-tolerant for unit
// tests using a hand-built fixture (returns false for any column).
//
// Cache is populated as a side effect of colTypesFor, so the same
// information_schema round-trip covers both the type map and the
// generated-column set.
func (a *ChangeApplier) generatedColsFor(schema, table string) map[string]bool {
	if a.generatedColCache == nil {
		return nil
	}
	return a.generatedColCache[schema+"."+table]
}

// colTypesFor returns the cached column-name → IR type map for the
// named table, loading it on the first sight of the table. The map
// is consulted for every value the applier binds so prepareValue can
// shape Array / Geometry / future special-cased values for pgx.
//
// The lookup uses information_schema directly (the same source the
// schema reader's populateColumns uses) and runs the existing
// translateType to produce IR types — keeping the applier's view of
// "what does this column hold" in lockstep with the rest of the
// engine.
func (a *ChangeApplier) colTypesFor(ctx context.Context, schema, table string) (map[string]ir.Type, error) {
	qn := schema + "." + table
	if cached, ok := a.colTypeCache[qn]; ok {
		return cached, nil
	}
	out, generated, err := loadColumnTypes(ctx, a.db, schema, table)
	if err != nil {
		return nil, err
	}
	a.colTypeCache[qn] = out
	if a.generatedColCache != nil {
		a.generatedColCache[qn] = generated
	}
	return out, nil
}

// loadColumnTypes queries information_schema for the column list of
// schema.table and produces a column-name → IR type map. Mirrors
// SchemaReader.populateColumns minus the index/FK/PK plumbing the
// applier doesn't need.
//
// Enum-valued columns are resolved through readEnumValuesForSchema,
// which loads the enum type values in the same call. Arrays of
// supported scalar element types are also resolved. Geometry is left
// without per-column subtype/SRID metadata — the applier doesn't
// re-encode geometry on the write path's IR-Geometry → EWKB step
// (that happens via prepareValue using the column's SRID, which here
// defaults to 0; row_writer's PostGIS-aware path is the canonical
// place to recover the SRID, and the applier does not currently do
// so for replicated UPDATE/DELETE rows). When the cross-engine
// PostGIS replication path lands we'll need to extend this to read
// the geometry_columns view too.
func loadColumnTypes(ctx context.Context, db *sql.DB, schema, table string) (types map[string]ir.Type, generated map[string]bool, err error) {
	enumValues, err := readEnumValuesForSchema(ctx, db, schema)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: applier: read enum values: %w", err)
	}

	// is_generated has been in information_schema.columns since PG 12
	// (the column itself shipped earlier but the values became reliable
	// in 12 with GENERATED ALWAYS AS … STORED). For older PG versions
	// it returns 'NEVER' for every column — fail-safe behaviour: the
	// applier behaves exactly as it did pre-fix.
	//
	// The pg_catalog join supplies format_type for ADR-0051/-0070
	// verbatim-carry types (money/xml/tsvector/range/multirange/
	// pg_lsn/txid_snapshot/pg_snapshot). Pre-v0.92.2 the applier's
	// query didn't fetch format_type and didn't set VerbatimEligible,
	// so [translateType] hit the generic loud refusal on the first
	// DML touching one of those types — Bug 97's applier-side gap.
	// The schema reader and CDC reader's OID switch already carried
	// the allowlist; v0.92.2 closes the third dispatch site.
	const q = `
		SELECT
			c.column_name,
			LOWER(c.data_type),
			c.udt_name,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.datetime_precision,
			c.is_identity,
			c.column_default,
			c.is_generated,
			COALESCE(pg_catalog.format_type(a.atttypid, a.atttypmod), '')
		FROM   information_schema.columns c
		LEFT JOIN pg_class      cl   ON cl.relname    = c.table_name
		                            AND cl.relnamespace = (
		                                  SELECT oid FROM pg_namespace WHERE nspname = c.table_schema)
		LEFT JOIN pg_attribute  a    ON a.attrelid    = cl.oid
		                            AND a.attname     = c.column_name
		                            AND a.attnum      > 0
		                            AND NOT a.attisdropped
		WHERE  c.table_schema = $1
		  AND  c.table_name   = $2
		ORDER  BY c.ordinal_position`

	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	types = map[string]ir.Type{}
	generated = map[string]bool{}
	for rows.Next() {
		var (
			colName, dataType, udtName string
			charMaxLen, numPrec        sql.NullInt64
			numScale, dtPrec           sql.NullInt64
			isIdentity                 string
			columnDefault              sql.NullString
			isGenerated                string
			formatType                 string
		)
		if err := rows.Scan(
			&colName, &dataType, &udtName,
			&charMaxLen, &numPrec, &numScale, &dtPrec,
			&isIdentity, &columnDefault, &isGenerated,
			&formatType,
		); err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(isGenerated, "ALWAYS") {
			generated[colName] = true
		}

		meta := columnMeta{
			DataType:        dataType,
			UDTName:         udtName,
			CharMaxLen:      nullInt64ToPtr(charMaxLen),
			NumPrec:         nullInt64ToPtr(numPrec),
			NumScale:        nullInt64ToPtr(numScale),
			DTPrec:          nullInt64ToPtr(dtPrec),
			IsAutoIncrement: isAutoIncrement(isIdentity, columnDefault),
			FormatType:      formatType,
			// VerbatimEligible=true: the applier writes to a PG target,
			// so ADR-0051/-0070 verbatim-carry types round-trip via
			// ir.VerbatimType. Cross-engine sources (MySQL) cannot
			// produce these types in the first place, so this flag has
			// no effect on cross-engine streams. (Bug 97 / v0.92.2
			// applier-side gap closure.)
			VerbatimEligible: true,
		}
		if dataType == "user-defined" || dataType == "USER-DEFINED" {
			if values, ok := enumValues[udtName]; ok {
				meta.EnumValues = values
			}
		}
		if dataType == "array" || dataType == "ARRAY" {
			elemDataType, ok := arrayElementDataType(udtName)
			if !ok {
				// Unknown array element types are surfaced (rather
				// than silently passing through) for the same reason
				// the schema reader surfaces them: an applier that
				// can't shape the value will fail on the wire and the
				// operator deserves a precise message.
				return nil, nil, fmt.Errorf("postgres: applier: array column %s.%s has unsupported element type %q", table, colName, udtName)
			}
			meta.ArrayElement = &columnMeta{
				DataType: elemDataType,
				UDTName:  strings.TrimPrefix(udtName, "_"),
			}
		}

		typ, err := translateType(meta)
		if err != nil {
			return nil, nil, fmt.Errorf("postgres: applier: translate %s.%s: %w", table, colName, err)
		}
		types[colName] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(types) == 0 {
		// Empty information_schema result: the table doesn't exist
		// on the destination. Wrap [errUnknownTable] so callers can
		// branch on errors.Is and skip the event with a warning
		// (Bug 13 defence-in-depth). See [ChangeApplier.dispatch].
		return nil, nil, fmt.Errorf("%w: %s.%s", errUnknownTable, schema, table)
	}
	return types, generated, nil
}

// readEnumValuesForSchema is the standalone variant of
// SchemaReader.readEnumValues — same query, no receiver, callable
// from the applier without instantiating a SchemaReader.
func readEnumValuesForSchema(ctx context.Context, db *sql.DB, schema string) (map[string][]string, error) {
	const q = `
		SELECT t.typname, e.enumlabel
		FROM   pg_enum e
		JOIN   pg_type t      ON t.oid = e.enumtypid
		JOIN   pg_namespace n ON n.oid = t.typnamespace
		WHERE  n.nspname = $1
		ORDER  BY t.typname, e.enumsortorder`

	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string][]string{}
	for rows.Next() {
		var typname, label string
		if err := rows.Scan(&typname, &label); err != nil {
			return nil, err
		}
		out[typname] = append(out[typname], label)
	}
	return out, rows.Err()
}

// applierSchema picks the schema name to use in SQL. The applier's
// configured schema (a.schema, derived from the target DSN) is
// authoritative — it is the destination database the operator
// pointed sluice at. The change's source-side schema is metadata
// only; using it would route writes to a same-named schema on the
// target, which is wrong whenever source and target schema names
// differ (e.g. cross-engine MySQL source_db → PG public). v.Schema
// is honoured only as a fallback when the applier wasn't configured
// with one — which shouldn't happen in practice but keeps the
// function total.
func applierSchema(defaultSchema, changeSchema string) string {
	if defaultSchema != "" {
		return defaultSchema
	}
	return changeSchema
}

// loadPrimaryKey reads the PK columns for the named table from
// pg_index. Returns an empty slice (not nil) for tables with no PK.
// Uses pg_index directly rather than information_schema.table_
// constraints so we get the column order from indkey natively.
func loadPrimaryKey(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	const q = `
		SELECT a.attname
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		WHERE  n.nspname = $1
		  AND  cl.relname = $2
		  AND  ix.indisprimary
		ORDER  BY u.ord`

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

// buildInsertSQL builds an INSERT statement. With a non-empty PK,
// uses ON CONFLICT (pk) DO UPDATE:
//
//	INSERT INTO "s"."t" ("a", "b", "id") VALUES ($1, $2, $3)
//	ON CONFLICT ("id") DO UPDATE SET "a" = EXCLUDED."a", "b" = EXCLUDED."b"
//
// With an empty PK list (tables without a PRIMARY KEY), falls back
// to a plain INSERT — see the ChangeApplier package doc for the
// resume-idempotency caveat.
//
// colTypes maps column names to their IR types and is the input to
// prepareValue. A missing entry (nil map, or column not present) is
// tolerated and the raw value is bound — preserving the pre-Bug-6
// shape so unit tests without a populated cache still produce valid
// SQL.
func buildInsertSQL(schema, table string, row ir.Row, pk []string, colTypes map[string]ir.Type, generated map[string]bool) (sqlStmt string, args []any, err error) {
	cols := nonGeneratedRowKeys(row, generated)
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
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		placeholders[i] = applyPlaceholder(c, i+1, colTypes)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableRef)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(colSQL, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(placeholders, ", "))
	sb.WriteByte(')')

	if len(pk) > 0 {
		// ON CONFLICT (pk) DO UPDATE SET non-pk-cols = EXCLUDED.non-pk-cols
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

		conflictTarget := make([]string, len(pk))
		for i, p := range pk {
			conflictTarget[i] = quoteIdent(p)
		}
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(strings.Join(conflictTarget, ", "))
		sb.WriteByte(')')

		if len(nonPK) > 0 {
			sb.WriteString(" DO UPDATE SET ")
			parts := make([]string, len(nonPK))
			for i, c := range nonPK {
				parts[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(c), quoteIdent(c))
			}
			sb.WriteString(strings.Join(parts, ", "))
		} else {
			// All columns are PK — nothing to update on conflict.
			// DO NOTHING absorbs the conflict silently.
			sb.WriteString(" DO NOTHING")
		}
	}
	return sb.String(), args, nil
}

// buildUpdateSQL builds an UPDATE statement. SET uses every column
// in After (unchanged-column detection is a v1.5 optimization).
// WHERE uses every column in Before with NULL-aware predicate
// building.
func buildUpdateSQL(schema, table string, before, after ir.Row, colTypes map[string]ir.Type, generated map[string]bool) (sqlStmt string, args []any, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	setSQL, setArgs, err := buildSetClause(after, 1, colTypes, generated)
	if err != nil {
		return "", nil, err
	}
	whereSQL, whereArgs, err := buildWhereClause(before, len(setArgs)+1, colTypes, generated)
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
func buildDeleteSQL(schema, table string, before ir.Row, colTypes map[string]ir.Type, generated map[string]bool) (sqlStmt string, args []any, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	whereSQL, whereArgs, err := buildWhereClause(before, 1, colTypes, generated)
	if err != nil {
		return "", nil, err
	}
	return "DELETE FROM " + tableRef + " WHERE " + whereSQL, whereArgs, nil
}

// buildTruncateSQL builds a TRUNCATE TABLE statement, appending the
// CASCADE / RESTART IDENTITY clauses when the source's pgoutput
// TruncateMessage carried those option flags (Bug 98 / v0.92.0).
// CASCADE must come before RESTART IDENTITY per PG's grammar.
func buildTruncateSQL(schema, table string, cascade, restartIdentity bool) string {
	stmt := "TRUNCATE TABLE " + quoteIdent(schema) + "." + quoteIdent(table)
	if restartIdentity {
		stmt += " RESTART IDENTITY"
	}
	if cascade {
		stmt += " CASCADE"
	}
	return stmt
}

// buildSetClause renders "col1 = $N, col2 = $N+1" for an UPDATE SET.
// startIdx is the next available placeholder number — Postgres uses
// numbered placeholders (unlike MySQL's `?`), so a SET + WHERE
// combination needs to share a sequence.
func buildSetClause(row ir.Row, startIdx int, colTypes map[string]ir.Type, generated map[string]bool) (clause string, args []any, err error) {
	cols := nonGeneratedRowKeys(row, generated)
	parts := make([]string, len(cols))
	args = make([]any, 0, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = %s", quoteIdent(c), applyPlaceholder(c, startIdx+i, colTypes))
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
// startIdx is the next available placeholder number.
//
// Generated columns are skipped (GitHub issue #12): including a
// STORED generated column in WHERE risks silent zero-rows-affected
// when the target's recomputation differs from the source's stored
// value (floating-point precision / NULL-coalescing differences are
// realistic). The PK + remaining-column equality is sufficient to
// identify the row.
func buildWhereClause(row ir.Row, startIdx int, colTypes map[string]ir.Type, generated map[string]bool) (clause string, args []any, err error) {
	cols := nonGeneratedRowKeys(row, generated)
	parts := make([]string, 0, len(cols))
	args = make([]any, 0, len(cols))
	idx := startIdx
	for _, c := range cols {
		v := row[c]
		if v == nil {
			parts = append(parts, quoteIdent(c)+" IS NULL")
			continue
		}
		parts = append(parts, equalityPredicate(c, idx, colTypes))
		prepared, perr := prepareApplierValue(v, colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, prepared)
		idx++
	}
	return strings.Join(parts, " AND "), args, nil
}

// equalityPredicate renders a single `col = $N` predicate for an apply
// WHERE clause, type-aware for column types whose underlying PG type
// lacks an `=` operator. PG's `json` (text-backed) has no equality
// operator and a bare `col = $N` against it errors with 42883 "could
// not identify an equality operator for type json" — so for `json`
// (but NOT `jsonb`, which has native `=`) cast both sides to text for
// a byte-exact comparison. This matters under REPLICA IDENTITY FULL
// where the apply WHERE includes every column of the OldTuple — a
// `json` column would otherwise break every UPDATE/DELETE apply on
// the target. Mirrors pgcopydb PR #28; see
// `docs/dev/notes/pgcopydb-planetscale-fork-review.md`.
//
// ir.VerbatimType columns (ADR-0051/-0070 — money / pg_lsn / xml /
// tsvector / ranges / multiranges / etc.) take a `$N::<type>` cast
// on the value side AND a left-side `col::text` so the equality
// comparison happens on the canonical text form. Without the cast,
// pgx falls back to bytea binary encoding (the value's ASCII bytes
// arrive as a `\x…` hex literal) and PG rejects with "invalid input
// syntax for type money/pg_lsn" — the v0.92.2 Bug-97 partial-close
// surfaced this for money + pg_lsn (the bytea-fallback families).
// xml / tsvector / int4range happened to round-trip because their
// PG text-IO accepts the bytea-hex form as a syntactic accident;
// the cast makes the apply unambiguous for every verbatim family.
// v0.92.3 closure.
func equalityPredicate(col string, paramIdx int, colTypes map[string]ir.Type) string {
	if t, ok := colTypes[col]; ok {
		if j, isJSON := t.(ir.JSON); isJSON && !j.Binary {
			return fmt.Sprintf("%s::text = $%d::text", quoteIdent(col), paramIdx)
		}
		if v, isVerbatim := t.(ir.VerbatimType); isVerbatim {
			return fmt.Sprintf("%s::text = %s::text", quoteIdent(col), verbatimPlaceholder(paramIdx, v))
		}
	}
	return fmt.Sprintf("%s = $%d", quoteIdent(col), paramIdx)
}

// verbatimPlaceholder renders `$N::<verbatim-type>` for a value bound
// to an ir.VerbatimType column. The Definition string comes from
// pg_catalog.format_type and is safe to interpolate directly — PG's
// own format_type produces canonical type names ("money", "pg_lsn",
// "int4range", "tsvector", "xml", etc.) with no user-controlled
// input. ranges and multiranges include the parameterized form
// ("int4range" rather than "int4multirange[]") which PG's cast
// machinery accepts verbatim. v0.92.3 Bug-97 wire-encoding closure.
func verbatimPlaceholder(paramIdx int, t ir.VerbatimType) string {
	return fmt.Sprintf("$%d::%s", paramIdx, t.Definition)
}

// applyPlaceholder is the canonical "$N or $N::TYPE" renderer for
// any value bound on the apply path (INSERT VALUES, UPDATE SET).
// Bare placeholder for everything except ir.VerbatimType, which gets
// the explicit cast — same v0.92.3 Bug-97 wire-encoding closure as
// equalityPredicate. The WHERE/equality predicate has its own
// renderer because it also needs to cast the LEFT side.
func applyPlaceholder(col string, paramIdx int, colTypes map[string]ir.Type) string {
	if t, ok := colTypes[col]; ok {
		if v, isVerbatim := t.(ir.VerbatimType); isVerbatim {
			return verbatimPlaceholder(paramIdx, v)
		}
	}
	return fmt.Sprintf("$%d", paramIdx)
}

// nonGeneratedRowKeys returns the row's keys in sorted order with
// any column flagged in `generated` filtered out. PG rejects
// non-DEFAULT values on generated columns (SQLSTATE 428C9 "cannot
// insert a non-DEFAULT value into column"), so INSERT column lists
// and UPDATE SET clauses must exclude them; including them in
// UPDATE/DELETE WHERE risks silent zero-rows-affected (see the
// buildWhereClause docstring).
//
// Mirrors the bulk-load writer's existing column-list filter
// (ADR-0026:100, row_reader.go). The CDC apply path was historically
// missing this filter — see GitHub issue #12 / v0.40.0.
//
// A nil `generated` map is tolerant: with no generated-column
// info, every row key passes through. This preserves the pre-fix
// behaviour for unit tests using hand-built fixtures and for the
// small race window before the applier's lazy cache populates.
func nonGeneratedRowKeys(row ir.Row, generated map[string]bool) []string {
	all := sortedKeys(row)
	if len(generated) == 0 {
		return all
	}
	out := make([]string, 0, len(all))
	for _, c := range all {
		if generated[c] {
			continue
		}
		out = append(out, c)
	}
	return out
}

// prepareApplierValue is the applier's wrapper around prepareValue.
// It looks up the column's IR type and routes the value through the
// shared shaping helper from row_writer.go. When the column isn't in
// the map (cache cold or column unknown — defensive), it falls back
// to the raw value, preserving the pre-Bug-6 shape.
//
// Routing through the shared helper keeps any future shaping rules
// added to prepareValue (for new IR types or new corner cases)
// automatically picked up by the applier without touching this file.
//
// v0.92.4 Bug 97 wire-encoding REDO: for ir.VerbatimType columns the
// applier emits explicit `$N::<TYPE>` casts in the SQL (v0.92.3). But
// pgx's database/sql adapter binds Go `[]byte` as PG bytea on the
// wire; PG then evaluates `bytea::<TYPE>` which goes through an
// implicit `bytea → text` cast that produces a `\x…` hex literal,
// which then fails the `text → <TYPE>` parse with `invalid input
// syntax for type <TYPE>: "\x…"`. (Observed concretely against money
// and pg_lsn on the v0.92.3 verification cycle.) The fix: when the
// shared helper returns `[]byte` for a verbatim column, convert to
// string so pgx binds as text, and PG's cast machinery sees the
// actual canonical text form (`$99.99`, `0/3000000`, …). xml /
// tsvector / int4range syntactically tolerated the bytea-hex form
// pre-v0.92.4 and round-tripped, but the conversion is uniform —
// every verbatim family lands as text on the wire.
func prepareApplierValue(v any, colTypes map[string]ir.Type, colName string) (any, error) {
	if colTypes == nil {
		return v, nil
	}
	t, ok := colTypes[colName]
	if !ok {
		return v, nil
	}
	out, err := prepareValue(v, t)
	if err != nil {
		return nil, err
	}
	if _, isVerbatim := t.(ir.VerbatimType); isVerbatim {
		if b, isBytes := out.([]byte); isBytes {
			return string(b), nil
		}
	}
	return out, nil
}

// (sortedKeys is shared with the schema reader — see schema_reader.go
// for the implementation. The applier uses it to render generated SQL
// in a deterministic column order.)
