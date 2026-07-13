// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// # Postgres adapter for the concurrent key-hash CDC apply (ADR-0105)
//
// The engine-neutral correctness core (the key-hash router, the contiguous
// checkpoint frontier, the lane orchestration with in-lane shrink-and-retry
// and the lane-local read cap) lives in [internal/laneapply], shared with
// the GA MySQL target (ADR-0104). This file is the Postgres side of the
// [laneapply.LaneApplier] seam: the PK-metadata-driven routing decision, the
// dedicated-backend lane commit (using the SERIAL dispatch on a *sql.Tx, so
// value encoding is byte-identical to the ADR-0092 serial/pipelined apply
// path), the position-checkpoint write, the barrier-path apply, and the
// Postgres error classification (serialization 40001 / deadlock 40P01 →
// retriable, the analog of MySQL's tx-killer).
//
// W=1 (or unset) is byte-identical to today's PG batch path. The position
// relaxation, the dependent-row hazard, and the exactly-once invariants are
// documented at the top of internal/laneapply/laneapply.go; PG inherits the
// ADR-0104 contract verbatim (ADR-0105 "The position relaxation").

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// --- Guarded metadata-cache accessors (ADR-0105 concurrency safety) ---
//
// Every read/write of pkCache, colTypeCache, conflictKeyCache,
// warnedKeyless and schemaDirtyTables funnels through these so the
// concurrent key-hash lanes (which call dispatch from W goroutines) never
// touch a map unguarded. The load-on-miss callers use the RLock-check →
// unlock → DB-load → Lock-store pattern so a cache miss does NOT serialize
// every lane on the DB round-trip; the double-store on a concurrent miss is
// idempotent. The serial path takes the same lock (single-goroutine, so
// uncontended).

func (a *ChangeApplier) cachedPK(qn string) ([]string, bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.pkCache[qn]
	return v, ok
}

func (a *ChangeApplier) storePK(qn string, pk []string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.pkCache == nil {
		a.pkCache = make(map[string][]string)
	}
	a.pkCache[qn] = pk
}

func (a *ChangeApplier) cachedColTypes(qn string) (map[string]*ir.Column, bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.colTypeCache[qn]
	return v, ok
}

func (a *ChangeApplier) storeColTypes(qn string, cols map[string]*ir.Column) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.colTypeCache == nil {
		a.colTypeCache = make(map[string]map[string]*ir.Column)
	}
	a.colTypeCache[qn] = cols
}

func (a *ChangeApplier) cachedConflictKey(qn string) ([]string, bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.conflictKeyCache[qn]
	return v, ok
}

func (a *ChangeApplier) storeConflictKey(qn string, key []string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.conflictKeyCache == nil {
		a.conflictKeyCache = make(map[string][]string)
	}
	a.conflictKeyCache[qn] = key
}

// tableSchemaDirty reports whether qn (a ROUTED qualified name) has crossed
// a forwarded schema boundary in this applier's lifetime (ADR-0091 F7a GAP
// #3). Read on every DML via execDMLArgs.
func (a *ChangeApplier) tableSchemaDirty(qn string) bool {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	return a.schemaDirtyTables[qn]
}

// markWarnedKeyless records that the keyless WARN has been emitted for qn
// and reports whether THIS call was the one that recorded it — so the caller
// logs the WARN exactly once even under concurrent lanes (the check-and-set
// is atomic under the write lock).
func (a *ChangeApplier) markWarnedKeyless(qn string) (firstTime bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.warnedKeyless == nil {
		a.warnedKeyless = make(map[string]bool)
	}
	if a.warnedKeyless[qn] {
		return false
	}
	a.warnedKeyless[qn] = true
	return true
}

// invalidateMetadataCaches drops the per-table PK + column-type +
// conflict-key cache entries for qn and marks it schema-dirty — the SAME set
// of caches the serial schema-boundary invalidation
// ([ChangeApplier.invalidateTargetCachesForBoundary]) drops, so lanes
// re-probe the live post-DDL catalog on the next change. Guarded so a
// barrier-path invalidation is safe against concurrent lane reads. PG has
// MORE caches than the MySQL adapter drops (conflictKeyCache + the
// schemaDirtyTables stamp), and all of them are handled here.
func (a *ChangeApplier) invalidateMetadataCaches(qn string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	delete(a.colTypeCache, qn)
	delete(a.pkCache, qn)
	delete(a.conflictKeyCache, qn)
	if a.schemaDirtyTables == nil {
		a.schemaDirtyTables = make(map[string]bool)
	}
	a.schemaDirtyTables[qn] = true
}

// --- Concurrent apply: PG adapter for the laneapply seam (ADR-0105) ---

// laneCommitHookForTest, when non-nil, is invoked by the lane adapter's
// ApplyLaneBatch just before a lane's commit with the batch about to commit;
// a non-nil return forces the commit to fail (rolled back), driving the
// in-lane retry / serialization-abort-convergence integration pin
// deterministically. Production leaves it nil — the apply path is
// byte-identical. Set only by single-test fixtures (set then reset in the
// same test), so no concurrent mutation across tests. Mirrors the MySQL
// adapter's hook (change_applier_concurrent.go).
var laneCommitHookForTest func(buf []laneChange) error

// lanePipelinedTakenForTest, when non-nil, is invoked once per lane batch that
// successfully begins a PIPELINED tx (ADR-0138) — never on the serial fallback.
// An integration test sets it to assert the concurrent path actually uses the
// pipelined SendBatch path and has not silently regressed to serial per-row exec
// (a regression the value-fidelity / exactly-once differentials would NOT catch,
// since serial is also correct — it would only show up as lost WAN throughput).
// nil in production (zero overhead).
var lanePipelinedTakenForTest func()

// laneChange is the per-change envelope [laneCommitHookForTest] receives. The
// orchestration owns its own [laneapply.LaneChange] (including the seq), so
// this thin type survives only as the parameter shape the integration pin's
// hook expects. The adapter fills `change`; the hook ignores its arg.
type laneChange struct {
	change ir.Change
}

// laneApplierAdapter is the Postgres implementation of
// [laneapply.LaneApplier]. It carries the [ChangeApplier] (for
// redact/stamp/dispatch/cache/position writes), the resolved streamID, the
// dedicated lane pool (MaxOpenConns == lanes, registering the SAME geometry
// codec the serial/pipelined pools use), and a copy of
// [laneCommitHookForTest] captured at construction.
type laneApplierAdapter struct {
	a              *ChangeApplier
	streamID       string
	laneDB         *sql.DB
	laneCommitHook func(buf []laneChange) error
}

// PKValuesForRouting decodes the row change's schema/table, loads the PK
// columns, and returns the routed-qualified name + ordered PK values for
// lane hashing. ok=false routes to the barrier path for: a non-row event, a
// keyless/malformed change, OR a PK-changing update (a key migration whose
// old/new effects must stay globally ordered) — exactly the cases the serial
// path treats as global-order-sensitive. The route identity is the PRIMARY
// KEY (matching MySQL), NOT the ON-CONFLICT conflict key: a no-PK-but-unique
// table still routes by its (empty) PK → barrier, preserving the ADR-0089
// at-least-once keyless guard. A PK-metadata lookup error is classified and
// aborts the run.
func (la *laneApplierAdapter) PKValuesForRouting(ctx context.Context, c ir.Change) (qualified string, pkVals []any, ok bool, err error) {
	schema, table := laneapply.RowChangeSchemaTable(c)
	routed := la.a.routedSchema(schema)
	pkCols, perr := la.a.pkForRedact(ctx, routed, table)
	if perr != nil {
		return "", nil, false, classifyApplierError(perr)
	}
	vals, routable := laneapply.PKValuesFromRow(c, pkCols)
	if !routable {
		// Keyless / malformed → barrier (ADR-0089 at-least-once; never
		// silently mis-routed).
		return "", nil, false, nil
	}
	if u, isUpd := c.(ir.Update); isUpd && laneapply.PKChangedUpdate(u, pkCols) {
		// PK-changing update → barrier so old-key/new-key effects stay
		// globally ordered (they could hash to different lanes).
		return "", nil, false, nil
	}
	return schemaTableKey(routed, table), vals, true, nil
}

// ApplyLaneBatch applies every change in batch on one lane transaction and
// commits it. Redaction + shard-stamp happen FIRST, in the SAME order the
// serial applyOne / RunOneBatch path uses; then — ADR-0138 (Bug 168) — the
// batch is dispatched through the ADR-0092 PIPELINED path (dispatchPipelined
// queues each change onto a pgx.Batch; flushAndCommit sends the whole batch in
// ONE SendBatch round trip and commits) instead of one serial Exec round trip
// per change, so a lane is no longer RTT-bound over WAN. Value encoding is
// byte-identical to the serial path because dispatchPipelined is the SAME
// builder + prepareApplierValue codec the single-lane pipelined path uses (the
// value-fidelity oracle is change_applier_pipelined_*_integration_test.go). The
// F7 synchronous_commit pin and the Bug-164 FK bypass are applied by
// beginPipelinedTxOn exactly as the serial BeginTx does (ADR-0007 durability).
// The lane writes NO position (the orchestrator's frontier owns it).
//
// If the raw-conn escape is unavailable (errPipelineUnavailable — a non-pgx /
// wrapped conn, e.g. some direct-API unit constructions), the lane falls back
// to the serial *sql.Tx dispatch (applyLaneBatchSerial) with a one-time WARN —
// loud, never silent, no throughput claim — mirroring the single-lane BeginTx
// closure. With the production DescribeExec lane pool the escape always
// succeeds, so the fallback is defensive parity, not the hot path.
//
// Returns len(batch) on success; on failure the tx is rolled back and the
// error is returned for the orchestrator's retry predicate. flushAndCommit
// returns an already-classified error, but classifyApplierError is idempotent
// for retriability (a 40001/40P01 stays retriable through re-classification via
// the retriablePGError Unwrap chain), so the in-lane shrink-and-retry still
// engages. The `lane` index is accepted for the seam contract but unused: the
// lane pool (MaxOpenConns == lanes) hands out one backend per in-flight tx.
func (la *laneApplierAdapter) ApplyLaneBatch(ctx context.Context, _ int, batch []ir.Change) (int, error) {
	b, err := la.a.beginPipelinedTxOn(ctx, la.laneDB)
	if err != nil {
		if errors.Is(err, errPipelineUnavailable) {
			la.a.warnPipelineFallbackOnce(ctx, err)
			return la.applyLaneBatchSerial(ctx, batch)
		}
		return 0, err
	}
	if lanePipelinedTakenForTest != nil {
		lanePipelinedTakenForTest()
	}
	for _, c := range batch {
		if err := la.a.redactChange(ctx, c); err != nil {
			_ = b.Rollback()
			return 0, fmt.Errorf("postgres: applier: redact: %w", err)
		}
		la.a.stampShardChange(c)
		if err := la.a.dispatchPipelined(ctx, b, la.streamID, c); err != nil {
			_ = b.Rollback()
			return 0, err
		}
	}
	// Test seam: force a commit-path failure deterministically (the lane
	// analogue of forcing a PG serialization abort). nil in production. Fires
	// after queueing, before the SendBatch flush — nothing has been sent yet,
	// so Rollback aborts the as-yet-empty tx, exactly as the serial path rolled
	// back before commit.
	if la.laneCommitHook != nil {
		buf := make([]laneChange, len(batch))
		for i, c := range batch {
			buf[i] = laneChange{change: c}
		}
		if herr := la.laneCommitHook(buf); herr != nil {
			_ = b.Rollback()
			return 0, herr
		}
	}
	// flushAndCommit sends the whole lane batch in one round trip, commits under
	// the Bug-56 watchdog, and releases the pinned backend on every path
	// (including its own rollback on a flush error).
	if err := la.a.flushAndCommit(b); err != nil {
		return 0, err
	}
	return len(batch), nil
}

// applyLaneBatchSerial is the pre-ADR-0138 serial lane apply: one *sql.Tx, one
// Exec round trip per change, one commit. It is the fallback ApplyLaneBatch
// takes only when the pipelined raw-conn escape is unavailable
// (errPipelineUnavailable); byte-identical to the historical lane path.
//
// The *sql.Tx is rolled back on every error path and committed via
// commitWithTimeout on success; sqlclosecheck can't track that
// commit-or-rollback discipline across the dispatch loop, so it's suppressed.
//
//nolint:sqlclosecheck
func (la *laneApplierAdapter) applyLaneBatchSerial(ctx context.Context, batch []ir.Change) (int, error) {
	tx, err := la.laneDB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("postgres: applier: lane begin tx: %w", err)
	}
	// F7: pin synchronous_commit on for the duration of this tx so a
	// role/db-level default of `off` can't silently break ADR-0007's
	// durability contract — identical to the serial BeginTx.
	if err := la.a.forceSynchronousCommitOn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	// Bug 164: bypass target FK + user-trigger enforcement for this lane's
	// apply tx — the per-lane key-hash split (ADR-0105) can commit a child
	// INSERT before its parent in a different lane, a transient FK violation
	// the target would otherwise reject. No-op without privilege.
	if err := la.a.bypassForeignKeyEnforcement(ctx, tx); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	for _, c := range batch {
		if err := la.a.redactChange(ctx, c); err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("postgres: applier: redact: %w", err)
		}
		la.a.stampShardChange(c)
		if err := la.a.dispatch(ctx, tx, la.streamID, c); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	if la.laneCommitHook != nil {
		buf := make([]laneChange, len(batch))
		for i, c := range batch {
			buf[i] = laneChange{change: c}
		}
		if herr := la.laneCommitHook(buf); herr != nil {
			_ = tx.Rollback()
			return 0, herr
		}
	}
	if err := la.a.commitWithTimeout(tx); err != nil {
		return 0, fmt.Errorf("postgres: applier: lane commit: %w", err)
	}
	return len(batch), nil
}

// ClassifyError maps a raw lane error to the engine's classified error so the
// orchestrator can derive retriability (the single source of truth — a PG
// serialization (40001) / deadlock (40P01) abort satisfies
// [ir.RetriableError], driving the in-lane shrink-and-retry).
func (la *laneApplierAdapter) ClassifyError(err error) error {
	return classifyApplierError(err)
}

// WriteCheckpoint persists the merged frontier position in its own
// transaction on the coordinator's primary pool (the ADR-0104 position
// relaxation). The orchestrator owns the frontier read + the seq-monotone
// guard; this does only the durable write. The F7 synchronous_commit pin is
// applied (the position is durable per ADR-0007's hardening), and each error
// is classified exactly as the serial position write would be.
func (la *laneApplierAdapter) WriteCheckpoint(ctx context.Context, pos ir.Position, rowsApplied int64) error {
	a := la.a
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: checkpoint begin: %w", err))
	}
	if err := a.forceSynchronousCommitOn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return classifyApplierError(err)
	}
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	werr := writePositionTx(posCtx, tx, a.controlSchema, la.streamID, pos.Token, a.slotName, a.sourceFingerprint, a.targetSchema, rowsApplied)
	posCancel()
	if werr != nil {
		_ = tx.Rollback()
		return classifyApplierError(fmt.Errorf("postgres: applier: checkpoint position write: %w", werr))
	}
	if err := a.commitWithTimeout(tx); err != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: checkpoint commit: %w", err))
	}
	return nil
}

// ApplyBarrierChange applies one barrier-path change on the coordinator
// backend via applyOne, which writes the barrier's position + data
// atomically per ADR-0007 AND — for a SchemaSnapshot — owns the ADR-0049
// active-schema cache-after-commit update + the GUARDED metadata-cache
// invalidation (cacheActiveSchemaAfterCommit → invalidateTargetCachesForBoundary,
// fired ONLY on a real signature-changing boundary, never on the first-touch
// baseline). This is the SAME guarded path the serial applier uses, so the
// concurrent path invalidates byte-identically. The orchestrator does NOT
// invalidate separately — Bug 158 was an unconditional orchestrator-side
// invalidation that bypassed the first-touch guard, marked the baseline
// SchemaSnapshot schema-dirty, and forced every subsequent lane DML onto the
// QueryExecModeExec text-encode path (json/jsonb → SQLSTATE 22P02 → silent
// total loss on the PG concurrent path).
func (la *laneApplierAdapter) ApplyBarrierChange(ctx context.Context, c ir.Change) error {
	// Position-free: the frontier checkpoint owns the resume position on the
	// concurrent path (writing the barrier's own metadata-anchored token —
	// 0/0 for a first-touch SchemaSnapshot — would regress it; Bug 158).
	return la.a.applyBarrierNoPosition(ctx, la.streamID, c)
}

// applyBatchConcurrent is the ADR-0105 concurrent key-hash apply entry,
// invoked from ApplyBatch when an operator wires --apply-concurrency (the
// lane count W) > 1 and a dedicated pool can be opened. It owns the lane pool
// for the call's lifetime, builds the PG [laneApplierAdapter], and drives the
// engine-neutral [laneapply.Orchestrator]. On any lane or coordinator error
// the whole run stops (ctx cancel + drain) and the error is returned; the
// persisted position reflects only fully-durable work, so warm-resume
// re-streams + idempotently re-applies the remainder (exactly-once for keyed
// tables; the keyless at-least-once guarantee is unchanged because keyless
// changes take the single-row barrier path).
func (a *ChangeApplier) applyBatchConcurrent(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize, lanes int) error {
	// ADR-0138 (Bug 168): the lane pool is opened in DescribeExec mode
	// (openPgxDBDescribeExec — the SAME constructor the single-lane pipelined
	// pool uses) rather than the Exec-mode openDBAs, so each lane can apply its
	// batch via the ADR-0092 pipelined SendBatch path (one round trip per
	// lane-batch, not one per row). With the Exec-mode pool the concurrent lanes
	// dispatched serially — one network round trip per change — capping apply at
	// ~lanes/RTT over WAN (invisible on a LAN, but ~50 changes/s over an 80ms
	// cross-region link; the lag diverged under load).
	//
	// CRITICAL: the lane pool MUST register the PostGIS geometry codec
	// (afterConnectRegisterGeometry) exactly as the serial applier pool
	// (engine.go OpenChangeApplier) and the pipelined pool (pipelinePool) do
	// — otherwise a geometry column would silently mis-encode (TEXT-refused)
	// on the lane path while passing on the serial path: a Bug-74-class
	// codec-coverage trap. Same role + DSN as those pools.
	laneDB, err := openPgxDBDescribeExec(a.pipelineCfg.dsn, roleApplier, a.pipelineCfg.appID,
		stdlib.OptionAfterConnect(afterConnectRegisterGeometry))
	if err != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: open concurrent lane pool: %w", err))
	}
	if err := laneDB.PingContext(ctx); err != nil {
		_ = laneDB.Close()
		return classifyApplierError(fmt.Errorf("postgres: applier: ping concurrent lane pool: %w", err))
	}
	defer func() { _ = laneDB.Close() }()
	laneDB.SetMaxOpenConns(lanes)
	laneDB.SetMaxIdleConns(lanes)

	byteCap := a.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}
	adapter := &laneApplierAdapter{
		a:              a,
		streamID:       streamID,
		laneDB:         laneDB,
		laneCommitHook: laneCommitHookForTest, // nil in production
	}
	orch := laneapply.NewOrchestrator(laneapply.Config{
		Lanes:           lanes,
		MaxBatchSize:    maxBatchSize,
		LaneControllers: a.laneControllers,
		MaxBufferBytes:  byteCap,
		IdleFlushPeriod: defaultIdleFlushPeriod,
	}, adapter)
	return orch.Run(ctx, changes)
}
