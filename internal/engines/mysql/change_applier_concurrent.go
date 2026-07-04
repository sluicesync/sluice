// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # MySQL adapter for the concurrent key-hash CDC apply (ADR-0104 / ADR-0105)
//
// The engine-neutral correctness core (the key-hash router, the contiguous
// checkpoint frontier, the lane orchestration with in-lane shrink-and-retry
// and the v0.99.81 lane-local read cap) lives in [internal/laneapply],
// extracted there by ADR-0105 STEP 1 so the Postgres target can reuse the
// exactly-once landmark without a second copy. This file is the MySQL side
// of the [laneapply.LaneApplier] seam: the PK-metadata-driven routing
// decision, the dedicated-backend lane commit (value encoding byte-identical
// to the serial RunOneBatch path), the position-checkpoint write, the
// barrier-path apply, and the MySQL/Vitess error classification.
//
// The position relaxation, the dependent-row hazard, and the exactly-once
// invariants are documented at the top of internal/laneapply/laneapply.go;
// they are unchanged by the extraction (this is a behavior-preserving
// re-wrap, not a logic change).

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// --- Guarded metadata-cache accessors (ADR-0104 concurrency safety) ---
//
// Every read/write of pkCache, colTypeCache, keylessCache and
// warnedKeyless funnels through these so the concurrent key-hash lanes
// (which call dispatch from W goroutines) never touch a map unguarded. The
// load-on-miss callers use the RLock-check → unlock → DB-load → Lock-store
// pattern so a cache miss does NOT serialize every lane on the DB
// round-trip; the double-store on a concurrent miss is idempotent.

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

func (a *ChangeApplier) cachedKeyless(qn string) (keyless, ok bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.keylessCache[qn]
	return v, ok
}

func (a *ChangeApplier) storeKeyless(qn string, keyless bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.keylessCache == nil {
		a.keylessCache = make(map[string]bool)
	}
	a.keylessCache[qn] = keyless
}

// markWarnedKeyless records that the keyless WARN has been emitted for qn
// and reports whether THIS call was the one that recorded it — so the
// caller logs the WARN exactly once even under concurrent lanes (the
// check-and-set is atomic under the write lock).
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

// invalidateMetadataCaches drops the PK + column-type cache entries for qn
// (the ADR-0049 schema-change cache invalidation). Guarded so a
// barrier-path invalidation is safe against concurrent lane reads.
func (a *ChangeApplier) invalidateMetadataCaches(qn string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	delete(a.colTypeCache, qn)
	delete(a.pkCache, qn)
}

// --- Concurrent apply: MySQL adapter for the laneapply seam (ADR-0105) ---

// laneCommitHookForTest, when non-nil, is invoked by the lane adapter's
// ApplyLaneBatch just before a lane's commit with the batch about to commit;
// a non-nil return forces the commit to fail (rolled back), driving the
// in-lane retry / tx-killer-convergence integration pin deterministically.
// Production leaves it nil — the apply path is byte-identical. Set only by
// single-test fixtures (set then reset in the same test), so no concurrent
// mutation across tests. The buffer arg is the {seq, change} envelope shape
// the GA path used; the integration pin ignores it (`_ []laneChange`).
var laneCommitHookForTest func(buf []laneChange) error

// laneChange is the per-change envelope [laneCommitHookForTest] receives. The
// GA concurrent path paired a source seq with each change here; the
// orchestration now lives in [laneapply] (which owns its own
// [laneapply.LaneChange] including the seq), so this thin type survives only
// as the parameter shape the integration pin's hook expects — keeping that
// hook signature unchanged across the ADR-0105 extraction. The adapter fills
// `change`; the hook ignores its arg entirely.
type laneChange struct {
	change ir.Change
}

// laneApplierAdapter is the MySQL implementation of [laneapply.LaneApplier].
// It carries the [ChangeApplier] (for redact/stamp/dispatch/cache/position
// writes), the resolved streamID, the dedicated lane pool (MaxOpenConns ==
// lanes), and a copy of [laneCommitHookForTest] captured at construction so a
// test that sets the hook before ApplyBatch drives the in-lane retry path.
type laneApplierAdapter struct {
	a              *ChangeApplier
	streamID       string
	laneDB         *sql.DB
	laneCommitHook func(buf []laneChange) error
}

// PKValuesForRouting decodes the row change's schema/table, loads the PK
// columns, and returns the source-qualified name + ordered PK values for
// lane hashing. ok=false routes to the barrier path for: a non-row event, a
// keyless/malformed change, OR a PK-changing update (a key migration whose
// old/new effects must stay globally ordered) — exactly the cases the GA
// routeRow barriered. A PK-metadata lookup error is classified and aborts.
func (la *laneApplierAdapter) PKValuesForRouting(ctx context.Context, c ir.Change) (qualified string, pkVals []any, ok bool, err error) {
	schema, table := laneapply.RowChangeSchemaTable(c)
	routed := la.a.routedSchema(schema)
	pkCols, perr := la.a.pkForRedact(ctx, schema, table)
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
	return qualifiedName(routed, table), vals, true, nil
}

// ApplyLaneBatch dispatches every change in batch onto a single lane
// transaction and commits it (the body of the GA commitLaneBatch). Redaction
// + shard-stamp happen FIRST, in the SAME order the serial RunOneBatch path
// uses, so value encoding is byte-identical; the lane writes NO position (the
// orchestrator's frontier owns it). Returns len(batch) on success, the RAW
// (unclassified) error on failure so the orchestrator's retry predicate can
// inspect it; on any failure the tx is rolled back. The `lane` index is
// accepted for the seam contract but unused by the MySQL adapter: the lane
// pool (MaxOpenConns == lanes) hands out one backend per in-flight tx, so a
// pooled connection per concurrent lane commit == one backend per lane in
// practice — matching the GA behavior (no per-lane dedicated *sql.Conn).
//
// ADR-0139/0140: the lane's changes run through the SAME coalescing accumulator
// ([mysqlBatchTx]) the single-lane batch path uses — consecutive same-table,
// same-shape keyed inserts (and non-PK-changing keyed updates as after-image
// upserts) become one multi-row INSERT, and consecutive keyed deletes become one
// DELETE … WHERE pk IN (…); each run is flushed before a kind switch, a serial
// (keyless / PK-changing / non-row) change, and once more before commit. A lane
// batch is key-hashed (same key → same lane), so its changes are distinct-PK
// rows of a small set of tables — coalescing is highly effective. Encoding
// reuses buildMultiRowInsertSQL / buildMultiRowDeleteSQL (byte-identical value
// binding to the serial single-row path).
//
// The *sql.Tx is rolled back on every error path and committed via
// commitWithTimeout on success; sqlclosecheck can't track that
// commit-or-rollback discipline across the dispatch loop, so it's suppressed.
//
//nolint:sqlclosecheck
func (la *laneApplierAdapter) ApplyLaneBatch(ctx context.Context, _ int, batch []ir.Change) (int, error) {
	tx, err := la.laneDB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("mysql: applier: lane begin tx: %w", err)
	}
	btx := &mysqlBatchTx{a: la.a, tx: tx, ctx: ctx}
	for _, c := range batch {
		if err := la.a.redactChange(ctx, c); err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("mysql: applier: redact: %w", err)
		}
		la.a.stampShardChange(c)
		if err := btx.dispatch(ctx, la.streamID, c); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	// Flush the trailing coalesced run (upsert-run or delete-run; ADR-0140)
	// before commit so all of the lane's data is durable in this tx (the lane
	// writes no position — the orchestrator's frontier checkpoint owns it).
	if err := btx.flushPending(ctx); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	// Test seam: force a commit-path failure deterministically (the lane
	// analogue of the serial path's removed pipelineTestCommitHook). nil in
	// production. Returns the error to take the same rollback+retry path a
	// real commit failure would. seq is no longer carried here (the
	// orchestrator owns seqs) and the hook ignores its arg, so the envelope
	// is filled with change only.
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
		return 0, fmt.Errorf("mysql: applier: lane commit: %w", err)
	}
	return len(batch), nil
}

// ClassifyError maps a raw lane error to the engine's classified error so the
// orchestrator can derive retriability (the single source of truth — a Vitess
// tx-killer abort satisfies [ir.RetriableError], driving the in-lane
// shrink-and-retry).
func (la *laneApplierAdapter) ClassifyError(err error) error {
	return classifyApplierError(err)
}

// WriteCheckpoint persists the merged frontier position in its own
// transaction on the coordinator's primary pool (the ADR-0104 position
// relaxation). The orchestrator owns the frontier read + the seq-monotone
// guard; this does only the durable write, wrapping each error in
// classifyApplierError exactly as the GA writeCheckpoint did.
func (la *laneApplierAdapter) WriteCheckpoint(ctx context.Context, pos ir.Position) error {
	a := la.a
	posCtx, cancel := a.execTimeoutCtx(ctx)
	defer cancel()
	tx, err := a.db.BeginTx(posCtx, nil)
	if err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint begin: %w", err))
	}
	if err := writePositionTx(posCtx, tx, la.streamID, pos.Token, a.slotName, a.sourceFingerprint, a.targetSchema); err != nil {
		_ = tx.Rollback()
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint position write: %w", err))
	}
	if err := a.commitWithTimeout(tx); err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint commit: %w", err))
	}
	return nil
}

// ApplyBarrierChange applies one barrier-path change on the coordinator
// backend via applyBarrierNoPosition — which applies the data + ADR-0049
// schema-history row + (for a SchemaSnapshot) the GUARDED cache-after-commit
// invalidation (cacheActiveSchemaAfterCommit → invalidateTargetCachesForBoundary,
// fired ONLY on a real signature-changing boundary, never on the first-touch
// baseline — the SAME guarded path the serial applier uses), but does NOT
// write the position. On the concurrent path the resume position is owned
// exclusively by the frontier checkpoint (the ADR-0104 relaxation), so the
// barrier must not write its own (metadata-anchored) token. The orchestrator
// does NOT invalidate separately either (Bug 158: an unconditional
// orchestrator-side invalidation bypassed the first-touch guard; on PG that
// silently dropped all post-baseline changes — MySQL's text bind tolerated it
// but the over-invalidation was still wrong, needlessly schema-dirtying every
// table on first touch).
func (la *laneApplierAdapter) ApplyBarrierChange(ctx context.Context, c ir.Change) error {
	return la.a.applyBarrierNoPosition(ctx, la.streamID, c)
}

// applyBatchConcurrent is the ADR-0104 concurrent key-hash apply entry,
// invoked from ApplyBatch when an operator wires --apply-concurrency
// (the lane count W) > 1 and a dedicated pool can be opened. It owns the
// lane pool for the call's lifetime, builds the MySQL [laneApplierAdapter],
// and drives the engine-neutral [laneapply.Orchestrator]. On any lane or
// coordinator error the whole run stops (ctx cancel + drain) and the error is
// returned; the persisted position reflects only fully-durable work, so
// warm-resume re-streams + idempotently re-applies the remainder
// (exactly-once for keyed tables; the keyless at-least-once guarantee is
// unchanged because keyless changes take the single-row barrier path).
func (a *ChangeApplier) applyBatchConcurrent(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize, lanes int) error {
	laneDB, err := openDB(ctx, a.pipelineCfg, a.sqlMode)
	if err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: open concurrent lane pool: %w", err))
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
