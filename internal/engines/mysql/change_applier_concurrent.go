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
	"log/slog"
	"time"

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

// cachedSkipVerdict reports whether qn has a CONFIRMED unknown-target-table
// skip verdict still within skipVerdictTTL (audit P-1). A hit lets colTypesFor
// skip the 3-probe catalog chain. RLock-guarded for the concurrent lanes.
func (a *ChangeApplier) cachedSkipVerdict(qn string) bool {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	at, ok := a.skipVerdictCache[qn]
	return ok && time.Since(at) < skipVerdictTTL
}

// storeSkipVerdict records that qn is CONFIRMED-missing as of now. Called ONLY
// on the recoverable errUnknownTargetTable verdict (never a probe error /
// routing fault), so the negative cache can never mask an M-2/SL-1 halt.
func (a *ChangeApplier) storeSkipVerdict(qn string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.skipVerdictCache == nil {
		a.skipVerdictCache = make(map[string]time.Time)
	}
	a.skipVerdictCache[qn] = time.Now()
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

// markWarnedPartialAfter is [markWarnedKeyless] for the C-10 partial
// after-image notice: same atomic check-and-set under the same lock, so the
// notice fires once per table even when several concurrent lanes hit the
// table's first partial UPDATE at once.
func (a *ChangeApplier) markWarnedPartialAfter(qn string) (firstTime bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.warnedPartialAfter == nil {
		a.warnedPartialAfter = make(map[string]bool)
	}
	if a.warnedPartialAfter[qn] {
		return false
	}
	a.warnedPartialAfter[qn] = true
	return true
}

func (a *ChangeApplier) cachedNonPKUnique(qn string) (hasUnique, ok bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.nonPKUniqueCache[qn]
	return v, ok
}

func (a *ChangeApplier) storeNonPKUnique(qn string, hasUnique bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.nonPKUniqueCache == nil {
		a.nonPKUniqueCache = make(map[string]bool)
	}
	a.nonPKUniqueCache[qn] = hasUnique
}

// markWarnedRouteProbe records that the fail-closed routing-probe WARN has
// been emitted for qn and reports whether THIS call recorded it, so a table
// whose index probe keeps failing is WARNed once rather than per change.
func (a *ChangeApplier) markWarnedRouteProbe(qn string) (firstTime bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.warnedRouteProbe == nil {
		a.warnedRouteProbe = make(map[string]bool)
	}
	if a.warnedRouteProbe[qn] {
		return false
	}
	a.warnedRouteProbe[qn] = true
	return true
}

// invalidateMetadataCaches drops the PK + column-type + lane-routing cache
// entries for qn (the ADR-0049 schema-change cache invalidation). Guarded so
// a barrier-path invalidation is safe against concurrent lane reads.
//
// nonPKUniqueCache belongs here because a DDL that ADDS a unique index flips
// the table's routing scope, and a stale "no unique index" verdict would keep
// its changes key-hashed across lanes — item 131's defect, re-armed by a
// schema change. That the re-route is SAFE rests on the barrier: schema
// events reach [Orchestrator.barrier], which drains every lane to the
// event's predecessor before applying it, so all key-scoped work is durably
// committed before any table-scoped work is routed. The barrier drain is
// pinned by TestBarrier_DrainsAllLanesBeforeApply.
func (a *ChangeApplier) invalidateMetadataCaches(qn string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	delete(a.colTypeCache, qn)
	delete(a.pkCache, qn)
	delete(a.nonPKUniqueCache, qn)
	// P-1: a same-stream DDL that creates/alters the table drops its
	// negative skip verdict too, so the table is picked up at the barrier
	// rather than waiting out skipVerdictTTL.
	delete(a.skipVerdictCache, qn)
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

// RouteForChange decodes the row change's schema/table, loads the PK columns,
// and returns the lane [laneapply.Route]. ok=false routes to the barrier path
// for: a non-row event, a keyless/malformed change, OR a PK-changing update (a
// key migration whose old/new effects must stay globally ordered) — exactly
// the cases the GA routeRow barriered. A PK-metadata lookup error is
// classified and aborts.
//
// The SCOPE is item 131. Per-key hashing is taken only for a table PROVEN to
// carry no unique index besides the PRIMARY KEY; everything else — including a
// failed probe — stays at the zero-value whole-table scope, because MySQL's ON
// DUPLICATE KEY UPDATE fires on ANY unique index (with the PK excluded from
// its SET list), which makes changes to different primary keys dependent on
// each other and silently loses a row when they commit out of source order.
func (la *laneApplierAdapter) RouteForChange(ctx context.Context, c ir.Change) (laneapply.Route, bool, error) {
	schema, table := laneapply.RowChangeSchemaTable(c)
	routed := la.a.routedSchema(schema)
	pkCols, perr := la.a.pkForRedact(ctx, schema, table)
	if perr != nil {
		return laneapply.Route{}, false, classifyApplierError(perr)
	}
	vals, routable := laneapply.PKValuesFromRow(c, pkCols)
	if !routable {
		// Keyless / malformed → barrier (ADR-0089 at-least-once; never
		// silently mis-routed).
		return laneapply.Route{}, false, nil
	}
	if u, isUpd := c.(ir.Update); isUpd && laneapply.PKChangedUpdate(u, pkCols) {
		// PK-changing update → barrier so old-key/new-key effects stay
		// globally ordered (they could hash to different lanes).
		return laneapply.Route{}, false, nil
	}
	route := laneapply.Route{Qualified: qualifiedName(routed, table), PKVals: vals}
	if !la.a.tableHasNonPKUniqueIndex(ctx, routed, table) {
		route.Scope = laneapply.RouteScopeKey
	}
	return route, true, nil
}

// tableHasNonPKUniqueIndex reports whether the TARGET table carries a UNIQUE
// index other than the PRIMARY KEY, caching the verdict per qualified name
// (one information_schema round trip per table, then a map read per change).
//
// It FAILS CLOSED: a probe error returns true — "assume the hazard" — is NOT
// cached (so a later probe can correct it once a transient clears), and WARNs
// once per table so an operator can see why that table lost its lane
// concurrency. Returning false on a failed probe would silently re-arm item
// 131 for the table, and the resulting loss is invisible (every lane reports
// success).
//
// Excluding the PK by NAME is exact on MySQL: the server names the clustered
// primary-key index 'PRIMARY' and reserves that name — a user cannot create
// another index called PRIMARY, and cannot rename the PK's. The premise is
// ground-truthed on a real server by the roster's `pk_only` /
// `composite_pk_only` cases, which demand MULTI-lane routing and therefore
// fail if 'PRIMARY' ever stopped being excluded.
func (a *ChangeApplier) tableHasNonPKUniqueIndex(ctx context.Context, schema, table string) bool {
	qn := qualifiedName(schema, table)
	if v, ok := a.cachedNonPKUnique(qn); ok {
		return v
	}
	const q = `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND non_unique = 0 AND index_name <> 'PRIMARY'`
	var n int
	if err := a.db.QueryRowContext(ctx, q, schema, table).Scan(&n); err != nil {
		if a.markWarnedRouteProbe(qn) {
			slog.WarnContext(ctx,
				"mysql: applier: unique-index probe failed; routing every change on this table to a SINGLE apply lane "+
					"(fail-closed — a wrong 'no unique index' verdict would let a secondary-unique reassignment lose a row, item 131)",
				slog.String("table", qn), slog.String("err", err.Error()))
		}
		return true
	}
	a.storeNonPKUnique(qn, n > 0)
	return n > 0
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
func (la *laneApplierAdapter) WriteCheckpoint(ctx context.Context, pos ir.Position, rowsApplied int64) error {
	a := la.a
	// H-4: the frontier checkpoint is the concurrent path's position-write
	// boundary, so flush the coalesced skip ledger the W lanes accumulated
	// here, BEFORE the merged position becomes durable. A flush failure aborts
	// the checkpoint (loud); the lanes' work re-streams from the last frontier.
	if err := a.flushSkippedTables(ctx); err != nil {
		return err
	}
	posCtx, cancel := a.execTimeoutCtx(ctx)
	defer cancel()
	tx, err := a.db.BeginTx(posCtx, nil)
	if err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint begin: %w", err))
	}
	if err := writePositionTx(posCtx, tx, a.controlKeyspace, la.streamID, pos.Token, a.slotName, a.publicationName, a.rowFilterHash, a.sourceFingerprint, a.targetSchema, rowsApplied, a.upsert); err != nil {
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
	if err := orch.Run(ctx, changes); err != nil {
		return err
	}
	// H-4: clean-stop drain of any skips the lanes recorded after the last
	// frontier checkpoint (the orchestrator drains all lanes before returning,
	// so their skips are accumulated and awaiting a flush).
	return a.flushSkippedTables(ctx)
}
