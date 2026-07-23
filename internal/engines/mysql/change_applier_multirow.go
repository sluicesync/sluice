// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # Multi-row INSERT coalescing for MySQL CDC apply (ADR-0139, Bug 169)
//
// ADR-0092/0138 made the Postgres apply path RTT-independent by pipelining a
// whole batch onto one pgx.Batch (one round trip per batch, regardless of N).
// MySQL has NO pipelining primitive — both its single-lane batch loop and its
// ADR-0104 concurrent lanes dispatch one serial tx.ExecContext per change, so
// a batch of N changes is N network round trips. On a LAN that is invisible;
// over WAN it caps apply at lanes/RTT and stalls behind Vitess's 20s
// transaction killer (Bug 169 — measured ~20/s to PlanetScale-MySQL vs PG's
// ~5,000/s).
//
// This file coalesces consecutive same-table, same-column-shape, KEYED
// INSERTs into ONE parameterised multi-row
//
//	INSERT INTO `s`.`t` (cols) VALUES (?,..),(?,..),… AS new
//	ON DUPLICATE KEY UPDATE col=new.col,…
//
// — one round trip for a run of N inserts. The load-bearing safety property
// (the Bug-74 reason this is safe where multi-statement interpolation would
// not be): every value is still bound to a `?` through the SAME
// prepareApplierValue codec the single-row path uses, so the wire encoding of
// each value is byte-IDENTICAL — there are only more placeholder groups per
// statement. No interpolation, no codec change.
//
// The [mysqlBatchTx] accumulator buffers a *pending run* of coalescable
// inserts and flushes it (emits the multi-row statement) before ANY
// non-coalescable change (Update / Delete / Truncate / SchemaSnapshot, a
// keyless-table Insert, an Insert to a different table, or one with a
// different column set) so the at-the-target apply order matches the source
// change order. Within one multi-row INSERT, MySQL applies the VALUES list
// left-to-right for ON DUPLICATE KEY UPDATE, so two same-PK inserts in one run
// resolve last-wins — identical to serial. Both the single-lane batch path
// (via the [appliershared.BatchConfig] seam) and the ADR-0104 concurrent lane
// path drive the SAME accumulator + builder, one source of truth.
//
// # ADR-0140: extend coalescing to UPDATE and DELETE (the Bug 169 U/D tail)
//
// Serial U/D were one round trip each (~37/s to PlanetScale-MySQL over WAN);
// ADR-0140 folds them into the same round-trip-efficient shape:
//
//   - A KEYED, non-PK-changing UPDATE is applied as the SAME multi-row
//     INSERT(after-image) … ON DUPLICATE KEY UPDATE upsert the INSERT path
//     uses — the row exists in a valid CDC stream (same PK → same lane →
//     source order, so its INSERT already landed), so MySQL takes the
//     ON DUPLICATE KEY UPDATE branch and sets the after-image BY PRIMARY KEY.
//     Inserts and update-upserts to the same table + identical column shape
//     coalesce into ONE statement.
//   - Consecutive KEYED DELETEs to one table coalesce into one parameterised
//     DELETE … WHERE pk IN (?,…) (single-col PK) or WHERE (a,b) IN ((?,?),…)
//     (composite PK). PK values bind through the SAME prepareApplierValue codec
//     — no interpolation, no per-type dispatch (the Bug-74 value-fidelity
//     property is preserved, exactly as for the multi-row INSERT).
//
// The accumulator holds at most ONE active run — an upsert-run OR a delete-run.
// A change that cannot extend the current run flushes it first (preserving
// apply order): a kind switch (upsert↔delete), a table/shape switch, a
// per-statement cap overflow, or a serial change. Exclusions stay on the serial
// full-before path unchanged: a keyless (no-PK) table's U/D (no PK to key on),
// a PK-changing UPDATE (upserting at the new PK would orphan the old-PK row),
// and any non-row event. The WHERE semantics for the coalesced forms change
// from full-before-match to PK-based — correct, indeed self-healing, for a
// keyed stream (ADR-0140 §Correctness): the PK form realises "this PK's row
// becomes <after> / is gone" and corrects a drifted target rather than
// silently skipping it.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// Per-statement caps on a single coalesced multi-row INSERT, so one statement
// never blows max_allowed_packet (the overall batch tx is still bounded by the
// AIMD batch size + the ADR-0028 byte cap). When the pending run would exceed
// either cap, it auto-flushes and the current row starts a fresh run.
//
//   - maxCoalescedStatementBytes: the pscale dumper's battle-tested 1 MiB
//     statement size — comfortably under any max_allowed_packet (MySQL default
//     64 MiB, PlanetScale 16 MiB+).
//   - maxCoalescedPlaceholders: MySQL's prepared-statement parameter count is a
//     16-bit field (hard limit 65,535); we cap below it on total bound `?`s
//     (rows × columns), which is the real binding constraint for narrow rows.
const (
	maxCoalescedStatementBytes int64 = 1 << 20 // 1 MiB
	maxCoalescedPlaceholders         = 60000
)

// multiRowFlushHookForTest, when non-nil, is invoked by [mysqlBatchTx.flushPending]
// with the number of rows in the just-emitted coalesced INSERT — so an
// integration pin can assert the multi-row path is actually taken (a flush with
// rows > 1) and that a silent regression to per-row apply is caught. Production
// leaves it nil. Set only by single-test fixtures (set then reset in the same
// test), mirroring the package's other test seams (laneCommitHookForTest).
var multiRowFlushHookForTest func(rows int)

// multiRowDeleteFlushHookForTest is the ADR-0140 delete-run analogue of
// [multiRowFlushHookForTest]: when non-nil it is invoked by
// [mysqlBatchTx.flushDeletes] with the number of keys in the just-emitted
// coalesced DELETE … IN, so an integration pin can assert the delete-coalescing
// path is actually taken (a flush with keys > 1) rather than silently falling
// back to serial per-row deletes. Production leaves it nil.
var multiRowDeleteFlushHookForTest func(keys int)

// insertRun is the pending run of coalescable inserts buffered by
// [mysqlBatchTx]: a contiguous sequence of Inserts that all target the same
// schema.table with the identical ordered (non-generated) column set and a
// keyed table, accumulated until a flush boundary. The raw rows are held and
// bound at flush time via [buildMultiRowInsertSQL] (same prepareApplierValue
// path as the single-row builder); args/bytes track the per-statement caps.
type insertRun struct {
	schema   string
	table    string
	cols     []string
	pk       []string
	colTypes map[string]*ir.Column
	rows     []ir.Row
	args     int
	bytes    int64
}

func (r *insertRun) empty() bool { return len(r.rows) == 0 }

func (r *insertRun) reset() { *r = insertRun{} }

// shouldFlushBefore reports whether the pending run must be flushed before
// appending an insert for schema.table with the given ordered column set and
// incremental placeholder/byte cost — i.e. the run is non-empty and the new row
// either targets a different table, carries a different column shape, or would
// push the coalesced statement past the placeholder/byte caps. A keyless or
// non-insert change is handled by the caller (it always flushes first); this
// pure helper governs only the insert-to-insert grouping decision, so it is
// unit-testable without a live connection.
func (r *insertRun) shouldFlushBefore(schema, table string, cols []string, addArgs int, addBytes int64) bool {
	if r.empty() {
		return false
	}
	if r.schema != schema || r.table != table || !slices.Equal(r.cols, cols) {
		return true
	}
	return r.args+addArgs > maxCoalescedPlaceholders || r.bytes+addBytes > maxCoalescedStatementBytes
}

// deleteRun is the ADR-0140 pending run of coalescable DELETEs buffered by
// [mysqlBatchTx]: a contiguous sequence of keyed Deletes to the same
// schema.table, accumulated until a flush boundary and emitted as one
// parameterised DELETE … WHERE pk IN (…). keys holds the ordered primary-key
// values extracted from each Delete's Before image (one []any per row, in pk
// order); they bind through the SAME prepareApplierValue codec at flush time
// ([buildMultiRowDeleteSQL]). args/bytes track the per-statement caps. The pk
// column list is table-derived, so a same-table run never changes shape — only
// a table/schema switch or a cap overflow forces a flush.
type deleteRun struct {
	schema   string
	table    string
	pk       []string
	colTypes map[string]*ir.Column
	keys     [][]any
	args     int
	bytes    int64
}

func (r *deleteRun) empty() bool { return len(r.keys) == 0 }

func (r *deleteRun) reset() { *r = deleteRun{} }

// shouldFlushBefore reports whether the pending delete-run must be flushed
// before appending a Delete for schema.table with the given incremental
// placeholder/byte cost — i.e. the run is non-empty and the new key either
// targets a different table or would push the coalesced statement past the
// caps. The pk shape is table-derived (no cols argument like the insert run),
// so a same-table append never changes shape. A keyless / kind-switch boundary
// is handled by the caller (which flushes first); this pure helper governs only
// the delete-to-delete grouping decision, so it is unit-testable without a live
// connection.
func (r *deleteRun) shouldFlushBefore(schema, table string, addArgs int, addBytes int64) bool {
	if r.empty() {
		return false
	}
	if r.schema != schema || r.table != table {
		return true
	}
	return r.args+addArgs > maxCoalescedPlaceholders || r.bytes+addBytes > maxCoalescedStatementBytes
}

// mysqlBatchTx is the ADR-0139/0140 coalescing batch-transaction handle. It
// wraps the batch's *sql.Tx and buffers AT MOST ONE active run — an upsert-run
// (coalescable inserts + non-PK-changing keyed update-upserts) OR a delete-run
// (coalescable keyed deletes). The shared batch loop
// ([appliershared.RunOneBatch]) only ever calls Rollback on it, while the
// applier's Dispatch / WritePosition / Commit closures type-assert it to drive
// the coalescing. It satisfies [appliershared.BatchTx].
type mysqlBatchTx struct {
	a   *ChangeApplier
	tx  *sql.Tx
	run insertRun
	del deleteRun

	// ctx is the batch's context, captured at BeginTx. The shared loop's
	// Commit closure takes no ctx, but flushPending (which Commit calls to
	// drain the buffer before committing) needs one; the batch lives entirely
	// within a single RunOneBatch call under this ctx, so storing it on the
	// handle is the correct lifetime (mirrors PG's pgxBatchTx.ctx).
	ctx context.Context
}

// beginCoalescingBatchTx opens the batch *sql.Tx and wraps it in a
// [mysqlBatchTx], returned as the [appliershared.BatchTx] the seam requires.
// It replaces the pre-ADR-0139 plain *sql.Tx BeginTx; the data-write semantics
// are unchanged (one tx per batch), only inserts now coalesce before exec.
func (a *ChangeApplier) beginCoalescingBatchTx(ctx context.Context) (appliershared.BatchTx, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql: applier: begin tx: %w", err)
	}
	return &mysqlBatchTx{a: a, tx: tx, ctx: ctx}, nil
}

// dispatch routes one change through the ADR-0139/0140 coalescing state machine
// on the open tx. The accumulator holds at most one active run; the per-kind
// helpers each flush the OTHER run first (a kind switch) so the at-the-target
// apply order matches the source change order:
//
//   - a keyed Insert / non-PK-changing keyed Update → the upsert-run
//     (multi-row INSERT … ON DUPLICATE KEY UPDATE);
//   - a keyed Delete → the delete-run (DELETE … WHERE pk IN (…));
//   - everything else — a keyless-table change (no PK to key on; ADR-0089), a
//     PK-changing Update (would orphan the old-PK row), or a non-row event
//     (Truncate / SchemaSnapshot / Tx*) — flushes both pending runs and applies
//     serially via the existing per-change [ChangeApplier.dispatch].
func (b *mysqlBatchTx) dispatch(ctx context.Context, streamID string, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		return b.dispatchInsert(ctx, streamID, c, v)
	case ir.Update:
		return b.dispatchUpdate(ctx, streamID, c, v)
	case ir.Delete:
		return b.dispatchDelete(ctx, streamID, c, v)
	default:
		// Non-row event (Truncate / SchemaSnapshot / Tx*): flush both runs so
		// the prior data lands first, then apply serially.
		return b.applySerial(ctx, streamID, c)
	}
}

// dispatchInsert buffers a coalescable Insert into the upsert-run, or applies it
// serially when the table is truly keyless (ADR-0089: keyless inserts are not
// idempotent, so they apply one-per-tx and are never coalesced — isKeylessInsert
// emits the once-per-table WARN itself).
func (b *mysqlBatchTx) dispatchInsert(ctx context.Context, streamID string, c ir.Change, ins ir.Insert) error {
	if b.a.isKeylessInsert(ctx, c) {
		return b.applySerial(ctx, streamID, c)
	}
	schema := b.a.routedSchema(ins.Schema)
	pk, err := b.a.pkFor(ctx, b.tx, schema, ins.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", schema, ins.Table, err)
	}
	colTypes, err := b.a.colTypesFor(ctx, b.tx, schema, ins.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, ins.Table, err)
	}
	return b.appendUpsert(ctx, schema, ins.Table, ins.Row, pk, colTypes, ir.ApproximateChangeBytes(c))
}

// dispatchUpdate buffers a keyed, non-PK-changing Update into the upsert-run as
// its after-image (the row exists in a valid CDC stream, so MySQL takes the
// ON DUPLICATE KEY UPDATE branch — same end state as the serial UPDATE, keyed by
// PK). It applies the Update serially (full-before WHERE) when the table has no
// PK to key on or the Update changes a PK column — upserting the after-image at
// the new PK would orphan the old-PK row (ADR-0140 §Correctness). The lane
// router already barriers PK-changing updates; this guard makes the single-lane
// path safe too.
func (b *mysqlBatchTx) dispatchUpdate(ctx context.Context, streamID string, c ir.Change, upd ir.Update) error {
	schema := b.a.routedSchema(upd.Schema)
	pk, err := b.a.pkFor(ctx, b.tx, schema, upd.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", schema, upd.Table, err)
	}
	if len(pk) == 0 || upd.After == nil || laneapply.PKChangedUpdate(upd, pk) {
		return b.applySerial(ctx, streamID, c)
	}
	colTypes, err := b.a.colTypesFor(ctx, b.tx, schema, upd.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, upd.Table, err)
	}
	return b.appendUpsert(ctx, schema, upd.Table, upd.After, pk, colTypes, ir.ApproximateChangeBytes(c))
}

// dispatchDelete buffers a keyed Delete into the delete-run (its primary-key
// values from the Before image), or applies it serially (full-before WHERE) when
// the table has no PK or the Before image is missing a PK column — exactly the
// cases for which a PK-keyed DELETE … IN cannot be built.
func (b *mysqlBatchTx) dispatchDelete(ctx context.Context, streamID string, c ir.Change, del ir.Delete) error {
	schema := b.a.routedSchema(del.Schema)
	pk, err := b.a.pkFor(ctx, b.tx, schema, del.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", schema, del.Table, err)
	}
	pkVals, ok := laneapply.PKValuesFromRow(c, pk)
	if len(pk) == 0 || !ok {
		return b.applySerial(ctx, streamID, c)
	}
	colTypes, err := b.a.colTypesFor(ctx, b.tx, schema, del.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, del.Table, err)
	}
	return b.appendDelete(ctx, schema, del.Table, pk, pkVals, colTypes, ir.ApproximateChangeBytes(c))
}

// applySerial flushes both pending runs (so prior coalesced data lands first,
// preserving apply order) then applies one change single-row via the existing
// per-change [ChangeApplier.dispatch] on the open tx.
func (b *mysqlBatchTx) applySerial(ctx context.Context, streamID string, c ir.Change) error {
	if err := b.flushPending(ctx); err != nil {
		return err
	}
	return b.a.dispatch(ctx, b.tx, streamID, c)
}

// appendUpsert buffers one row (an Insert row or an Update after-image) into the
// upsert-run. It first flushes any pending delete-run (a kind switch), then
// flushes the upsert-run if the new row switches table/shape or would overflow a
// per-statement cap, and finally appends. Inserts and update-upserts to the same
// table + identical column shape coalesce into ONE multi-row statement.
func (b *mysqlBatchTx) appendUpsert(ctx context.Context, schema, table string, row ir.Row, pk []string, colTypes map[string]*ir.Column, addBytes int64) error {
	if err := b.flushDeletes(ctx); err != nil {
		return err
	}
	cols := appliershared.NonGeneratedRowKeys(row, colTypes)
	addArgs := len(cols)
	if b.run.shouldFlushBefore(schema, table, cols, addArgs, addBytes) {
		if err := b.flushUpserts(ctx); err != nil {
			return err
		}
	}
	if b.run.empty() {
		b.run.schema = schema
		b.run.table = table
		b.run.cols = cols
		b.run.pk = pk
		b.run.colTypes = colTypes
	}
	b.run.rows = append(b.run.rows, row)
	b.run.args += addArgs
	b.run.bytes += addBytes
	return nil
}

// appendDelete buffers one Delete's ordered primary-key values into the
// delete-run. It first flushes any pending upsert-run (a kind switch), then
// flushes the delete-run if the new key switches table or would overflow a
// per-statement cap, and finally appends.
func (b *mysqlBatchTx) appendDelete(ctx context.Context, schema, table string, pk []string, pkVals []any, colTypes map[string]*ir.Column, addBytes int64) error {
	if err := b.flushUpserts(ctx); err != nil {
		return err
	}
	addArgs := len(pkVals)
	if b.del.shouldFlushBefore(schema, table, addArgs, addBytes) {
		if err := b.flushDeletes(ctx); err != nil {
			return err
		}
	}
	if b.del.empty() {
		b.del.schema = schema
		b.del.table = table
		b.del.pk = pk
		b.del.colTypes = colTypes
	}
	b.del.keys = append(b.del.keys, pkVals)
	b.del.args += addArgs
	b.del.bytes += addBytes
	return nil
}

// flushPending drains BOTH coalesced runs. By the accumulator's at-most-one-
// active-run invariant only one is ever non-empty, but draining both is correct
// and safe (the empty one no-ops). Safe to call before a serial change, before a
// position write, and before commit.
func (b *mysqlBatchTx) flushPending(ctx context.Context) error {
	if err := b.flushUpserts(ctx); err != nil {
		return err
	}
	return b.flushDeletes(ctx)
}

// flushUpserts emits the buffered upsert-run as one multi-row INSERT on the open
// tx (via [buildMultiRowInsertSQL] — byte-identical value encoding to the
// single-row path) and clears it. A no-op when the run is empty.
func (b *mysqlBatchTx) flushUpserts(ctx context.Context) error {
	if b.run.empty() {
		return nil
	}
	stmt, args, err := buildMultiRowInsertSQL(b.run.schema, b.run.table, b.run.rows, b.run.pk, b.run.colTypes, b.a.upsert)
	if err != nil {
		return fmt.Errorf("mysql: applier: build multi-row insert for %s.%s: %w", b.run.schema, b.run.table, err)
	}
	if _, err := b.a.txExec(ctx, b.tx, stmt, args...); err != nil {
		return fmt.Errorf("mysql: applier: multi-row insert into %s.%s: %w", b.run.schema, b.run.table, err)
	}
	if multiRowFlushHookForTest != nil {
		multiRowFlushHookForTest(len(b.run.rows))
	}
	b.a.noteCoalescedFlush(ctx, len(b.run.rows))
	b.run.reset()
	return nil
}

// flushDeletes emits the buffered delete-run as one parameterised
// DELETE … WHERE pk IN (…) on the open tx (via [buildMultiRowDeleteSQL] — PK
// values bound through the same prepareApplierValue codec) and clears it. A
// no-op when the run is empty.
func (b *mysqlBatchTx) flushDeletes(ctx context.Context) error {
	if b.del.empty() {
		return nil
	}
	stmt, args, err := buildMultiRowDeleteSQL(b.del.schema, b.del.table, b.del.pk, b.del.keys, b.del.colTypes)
	if err != nil {
		return fmt.Errorf("mysql: applier: build multi-row delete for %s.%s: %w", b.del.schema, b.del.table, err)
	}
	if _, err := b.a.txExec(ctx, b.tx, stmt, args...); err != nil {
		return fmt.Errorf("mysql: applier: multi-row delete from %s.%s: %w", b.del.schema, b.del.table, err)
	}
	if multiRowDeleteFlushHookForTest != nil {
		multiRowDeleteFlushHookForTest(len(b.del.keys))
	}
	b.a.noteCoalescedFlush(ctx, len(b.del.keys))
	b.del.reset()
	return nil
}

// writePosition flushes the pending run (all data durable before the position)
// then writes the stream position on the same tx — the first half of the
// ADR-0007 position-and-data atomicity contract. Mirrors the serial batch
// path's WritePosition closure, with the leading flush added.
func (b *mysqlBatchTx) writePosition(ctx context.Context, streamID, token string, rowsApplied int64) error {
	if err := b.flushPending(ctx); err != nil {
		return err
	}
	posCtx, posCancel := b.a.execTimeoutCtx(ctx)
	defer posCancel()
	return writePositionTx(posCtx, b.tx, b.a.controlKeyspace, streamID, token, b.a.slotName, b.a.publicationName, b.a.sourceFingerprint, b.a.targetSchema, rowsApplied, b.a.upsert)
}

// commit flushes any remaining pending run (the CheckpointOnlyAtTxBoundary
// mid-tx flush path skips writePosition, so the data buffer can still be
// non-empty here), then commits under the Bug-56 watchdog. On a flush error
// the tx is rolled back so a half-applied batch can never commit.
func (b *mysqlBatchTx) commit() error {
	if err := b.flushPending(b.ctx); err != nil {
		_ = b.tx.Rollback()
		return err
	}
	return b.a.commitWithTimeout(b.tx)
}

// Rollback discards both buffered runs and rolls back the tx, satisfying
// [appliershared.BatchTx]. The shared loop calls it on every error path
// (dispatch failure, ctx cancel, position-write failure); any emitted multi-row
// statement was either already exec'd-and-will-roll-back or never sent, so this
// discards both data and position atomically.
func (b *mysqlBatchTx) Rollback() error {
	b.run.reset()
	b.del.reset()
	return b.tx.Rollback()
}

// buildMultiRowInsertSQL builds a parameterised multi-row upsert for `rows`,
// all of which MUST share the identical ordered non-generated column set
// (the caller — [mysqlBatchTx.appendUpsert] — guarantees this; the column list
// is derived from rows[0]):
//
//	INSERT INTO `s`.`t` (`a`, `b`) VALUES (?, ?), (?, ?), … AS new
//	ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
//
// Args are flattened row-major; every value is bound through the SAME
// prepareApplierValue codec the single-row [buildInsertSQL] uses, so the wire
// encoding is byte-identical (ADR-0139's value-fidelity invariant — only the
// number of placeholder groups changes). The ON DUPLICATE KEY UPDATE clause
// (idempotency, ADR-0010) depends only on the column set + PK, so it is shared
// with the single-row path via [onDuplicateKeyUpdateClause] and is identical
// for any N. For N == 1 the output is byte-identical to the pre-ADR-0139
// single-row INSERT — buildInsertSQL delegates here to guarantee that.
//
// colTypes maps column names to their full IR descriptors and is the input to
// prepareValue; a missing entry (cold cache / unknown column) is tolerated and
// the raw value bound, the same pre-Bug-6 shape buildInsertSQL preserves.
func buildMultiRowInsertSQL(schema, table string, rows []ir.Row, pk []string, colTypes map[string]*ir.Column, upsert upsertSpelling) (sqlStmt string, args []any, err error) {
	if len(rows) == 0 {
		return "", nil, errors.New("mysql: applier: buildMultiRowInsertSQL: no rows")
	}
	cols := appliershared.NonGeneratedRowKeys(rows[0], colTypes)
	colSQL := make([]string, len(cols))
	for i, c := range cols {
		colSQL[i] = quoteIdent(c)
	}

	args = make([]any, 0, len(cols)*len(rows))
	for _, row := range rows {
		for _, c := range cols {
			v, perr := prepareApplierValue(row[c], colTypes, c)
			if perr != nil {
				return "", nil, fmt.Errorf("column %q: %w", c, perr)
			}
			args = append(args, v)
		}
	}

	group := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ") + ")"
	groups := make([]string, len(rows))
	for i := range groups {
		groups[i] = group
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteIdent(schema) + "." + quoteIdent(table))
	sb.WriteString(" (")
	sb.WriteString(strings.Join(colSQL, ", "))
	sb.WriteString(") VALUES ")
	sb.WriteString(strings.Join(groups, ", "))
	sb.WriteString(onDuplicateKeyUpdateClause(cols, pk, upsert))
	return sb.String(), args, nil
}

// onDuplicateKeyUpdateClause renders the upsert tail shared by the single-row
// and multi-row INSERT builders, in the caller's [upsertSpelling] — the
// MySQL 8.0.20+ row-alias form on every flavor except mariadb, which never
// implemented the alias and keeps the legacy VALUES() function (roadmap item
// 73). With a non-empty PK the SET-list reassigns every non-PK column to the
// new row's value:
//
//	AS new ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`      (row alias)
//	ON DUPLICATE KEY UPDATE `a` = VALUES(`a`), `b` = VALUES(`b`)     (mariadb)
//
// PK columns are excluded from the SET list because updating them on conflict
// is a no-op at best (they are equal by definition during the conflict) and
// silently incorrect if the new and existing rows have differing PK shapes.
// When every column is a PK column the clause degrades to a single no-op
// `pk = new.pk` so the conflict is absorbed silently.
//
// With an empty PK list it STILL emits ON DUPLICATE KEY UPDATE — with a full-
// row SET-list — because MySQL fires that clause on a conflict against ANY
// unique index, not just the PK. This makes a no-PK table with a UNIQUE key
// idempotent on re-apply (the ADR-0072 Gap-2 interlock: a resumed cold-start
// COPY's re-sent rows upsert instead of 1062-ing); a truly keyless table never
// collides, so the clause is inert and behaviour is effectively plain INSERT.
// See the ChangeApplier package doc for the full resume-idempotency contract.
func onDuplicateKeyUpdateClause(cols, pk []string, upsert upsertSpelling) string {
	var sb strings.Builder
	if len(pk) > 0 {
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
			sb.WriteString(upsert.clauseOpen())
			parts := make([]string, len(nonPK))
			for i, c := range nonPK {
				parts[i] = quoteIdent(c) + " = " + upsert.newRowRef(quoteIdent(c))
			}
			sb.WriteString(strings.Join(parts, ", "))
			return sb.String()
		}
		// Every column is a PK column — the row IS its own key. On conflict
		// there's nothing to update; emit a no-op assignment so the conflict
		// is absorbed silently.
		sb.WriteString(upsert.clauseOpen())
		sb.WriteString(quoteIdent(pk[0]))
		sb.WriteString(" = ")
		sb.WriteString(upsert.newRowRef(quoteIdent(pk[0])))
		return sb.String()
	}
	// No PRIMARY KEY: full-row SET-list so a collision on any unique index is
	// absorbed idempotently rather than erroring with MySQL 1062 (the ADR-0072
	// Gap-2 interlock; see the doc above).
	sb.WriteString(upsert.clauseOpen())
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c) + " = " + upsert.newRowRef(quoteIdent(c))
	}
	sb.WriteString(strings.Join(parts, ", "))
	return sb.String()
}

// buildMultiRowDeleteSQL builds a parameterised, PK-keyed multi-row DELETE for
// `keys` — one ordered primary-key tuple per buffered Delete, all targeting the
// same schema.table with the same PK (the caller — [mysqlBatchTx.appendDelete] —
// guarantees this). The single-column-PK form is a flat IN list; a composite PK
// uses the row-value tuple form:
//
//	DELETE FROM `s`.`t` WHERE `id` IN (?, ?, …)
//	DELETE FROM `s`.`t` WHERE (`a`, `b`) IN ((?, ?), (?, ?), …)
//
// Every PK value binds to a `?` through the SAME prepareApplierValue codec the
// serial [buildWhereClause] uses, so the wire encoding is byte-identical — no
// interpolation, no per-type dispatch (the ADR-0140 value-fidelity invariant).
// PK columns are never JSON in MySQL, so no CAST(? AS JSON) placeholder shaping
// is needed (that is the [placeholderFor] concern for the full-before WHERE).
// Set membership is order-independent, so the apply order across deletes within
// one statement does not matter; the kind/table flush boundaries preserve order
// against everything else (ADR-0140 §Correctness).
//
// colTypes maps column names to their full IR descriptors and is the input to
// prepareValue; a missing entry (cold cache / unknown column) is tolerated and
// the raw value bound, mirroring the insert builder.
func buildMultiRowDeleteSQL(schema, table string, pk []string, keys [][]any, colTypes map[string]*ir.Column) (sqlStmt string, args []any, err error) {
	if len(keys) == 0 {
		return "", nil, errors.New("mysql: applier: buildMultiRowDeleteSQL: no keys")
	}
	if len(pk) == 0 {
		return "", nil, errors.New("mysql: applier: buildMultiRowDeleteSQL: empty primary key")
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	args = make([]any, 0, len(keys)*len(pk))

	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(tableRef)
	sb.WriteString(" WHERE ")

	if len(pk) == 1 {
		sb.WriteString(quoteIdent(pk[0]))
		sb.WriteString(" IN (")
		placeholders := make([]string, len(keys))
		for i, k := range keys {
			if len(k) != 1 {
				return "", nil, fmt.Errorf("mysql: applier: buildMultiRowDeleteSQL: key %d has %d values, want 1", i, len(k))
			}
			v, perr := prepareApplierValue(k[0], colTypes, pk[0])
			if perr != nil {
				return "", nil, fmt.Errorf("column %q: %w", pk[0], perr)
			}
			args = append(args, v)
			placeholders[i] = "?"
		}
		sb.WriteString(strings.Join(placeholders, ", "))
		sb.WriteString(")")
		return sb.String(), args, nil
	}

	// Composite PK: row-value tuple form (`a`, `b`) IN ((?, ?), …).
	pkSQL := make([]string, len(pk))
	for i, p := range pk {
		pkSQL[i] = quoteIdent(p)
	}
	group := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(pk)), ", ") + ")"
	groups := make([]string, len(keys))
	for i, k := range keys {
		if len(k) != len(pk) {
			return "", nil, fmt.Errorf("mysql: applier: buildMultiRowDeleteSQL: key %d has %d values, want %d", i, len(k), len(pk))
		}
		for j, p := range pk {
			v, perr := prepareApplierValue(k[j], colTypes, p)
			if perr != nil {
				return "", nil, fmt.Errorf("column %q: %w", p, perr)
			}
			args = append(args, v)
		}
		groups[i] = group
	}
	sb.WriteString("(")
	sb.WriteString(strings.Join(pkSQL, ", "))
	sb.WriteString(") IN (")
	sb.WriteString(strings.Join(groups, ", "))
	sb.WriteString(")")
	return sb.String(), args, nil
}

// # Coalescing-ratio observability (ADR-0139/0140, Bug 169 follow-up)
//
// The coalescing above helps same-kind runs a lot and alternating workloads
// little; an operator otherwise can't tell which they have. These counters +
// the rate-limited INFO line turn that into a visible metric: the running
// ratio of rows folded into multi-row statements ÷ statements emitted (avg
// rows per coalesced statement). A high ratio means long same-kind runs (one
// round trip absorbs many rows); ~1 means apply stays RTT-bound (the workload
// alternates kinds or has no runs to coalesce).

// coalesceLogInterval rate-limits the coalescing-ratio INFO line to at most
// once per window across all concurrent lanes. 30s mirrors the floor of
// appliercontrol.NoteByteCapDominant's rate limit — often enough to track a
// workload shift, quiet enough never to be per-flush noise at INFO.
const coalesceLogInterval = 30 * time.Second

// coalesceClockForTest, when non-nil, overrides the wall clock the coalescing-
// ratio rate-limiter reads so a unit test can pin the once-per-window
// behaviour deterministically. Production leaves it nil (real time.Now).
// Mirrors the package's other test seams (multiRowFlushHookForTest).
var coalesceClockForTest func() time.Time

func coalesceNow() time.Time {
	if coalesceClockForTest != nil {
		return coalesceClockForTest()
	}
	return time.Now()
}

// noteCoalescedFlush records one multi-row flush of `rows` rows against the
// coalescing counters and, at most once per [coalesceLogInterval], emits an
// INFO line reporting the running coalescing ratio + totals — so an operator
// periodically sees whether the workload benefits from coalescing.
//
// Cheap and lock-free on the apply hot path: two atomic adds, plus on the rare
// logging tick one atomic CompareAndSwap that lets exactly ONE of the W
// concurrent lanes (ADR-0104) claim the window. A rows<=0 flush (a flush
// always carries >=1 row, so this is defensive) is ignored so it can't skew
// the ratio or fire the line.
func (a *ChangeApplier) noteCoalescedFlush(ctx context.Context, rows int) {
	if rows <= 0 {
		return
	}
	totalRows := a.coalescedRows.Add(int64(rows))
	totalFlushes := a.coalescedFlushes.Add(1)

	now := coalesceNow().UnixNano()
	last := a.lastCoalesceLogNanos.Load()
	if !shouldLogCoalescing(last, now, int64(coalesceLogInterval)) {
		return
	}
	// Claim this window; if another lane beat us to it, stay quiet.
	if !a.lastCoalesceLogNanos.CompareAndSwap(last, now) {
		return
	}
	ratio := coalescingRatio(totalRows, totalFlushes)
	slog.InfoContext(
		ctx, "mysql: applier: coalescing ratio",
		slog.String("stream_id", a.streamID),
		slog.Float64("rows_per_stmt", ratio),
		slog.Int64("coalesced_rows", totalRows),
		slog.Int64("coalesced_statements", totalFlushes),
		slog.String("assessment", coalescingAssessment(ratio)),
	)
}

// shouldLogCoalescing reports whether the coalescing-ratio line is due: the
// first line ever (lastNanos == 0) always fires; thereafter only once the
// window has elapsed. Pure so the window logic is unit-testable without a clock.
func shouldLogCoalescing(lastNanos, nowNanos, intervalNanos int64) bool {
	if lastNanos == 0 {
		return true
	}
	return nowNanos-lastNanos >= intervalNanos
}

// coalescingRatio is the average rows per coalesced statement. Zero flushes
// yields 0 (no statements emitted yet).
func coalescingRatio(rows, flushes int64) float64 {
	if flushes <= 0 {
		return 0
	}
	return float64(rows) / float64(flushes)
}

// coalescingAssessment renders a short operator-facing verdict on the ratio.
// The thresholds bucket the ratio so the log reads at a glance ("good" vs
// "RTT-bound"); they are advisory only and gate nothing.
func coalescingAssessment(ratio float64) string {
	switch {
	case ratio >= 10:
		return "good — same-kind runs coalescing well"
	case ratio >= 2:
		return "moderate — some same-kind runs"
	default:
		return "RTT-bound — workload alternates kinds / no same-kind runs"
	}
}
