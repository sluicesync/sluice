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

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
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

// mysqlBatchTx is the ADR-0139 coalescing batch-transaction handle. It wraps
// the batch's *sql.Tx and buffers a pending run of coalescable inserts; the
// shared batch loop ([appliershared.RunOneBatch]) only ever calls Rollback on
// it, while the applier's Dispatch / WritePosition / Commit closures type-
// assert it to drive the coalescing. It satisfies [appliershared.BatchTx].
type mysqlBatchTx struct {
	a   *ChangeApplier
	tx  *sql.Tx
	run insertRun

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

// dispatch routes one change through the coalescing state machine on the open
// tx. A coalescable Insert (keyed table, same schema.table + identical ordered
// column set as the pending run, within the per-statement caps) is buffered
// with no round trip. ANY other change — a non-insert, a keyless-table Insert
// (ADR-0089: never coalesced, applies single-row), a table switch, a column-
// shape change, or a cap overflow — first flushes the pending run (preserving
// apply order), then either appends (table/shape/cap) or applies serially
// (non-insert / keyless) via the existing per-change [ChangeApplier.dispatch].
func (b *mysqlBatchTx) dispatch(ctx context.Context, streamID string, c ir.Change) error {
	ins, isInsert := c.(ir.Insert)
	// Non-insert OR keyless Insert: not coalescable. Flush the pending run
	// first so the prior inserts land before this change (apply order), then
	// apply it single-row via the serial dispatch. isKeylessInsert returns
	// false for non-inserts and emits the ADR-0089 once-per-table WARN itself.
	if !isInsert || b.a.isKeylessInsert(ctx, c) {
		if err := b.flushPending(ctx); err != nil {
			return err
		}
		return b.a.dispatch(ctx, b.tx, streamID, c)
	}

	// Coalescable Insert. Resolve the routed namespace + cached metadata
	// exactly as the serial Insert dispatch does (cache hits cost nothing).
	schema := b.a.routedSchema(ins.Schema)
	pk, err := b.a.pkFor(ctx, b.tx, schema, ins.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", schema, ins.Table, err)
	}
	colTypes, err := b.a.colTypesFor(ctx, b.tx, schema, ins.Table)
	if err != nil {
		return fmt.Errorf("mysql: applier: column types for %s.%s: %w", schema, ins.Table, err)
	}
	cols := appliershared.NonGeneratedRowKeys(ins.Row, colTypes)
	addArgs := len(cols)
	addBytes := ir.ApproximateChangeBytes(c)
	if b.run.shouldFlushBefore(schema, ins.Table, cols, addArgs, addBytes) {
		if err := b.flushPending(ctx); err != nil {
			return err
		}
	}
	if b.run.empty() {
		b.run.schema = schema
		b.run.table = ins.Table
		b.run.cols = cols
		b.run.pk = pk
		b.run.colTypes = colTypes
	}
	b.run.rows = append(b.run.rows, ins.Row)
	b.run.args += addArgs
	b.run.bytes += addBytes
	return nil
}

// flushPending emits the buffered run as one multi-row INSERT on the open tx
// (via [buildMultiRowInsertSQL] — byte-identical value encoding to the single-
// row path) and clears the run. A no-op when the run is empty, so it is safe to
// call before a serial change, before a position write, and before commit.
func (b *mysqlBatchTx) flushPending(ctx context.Context) error {
	if b.run.empty() {
		return nil
	}
	stmt, args, err := buildMultiRowInsertSQL(b.run.schema, b.run.table, b.run.rows, b.run.pk, b.run.colTypes)
	if err != nil {
		return fmt.Errorf("mysql: applier: build multi-row insert for %s.%s: %w", b.run.schema, b.run.table, err)
	}
	if _, err := b.a.txExec(ctx, b.tx, stmt, args...); err != nil {
		return fmt.Errorf("mysql: applier: multi-row insert into %s.%s: %w", b.run.schema, b.run.table, err)
	}
	if multiRowFlushHookForTest != nil {
		multiRowFlushHookForTest(len(b.run.rows))
	}
	b.run.reset()
	return nil
}

// writePosition flushes the pending run (all data durable before the position)
// then writes the stream position on the same tx — the first half of the
// ADR-0007 position-and-data atomicity contract. Mirrors the serial batch
// path's WritePosition closure, with the leading flush added.
func (b *mysqlBatchTx) writePosition(ctx context.Context, streamID, token string) error {
	if err := b.flushPending(ctx); err != nil {
		return err
	}
	posCtx, posCancel := b.a.execTimeoutCtx(ctx)
	defer posCancel()
	return writePositionTx(posCtx, b.tx, streamID, token, b.a.slotName, b.a.sourceFingerprint, b.a.targetSchema)
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

// Rollback discards the buffered run and rolls back the tx, satisfying
// [appliershared.BatchTx]. The shared loop calls it on every error path
// (dispatch failure, ctx cancel, position-write failure); the multi-row
// statement was either already exec'd-and-will-roll-back or never sent, so
// this discards both data and position atomically.
func (b *mysqlBatchTx) Rollback() error {
	b.run.reset()
	return b.tx.Rollback()
}

// buildMultiRowInsertSQL builds a parameterised multi-row upsert for `rows`,
// all of which MUST share the identical ordered non-generated column set
// (the caller — [mysqlBatchTx.dispatch] — guarantees this; the column list is
// derived from rows[0]):
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
func buildMultiRowInsertSQL(schema, table string, rows []ir.Row, pk []string, colTypes map[string]*ir.Column) (sqlStmt string, args []any, err error) {
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
	sb.WriteString(onDuplicateKeyUpdateClause(cols, pk))
	return sb.String(), args, nil
}

// onDuplicateKeyUpdateClause renders the row-alias upsert tail (8.0.20+) shared
// by the single-row and multi-row INSERT builders. With a non-empty PK the
// SET-list reassigns every non-PK column to the new row's value:
//
//	AS new ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
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
func onDuplicateKeyUpdateClause(cols, pk []string) string {
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
			sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
			parts := make([]string, len(nonPK))
			for i, c := range nonPK {
				parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
			}
			sb.WriteString(strings.Join(parts, ", "))
			return sb.String()
		}
		// Every column is a PK column — the row IS its own key. On conflict
		// there's nothing to update; emit a no-op assignment so the conflict
		// is absorbed silently.
		sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
		sb.WriteString(quoteIdent(pk[0]))
		sb.WriteString(" = new.")
		sb.WriteString(quoteIdent(pk[0]))
		return sb.String()
	}
	// No PRIMARY KEY: full-row SET-list so a collision on any unique index is
	// absorbed idempotently rather than erroring with MySQL 1062 (the ADR-0072
	// Gap-2 interlock; see the doc above).
	sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
	}
	sb.WriteString(strings.Join(parts, ", "))
	return sb.String()
}
