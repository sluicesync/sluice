// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The ADR-0058 §1c added-column backfill, default-on.
//
// A forwarded ADD COLUMN lands on the target with whatever DEFAULT the
// forward carried, and the target fills every row it already holds with
// that DEFAULT. The carried DEFAULT is the source's CURRENT one, read when
// the boundary reaches the intercept — which is after any later
// `ALTER COLUMN … DROP DEFAULT` / `SET DEFAULT` in the same window, and
// for a non-constant DEFAULT is re-evaluated by the target's own clock or
// generator. Django emits `ADD COLUMN c … DEFAULT 'v'` then
// `ALTER COLUMN c DROP DEFAULT` for every AddField with a default, so the
// forward carried no default and every pre-existing target row held NULL
// where the source holds 'v' — silent, at exit 0, and never corrected,
// because adding a column writes no per-row change on either engine.
//
// The backfill is the only thing that reads what the source actually
// holds: after the ALTER, it pages the source table by primary key and
// emits one synthetic [ir.Update] (PK in Before, the added columns in
// After) per row, into the SAME change channel, ahead of every change
// that follows the boundary. That placement is what makes it safe against
// concurrent source writes. Each backfilled value was read at some
// instant T after the boundary; every later change to that row is either
// already committed by T (so the value read reflects it, and its own
// replay afterwards rewrites the same value) or committed after T (so its
// replay, which comes after the backfill in channel order, wins). A row
// updated right after the ALTER therefore ends at its updated value, never
// at the backfilled one — pinned by the update-after-ALTER cells.
//
// Default-on, opt-out: [Streamer.SuppressAddedColumnBackfill] is the only
// way to skip it, so every construction that is not the CLI gets it.
//
// An interrupted backfill does NOT resume — the boundary is not seen again
// by a reopened stream. What stands between that and silent loss is the
// ledger in schema_forward_backfill_ledger.go, which ends the run with the
// recorded ADD-COLUMN-BACKFILL-INCOMPLETE refusal; its residual (a process
// killed without running its exit path) is stated there.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// lazyBackfillReader opens the source-side row reader the added-column
// backfill pages through on first use and keeps it for the Run. Opened
// lazily because the backfill is default-on while most streams never
// forward an ADD COLUMN, and because a source whose row reader cannot
// paginate by primary key (the D1 trigger source's HTTP reader) must not be
// refused at start for a backfill it will never run: the refusal fires at
// the boundary that actually needs it, naming the table.
//
// Safe for the one intercept goroutine that uses it plus the Run-exit
// close; the mutex orders the two.
type lazyBackfillReader struct {
	open func(ctx context.Context) (ir.RowReader, error)

	mu     sync.Mutex
	opened ir.RowReader
	closed bool
}

// reader returns the batched source reader, opening it on the first call.
// A source reader without [ir.BatchedRowReader] is refused: the backfill
// cannot run, and forwarding the ADD COLUMN without it is the silent loss
// the backfill exists to prevent.
func (l *lazyBackfillReader) reader(ctx context.Context) (ir.BatchedRowReader, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		// The attempt that owned it has ended; an intercept goroutine still
		// unwinding must not open a reader nothing will close.
		return nil, errors.New("backfill: the source row reader was closed with its attempt")
	}
	if l.opened == nil {
		rr, err := l.open(ctx)
		if err != nil {
			return nil, err
		}
		l.opened = rr
	}
	br, ok := l.opened.(ir.BatchedRowReader)
	if !ok {
		return nil, errBackfillSourceNotBatched
	}
	return br, nil
}

// Close releases the reader if one was opened. Idempotent.
func (l *lazyBackfillReader) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.opened != nil {
		_ = closeIfErrIgnored(l.opened)
		l.opened = nil
	}
}

// errBackfillSourceNotBatched is the refusal when the source engine's row
// reader cannot page by primary key ([ir.BatchedRowReader], ADR-0018).
// Every engine that forwards ADD COLUMN implements it; the refusal exists
// for a future source that forwards without it.
var errBackfillSourceNotBatched = errors.New(
	"the source engine's row reader does not implement BatchedRowReader (ADR-0018), which the " +
		"added-column backfill pages through; the rows the target already held cannot be filled " +
		"from the source. Recovery: pass --no-backfill-added-column to forward the column without " +
		"the backfill (pre-existing target rows then hold the target's fill, not the source's " +
		"values), or apply the change with the drained model",
)

// addedColumnBackfill returns the backfill the ADD COLUMN forward paths
// run, or nil when the operator opted out. The source reader behind it is
// created once per Run and opened on first use; --where is pushed into it
// (audit 2026-07-26 SL-11) so a filtered sync reads only its own rows.
//
// Why the predicate goes into the READER rather than through the row
// filter: the backfill is wired downstream of the --where intercept, and a
// synthetic Update carries a PK-only Before, which the row-move dispatch
// would refuse as an incomplete image. Filtering at the source SELECT also
// means an out-of-scope row is never read.
//
// catalog is the forward path's own source SchemaReader, through which a
// primary key the change stream's projection does not carry is resolved
// ([sourcePrimaryKeyResolver]); nil resolves nothing.
func (s *Streamer) addedColumnBackfill(streamID string, catalog ir.SchemaReader) *schemaForwardBackfill {
	if s.SuppressAddedColumnBackfill {
		return nil
	}
	if s.addedColumnBackfillReader == nil {
		s.addedColumnBackfillReader = &lazyBackfillReader{open: s.openAddedColumnBackfillReader}
	}
	bf := &schemaForwardBackfill{
		reader:    s.addedColumnBackfillReader.reader,
		streamID:  streamID,
		batchSize: migcore.DefaultBulkBatchSize,
		ledger:    &s.addedColumnBackfills,
	}
	if catalog != nil {
		bf.primaryKey = sourcePrimaryKeyResolver(catalog)
	}
	return bf
}

// sourcePrimaryKeyResolver returns the source catalog's primary key for a
// table, read through sr — for a change stream whose boundary projection
// carries none. The VStream FIELD event is one: it names each column's
// PRI_KEY flag but not the key's column ORDER, which the cursor's
// ORDER BY must follow for the page query to walk the key's index rather
// than sort the table on every page. One schema read per forwarded ADD
// COLUMN, the same cost as the DEFAULT probe beside it.
func sourcePrimaryKeyResolver(sr ir.SchemaReader) func(ctx context.Context, schema, table string) (*ir.Index, error) {
	return func(ctx context.Context, schema, table string) (*ir.Index, error) {
		sch, err := sr.ReadSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("read source schema: %w", err)
		}
		for _, t := range sch.Tables {
			if t == nil || t.Name != table {
				continue
			}
			// The MySQL SchemaReader leaves Schema empty; match by name
			// then, as readSchemaColumnDefault does.
			if schema != "" && t.Schema != "" && schema != t.Schema {
				continue
			}
			return t.PrimaryKey, nil
		}
		return nil, fmt.Errorf("table %q.%q not present in the source catalog", schema, table)
	}
}

// openAddedColumnBackfillReader opens the source row reader the backfill
// pages through, with the stream's --where predicates applied.
func (s *Streamer) openAddedColumnBackfillReader(ctx context.Context) (ir.RowReader, error) {
	if s.Source == nil {
		return nil, errors.New("backfill: nil source engine")
	}
	rr, err := s.Source.OpenRowReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("backfill: open source row reader: %w", err)
	}
	if err := migcore.ApplyRowFilters(rr, s.RowFilters, s.Source.Name()); err != nil {
		_ = closeIfErrIgnored(rr)
		return nil, fmt.Errorf("backfill: %w", err)
	}
	return rr, nil
}

// boundaryBackfill is the added-column backfill one forwarded boundary
// owes, planned BEFORE the boundary's snapshot is forwarded and run after
// it. Both forward paths use it — the single-stream intercept and the
// Shape A boundary intercept — once the boundary's ALTER has landed
// (applied by this stream, or on the Shape A path observed from the lease
// holder).
//
// It runs AFTER the snapshot on purpose. The applier drops its cached view
// of the table (column types, keys) when it commits the snapshot; a
// backfilled value reaching it before that is encoded against the
// pre-ALTER table, which does not know the new column — measured as a
// refused array on Postgres and a BIT(8) value written as its 8-character
// text on MySQL. Every change that follows the boundary still comes after
// the backfill, which is what the ordering argument at the top of this
// file needs.
//
// It is PLANNED before the snapshot for the same interruption reason the
// ledger exists: the owed backfill is on the ledger ([addedColumnBackfillLedger])
// from the moment the ALTER has landed, so a stop that lands while the
// snapshot is being forwarded is still reported.
//
// On the Shape A path the scope is this shard's rows by construction: the
// backfill pages THIS stream's source, and the applier stamps this
// stream's shard discriminator into every UPDATE's before-image (the
// WHERE), so a row another shard owns on the consolidated target is never
// matched, even where it shares the primary key.
type boundaryBackfill struct {
	bf        *schemaForwardBackfill // nil: the operator opted out
	tableName string
	snap      ir.SchemaSnapshot
	added     []*ir.Column
	hint      func(tableName string) string
	owed      *addedColumnBackfillEntry
}

// planBoundaryBackfill returns the backfill a routed boundary owes, or nil
// when the boundary was not an ADD COLUMN. hint is the caller's recovery
// text ([forwardRecoveryHint] or the fleet-wide [RecoveryHint]).
//
// The shape is re-derived with [ClassifyShape] over the same
// comparison-form (pre, post) pair the forward classified — a pure
// function of its inputs, so the two cannot disagree, and neither the
// router nor the forwarder needs the change channel for it.
func planBoundaryBackfill(
	bf *schemaForwardBackfill,
	tableName string,
	pre, post *ir.Table,
	snap ir.SchemaSnapshot,
	hint func(tableName string) string,
) (*boundaryBackfill, error) {
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		return nil, fmt.Errorf("classify shape for the added-column backfill on %q: %w. %s",
			tableName, err, hint(tableName))
	}
	if shape.Kind != ShapeKindAddColumn {
		return nil, nil
	}
	b := &boundaryBackfill{bf: bf, tableName: tableName, snap: snap, added: shape.AddedColumns, hint: hint}
	if bf != nil {
		b.owed = bf.ledger.open(tableName, columnNames(shape.AddedColumns))
	}
	return b, nil
}

// run executes the planned backfill (or, opted out, reports what that
// leaves behind) and settles its ledger entry. nil-safe: a boundary that
// owed nothing runs nothing.
func (b *boundaryBackfill) run(ctx context.Context, out chan<- ir.Change) error {
	if b == nil {
		return nil
	}
	err := backfillAddedColumns(ctx, b.bf, b.tableName, b.snap, b.added, out)
	if b.bf != nil {
		b.bf.ledger.finished(b.owed, err)
	}
	if err != nil {
		return fmt.Errorf("%w. %s", err, b.hint(b.tableName))
	}
	return nil
}

// backfillProgressInterval is how often a running backfill reports its row
// count, so a large table's backfill — which holds the stream behind it
// until it finishes — is visibly progressing rather than a silent stall.
const backfillProgressInterval = 30 * time.Second

// runBackfillForAddedColumn pages the just-ALTERed table on the source by
// primary key and emits one synthetic [ir.Update] per row into out: the PK
// in Before (the UPDATE's WHERE), the added columns in After (its SET).
//
// It runs inline in the intercept, so the stream behind the boundary waits
// for it; the start, a progress line every [backfillProgressInterval], and
// the end are logged at INFO with the row count so that wait is visible.
//
// An UPDATE that matches no target row is not an error — a row the source
// holds but the target does not yet (inserted after the ALTER, its INSERT
// still behind the boundary in the channel) is written by that INSERT, with
// its own explicit value.
//
// Refuse-loudly cases:
//   - the table has no primary key (cursor pagination is unsafe);
//   - the reader cannot be opened or cannot paginate;
//   - a page's read fails, including a failure the reader reports only
//     through Err() after closing the page's channel early — without that
//     check a short page reads as the end of the table and the rest is
//     never backfilled, at exit 0;
//   - an added column the source row does not carry (writing NULL for it
//     would be the silent loss this backfill exists to prevent).
//
// A cancelled ctx returns ctx.Err().
func runBackfillForAddedColumn(
	ctx context.Context,
	bf *schemaForwardBackfill,
	snap ir.SchemaSnapshot,
	addedCols []*ir.Column,
	out chan<- ir.Change,
) error {
	if bf == nil || bf.reader == nil {
		return errors.New("backfill: missing reader")
	}
	if snap.IR == nil {
		return errors.New("backfill: snapshot has nil IR")
	}
	table, err := backfillTableWithPrimaryKey(ctx, bf, snap)
	if err != nil {
		return err
	}
	addedNames := backfillColumnNames(table, addedCols)
	if len(addedNames) == 0 {
		// Every added column is generated: the target computes it from
		// the row's other columns, so there is nothing to carry.
		return nil
	}
	reader, err := bf.reader(ctx)
	if err != nil {
		return err
	}
	batchSize := bf.batchSize
	if batchSize <= 0 {
		batchSize = migcore.DefaultBulkBatchSize
	}
	pkColNames := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		pkColNames[i] = c.Column
	}
	slog.InfoContext(
		ctx, "forward-add-column: backfilling the added columns on the rows the target already held, from the source",
		"table", table.Name,
		"stream_id", bf.streamID,
		"added_columns", addedNames,
	)
	started := time.Now()
	lastProgress := started
	var cursor []any
	total := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rows, err := reader.ReadRowsBatch(ctx, table, cursor, batchSize)
		if err != nil {
			return fmt.Errorf("read rows batch: %w", err)
		}
		batchCount := 0
		var lastRow ir.Row
		for r := range rows {
			update, err := synthesizeBackfillUpdate(snap, r, pkColNames, addedNames)
			if err != nil {
				return err
			}
			if !forwardChange(ctx, out, update) {
				return ctx.Err()
			}
			lastRow = r
			batchCount++
		}
		if err := reader.Err(); err != nil {
			return fmt.Errorf("read rows batch after %d rows: %w", total+batchCount, err)
		}
		total += batchCount
		if batchCount < batchSize {
			// A short (or empty) page — with Err() clean — is the end
			// of the table.
			break
		}
		// Advance the cursor to the last row's PK so the next batch
		// is strictly greater. Matches the bulk-copy resume cursor
		// (ADR-0018).
		nextCursor := make([]any, len(pkColNames))
		for i, name := range pkColNames {
			nextCursor[i] = lastRow[name]
		}
		cursor = nextCursor
		if time.Since(lastProgress) >= backfillProgressInterval {
			lastProgress = time.Now()
			slog.InfoContext(
				ctx, "forward-add-column: backfill in progress",
				"table", table.Name,
				"stream_id", bf.streamID,
				"rows_backfilled", total,
				"elapsed", time.Since(started).Round(time.Second),
			)
		}
	}
	slog.InfoContext(
		ctx, "forward-add-column: backfill complete",
		"table", table.Name,
		"stream_id", bf.streamID,
		"rows_backfilled", total,
		"added_columns", addedNames,
		"elapsed", time.Since(started).Round(time.Millisecond),
	)
	return nil
}

// backfillTableWithPrimaryKey returns the table the backfill pages: the
// boundary's own IR, with its primary key resolved from the source catalog
// ([schemaForwardBackfill.primaryKey]) when the change stream's projection
// did not carry one. A key that names a column the projection lacks, or no
// key anywhere, is refused — cursor pagination is unsafe without one.
func backfillTableWithPrimaryKey(ctx context.Context, bf *schemaForwardBackfill, snap ir.SchemaSnapshot) (*ir.Table, error) {
	table := snap.IR
	if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 {
		return table, nil
	}
	var pk *ir.Index
	if bf.primaryKey != nil {
		resolved, err := bf.primaryKey(ctx, snap.Schema, snap.Table)
		if err != nil {
			return nil, fmt.Errorf("backfill: resolve the primary key of %q: %w", table.Name, err)
		}
		pk = resolved
	}
	if pk == nil || len(pk.Columns) == 0 {
		// No PK — can't safely iterate. Refuse loudly. Tables
		// without a PK are also rejected by the bulk-copy
		// orchestrator (ADR-0018); same recovery hint applies.
		return nil, fmt.Errorf(
			"backfill: table %q has no primary key — cursor-paginated "+
				"backfill is unsafe without a PK",
			table.Name,
		)
	}
	for _, kc := range pk.Columns {
		if !slices.ContainsFunc(table.Columns, func(c *ir.Column) bool { return c != nil && c.Name == kc.Column }) {
			return nil, fmt.Errorf("backfill: the source catalog's primary key of %q names column %q, "+
				"which the change stream's projection of the table does not carry", table.Name, kc.Column)
		}
	}
	withKey := *table
	withKey.PrimaryKey = pk
	return &withKey, nil
}

// backfillColumnNames resolves the added columns the backfill carries: each
// one by name against the post-ALTER table, less the generated ones (the
// target computes those itself, and its applier drops them from a SET).
func backfillColumnNames(table *ir.Table, added []*ir.Column) []string {
	byName := make(map[string]*ir.Column, len(table.Columns))
	for _, c := range table.Columns {
		if c != nil {
			byName[c.Name] = c
		}
	}
	names := make([]string, 0, len(added))
	for _, c := range added {
		if c == nil {
			continue
		}
		col := c
		if resolved, ok := byName[c.Name]; ok {
			col = resolved
		}
		if col.IsGenerated() {
			continue
		}
		names = append(names, c.Name)
	}
	return names
}

// synthesizeBackfillUpdate constructs the backfill's [ir.Update] for one
// source row. Before carries the PK columns (the UPDATE's WHERE predicate —
// under --inject-shard-column the applier stamps the shard discriminator
// onto it too, so a Shape A stream's backfill only ever touches its own
// shard's rows); After carries the added columns (the UPDATE's SET).
//
// Position is the boundary's own, so the applier's position write stays at
// the ALTER.
//
// A source row that does not carry an added column is refused rather than
// written as NULL: that would be a reader that failed to project the
// column, and a NULL there is the very outcome the backfill exists to
// prevent.
func synthesizeBackfillUpdate(
	snap ir.SchemaSnapshot,
	row ir.Row,
	pkColNames []string,
	addedNames []string,
) (ir.Update, error) {
	before := make(ir.Row, len(pkColNames))
	for _, name := range pkColNames {
		before[name] = row[name]
	}
	after := make(ir.Row, len(addedNames))
	for _, name := range addedNames {
		v, ok := row[name]
		if !ok {
			return ir.Update{}, fmt.Errorf(
				"backfill: the source row read for %q carries no value for added column %q "+
					"(primary key %v); refusing rather than writing NULL over it",
				snap.Table, name, before,
			)
		}
		after[name] = v
	}
	return ir.Update{
		Position: snap.Position,
		Schema:   snap.Schema,
		Table:    snap.Table,
		Before:   before,
		After:    after,
	}, nil
}
