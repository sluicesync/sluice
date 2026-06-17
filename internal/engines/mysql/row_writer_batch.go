// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Idempotent INSERT path for the resume-mid-table bulk-copy.
//
// See the postgres engine's row_writer_batch.go for the design
// rationale — same shape, different syntax. MySQL uses the row-alias
// UPSERT form (8.0.20+):
//
//	INSERT INTO `tbl` (`a`, `b`, `id`) VALUES (?, ?, ?), ... AS new
//	ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
//
// The row alias (`AS new`) is the modern form that lets us reference
// the new row's values by column without VALUES() (which is
// deprecated on MySQL 8.0.20+). PlanetScale's Vitess proxy supports
// the alias form; pre-8.0.20 MySQL deployments are below the project's
// declared minimum so the alias form is safe.
//
// When every column is a PK column there's nothing meaningful to
// SET on conflict; the statement falls back to a no-op
// `id = new.id` reassignment so the conflict is absorbed silently.
// Same shape as the change_applier's buildInsertSQL fallback for
// no-non-PK tables.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// WriteRowsIdempotent implements [ir.IdempotentRowWriter]. The shape
// mirrors [writeBatched] but the SQL goes through
// [buildBatchUpsert] which appends the ON DUPLICATE KEY UPDATE clause.
func (w *RowWriter) WriteRowsIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("mysql: WriteRowsIdempotent: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("mysql: WriteRowsIdempotent: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("mysql: WriteRowsIdempotent: rows channel is nil")
	}
	return w.writeBatchedIdempotent(ctx, table, rows)
}

// HandlesNoPKIdempotentCopy implements [ir.IdempotentCopyWriter]: the
// MySQL idempotent copy path keys the upsert on a non-null UNIQUE index
// when the table has no PRIMARY KEY ([effectiveUpsertKeyColumns]) and
// refuses a truly keyless table loudly ([errKeylessIdempotent]), so it
// never plain-INSERTs a no-PK table — which would duplicate VStream COPY
// catchup re-emissions (Bug 125). The orchestrator gates the cold-start
// no-PK path on this capability.
func (w *RowWriter) HandlesNoPKIdempotentCopy() bool { return true }

// WriteRowsIdempotentParallel implements [ir.ParallelIdempotentCopyWriter]
// (ADR-0097): the WRITE-side fan-out for the VStream/CDC snapshot
// cold-start copy. It runs one worker goroutine per supplied channel,
// each pinning its OWN connection from the shared *sql.DB pool and
// running the same [writeBatchedIdempotentConn] core the serial path
// runs — so per-row value fidelity and the Vector-B warning probe are
// byte-identical to the serial path; fan-out changes only how many
// connections carry the load.
//
// Correctness contract (the load-bearing part):
//   - The PIPELINE owns the PK-hash partition that routed each row to
//     exactly one of these channels (no drop/dup); this method owns
//     only the N-worker execution.
//   - It returns only AFTER every worker has drained its channel and
//     durably committed its final batch (the join below) — so the
//     orchestrator advances no position until every row is durable
//     (ADR-0007). A worker still in flight when the position commits
//     would be a silent-loss-on-resume gap; the join makes that
//     structurally impossible.
//   - The first worker error cancels a shared child context so the
//     other workers (and the pipeline's reader) unwind; the first
//     error is returned (loud abort, no partial silent success).
//   - On ctx cancel every worker's loop selects on ctx.Done() and
//     exits; each worker's pinned conn is Closed in its own defer, so
//     no goroutine or connection leaks.
//
// MID-COPY DURABLE-PROGRESS CHECKPOINT IS DISABLED ON THIS PATH (the
// silent-loss guard). The ADR-0072 Phase B watermark (w.copyDurableProgress
// → the snapshot reader's AdvanceDurableRows) is only sound when the
// durable flushed-row COUNT is order-equivalent to the reader's
// enqueue-order breadcrumb frontier — true under a SINGLE serial FIFO
// stream (durable-count K ⟹ the first K enqueued rows are durable), FALSE
// under fan-out: rows flush in per-worker order with independent batch
// buffers, so a fast worker can push the flat count to K while a lagging
// worker still holds an EARLY-enqueued row un-flushed. A breadcrumb at
// rowsCovered=K would then be checkpointed as durable while an early row
// is not yet on the target — a hard crash after the checkpoint resumes
// PAST that row (silent loss, exit 0). So every worker here is run with
// reportDurable=false; the whole-table join below (wg.Wait before any
// position advances, ADR-0007) is the SOLE durability guarantee for a
// fanned-out table, and the orchestrator's final position persistence at
// COPY_COMPLETED fires only after that join (the fully-durable position).
// Resume never fans out (ADR-0095 single-stream v1), so no resume path
// consumes a mid-COPY fan-out cursor anyway.
//
// keyCols is resolved ONCE here (not per worker) so a keyless table is
// refused loudly before any worker spawns, identical to the serial
// path's gate.
func (w *RowWriter) WriteRowsIdempotentParallel(ctx context.Context, table *ir.Table, workers []<-chan ir.Row) error {
	if table == nil {
		return errors.New("mysql: WriteRowsIdempotentParallel: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("mysql: WriteRowsIdempotentParallel: table %q has no columns", table.Name)
	}
	if len(workers) == 0 {
		return errors.New("mysql: WriteRowsIdempotentParallel: no worker channels")
	}
	for i, ch := range workers {
		if ch == nil {
			return fmt.Errorf("mysql: WriteRowsIdempotentParallel: worker channel %d is nil", i)
		}
	}
	// Resolve the conflict key once — refuse a keyless table loudly
	// before spawning any worker (Bug 125), same gate as the serial path.
	keyCols, ok := effectiveUpsertKeyColumns(table)
	if !ok {
		return errKeylessIdempotent(table)
	}

	// A single worker is just the serial core — no fan-out machinery
	// needed (and the pipeline only calls this with len==1 on the
	// degenerate path). reportDurable=false: this method IS the fan-out
	// path (see the mid-COPY-checkpoint note below); even the 1-worker
	// case never advances the durable watermark, so the fan-out vs serial
	// durability contract is uniform regardless of degree.
	if len(workers) == 1 {
		conn, err := w.db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("mysql: idempotent insert into %q: pin connection: %w", table.Name, err)
		}
		defer func() { _ = conn.Close() }()
		return w.writeBatchedIdempotentConn(ctx, conn, table, keyCols, workers[0], false)
	}

	// Shared child ctx: the first worker error cancels it so peers (and
	// the pipeline's reader, which selects on the same parent ctx via
	// the caller) unwind deterministically.
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		firstEr error
	)
	for _, ch := range workers {
		wg.Add(1)
		go func(rows <-chan ir.Row) {
			defer wg.Done()
			conn, err := w.db.Conn(workerCtx)
			if err != nil {
				errOnce.Do(func() { firstEr = fmt.Errorf("mysql: idempotent insert into %q: pin connection: %w", table.Name, err) })
				cancel()
				return
			}
			defer func() { _ = conn.Close() }()
			// reportDurable=false — fan-out workers must not advance the
			// mid-COPY durable watermark (see the doc comment above).
			if err := w.writeBatchedIdempotentConn(workerCtx, conn, table, keyCols, rows, false); err != nil {
				errOnce.Do(func() { firstEr = err })
				cancel()
			}
		}(ch)
	}
	wg.Wait()
	return firstEr
}

// writeBatchedIdempotent is the upsert-form of [writeBatched]. The
// per-batch flush mechanics are identical (flush on whichever of
// row-count cap and byte-size cap fires first; ADR-0028); only the
// SQL changes. It pins one connection and delegates the loop to
// [writeBatchedIdempotentConn].
func (w *RowWriter) writeBatchedIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	// Bug 125: the conflict key the upsert keys on is the table's PK
	// when present, else a deterministic non-null UNIQUE index. MySQL's
	// ON DUPLICATE KEY UPDATE fires on ANY unique key on the target, so
	// keyCols only selects which columns to EXCLUDE from the SET-list;
	// the absorb-on-collision behaviour itself doesn't depend on naming
	// the right key. A truly-keyless table (no PK, no non-null UNIQUE)
	// has nothing to collide on — refuse loudly rather than silently
	// duplicate catchup re-emissions.
	keyCols, ok := effectiveUpsertKeyColumns(table)
	if !ok {
		return errKeylessIdempotent(table)
	}

	// Pin a single connection for the whole write so the per-flush Vector
	// B warning check (reportBulkWriteWarnings) reads SHOW WARNINGS on the
	// SAME session that ran the upsert — session-scoped, like writeBatched.
	// This is the idempotent path the orchestrator takes on resume,
	// parallel chunked copy (>100k threshold), add-table, and the VStream
	// cold-start COPY; without the check a silent clamp under
	// --mysql-sql-mode='' would go unreported on exactly those runs.
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: idempotent insert into %q: pin connection: %w", table.Name, err)
	}
	defer func() { _ = conn.Close() }()
	// reportDurable=true — the serial path is a single FIFO stream, so the
	// mid-COPY durable watermark (ADR-0072 Phase B) is order-equivalent and
	// safe to advance per flush.
	return w.writeBatchedIdempotentConn(ctx, conn, table, keyCols, rows, true)
}

// writeBatchedIdempotentConn is the per-connection idempotent batched
// upsert loop, shared by the serial [writeBatchedIdempotent] (one conn)
// and the parallel [WriteRowsIdempotentParallel] (N conns, one per
// worker). The caller owns pinning + closing conn and resolving keyCols.
//
// reportDurable gates the ADR-0072 Phase B mid-COPY durable watermark
// (w.copyDurableProgress). The SERIAL path passes true (single FIFO
// stream ⟹ flushed-count is order-equivalent to the reader's enqueue-order
// frontier). The FAN-OUT path ([WriteRowsIdempotentParallel]) passes
// false: per-worker flush ordering is NOT order-equivalent to enqueue
// order, so advancing the watermark mid-COPY could checkpoint past an
// un-flushed early row (silent-loss-on-resume). See the
// WriteRowsIdempotentParallel doc comment for the full argument.
//
// Concurrency note (ADR-0097): when N of these run on one RowWriter,
// they share w.warnedClamp (a sync.Map — safe). copyDurableProgress is
// never called on the fan-out path (reportDurable=false), so no worker
// touches the watermark concurrently. Each invocation keeps its own batch
// buffer and flush counter, so the per-call Vector-B sampling schedule is
// per-worker (defensible: a systematic clamp still trips every worker's
// first-N exhaustive flushes and the final flush). No shared mutable
// batch state crosses workers.
func (w *RowWriter) writeBatchedIdempotentConn(ctx context.Context, conn *sql.Conn, table *ir.Table, keyCols []string, rows <-chan ir.Row, reportDurable bool) error {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}
	byteCap := w.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	batch := make([]ir.Row, 0, limit)
	var batchBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchUpsert(table, len(batch), keyCols)
		args := flattenArgs(batch, table)
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("mysql: idempotent insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		// Vector B: strict sql_mode errors at the upsert above; under
		// --mysql-sql-mode='' MySQL silently clamps and only flags the
		// warning list — check it before advancing any watermark so a
		// strict-mode refusal (returned error) doesn't mark a bad batch
		// durable, and a relaxed-mode clamp is WARNed once per table.
		if err := w.reportBulkWriteWarnings(ctx, conn, table.Name); err != nil {
			return err
		}
		// Report the durable-write delta (v0.99.9): this batch is now
		// committed (autocommit Exec), so the snapshot reader's checkpoint
		// may advance to cover these rows. Reported AFTER the Exec +
		// warning check succeed — never before — so the watermark stays
		// at-or-behind the durable frontier. Suppressed entirely on the
		// fan-out path (reportDurable=false): per-worker flush order is not
		// enqueue order, so a mid-COPY breadcrumb could land past an
		// un-flushed early row (silent-loss-on-resume; ADR-0097 §3).
		if reportDurable && w.copyDurableProgress != nil {
			w.copyDurableProgress(int64(len(batch)))
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return flush()
			}
			batch = append(batch, row)
			batchBytes += ir.ApproximateRowBytes(row)
			if len(batch) >= limit || batchBytes >= byteCap {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// buildBatchUpsert returns the upsert-form of the multi-row INSERT.
// keyCols are the conflict-key column names in declaration order —
// the table's PK when present, else a deterministic non-null UNIQUE
// index (see [effectiveUpsertKeyColumns], Bug 125). They are excluded
// from the SET-list (re-assigning a conflict-key column to itself is a
// no-op).
//
// MySQL's ON DUPLICATE KEY UPDATE collides on ANY unique key on the
// target, not only the columns named here — so keyCols only governs
// which columns the SET-list overwrites, not which key triggers the
// upsert. This is why a row arriving with a behind-the-scan or out-of-
// order PK still absorbs cleanly: whatever unique key it collides on,
// the SET-list refreshes the non-key columns.
//
// When keyCols is empty the function falls back to a plain INSERT —
// the idempotent writer refuses keyless tables before reaching here
// (errKeylessIdempotent), so this fallback is defensive for direct
// callers and unit tests.
func buildBatchUpsert(table *ir.Table, rowCount int, keyCols []string) string {
	cols := nonGeneratedColumns(table.Columns)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}

	rowPart := buildRowPlaceholder(len(cols))
	rowParts := make([]string, rowCount)
	for i := range rowParts {
		rowParts[i] = rowPart
	}

	var sb strings.Builder
	fmt.Fprintf(
		&sb, "INSERT INTO %s (%s) VALUES %s",
		quoteIdent(table.Name),
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)

	if len(keyCols) == 0 {
		return sb.String()
	}

	keySet := make(map[string]struct{}, len(keyCols))
	for _, c := range keyCols {
		keySet[c] = struct{}{}
	}
	nonKey := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isKey := keySet[c.Name]; isKey {
			continue
		}
		nonKey = append(nonKey, c.Name)
	}
	sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
	if len(nonKey) == 0 {
		// Every column is a conflict-key column. Re-assign the first
		// key column to itself so the statement parses; the conflict
		// resolves to a no-op UPDATE which is the right semantic.
		sb.WriteString(quoteIdent(keyCols[0]))
		sb.WriteString(" = new.")
		sb.WriteString(quoteIdent(keyCols[0]))
		return sb.String()
	}
	parts := make([]string, len(nonKey))
	for i, c := range nonKey {
		parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
	}
	sb.WriteString(strings.Join(parts, ", "))
	return sb.String()
}

// primaryKeyColumns returns the PK column names in declaration order,
// or an empty slice when the table has no PK. Centralised so the
// idempotent writer and any future callers share the same extraction
// shape.
func primaryKeyColumns(table *ir.Table) []string {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	out := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		out[i] = c.Column
	}
	return out
}

// effectiveUpsertKeyColumns returns the column set the idempotent
// writer keys its ON DUPLICATE KEY UPDATE on, plus an ok flag.
//
// Selection order (Bug 125):
//
//  1. The PRIMARY KEY columns, when the table declares a PK.
//  2. Else a deterministic non-null UNIQUE index, chosen by
//     [pickNonNullUniqueIndex]: every column NOT NULL, then fewest
//     columns, then lexicographically smallest index name. This is the
//     same key [inlineUniqueKeyForCopy] promotes inline during the
//     cold-start COPY so the target actually carries it while rows land.
//
// ok is false ONLY for a truly-keyless table — no PK and no non-null
// UNIQUE index. Such a table has nothing for the upsert to collide on;
// the caller refuses loudly (errKeylessIdempotent) rather than let
// catchup re-emissions create duplicate rows.
//
// A UNIQUE index over a NULLABLE column is intentionally NOT eligible:
// MySQL allows multiple rows with NULL in a UNIQUE column, so such a
// key wouldn't reliably collide on re-emission — the same silent-
// duplicate hazard as no key at all.
func effectiveUpsertKeyColumns(table *ir.Table) ([]string, bool) {
	if table == nil {
		return nil, false
	}
	if pk := primaryKeyColumns(table); len(pk) > 0 {
		return pk, true
	}
	idx := pickNonNullUniqueIndex(table)
	if idx == nil {
		return nil, false
	}
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = c.Column
	}
	return cols, true
}

// pickNonNullUniqueIndex returns the deterministic non-null UNIQUE
// index the cold-start COPY upsert keys on for a PK-less table, or nil
// when none qualifies. Determinism (so the inline-promotion and the
// writer agree on the same key): all columns NOT NULL, then fewest
// columns, then lexicographically smallest name.
//
// Only plain column indexes qualify — an expression/functional index
// (IndexColumn.Expression set) can't be a stable upsert conflict key
// and is skipped.
func pickNonNullUniqueIndex(table *ir.Table) *ir.Index {
	if table == nil {
		return nil
	}
	notNull := make(map[string]bool, len(table.Columns))
	for _, c := range table.Columns {
		if c != nil && !c.Nullable {
			notNull[c.Name] = true
		}
	}
	var best *ir.Index
	for _, idx := range table.Indexes {
		if idx == nil || !idx.Unique || len(idx.Columns) == 0 {
			continue
		}
		allNotNull := true
		for _, c := range idx.Columns {
			if c.Expression != "" || !notNull[c.Column] {
				allNotNull = false
				break
			}
		}
		if !allNotNull {
			continue
		}
		if best == nil ||
			len(idx.Columns) < len(best.Columns) ||
			(len(idx.Columns) == len(best.Columns) && idx.Name < best.Name) {
			best = idx
		}
	}
	return best
}

// errKeylessIdempotent is the loud refusal for a table with no PRIMARY
// KEY and no non-null UNIQUE index reaching the idempotent COPY writer
// (Bug 125). Such a table has no key for ON DUPLICATE KEY UPDATE to
// collide on, so VStream COPY catchup re-emissions would create
// duplicate rows. Per the loud-failure tenet we refuse rather than
// silently duplicate.
func errKeylessIdempotent(table *ir.Table) error {
	name := "<nil>"
	if table != nil {
		name = table.Name
	}
	return fmt.Errorf(
		"mysql: table %q has no PRIMARY KEY and no non-null UNIQUE index; "+
			"the cold-start VStream COPY needs a unique key to absorb Vitess's "+
			"catchup-phase re-emissions idempotently (Bug 125). Add a PRIMARY KEY "+
			"or a NOT NULL UNIQUE index on the source table, or exclude it from the sync",
		name,
	)
}
