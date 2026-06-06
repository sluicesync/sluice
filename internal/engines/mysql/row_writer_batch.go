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
	"errors"
	"fmt"
	"strings"

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

// writeBatchedIdempotent is the upsert-form of [writeBatched]. The
// per-batch flush mechanics are identical (flush on whichever of
// row-count cap and byte-size cap fires first; ADR-0028); only the
// SQL changes.
func (w *RowWriter) writeBatchedIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}
	byteCap := w.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

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

	batch := make([]ir.Row, 0, limit)
	var batchBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchUpsert(table, len(batch), keyCols)
		args := flattenArgs(batch, table)
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("mysql: idempotent insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		// Report the durable-write delta (v0.99.9): this batch is now
		// committed (autocommit Exec), so the snapshot reader's checkpoint
		// may advance to cover these rows. Reported AFTER the Exec
		// succeeds — never before — so the watermark stays at-or-behind
		// the durable frontier.
		if w.copyDurableProgress != nil {
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
