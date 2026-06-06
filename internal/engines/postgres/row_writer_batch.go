// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Idempotent INSERT path for the resume-mid-table bulk-copy.
//
// The cold-start bulk-copy uses COPY FROM STDIN (faster, atomic at
// the table level). Per-batch resume can't use COPY because pgx
// CopyFrom streams in a single logical operation that commits all-
// or-nothing — there's no checkpoint boundary to plant a cursor at.
// The resume path therefore drops to batched INSERT statements with
// ON CONFLICT (PK) DO UPDATE on the conflict target.
//
// The shape of the INSERT mirrors [buildBatchInsert] (the
// non-resume batched path) but adds the conflict clause:
//
//	INSERT INTO "schema"."tbl" ("a", "b", "id") VALUES ($1, $2, $3), ...
//	ON CONFLICT ("id") DO UPDATE SET "a" = EXCLUDED."a", "b" = EXCLUDED."b"
//
// Why DO UPDATE rather than DO NOTHING: the bulk-copy replay window
// is between batch commit and checkpoint write. In the rare case
// where a row in the to-be-replayed batch has a different value on
// the source vs. what landed pre-crash (e.g. someone updated it
// between commits), DO UPDATE re-applies the source row. DO NOTHING
// would silently keep the stale value. ADR-0018 covers this.
//
// PG-specific: when every column is a conflict-key column there's
// nothing to SET on conflict, so the statement falls back to DO
// NOTHING — which is the right semantic ("the conflicting row already
// has the same key and there are no non-key columns to overwrite").
//
// Bug 125 cross-engine symmetry: a PK-less table with a NOT-NULL
// UNIQUE key is supported too (no longer refused on PG). The conflict
// key is the PK when present, else a deterministic non-null UNIQUE
// index ([effectiveUpsertKeyColumns]); [emitTableDef] inline-promotes
// that same key so PG's `ON CONFLICT (cols)` has a real unique index
// to infer against while rows land. A truly-keyless table is refused
// loudly ([errKeylessIdempotent]).

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// HandlesNoPKIdempotentCopy implements [ir.IdempotentCopyWriter]: the
// PG idempotent copy path keys the upsert on a non-null UNIQUE index
// when the table has no PRIMARY KEY ([effectiveUpsertKeyColumns]) and
// refuses a truly keyless table loudly ([errKeylessIdempotent]), so it
// never plain-INSERTs a no-PK table — which would duplicate VStream COPY
// catchup re-emissions (Bug 125). [emitTableDef] inline-promotes the
// same chosen unique key as a CONSTRAINT so PG's `ON CONFLICT (cols)`
// has a real matching unique index to infer against while rows land.
// The orchestrator gates the cold-start no-PK path on this capability
// (the same gate MySQL passes), giving the two engines cross-engine
// symmetry on the Bug-125 table class.
func (w *RowWriter) HandlesNoPKIdempotentCopy() bool { return true }

// WriteRowsIdempotent implements [ir.IdempotentRowWriter]. The shape
// mirrors [writeViaBatch] but the SQL goes through
// [buildBatchUpsert] which appends the ON CONFLICT clause.
func (w *RowWriter) WriteRowsIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("postgres: WriteRowsIdempotent: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("postgres: WriteRowsIdempotent: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("postgres: WriteRowsIdempotent: rows channel is nil")
	}
	return w.writeViaBatchIdempotent(ctx, table, rows)
}

// writeViaBatchIdempotent is the upsert-form of [writeViaBatch]. It
// uses the same per-batch flush mechanics — accumulate rows up to
// maxRowsPerBatch (or maxBufferBytes, ADR-0028, whichever fires
// first), then Exec a multi-row INSERT — but the SQL adds
// ON CONFLICT (pk) DO UPDATE.
//
// We deliberately do not pin a connection here: the orchestrator
// commits cursor state via [ir.MigrationStateStore.Write] *between*
// batch flushes (sequential commits, not co-tx — see ADR-0018), so
// each batch can run on whatever pool connection database/sql hands
// us.
func (w *RowWriter) writeViaBatchIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}
	byteCap := w.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	// Bug 125 cross-engine symmetry: the conflict key the upsert keys on
	// is the table's PK when present, else a deterministic non-null UNIQUE
	// index (see [effectiveUpsertKeyColumns]). Unlike MySQL's ON DUPLICATE
	// KEY UPDATE (which fires on ANY unique key regardless of the named
	// columns), PG's `ON CONFLICT (cols)` can only infer against a real
	// unique index matching those exact columns — so [emitTableDef]
	// inline-promotes this same key as a CONSTRAINT at CREATE TABLE time so
	// it physically exists on the target while rows land. A truly-keyless
	// table (no PK, no non-null UNIQUE) has nothing to collide on — refuse
	// loudly rather than silently duplicate catchup re-emissions.
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
		query := buildBatchUpsert(w.schema, table, len(batch), keyCols)
		args, err := flattenArgs(batch, table)
		if err != nil {
			return fmt.Errorf("postgres: prepare args for %q: %w", table.Name, err)
		}
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("postgres: idempotent insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		// Report the durable-write delta (v0.99.9): this batch is now
		// committed, so a resumable source reader's (VStream→PG) checkpoint
		// may advance to cover these rows. Reported AFTER the Exec
		// succeeds so the watermark stays at-or-behind the durable frontier.
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
// keyCols are the conflict-key column names in declaration order — the
// table's PK when present, else a deterministic non-null UNIQUE index
// (see [effectiveUpsertKeyColumns], Bug 125). They become the
// `ON CONFLICT (keyCols)` inference target; PG requires a real unique
// index over exactly these columns, which [emitTableDef] guarantees by
// inline-promoting the same key as a CONSTRAINT for a PK-less table.
//
// When keyCols is empty the function falls back to a plain INSERT —
// the idempotent writer refuses keyless tables before reaching here
// (errKeylessIdempotent), so this fallback is defensive for direct
// callers and unit tests.
func buildBatchUpsert(schema string, table *ir.Table, rowCount int, keyCols []string) string {
	cols := nonGeneratedColumns(table.Columns)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}

	numCols := len(cols)
	rowParts := make([]string, rowCount)
	paramIdx := 0
	for i := range rowParts {
		params := make([]string, numCols)
		for j := range params {
			paramIdx++
			params[j] = fmt.Sprintf("$%d", paramIdx)
		}
		rowParts[i] = "(" + strings.Join(params, ", ") + ")"
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)
	var sb strings.Builder
	fmt.Fprintf(
		&sb, "INSERT INTO %s (%s) VALUES %s",
		tableRef,
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)

	if len(keyCols) == 0 {
		return sb.String()
	}

	// ON CONFLICT (keyCols) DO UPDATE SET non-key = EXCLUDED.non-key
	keySet := make(map[string]struct{}, len(keyCols))
	for _, c := range keyCols {
		keySet[c] = struct{}{}
	}
	conflictTarget := make([]string, len(keyCols))
	for i, c := range keyCols {
		conflictTarget[i] = quoteIdent(c)
	}

	nonKey := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isKey := keySet[c.Name]; isKey {
			continue
		}
		// Generated columns are already excluded by nonGeneratedColumns
		// above; no need to re-check.
		nonKey = append(nonKey, c.Name)
	}

	sb.WriteString(" ON CONFLICT (")
	sb.WriteString(strings.Join(conflictTarget, ", "))
	sb.WriteByte(')')
	if len(nonKey) == 0 {
		// Every column is a conflict-key column — the conflicting row IS
		// the row we wanted to write. DO NOTHING absorbs it silently.
		sb.WriteString(" DO NOTHING")
		return sb.String()
	}
	sb.WriteString(" DO UPDATE SET ")
	parts := make([]string, len(nonKey))
	for i, c := range nonKey {
		parts[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(c), quoteIdent(c))
	}
	sb.WriteString(strings.Join(parts, ", "))
	return sb.String()
}

// primaryKeyColumns returns the PK column names in declaration order,
// or an empty slice when the table has no PK. Centralised so both the
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
// writer keys its ON CONFLICT inference on, plus an ok flag.
//
// Selection order (Bug 125, mirrors the MySQL engine):
//
//  1. The PRIMARY KEY columns, when the table declares a PK.
//  2. Else a deterministic non-null UNIQUE index, chosen by
//     [pickNonNullUniqueIndex]: every column NOT NULL, then fewest
//     columns, then lexicographically smallest index name. This is the
//     same key [inlineUniqueKeyForCopy] promotes inline during the
//     cold-start COPY so the target actually carries the matching
//     unique index PG's ON CONFLICT requires while rows land.
//
// ok is false ONLY for a truly-keyless table — no PK and no non-null
// UNIQUE index. Such a table has nothing for the upsert to collide on;
// the caller refuses loudly (errKeylessIdempotent) rather than let
// catchup re-emissions create duplicate rows.
//
// A UNIQUE index over a NULLABLE column is intentionally NOT eligible:
// PG's default NULLS DISTINCT lets multiple rows carry NULL in a UNIQUE
// column, so such a key wouldn't reliably collide on re-emission — the
// same silent-duplicate hazard as no key at all (identical to the
// MySQL-side reasoning).
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
// and is skipped. Mirrors the MySQL engine's helper of the same name.
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
// (Bug 125). Such a table has no key for ON CONFLICT to infer against,
// so VStream COPY catchup re-emissions would create duplicate rows. Per
// the loud-failure tenet we refuse rather than silently duplicate.
func errKeylessIdempotent(table *ir.Table) error {
	name := "<nil>"
	if table != nil {
		name = table.Name
	}
	return fmt.Errorf(
		"postgres: table %q has no PRIMARY KEY and no non-null UNIQUE index; "+
			"the cold-start VStream COPY needs a unique key to absorb Vitess's "+
			"catchup-phase re-emissions idempotently (Bug 125). Add a PRIMARY KEY "+
			"or a NOT NULL UNIQUE index on the source table, or exclude it from the sync",
		name,
	)
}
