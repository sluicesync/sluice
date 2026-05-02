package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

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
	db     *sql.DB
	schema string

	// pkCache maps "schema.table" → ordered list of PK column names.
	// Populated lazily via a single information_schema query the
	// first time a change for the table arrives. An empty slice
	// (length 0) means "table exists but has no PK" — in that case
	// Insert falls back to plain INSERT (see the package comment).
	pkCache map[string][]string
}

// Close releases the underlying connection pool.
func (a *ChangeApplier) Close() error {
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}

// Apply consumes changes from the channel and applies each to the
// target in its own transaction. Returns when the channel closes
// (clean shutdown), when ctx is cancelled, or when a target write
// fails (in which case the error is wrapped with the change kind
// and table name).
func (a *ChangeApplier) Apply(ctx context.Context, changes <-chan ir.Change) error {
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return nil
			}
			if err := a.applyOne(ctx, c); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyOne dispatches a single change to its SQL form and runs it
// in a target transaction. The transaction shape is one statement
// per change; this future-proofs the §5 control-table integration,
// which adds a position UPDATE inside the same BEGIN/COMMIT.
func (a *ChangeApplier) applyOne(ctx context.Context, c ir.Change) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: applier: begin tx: %w", err)
	}
	if err := a.dispatch(ctx, tx, c); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: applier: commit: %w", err)
	}
	return nil
}

// dispatch routes a single change to its SQL form on the open tx.
func (a *ChangeApplier) dispatch(ctx context.Context, tx *sql.Tx, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		schema := applierSchema(a.schema, v.Schema)
		pk, err := a.pkFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: pk lookup for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args := buildInsertSQL(schema, v.Table, v.Row, pk)
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("postgres: applier: insert into %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.Update:
		schema := applierSchema(a.schema, v.Schema)
		stmt, args := buildUpdateSQL(schema, v.Table, v.Before, v.After)
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("postgres: applier: update %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.Delete:
		schema := applierSchema(a.schema, v.Schema)
		stmt, args := buildDeleteSQL(schema, v.Table, v.Before)
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("postgres: applier: delete from %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.Truncate:
		schema := applierSchema(a.schema, v.Schema)
		stmt := buildTruncateSQL(schema, v.Table)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: applier: truncate %s.%s: %w", schema, v.Table, err)
		}
		return nil
	}
	return fmt.Errorf("postgres: applier: unknown change type %T", c)
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

// applierSchema picks the schema name to use in SQL: the change's
// schema if present, otherwise the applier's default. CDC events
// from Postgres always carry the schema name; the default is only
// used as a safety net.
func applierSchema(defaultSchema, changeSchema string) string {
	if changeSchema != "" {
		return changeSchema
	}
	return defaultSchema
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
func buildInsertSQL(schema, table string, row ir.Row, pk []string) (sqlStmt string, args []any) {
	cols := sortedKeys(row)
	args = make([]any, 0, len(cols))
	colSQL := make([]string, len(cols))
	for i, c := range cols {
		colSQL[i] = quoteIdent(c)
		args = append(args, row[c])
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
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
	return sb.String(), args
}

// buildUpdateSQL builds an UPDATE statement. SET uses every column
// in After (unchanged-column detection is a v1.5 optimization).
// WHERE uses every column in Before with NULL-aware predicate
// building.
func buildUpdateSQL(schema, table string, before, after ir.Row) (sqlStmt string, args []any) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	setSQL, setArgs := buildSetClause(after, 1)
	whereSQL, whereArgs := buildWhereClause(before, len(setArgs)+1)

	args = make([]any, 0, len(setArgs)+len(whereArgs))
	args = append(args, setArgs...)
	args = append(args, whereArgs...)
	return "UPDATE " + tableRef + " SET " + setSQL + " WHERE " + whereSQL, args
}

// buildDeleteSQL builds a DELETE statement using the Before image
// as the WHERE predicate.
func buildDeleteSQL(schema, table string, before ir.Row) (sqlStmt string, args []any) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	whereSQL, whereArgs := buildWhereClause(before, 1)
	return "DELETE FROM " + tableRef + " WHERE " + whereSQL, whereArgs
}

// buildTruncateSQL builds a TRUNCATE TABLE statement.
func buildTruncateSQL(schema, table string) string {
	return "TRUNCATE TABLE " + quoteIdent(schema) + "." + quoteIdent(table)
}

// buildSetClause renders "col1 = $N, col2 = $N+1" for an UPDATE SET.
// startIdx is the next available placeholder number — Postgres uses
// numbered placeholders (unlike MySQL's `?`), so a SET + WHERE
// combination needs to share a sequence.
func buildSetClause(row ir.Row, startIdx int) (clause string, args []any) {
	cols := sortedKeys(row)
	parts := make([]string, len(cols))
	args = make([]any, 0, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = $%d", quoteIdent(c), startIdx+i)
		args = append(args, row[c])
	}
	return strings.Join(parts, ", "), args
}

// buildWhereClause renders an AND-joined predicate with NULL-aware
// handling: nil row values produce "col IS NULL" (no parameter) so
// SQL's NULL semantics don't make the predicate unsatisfiable.
// startIdx is the next available placeholder number.
func buildWhereClause(row ir.Row, startIdx int) (clause string, args []any) {
	cols := sortedKeys(row)
	parts := make([]string, 0, len(cols))
	args = make([]any, 0, len(cols))
	idx := startIdx
	for _, c := range cols {
		v := row[c]
		if v == nil {
			parts = append(parts, quoteIdent(c)+" IS NULL")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(c), idx))
		args = append(args, v)
		idx++
	}
	return strings.Join(parts, " AND "), args
}

// (sortedKeys is shared with the schema reader — see schema_reader.go
// for the implementation. The applier uses it to render generated SQL
// in a deterministic column order.)
