package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ChangeApplier applies [ir.Change] events to a MySQL target, one
// source change per target transaction. It implements
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
//   - **Tables without any PK fall back to plain INSERT.** Both PG's
//     ON CONFLICT and MySQL's ON DUPLICATE KEY UPDATE require a key
//     to collide with; without one, the syntax is unusable. Plain
//     INSERT means a re-applied Insert produces a duplicate row.
//     Resume idempotency on no-PK tables is therefore best-effort,
//     and continuous-sync on such tables is not recommended. Add a
//     PRIMARY KEY to the source table before running sluice in
//     continuous-sync mode.
//
//   - **Tables with a UNIQUE KEY but no PRIMARY KEY** are a known
//     trouble spot in MySQL replication generally — sluice doesn't
//     special-case the unique-key as a conflict target either. The
//     applier behaves as if there's no PK (plain INSERT path). If
//     you need upsert semantics here, declare the unique column as
//     the PRIMARY KEY on the source table.
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
		return fmt.Errorf("mysql: applier: begin tx: %w", err)
	}
	if err := a.dispatch(ctx, tx, c); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: applier: commit: %w", err)
	}
	return nil
}

// dispatch routes a single change to its SQL form on the open tx.
func (a *ChangeApplier) dispatch(ctx context.Context, tx *sql.Tx, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		pk, err := a.pkFor(ctx, tx, v.Schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", v.Schema, v.Table, err)
		}
		stmt, args := buildInsertSQL(applierSchema(a.schema, v.Schema), v.Table, v.Row, pk)
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("mysql: applier: insert into %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil

	case ir.Update:
		stmt, args := buildUpdateSQL(applierSchema(a.schema, v.Schema), v.Table, v.Before, v.After)
		// Update misses are tolerated (zero rows affected). On resume
		// we may replay an Update whose target row was already
		// updated — that's expected, not an error.
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("mysql: applier: update %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil

	case ir.Delete:
		stmt, args := buildDeleteSQL(applierSchema(a.schema, v.Schema), v.Table, v.Before)
		// Delete misses are tolerated for the same reason as Update.
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("mysql: applier: delete from %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil

	case ir.Truncate:
		stmt := buildTruncateSQL(applierSchema(a.schema, v.Schema), v.Table)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: applier: truncate %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil
	}
	return fmt.Errorf("mysql: applier: unknown change type %T", c)
}

// pkFor returns the cached PK column list for the named table,
// loading it on the first sight of the table. An empty slice means
// "no PK" — Insert falls back to plain INSERT in that case.
func (a *ChangeApplier) pkFor(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	qn := qualifiedName(schema, table)
	if cached, ok := a.pkCache[qn]; ok {
		return cached, nil
	}
	pk, err := loadPrimaryKey(ctx, tx, applierSchema(a.schema, schema), table)
	if err != nil {
		return nil, err
	}
	a.pkCache[qn] = pk
	return pk, nil
}

// applierSchema picks the schema name to use in SQL: the change's
// schema if present, otherwise the applier's default. CDC events
// from MySQL carry the schema name in the ir.Change; the default is
// only used as a safety net.
func applierSchema(defaultSchema, changeSchema string) string {
	if changeSchema != "" {
		return changeSchema
	}
	return defaultSchema
}

// loadPrimaryKey reads the PK columns for the named table from
// information_schema. Returns an empty slice (not nil) for tables
// with no PK; nil indicates a query error.
func loadPrimaryKey(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	const q = `
		SELECT column_name
		FROM   information_schema.statistics
		WHERE  table_schema = ?
		  AND  table_name   = ?
		  AND  index_name   = 'PRIMARY'
		ORDER  BY seq_in_index`

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
// uses the row-alias UPSERT form (8.0.20+):
//
//	INSERT INTO `s`.`t` (`a`, `b`) VALUES (?, ?) AS new
//	ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
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
	placeholders := strings.Repeat("?, ", len(cols))
	placeholders = strings.TrimSuffix(placeholders, ", ")

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableRef)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(colSQL, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(placeholders)
	sb.WriteByte(')')

	if len(pk) > 0 {
		// Row-alias UPSERT: every non-PK column gets reassigned to
		// the new row's value. PK columns are excluded from the
		// SET list because updating them on conflict would be a
		// no-op at best (PK columns equal by definition during the
		// conflict) and silently incorrect if the new and existing
		// rows have differing PK shapes.
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
		} else {
			// Every column is a PK column — the row IS its own key.
			// On conflict there's nothing to update; emit
			// ON DUPLICATE KEY UPDATE with a no-op assignment so
			// the conflict is absorbed silently.
			sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
			sb.WriteString(quoteIdent(pk[0]))
			sb.WriteString(" = new.")
			sb.WriteString(quoteIdent(pk[0]))
		}
	}
	return sb.String(), args
}

// buildUpdateSQL builds an UPDATE statement. SET uses every column
// in After (including ones whose value didn't change — unchanged-
// column detection is a v1.5 optimization). WHERE uses every column
// in Before with NULL-aware predicate building.
func buildUpdateSQL(schema, table string, before, after ir.Row) (sqlStmt string, args []any) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	setSQL, setArgs := buildSetClause(after)
	whereSQL, whereArgs := buildWhereClause(before)

	args = make([]any, 0, len(setArgs)+len(whereArgs))
	args = append(args, setArgs...)
	args = append(args, whereArgs...)
	return "UPDATE " + tableRef + " SET " + setSQL + " WHERE " + whereSQL, args
}

// buildDeleteSQL builds a DELETE statement using the Before image
// as the WHERE predicate.
func buildDeleteSQL(schema, table string, before ir.Row) (sqlStmt string, args []any) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	whereSQL, whereArgs := buildWhereClause(before)
	return "DELETE FROM " + tableRef + " WHERE " + whereSQL, whereArgs
}

// buildTruncateSQL builds a TRUNCATE TABLE statement.
func buildTruncateSQL(schema, table string) string {
	return "TRUNCATE TABLE " + quoteIdent(schema) + "." + quoteIdent(table)
}

// buildSetClause renders "col1 = ?, col2 = ?" for an UPDATE SET.
// NULL values bind through database/sql normally; no special form
// is needed in SET (unlike WHERE).
func buildSetClause(row ir.Row) (clause string, args []any) {
	cols := sortedKeys(row)
	parts := make([]string, len(cols))
	args = make([]any, 0, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c) + " = ?"
		args = append(args, row[c])
	}
	return strings.Join(parts, ", "), args
}

// buildWhereClause renders an AND-joined predicate with NULL-aware
// handling: nil row values produce "col IS NULL" (no parameter) so
// SQL's NULL semantics don't make the predicate unsatisfiable.
func buildWhereClause(row ir.Row) (clause string, args []any) {
	cols := sortedKeys(row)
	parts := make([]string, 0, len(cols))
	args = make([]any, 0, len(cols))
	for _, c := range cols {
		v := row[c]
		if v == nil {
			parts = append(parts, quoteIdent(c)+" IS NULL")
			continue
		}
		parts = append(parts, quoteIdent(c)+" = ?")
		args = append(args, v)
	}
	return strings.Join(parts, " AND "), args
}

// (sortedKeys is shared with the schema reader — see schema_reader.go
// for the implementation. The applier uses it to render generated SQL
// in a deterministic column order.)
