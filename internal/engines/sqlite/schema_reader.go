// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// SchemaReader reads schema metadata from a SQLite file via sqlite_master
// and the table-introspection PRAGMAs. It implements [ir.SchemaReader].
//
// Scope (prototype):
//   - User tables (sqlite_master type='table', excluding sqlite_* internal
//     tables), their columns, primary key, foreign keys, and plain-column
//     secondary/unique indexes.
//   - Column IR types resolved from declared-type affinity (see types.go).
//
// Out of scope (deferred): CHECK constraints, generated columns,
// expression indexes, partial-index predicates, triggers, views, and any
// date/bool convention policy.
type SchemaReader struct {
	db   *sql.DB
	path string

	// tempPath is the materialized dump DB this reader owns, removed on Close
	// (ADR-0130). Empty when the source was a real binary `.db` — then Close
	// removes nothing.
	tempPath string
}

// Close releases the underlying connection pool and, for a materialized `.sql`
// dump, removes the temp DB after the pool is closed (the file handle must be
// released first, which matters on Windows). A `.db` source removes nothing.
func (r *SchemaReader) Close() error {
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	if r.tempPath != "" {
		// Clear the path first so a repeated Close is a no-op, not a remove of
		// an already-gone file.
		path := r.tempPath
		r.tempPath = ""
		if rmErr := os.Remove(path); rmErr != nil && err == nil {
			err = rmErr
		}
	}
	return err
}

// ReadSchema queries sqlite_master + PRAGMAs and returns a fully
// populated IR Schema for the file the reader is bound to. SQLite has a
// flat namespace (one database file), so every IR Table.Schema is empty.
func (r *SchemaReader) ReadSchema(ctx context.Context) (*ir.Schema, error) {
	names, err := r.tableNames(ctx)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return &ir.Schema{}, nil
	}

	tables := make([]*ir.Table, 0, len(names))
	byName := make(map[string]*ir.Table, len(names))
	for _, name := range names {
		t := &ir.Table{Name: name}
		if err := r.readColumnsAndPK(ctx, t); err != nil {
			return nil, err
		}
		if err := r.readIndexes(ctx, t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
		byName[name] = t
	}

	// Foreign keys are read after every table's PK is known so an FK that
	// omits the parent column (SQLite leaves `to` NULL — it references the
	// parent's primary key) can be resolved against the parent's PK.
	for _, t := range tables {
		if err := r.readForeignKeys(ctx, t, byName); err != nil {
			return nil, err
		}
	}

	return &ir.Schema{Tables: tables}, nil
}

// tableNames lists user tables, excluding SQLite's internal sqlite_* tables
// AND Cloudflare D1's internal `_cf_*` tables (e.g. `_cf_KV`), so a D1 dump
// migrates without the operator needing `--exclude-table _cf_KV` (ADR-0130).
// The `_cf_` underscores are ESCAPED (LIKE's `_` is a single-char wildcard), so
// a legitimate user table whose name merely happens to have "cf" near the front
// — e.g. `xcfg_settings` — is NOT silently dropped from the migration (that
// would be exactly the silent-loss the tenets forbid). `--exclude-table`
// remains available for anything else. Ordered by name for stable diffs/logs.
func (r *SchemaReader) tableNames(ctx context.Context) ([]string, error) {
	const q = `
		SELECT name FROM sqlite_master
		WHERE type = 'table'
		  AND name NOT LIKE 'sqlite_%'
		  AND name NOT LIKE '\_cf\_%' ESCAPE '\'
		ORDER BY name`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list tables in %q: %w", r.path, err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("sqlite: scan table name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate tables: %w", err)
	}
	return names, nil
}

// readColumnsAndPK populates t.Columns (with affinity-resolved IR types,
// nullability, and defaults) and t.PrimaryKey from PRAGMA table_info.
func (r *SchemaReader) readColumnsAndPK(ctx context.Context, t *ir.Table) error {
	rows, err := r.db.QueryContext(ctx, "PRAGMA table_info("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return fmt.Errorf("sqlite: table_info(%q): %w", t.Name, err)
	}
	defer func() { _ = rows.Close() }()

	// pkMember collects (pk-position, column-name) so the PK columns can be
	// emitted in the order SQLite records (table_info.pk is 1-based).
	type pkEntry struct {
		pos  int
		name string
	}
	var pkEntries []pkEntry

	for rows.Next() {
		var (
			cid       int
			name      string
			declType  string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &declType, &notNull, &dfltValue, &pk); err != nil {
			return fmt.Errorf("sqlite: scan table_info(%q): %w", t.Name, err)
		}
		col := &ir.Column{
			Name:     name,
			Type:     resolveColumnType(declType),
			Nullable: notNull == 0,
			Default:  parseDefault(dfltValue),
		}
		if pk > 0 {
			pkEntries = append(pkEntries, pkEntry{pos: pk, name: name})
		}
		t.Columns = append(t.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: iterate table_info(%q): %w", t.Name, err)
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("sqlite: table %q has no columns", t.Name)
	}

	if len(pkEntries) > 0 {
		sort.Slice(pkEntries, func(i, j int) bool { return pkEntries[i].pos < pkEntries[j].pos })
		pkCols := make([]ir.IndexColumn, len(pkEntries))
		for i, e := range pkEntries {
			pkCols[i] = ir.IndexColumn{Column: e.name}
		}
		t.PrimaryKey = &ir.Index{Columns: pkCols, Unique: true}

		// A single-column INTEGER-affinity primary key is SQLite's rowid
		// alias: it auto-assigns on NULL insert (whether or not the
		// AUTOINCREMENT keyword is present). Mark it so the target emits an
		// identity / AUTO_INCREMENT column and the post-copy sequence sync
		// advances past the bulk-copied max. The AUTOINCREMENT keyword
		// itself is not exposed by table_info and is irrelevant to this
		// auto-assigning property, so we don't parse the DDL for it.
		if len(pkEntries) == 1 {
			markRowidAutoIncrement(t, pkEntries[0].name)
		}
	}
	return nil
}

// markRowidAutoIncrement flips Integer.AutoIncrement on the named column
// when it carries INTEGER affinity (the SQLite rowid-alias condition).
func markRowidAutoIncrement(t *ir.Table, colName string) {
	for _, c := range t.Columns {
		if c.Name != colName {
			continue
		}
		if iv, ok := c.Type.(ir.Integer); ok {
			iv.AutoIncrement = true
			c.Type = iv
		}
		return
	}
}

// readIndexes populates t.Indexes from PRAGMA index_list / index_info.
// The primary-key index (origin 'pk') is skipped — the PK is captured
// from table_info. Expression-index entries (a NULL column name in
// index_info) cause the whole index to be skipped in the prototype: it
// is a performance structure, never a data-fidelity concern, and carrying
// a partial column list would misrepresent it.
func (r *SchemaReader) readIndexes(ctx context.Context, t *ir.Table) error {
	metas, err := r.indexListMetas(ctx, t.Name)
	if err != nil {
		return err
	}
	for _, m := range metas {
		if m.origin == "pk" {
			continue // captured from table_info
		}
		cols, ok, err := r.readIndexColumns(ctx, m.name)
		if err != nil {
			return err
		}
		if !ok {
			continue // expression index — skipped in the prototype
		}
		t.Indexes = append(t.Indexes, &ir.Index{
			Name:    m.name,
			Columns: cols,
			Unique:  m.unique,
		})
	}
	return nil
}

// idxMeta is one PRAGMA index_list row sluice cares about.
type idxMeta struct {
	name   string
	unique bool
	origin string // 'c' (CREATE INDEX), 'u' (UNIQUE constraint), 'pk' (PRIMARY KEY)
}

// indexListMetas reads PRAGMA index_list for one table. Split out so the
// list rows close (via defer) before per-index index_info queries open.
func (r *SchemaReader) indexListMetas(ctx context.Context, table string) ([]idxMeta, error) {
	rows, err := r.db.QueryContext(ctx, "PRAGMA index_list("+quotePragmaArg(table)+")")
	if err != nil {
		return nil, fmt.Errorf("sqlite: index_list(%q): %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var metas []idxMeta
	for rows.Next() {
		var (
			seq     int
			name    string
			unique  int
			origin  string
			partial int
		)
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, fmt.Errorf("sqlite: scan index_list(%q): %w", table, err)
		}
		metas = append(metas, idxMeta{name: name, unique: unique == 1, origin: origin})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate index_list(%q): %w", table, err)
	}
	return metas, nil
}

// readIndexColumns returns the plain-column entries of one index, in
// position order. ok is false when the index has any expression entry
// (NULL column name) so the caller can skip it.
func (r *SchemaReader) readIndexColumns(ctx context.Context, indexName string) (cols []ir.IndexColumn, ok bool, err error) {
	rows, err := r.db.QueryContext(ctx, "PRAGMA index_info("+quotePragmaArg(indexName)+")")
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: index_info(%q): %w", indexName, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			seqno int
			cid   int
			name  sql.NullString
		)
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, false, fmt.Errorf("sqlite: scan index_info(%q): %w", indexName, err)
		}
		if !name.Valid {
			return nil, false, nil // expression entry — skip whole index
		}
		cols = append(cols, ir.IndexColumn{Column: name.String})
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("sqlite: iterate index_info(%q): %w", indexName, err)
	}
	return cols, true, nil
}

// readForeignKeys populates t.ForeignKeys from PRAGMA foreign_key_list.
// Rows are grouped by the pragma's `id` column (one id per FK; multi-
// column FKs span several rows, ordered by `seq`). An FK whose parent
// column is NULL (SQLite's "references the parent PK implicitly" form) is
// resolved against the referenced table's primary key.
func (r *SchemaReader) readForeignKeys(ctx context.Context, t *ir.Table, byName map[string]*ir.Table) error {
	rows, err := r.db.QueryContext(ctx, "PRAGMA foreign_key_list("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return fmt.Errorf("sqlite: foreign_key_list(%q): %w", t.Name, err)
	}
	defer func() { _ = rows.Close() }()

	type fkAccum struct {
		refTable string
		cols     []string
		refCols  []string
		hasNull  bool // any parent column was NULL (implicit-PK reference)
		onDelete ir.FKAction
		onUpdate ir.FKAction
	}
	byID := map[int]*fkAccum{}
	var order []int

	for rows.Next() {
		var (
			id, seq         int
			refTable        string
			from            string
			to              sql.NullString
			onUpdate, onDel string
			match           string
		)
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDel, &match); err != nil {
			return fmt.Errorf("sqlite: scan foreign_key_list(%q): %w", t.Name, err)
		}
		fk, ok := byID[id]
		if !ok {
			fk = &fkAccum{
				refTable: refTable,
				onDelete: parseFKAction(onDel),
				onUpdate: parseFKAction(onUpdate),
			}
			byID[id] = fk
			order = append(order, id)
		}
		fk.cols = append(fk.cols, from)
		if to.Valid {
			fk.refCols = append(fk.refCols, to.String)
		} else {
			fk.hasNull = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: iterate foreign_key_list(%q): %w", t.Name, err)
	}

	sort.Ints(order)
	for _, id := range order {
		fk := byID[id]
		refCols := fk.refCols
		if fk.hasNull {
			// Implicit parent-PK reference: resolve against the referenced
			// table's primary key columns.
			parent, ok := byName[fk.refTable]
			if !ok || parent.PrimaryKey == nil {
				return fmt.Errorf(
					"sqlite: table %q foreign key to %q omits the parent column but the parent has no resolvable primary key",
					t.Name, fk.refTable,
				)
			}
			refCols = refCols[:0]
			for _, ic := range parent.PrimaryKey.Columns {
				refCols = append(refCols, ic.Column)
			}
		}
		t.ForeignKeys = append(t.ForeignKeys, &ir.ForeignKey{
			Columns:           fk.cols,
			ReferencedTable:   fk.refTable,
			ReferencedColumns: refCols,
			OnDelete:          fk.onDelete,
			OnUpdate:          fk.onUpdate,
		})
	}
	return nil
}

// parseFKAction maps a SQLite foreign_key_list action string to the IR
// action. SQLite emits the canonical SQL spellings.
func parseFKAction(s string) ir.FKAction {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "CASCADE":
		return ir.FKActionCascade
	case "RESTRICT":
		return ir.FKActionRestrict
	case "SET NULL":
		return ir.FKActionSetNull
	case "SET DEFAULT":
		return ir.FKActionSetDefault
	default: // "NO ACTION" and anything unrecognised
		return ir.FKActionNoAction
	}
}

// parseDefault classifies a PRAGMA table_info.dflt_value into the IR
// DefaultValue sum. SQLite stores the default's raw SQL text:
//
//   - NULL / absent / the NULL keyword → DefaultNone (no meaningful default)
//   - 'text' string literal → DefaultLiteral with quotes stripped/unescaped
//   - numeric literal → DefaultLiteral carrying the digits verbatim
//   - anything else (CURRENT_TIMESTAMP, function calls, (expr), x'..') →
//     DefaultExpression tagged dialect "sqlite"
//
// A DefaultExpression tagged "sqlite" is run through the target writer's
// cross-dialect default translator at CREATE TABLE time: portable spellings
// (CURRENT_TIMESTAMP) emit correctly, while a SQLite-only function
// (julianday(...), etc.) passes through and the target parser rejects it
// LOUDLY at CREATE TABLE — never a silently-dropped default. (A SQLite
// expression the translator mis-rewrites into valid-but-different target SQL
// is a residual schema-fidelity edge, NOT data loss: DEFAULTs affect only
// post-migration inserts, never the migrated rows, which are inserted with
// explicit values.) A full SQLite→target default translator is deferred.
func parseDefault(dflt sql.NullString) ir.DefaultValue {
	if !dflt.Valid {
		return ir.DefaultNone{}
	}
	s := strings.TrimSpace(dflt.String)
	if s == "" || strings.EqualFold(s, "NULL") {
		return ir.DefaultNone{}
	}
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		inner := strings.ReplaceAll(s[1:len(s)-1], "''", "'")
		return ir.DefaultLiteral{Value: inner}
	}
	if isNumericLiteral(s) {
		return ir.DefaultLiteral{Value: s}
	}
	return ir.DefaultExpression{Expr: s, Dialect: "sqlite"}
}

// isNumericLiteral reports whether s is a plain integer or float literal
// (optionally signed). Used to keep numeric defaults as literals rather
// than routing them through the expression path.
func isNumericLiteral(s string) bool {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

// quotePragmaArg renders a table/index name as a single-quoted SQL string
// literal for use as a PRAGMA argument, escaping embedded single quotes by
// doubling. PRAGMA does not accept bound parameters for its argument, so
// the name is inlined; names come from sqlite_master (trusted catalog),
// and the quoting still defends against an embedded-quote identifier.
func quotePragmaArg(name string) string {
	return "'" + strings.ReplaceAll(name, "'", "''") + "'"
}
