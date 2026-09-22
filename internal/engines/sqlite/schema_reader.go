// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// SchemaReader reads schema metadata from a SQLite file via sqlite_master
// and the table-introspection PRAGMAs. It implements [ir.SchemaReader].
//
// Scope:
//   - User tables (sqlite_master type='table', excluding sqlite_* internal
//     tables), their columns, primary key, foreign keys, and secondary/unique
//     indexes (plain-column AND expression/partial — ADR-0133).
//   - Column IR types resolved from declared-type affinity (see types.go).
//   - Generated columns, CHECK constraints, and partial/expression indexes are
//     carried into the IR's existing fields, tagged dialect "sqlite". Their
//     expression bodies are carried VERBATIM (parsed from the CREATE TABLE /
//     CREATE INDEX SQL) and emit verbatim on the target; non-portable
//     constructs are rejected loudly at target DDL time. Cross-dialect
//     TRANSLATION of the verbatim tail is the deferred next increment.
//
// Out of scope (deferred): triggers, views, and a SQLite→canonical expression
// translator.
type SchemaReader struct {
	db   *sql.DB
	path string

	// tempPath is the materialized dump DB this reader owns, removed on Close
	// (ADR-0130). Empty when the source was a real binary `.db` — then Close
	// removes nothing.
	tempPath string

	// tempRelease, when set, is called on Close INSTEAD of removing
	// tempPath: the staged file is shared and refcounted by its owner
	// (the flatfile stage-once handle — audit 2026-07-15 MED-P2), and
	// the owner's LAST release removes it.
	tempRelease func() error
}

// Close releases the underlying connection pool and, for a materialized `.sql`
// dump, removes (or, when shared, releases) the temp DB after the pool is
// closed (the file handle must be released first, which matters on Windows).
// A `.db` source removes nothing.
func (r *SchemaReader) Close() error {
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	switch {
	case r.tempRelease != nil:
		// Clear the hook first so a repeated Close is a no-op, not a
		// double release of the shared refcount.
		release := r.tempRelease
		r.tempRelease = nil
		r.tempPath = ""
		if rerr := release(); rerr != nil && err == nil {
			err = rerr
		}
	case r.tempPath != "":
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
		genStored, err := r.readColumnsAndPK(ctx, t)
		if err != nil {
			return nil, err
		}
		// Generated-column bodies and CHECK constraints live only in the
		// CREATE TABLE SQL (no PRAGMA exposes them); parse it once per table
		// to populate the IR's existing fields (ADR-0133).
		createSQL, err := r.objectSQL(ctx, "table", name)
		if err != nil {
			return nil, err
		}
		applyGeneratedAndChecks(ctx, t, createSQL, genStored)
		pkIndexPresent, err := r.readIndexes(ctx, t)
		if err != nil {
			return nil, err
		}
		markRowidAutoIncrement(t, pkIndexPresent)
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
//
// sluice's own control tables ([appliershared.ControlTableNames] — the
// trigger-CDC trio, CDC positions, migrate-state, …) are also excluded by
// EXACT name (not a LIKE wildcard, so a legitimate user table merely prefixed
// `sluice_` is untouched): they are sluice-managed bookkeeping, never user
// data, so a cold-start (or a plain `sluice migrate` against a promoted
// ex-target or trigger-instrumented file) must never copy them, and the
// trigger installer must never see them as replication candidates. This
// reader previously excluded only the trigger trio; the full-roster filter
// runs Go-side and logs any exclusion that actually bites, matching the
// other engine doors (roadmap item 65b, audit-2026-07-15 MED-D0-6).
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

	var names, excluded []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("sqlite: scan table name: %w", err)
		}
		if appliershared.IsControlTable(name) {
			excluded = append(excluded, name)
			continue
		}
		names = append(names, name)
	}
	appliershared.LogExcludedControlTables("sqlite", excluded)
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate tables: %w", err)
	}
	return names, nil
}

// readColumnsAndPK populates t.Columns (with affinity-resolved IR types,
// nullability, and defaults) and t.PrimaryKey from PRAGMA table_xinfo.
//
// table_xinfo is table_info plus a trailing `hidden` column: 0 = ordinary,
// 1 = hidden (virtual-table columns table_info omits), 2 = VIRTUAL generated,
// 3 = STORED generated. We skip hidden==1 so the visible-column set stays
// byte-identical to the pre-ADR-0133 table_info read, and return a
// name→stored map for the generated columns (hidden 2/3) so ReadSchema can fill
// their generation expressions from the CREATE TABLE SQL.
func (r *SchemaReader) readColumnsAndPK(ctx context.Context, t *ir.Table) (map[string]bool, error) {
	rows, err := r.db.QueryContext(ctx, "PRAGMA table_xinfo("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return nil, fmt.Errorf("sqlite: table_xinfo(%q): %w", t.Name, err)
	}
	defer func() { _ = rows.Close() }()

	// pkMember collects (pk-position, column-name) so the PK columns can be
	// emitted in the order SQLite records (table_xinfo.pk is 1-based).
	type pkEntry struct {
		pos  int
		name string
	}
	var pkEntries []pkEntry
	genStored := map[string]bool{}

	for rows.Next() {
		var (
			cid       int
			name      string
			declType  string
			notNull   int
			dfltValue sql.NullString
			pk        int
			hidden    int
		)
		if err := rows.Scan(&cid, &name, &declType, &notNull, &dfltValue, &pk, &hidden); err != nil {
			return nil, fmt.Errorf("sqlite: scan table_xinfo(%q): %w", t.Name, err)
		}
		if hidden == 1 {
			continue // virtual-table hidden column; not part of the visible set
		}
		col := &ir.Column{
			Name:     name,
			Type:     resolveColumnType(declType),
			Nullable: notNull == 0,
			Default:  parseDefault(dfltValue),
		}
		if hidden == 2 || hidden == 3 {
			genStored[name] = hidden == 3 // 3 = STORED, 2 = VIRTUAL
		}
		if pk > 0 {
			pkEntries = append(pkEntries, pkEntry{pos: pk, name: name})
		}
		t.Columns = append(t.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate table_xinfo(%q): %w", t.Name, err)
	}
	if len(t.Columns) == 0 {
		return nil, fmt.Errorf("sqlite: table %q has no columns", t.Name)
	}

	if len(pkEntries) > 0 {
		sort.Slice(pkEntries, func(i, j int) bool { return pkEntries[i].pos < pkEntries[j].pos })
		pkCols := make([]ir.IndexColumn, len(pkEntries))
		for i, e := range pkEntries {
			pkCols[i] = ir.IndexColumn{Column: e.name}
		}
		t.PrimaryKey = &ir.Index{Columns: pkCols, Unique: true}
		// Whether that key is the auto-assigning rowid alias is decided by
		// [markRowidAutoIncrement] once the index catalog has been read —
		// the column list alone cannot tell (GC-4).
	}
	return genStored, nil
}

// objectSQL returns the verbatim `sql` text sqlite_master records for the named
// object (objType is "table" or "index"). The CREATE TABLE / CREATE INDEX text
// is the only place generated-column, CHECK, partial-predicate, and
// expression-index bodies live (no PRAGMA exposes them). A missing or NULL `sql`
// (e.g. an auto-index from a UNIQUE constraint) returns "" with no error — such
// objects carry none of these features.
func (r *SchemaReader) objectSQL(ctx context.Context, objType, name string) (string, error) {
	var s sql.NullString
	err := r.db.QueryRowContext(ctx,
		"SELECT sql FROM sqlite_master WHERE type = ? AND name = ?", objType, name).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("sqlite: read %s SQL for %q: %w", objType, name, err)
	}
	return s.String, nil
}

// markRowidAutoIncrement flips Integer.AutoIncrement on the primary-key
// column when that key is SQLite's ROWID ALIAS — the one shape SQLite
// auto-assigns on a NULL insert (with or without the AUTOINCREMENT keyword).
// The flag is what makes a Postgres target emit `GENERATED BY DEFAULT AS
// IDENTITY` and a MySQL target `AUTO_INCREMENT`, so over-marking hands the
// target an auto-assigning key the source never had (GC-4: the previous
// predicate was "any single-column INTEGER-affinity PK", which marked
// `BIGINT PRIMARY KEY` — a column SQLite stores NULL into — and every
// `WITHOUT ROWID` table's PK).
//
// SQLite's documented rule (lang_createtable.html#rowid): the alias exists
// only when the declared type is EXACTLY `INTEGER` (case-insensitive; `INT`,
// `BIGINT`, `INTEGER(10)` do not alias), on a single-column PRIMARY KEY, in
// a rowid table (not `WITHOUT ROWID`), and not spelled `PRIMARY KEY DESC`
// as a column constraint (a documented quirk; the table-constraint
// `PRIMARY KEY(x DESC)` still aliases). Rather than re-derive those four
// sub-rules from the CREATE TABLE text, the reader asks the catalog: SQLite
// materialises a separate PK index (`PRAGMA index_list` origin 'pk') EXACTLY
// when the key is NOT the rowid alias — a rowid alias is the table's own
// b-tree and needs none. pkIndexPresent is that fact. Ground-truthed against
// the driver across every spelling × constraint form × rowid/WITHOUT ROWID
// cell, plus the AUTOINCREMENT keyword, by TestRowidAlias_ReaderAndWriterMatchTheDriver,
// which measures each cell by inserting NULL rather than reasoning about it.
func markRowidAutoIncrement(t *ir.Table, pkIndexPresent bool) {
	if pkIndexPresent || t.PrimaryKey == nil || len(t.PrimaryKey.Columns) != 1 {
		return
	}
	colName := t.PrimaryKey.Columns[0].Column
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

// readIndexes populates t.Indexes from PRAGMA index_list / index_info and
// reports whether the catalog holds a primary-key index (origin 'pk') — the
// fact [markRowidAutoIncrement] needs. The PK index itself is skipped (the PK
// is captured from table_xinfo). A UNIQUE-constraint auto-index (origin 'u')
// is carried as a constraint-backed unique index under a sluice-generated
// name — see [uniqueConstraintIndex]. Expression-index entries (a NULL column
// name in index_info) carry their indexed expression (parsed from the CREATE
// INDEX SQL, tagged "sqlite") rather than being dropped; a partial index
// carries its WHERE predicate too (ADR-0133). An expression index whose
// column list can't be cleanly parsed is WARN-skipped rather than carrying a
// guessed column set.
func (r *SchemaReader) readIndexes(ctx context.Context, t *ir.Table) (pkIndexPresent bool, err error) {
	metas, err := r.indexListMetas(ctx, t.Name)
	if err != nil {
		return false, err
	}
	for _, m := range metas {
		entries, err := r.readIndexColumns(ctx, m.name)
		if err != nil {
			return false, err
		}
		if m.origin == "pk" {
			// The key itself is captured from table_xinfo; its index only
			// contributes per-column DESC / collation (GC-5).
			pkIndexPresent = true
			applyPrimaryKeyIndexAttributes(t, entries)
			continue
		}
		// The CREATE INDEX SQL is needed only for an expression index or a
		// partial index; an ordinary plain-column index reads from index_info
		// alone (byte-identical to the pre-ADR-0133 reader).
		var createSQL string
		if m.partial || hasExprEntry(entries) {
			if createSQL, err = r.objectSQL(ctx, "index", m.name); err != nil {
				return false, err
			}
		}
		cols, exprCount, ok := buildIndexColumns(ctx, t.Name, m.name, entries, createSQL)
		if !ok {
			continue // expression index that couldn't be parsed — WARN-skipped
		}
		if m.origin == "u" {
			t.Indexes = append(t.Indexes, uniqueConstraintIndex(t.Name, cols))
			continue
		}
		idx := &ir.Index{
			Name:    m.name,
			Columns: cols,
			Unique:  m.unique,
		}
		hasPred := false
		if m.partial {
			if pred, pok := extractIndexPredicate(createSQL); pok {
				idx.Predicate = pred
				idx.PredicateDialect = sqliteDialect
				hasPred = true
			}
		}
		warnIndexVerbatim(ctx, t.Name, m.name, hasPred, exprCount)
		t.Indexes = append(t.Indexes, idx)
	}
	return pkIndexPresent, nil
}

// uniqueConstraintIndex is the IR carry of a SQLite UNIQUE constraint's
// auto-index (PRAGMA index_list origin 'u' — an inline `email TEXT UNIQUE` or
// a table-level `UNIQUE (a, b)`). Shared by the file and d1 readers.
//
// SQLite names that index `sqlite_autoindex_<table>_<N>`, and every object
// name beginning `sqlite_` is reserved: a SQLite target refuses
// `CREATE UNIQUE INDEX IF NOT EXISTS "sqlite_autoindex_t_1"` with "object name
// reserved for internal use" (IF NOT EXISTS does not suppress it), and it did
// so in the index phase — AFTER the whole copy (GC-22). So the auto-index is
// carried the way the source declared it, as a CONSTRAINT: ConstraintBacked,
// so a Postgres target re-emits `ADD CONSTRAINT … UNIQUE` (the shape it would
// have given the same inline UNIQUE) while MySQL and SQLite emit a unique
// index, the only shape they have — and under the name Postgres itself
// generates for an inline UNIQUE, `<table>_<col>[_<col>…]_key`, which is
// legal on every target, embeds the table so it is schema-unique by
// construction, and lets schema diff/drift match it by name (an unnamed index
// is invisible there). A user index that happens to carry the same name is
// refused by the target namespace gates, loudly, rather than silently
// no-opped. Uniqueness is never relaxed: the index is Unique on every target.
//
// ConstraintNamed stays false: the name is sluice's, not one the operator
// wrote, so a Postgres target must not treat it as an identity to preserve.
func uniqueConstraintIndex(table string, cols []ir.IndexColumn) *ir.Index {
	parts := make([]string, 0, len(cols)+2)
	parts = append(parts, table)
	for _, c := range cols {
		parts = append(parts, c.Column)
	}
	parts = append(parts, "key")
	return &ir.Index{
		Name:             strings.Join(parts, "_"),
		Columns:          cols,
		Unique:           true,
		ConstraintBacked: true,
	}
}

// hasExprEntry reports whether any index_info entry is an expression (NULL
// column name), so the caller knows whether the CREATE INDEX SQL is needed.
func hasExprEntry(entries []indexInfoEntry) bool {
	for _, e := range entries {
		if e.isExpr {
			return true
		}
	}
	return false
}

// idxMeta is one PRAGMA index_list row sluice cares about.
type idxMeta struct {
	name    string
	unique  bool
	origin  string // 'c' (CREATE INDEX), 'u' (UNIQUE constraint), 'pk' (PRIMARY KEY)
	partial bool   // index_list.partial = 1 → has a WHERE predicate
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
		metas = append(metas, idxMeta{name: name, unique: unique == 1, origin: origin, partial: partial == 1})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate index_list(%q): %w", table, err)
	}
	return metas, nil
}

// readIndexColumns returns the KEY entries of one index in position order,
// from PRAGMA index_xinfo — index_info plus each entry's `desc`, `coll` and
// `key` (GC-5). Auxiliary entries (key = 0: the rowid, or a WITHOUT ROWID
// table's remaining PK columns, which SQLite appends to every index) are
// skipped, so the column set is exactly what index_info returned. An entry
// whose column name is NULL is an expression entry (isExpr true); its
// expression text lives in the CREATE INDEX SQL.
func (r *SchemaReader) readIndexColumns(ctx context.Context, indexName string) ([]indexInfoEntry, error) {
	rows, err := r.db.QueryContext(ctx, "PRAGMA index_xinfo("+quotePragmaArg(indexName)+")")
	if err != nil {
		return nil, fmt.Errorf("sqlite: index_xinfo(%q): %w", indexName, err)
	}
	defer func() { _ = rows.Close() }()

	var entries []indexInfoEntry
	for rows.Next() {
		var (
			seqno int
			cid   int
			name  sql.NullString
			desc  int
			coll  string
			key   int
		)
		if err := rows.Scan(&seqno, &cid, &name, &desc, &coll, &key); err != nil {
			return nil, fmt.Errorf("sqlite: scan index_xinfo(%q): %w", indexName, err)
		}
		if key == 0 {
			continue
		}
		entries = append(entries, indexInfoEntry{name: name.String, isExpr: !name.Valid, desc: desc == 1, coll: coll})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate index_xinfo(%q): %w", indexName, err)
	}
	return entries, nil
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
// DefaultValue sum. SQLite stores the default's raw SQL text, with one
// surface trap (probed on modernc): PRAGMA reports a parenthesised
// expression default with the OUTER parens stripped, so `DEFAULT
// ('a' || 'b')` arrives as `'a' || 'b'` — a string that merely starts
// and ends with a quote. String-literal classification therefore lexes
// the whole text for well-formedness ([singleQuotedLiteral]) instead of
// endpoint-checking it; the pre-fix endpoint check swallowed that
// expression as the silently WRONG literal `a' || 'b`.
//
//   - NULL / absent / the NULL keyword → DefaultNone (no meaningful default)
//   - well-formed 'text' string literal → DefaultLiteral with quotes
//     stripped and the doubled-quote escape unescaped
//   - numeric literal → DefaultLiteral carrying the digits verbatim
//   - anything else (CURRENT_TIMESTAMP, function calls, residual (expr)
//     nesting, x'..' blobs, TRUE/FALSE, 0x hex, the double-quoted-string
//     misfeature) → DefaultExpression tagged dialect "sqlite"
//
// A DefaultExpression tagged "sqlite" is handled by the target writer at
// CREATE TABLE time. The Postgres writer (D1/SQLite robustness Chunk A +
// the ADR-0133 translator) translates the portable subset — the "current
// instant" spellings (datetime('now')/CURRENT_TIMESTAMP → CURRENT_TIMESTAMP,
// date('now')/CURRENT_DATE, time('now')/CURRENT_TIME) plus the shared
// SQLite→PG expression allowlist ('a' || 'b', arithmetic, coalesce, …) —
// and DROPS any other SQLite-only expression (julianday(...),
// strftime(...), the double-quoted-string misfeature, …) with a LOUD
// per-column warn naming the table+column — never emitted verbatim (which
// aborted the whole migration at CREATE TABLE) and never silently dropped.
// The MySQL target routes the portable subset through the SQLite→MySQL
// translator (`||` means concat on SQLite but LOGICAL OR under MySQL's
// default sql_mode, so verbatim emission would compute a silently wrong
// value), carries only the proven-faithful residues verbatim (a bare
// double-quoted misfeature token, an x'..' blob literal, a lone keyword),
// and DROPS everything else with a loud table+column-named warn — MySQL
// PARSES many non-portable spellings with different semantics, so relying
// on its parser to reject them is unsafe. A body whose string literal
// contains a backslash is REFUSED outright rather than dropped: MySQL's
// parser would ACCEPT and silently reinterpret it (default sql_mode
// treats \ as an escape introducer) — SEC-1,
// refuseBackslashSQLiteDefaultMySQL. (A dropped DEFAULT is schema
// metadata, NOT data loss: DEFAULTs affect only post-migration inserts,
// never the migrated rows, which are inserted with explicit values.)
func parseDefault(dflt sql.NullString) ir.DefaultValue {
	if !dflt.Valid {
		return ir.DefaultNone{}
	}
	s := strings.TrimSpace(dflt.String)
	if s == "" || strings.EqualFold(s, "NULL") {
		return ir.DefaultNone{}
	}
	if inner, ok := singleQuotedLiteral(s); ok {
		return ir.DefaultLiteral{Value: inner}
	}
	if isNumericLiteral(s) {
		return ir.DefaultLiteral{Value: s}
	}
	return ir.DefaultExpression{Expr: s, Dialect: "sqlite"}
}

// singleQuotedLiteral reports whether s is EXACTLY one well-formed
// single-quoted SQL string literal — an opening ', a body in which every
// interior ' belongs to a doubled-quote escape pair, a closing ', and
// nothing after it — and returns the unescaped body. An undoubled quote
// followed by more text means s is an expression that merely BEGINS with a
// string literal (`'a' || 'b'`), and an unterminated tail (a trailing
// escape pair with no terminator) means s is not a literal at all; both
// must classify as expressions, never be endpoint-guessed into a mangled
// literal.
func singleQuotedLiteral(s string) (string, bool) {
	if len(s) < 2 || s[0] != '\'' {
		return "", false
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] != '\'' {
			b.WriteByte(s[i])
			continue
		}
		if i+1 < len(s) && s[i+1] == '\'' {
			b.WriteByte('\'') // doubled '' escape → one literal quote
			i++
			continue
		}
		// Undoubled quote: the literal's terminator. Well-formed only
		// when it is the final byte; residue after it means expression.
		if i == len(s)-1 {
			return b.String(), true
		}
		return "", false
	}
	return "", false // ran off the end without a terminator
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
