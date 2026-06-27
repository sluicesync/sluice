// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"sluicesync.dev/sluice/internal/ir"
)

// D1SchemaReader reads schema metadata from a live Cloudflare D1 database over
// the HTTP query API. It runs the SAME catalog queries the file engine's
// [SchemaReader] uses (sqlite_master + the table-introspection PRAGMAs) and
// builds an identical [ir.Schema] by reusing the package-private resolution
// ([resolveColumnType], [parseDefault], [parseFKAction], [markRowidAutoIncrement]).
// The only difference from the file reader is the row source: D1-shaped JSON
// over HTTP instead of *sql.Rows. PRAGMA scalar columns are small and safe as
// plain JSON, so only the data read (D1RowReader) needs the CAST/typeof
// exactness (ADR-0132 §5).
type D1SchemaReader struct {
	client *d1Client
}

// Close releases the reader. There is no pool or temp file to clean up for the
// HTTP transport, so it is a no-op; it exists so the orchestrator's io.Closer
// probe behaves the same as it does for the file engine.
func (r *D1SchemaReader) Close() error { return nil }

// ReadSchema queries sqlite_master + PRAGMAs over HTTP and returns a fully
// populated IR Schema. D1 has a flat namespace (one database), so every IR
// Table.Schema is empty — identical to the file engine.
func (r *D1SchemaReader) ReadSchema(ctx context.Context) (*ir.Schema, error) {
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
		// Generated-column bodies + CHECK constraints live only in the CREATE
		// TABLE SQL — fetched here over HTTP, parsed by the SAME shared helpers
		// the file engine uses (ADR-0133).
		createSQL, err := r.objectSQL(ctx, "table", name)
		if err != nil {
			return nil, err
		}
		applyGeneratedAndChecks(ctx, t, createSQL, genStored)
		if err := r.readIndexes(ctx, t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
		byName[name] = t
	}

	// Foreign keys are read after every PK is known so an implicit parent-PK
	// reference (SQLite leaves `to` NULL) can be resolved — mirrors the file
	// engine's two-pass order.
	for _, t := range tables {
		if err := r.readForeignKeys(ctx, t, byName); err != nil {
			return nil, err
		}
	}

	return &ir.Schema{Tables: tables}, nil
}

// tableNames lists user tables, excluding SQLite's internal sqlite_* tables AND
// Cloudflare D1's internal `_cf_*` tables — the SAME query and escaping the file
// engine uses (a user table merely containing "cf" is NOT dropped, ADR-0130).
func (r *D1SchemaReader) tableNames(ctx context.Context) ([]string, error) {
	const q = `
		SELECT name FROM sqlite_master
		WHERE type = 'table'
		  AND name NOT LIKE 'sqlite_%'
		  AND name NOT LIKE '\_cf\_%' ESCAPE '\'
		ORDER BY name`
	rows, err := r.client.queryRows(ctx, q)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		name, err := rowString(row, "name")
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// readColumnsAndPK populates t.Columns (affinity-resolved IR types, nullability,
// defaults) and t.PrimaryKey from PRAGMA table_xinfo, reusing the file engine's
// [resolveColumnType] / [parseDefault] / [markRowidAutoIncrement]. The `hidden`
// column (table_xinfo superset of table_info) identifies generated columns
// (2 = VIRTUAL, 3 = STORED) and hidden virtual-table columns (1, skipped) —
// the SAME rules as the file engine; the returned name→stored map drives the
// shared generated-column carry (ADR-0133).
func (r *D1SchemaReader) readColumnsAndPK(ctx context.Context, t *ir.Table) (map[string]bool, error) {
	rows, err := r.client.queryRows(ctx, "PRAGMA table_xinfo("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return nil, err
	}

	type pkEntry struct {
		pos  int
		name string
	}
	var pkEntries []pkEntry
	genStored := map[string]bool{}

	for _, row := range rows {
		name, err := rowString(row, "name")
		if err != nil {
			return nil, err
		}
		declType, _, err := rowNullString(row, "type")
		if err != nil {
			return nil, err
		}
		notNull, err := rowInt(row, "notnull")
		if err != nil {
			return nil, err
		}
		pk, err := rowInt(row, "pk")
		if err != nil {
			return nil, err
		}
		hidden, err := rowInt(row, "hidden")
		if err != nil {
			return nil, err
		}
		dfltVal, dfltOK, err := rowNullString(row, "dflt_value")
		if err != nil {
			return nil, err
		}
		if hidden == 1 {
			continue // virtual-table hidden column; not part of the visible set
		}
		col := &ir.Column{
			Name:     name,
			Type:     resolveColumnType(declType),
			Nullable: notNull == 0,
			Default:  parseDefault(sql.NullString{String: dfltVal, Valid: dfltOK}),
		}
		if hidden == 2 || hidden == 3 {
			genStored[name] = hidden == 3 // 3 = STORED, 2 = VIRTUAL
		}
		if pk > 0 {
			pkEntries = append(pkEntries, pkEntry{pos: int(pk), name: name})
		}
		t.Columns = append(t.Columns, col)
	}
	if len(t.Columns) == 0 {
		return nil, fmt.Errorf("d1: table %q has no columns", t.Name)
	}

	if len(pkEntries) > 0 {
		sort.Slice(pkEntries, func(i, j int) bool { return pkEntries[i].pos < pkEntries[j].pos })
		pkCols := make([]ir.IndexColumn, len(pkEntries))
		for i, e := range pkEntries {
			pkCols[i] = ir.IndexColumn{Column: e.name}
		}
		t.PrimaryKey = &ir.Index{Columns: pkCols, Unique: true}

		// A single-column INTEGER-affinity PK is SQLite's rowid alias
		// (auto-assigning); mark it so the target emits identity/AUTO_INCREMENT
		// — the same rule as the file engine.
		if len(pkEntries) == 1 {
			markRowidAutoIncrement(t, pkEntries[0].name)
		}
	}
	return genStored, nil
}

// objectSQL returns the verbatim `sql` text sqlite_master records for the named
// object (objType "table"/"index") over HTTP — the same query the file engine
// runs locally. A missing/NULL `sql` (e.g. a UNIQUE-constraint auto-index)
// returns "" with no error.
func (r *D1SchemaReader) objectSQL(ctx context.Context, objType, name string) (string, error) {
	rows, err := r.client.queryRows(ctx,
		"SELECT sql FROM sqlite_master WHERE type = ? AND name = ?", objType, name)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	s, _, err := rowNullString(rows[0], "sql")
	if err != nil {
		return "", err
	}
	return s, nil
}

// readIndexes populates t.Indexes from PRAGMA index_list / index_info. The PK
// index (origin 'pk') is skipped (captured from table_xinfo); expression and
// partial indexes carry their expression/predicate (parsed from the CREATE
// INDEX SQL, tagged "sqlite") — identical behaviour to the file engine via the
// shared [buildIndexColumns] / [extractIndexPredicate] helpers (ADR-0133).
func (r *D1SchemaReader) readIndexes(ctx context.Context, t *ir.Table) error {
	listRows, err := r.client.queryRows(ctx, "PRAGMA index_list("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return err
	}
	for _, row := range listRows {
		origin, _, err := rowNullString(row, "origin")
		if err != nil {
			return err
		}
		if origin == "pk" {
			continue
		}
		name, err := rowString(row, "name")
		if err != nil {
			return err
		}
		unique, err := rowInt(row, "unique")
		if err != nil {
			return err
		}
		partial, err := rowInt(row, "partial")
		if err != nil {
			return err
		}
		entries, err := r.readIndexColumns(ctx, name)
		if err != nil {
			return err
		}
		var createSQL string
		if partial == 1 || hasExprEntry(entries) {
			if createSQL, err = r.objectSQL(ctx, "index", name); err != nil {
				return err
			}
		}
		cols, exprCount, ok := buildIndexColumns(ctx, t.Name, name, entries, createSQL)
		if !ok {
			continue // expression index that couldn't be parsed — WARN-skipped
		}
		idx := &ir.Index{
			Name:    name,
			Columns: cols,
			Unique:  unique == 1,
		}
		hasPred := false
		if partial == 1 {
			if pred, pok := extractIndexPredicate(createSQL); pok {
				idx.Predicate = pred
				idx.PredicateDialect = sqliteDialect
				hasPred = true
			}
		}
		warnIndexVerbatim(ctx, t.Name, name, hasPred, exprCount)
		t.Indexes = append(t.Indexes, idx)
	}
	return nil
}

// readIndexColumns returns the index_info entries of one index in position
// order; an entry with a NULL column name is an expression entry (isExpr) whose
// text lives in the CREATE INDEX SQL — mirrors the file engine.
func (r *D1SchemaReader) readIndexColumns(ctx context.Context, indexName string) ([]indexInfoEntry, error) {
	rows, err := r.client.queryRows(ctx, "PRAGMA index_info("+quotePragmaArg(indexName)+")")
	if err != nil {
		return nil, err
	}
	var entries []indexInfoEntry
	for _, row := range rows {
		name, present, err := rowNullString(row, "name")
		if err != nil {
			return nil, err
		}
		entries = append(entries, indexInfoEntry{name: name, isExpr: !present})
	}
	return entries, nil
}

// readForeignKeys populates t.ForeignKeys from PRAGMA foreign_key_list, grouping
// by the pragma's `id` and resolving an implicit parent-PK reference (NULL `to`)
// against the referenced table's primary key — the same logic, and the same
// loud refusal on an unresolvable parent PK, as the file engine.
func (r *D1SchemaReader) readForeignKeys(ctx context.Context, t *ir.Table, byName map[string]*ir.Table) error {
	rows, err := r.client.queryRows(ctx, "PRAGMA foreign_key_list("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return err
	}

	type fkAccum struct {
		refTable string
		cols     []string
		refCols  []string
		hasNull  bool
		onDelete ir.FKAction
		onUpdate ir.FKAction
	}
	byID := map[int]*fkAccum{}
	var order []int

	for _, row := range rows {
		id, err := rowInt(row, "id")
		if err != nil {
			return err
		}
		refTable, err := rowString(row, "table")
		if err != nil {
			return err
		}
		from, err := rowString(row, "from")
		if err != nil {
			return err
		}
		to, toOK, err := rowNullString(row, "to")
		if err != nil {
			return err
		}
		onUpdate, _, err := rowNullString(row, "on_update")
		if err != nil {
			return err
		}
		onDelete, _, err := rowNullString(row, "on_delete")
		if err != nil {
			return err
		}
		fk, ok := byID[int(id)]
		if !ok {
			fk = &fkAccum{
				refTable: refTable,
				onDelete: parseFKAction(onDelete),
				onUpdate: parseFKAction(onUpdate),
			}
			byID[int(id)] = fk
			order = append(order, int(id))
		}
		fk.cols = append(fk.cols, from)
		if toOK {
			fk.refCols = append(fk.refCols, to)
		} else {
			fk.hasNull = true
		}
	}

	sort.Ints(order)
	for _, id := range order {
		fk := byID[id]
		refCols := fk.refCols
		if fk.hasNull {
			parent, ok := byName[fk.refTable]
			if !ok || parent.PrimaryKey == nil {
				return fmt.Errorf(
					"d1: table %q foreign key to %q omits the parent column but the parent has no resolvable primary key",
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
