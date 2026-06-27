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
		if err := r.readColumnsAndPK(ctx, t); err != nil {
			return nil, err
		}
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
// defaults) and t.PrimaryKey from PRAGMA table_info, reusing the file engine's
// [resolveColumnType] / [parseDefault] / [markRowidAutoIncrement].
func (r *D1SchemaReader) readColumnsAndPK(ctx context.Context, t *ir.Table) error {
	rows, err := r.client.queryRows(ctx, "PRAGMA table_info("+quotePragmaArg(t.Name)+")")
	if err != nil {
		return err
	}

	type pkEntry struct {
		pos  int
		name string
	}
	var pkEntries []pkEntry

	for _, row := range rows {
		name, err := rowString(row, "name")
		if err != nil {
			return err
		}
		declType, _, err := rowNullString(row, "type")
		if err != nil {
			return err
		}
		notNull, err := rowInt(row, "notnull")
		if err != nil {
			return err
		}
		pk, err := rowInt(row, "pk")
		if err != nil {
			return err
		}
		dfltVal, dfltOK, err := rowNullString(row, "dflt_value")
		if err != nil {
			return err
		}
		col := &ir.Column{
			Name:     name,
			Type:     resolveColumnType(declType),
			Nullable: notNull == 0,
			Default:  parseDefault(sql.NullString{String: dfltVal, Valid: dfltOK}),
		}
		if pk > 0 {
			pkEntries = append(pkEntries, pkEntry{pos: int(pk), name: name})
		}
		t.Columns = append(t.Columns, col)
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("d1: table %q has no columns", t.Name)
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
	return nil
}

// readIndexes populates t.Indexes from PRAGMA index_list / index_info. The PK
// index (origin 'pk') is skipped (captured from table_info); an expression
// index (a NULL index_info column name) is skipped — identical scope to the
// file engine.
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
		cols, ok, err := r.readIndexColumns(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			continue // expression index — skipped
		}
		t.Indexes = append(t.Indexes, &ir.Index{
			Name:    name,
			Columns: cols,
			Unique:  unique == 1,
		})
	}
	return nil
}

// readIndexColumns returns the plain-column entries of one index in position
// order. ok is false when the index has any expression entry (NULL column
// name), so the caller skips it — mirrors the file engine.
func (r *D1SchemaReader) readIndexColumns(ctx context.Context, indexName string) (cols []ir.IndexColumn, ok bool, err error) {
	rows, err := r.client.queryRows(ctx, "PRAGMA index_info("+quotePragmaArg(indexName)+")")
	if err != nil {
		return nil, false, err
	}
	for _, row := range rows {
		name, present, err := rowNullString(row, "name")
		if err != nil {
			return nil, false, err
		}
		if !present {
			return nil, false, nil // expression entry — skip whole index
		}
		cols = append(cols, ir.IndexColumn{Column: name})
	}
	return cols, true, nil
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
