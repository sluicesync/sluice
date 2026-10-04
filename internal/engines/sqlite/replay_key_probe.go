// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// The F-E1 replay-duplication surfaces for a SQLite target.
//
// The SQLite writer's bulk path is a plain multi-row INSERT
// ([RowWriter.WriteRows]): no ON CONFLICT, no OR REPLACE, no OR IGNORE. So a
// row re-written into a table that already holds it either fails loudly on a
// key (`UNIQUE constraint failed`, which aborts the per-table transaction) or
// lands a second time. The restore re-run door needs to tell those apart
// before writing, and refuses a writer that cannot answer.

// tableExists reports whether a table of that name exists in the main
// schema. SQLite resolves identifiers case-insensitively, so the match is
// too.
func (w *RowWriter) tableExists(ctx context.Context, name string) (bool, error) {
	var n int
	err := w.db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE`, name).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("sqlite: probe existence of %q: %w", name, err)
	}
	return n > 0, nil
}

// IsTableEmpty implements [ir.TableEmptyChecker]. A table that does not
// exist is empty, per the interface contract.
func (w *RowWriter) IsTableEmpty(ctx context.Context, table *ir.Table) (bool, error) {
	if table == nil {
		return false, errors.New("sqlite: IsTableEmpty: table is nil")
	}
	exists, err := w.tableExists(ctx, table.Name)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	var one int
	err = w.db.QueryRowContext(ctx, "SELECT 1 FROM "+quoteIdent(table.Name)+" LIMIT 1").Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlite: probe %q for emptiness: %w", table.Name, err)
	}
	return false, nil
}

// ProbeReplayKey implements [ir.ReplayKeyProber]. A table is keyed when its
// PRIMARY KEY or some UNIQUE index — any of them, since a plain INSERT
// collides on every one — is non-partial and every key column is
//
//   - a plain column (an expression entry never qualifies, conservatively);
//   - not generated on the target (no writer binds a generated column);
//   - SUPPLIED: a non-generated column of the recorded table, matched
//     case-insensitively as SQLite resolves column names;
//   - unable to carry NULL in a replayed row: NOT NULL on the target, OR
//     NOT NULL in the recorded table (the source enforced it, so no replayed
//     value is NULL). SQLite's own NOT NULL is not implied by PRIMARY KEY —
//     outside WITHOUT ROWID tables a non-INTEGER primary key admits NULLs,
//     which never collide, and NULL written to an INTEGER PRIMARY KEY draws
//     a fresh rowid — so a column nullable on both sides refuses.
//
// An INTEGER PRIMARY KEY (the rowid alias) has no index_list entry; it is
// read from table_xinfo's pk column like every other primary key.
func (w *RowWriter) ProbeReplayKey(ctx context.Context, table *ir.Table) (exists, keyed bool, err error) {
	if table == nil {
		return false, false, errors.New("sqlite: ProbeReplayKey: table is nil")
	}
	supplied := map[string]bool{}
	for _, c := range irbackup.ReplaySuppliedColumns(table) {
		supplied[strings.ToLower(c)] = true
	}
	if len(supplied) == 0 {
		return false, false, fmt.Errorf("sqlite: ProbeReplayKey: recorded table %q carries no insertable columns; "+
			"cannot judge whether its rows supply a key", table.Name)
	}
	recordedNotNull := map[string]bool{}
	for _, c := range table.Columns {
		if c != nil && !c.Nullable {
			recordedNotNull[strings.ToLower(c.Name)] = true
		}
	}
	if exists, err = w.tableExists(ctx, table.Name); err != nil || !exists {
		return false, false, err
	}
	cols, pk, err := w.loadKeyColumns(ctx, table.Name)
	if err != nil {
		return true, false, err
	}
	keys, err := w.loadUniqueIndexKeys(ctx, table.Name)
	if err != nil {
		return true, false, err
	}
	if len(pk) > 0 {
		keys = append(keys, pk)
	}
	for _, key := range keys {
		if sqliteKeyCollides(key, cols, supplied, recordedNotNull) {
			return true, true, nil
		}
	}
	return true, false, nil
}

// sqliteColumn is a target column's key-relevant shape.
type sqliteColumn struct {
	notNull   bool
	generated bool
}

// sqliteKeyCollides applies [RowWriter.ProbeReplayKey]'s per-column rule to
// one key ("" = expression entry). Every map is keyed by lower-cased name.
func sqliteKeyCollides(key []string, cols map[string]sqliteColumn, supplied, recordedNotNull map[string]bool) bool {
	if len(key) == 0 {
		return false
	}
	for _, part := range key {
		if part == "" {
			return false
		}
		lc := strings.ToLower(part)
		c, ok := cols[lc]
		if !ok || c.generated || !supplied[lc] || (!c.notNull && !recordedNotNull[lc]) {
			return false
		}
	}
	return true
}

// loadKeyColumns reads table_xinfo: every column's nullability and
// generated flag, and the PRIMARY KEY's columns in key order.
func (w *RowWriter) loadKeyColumns(ctx context.Context, name string) (cols map[string]sqliteColumn, pk []string, err error) {
	rows, err := w.db.QueryContext(ctx, "PRAGMA table_xinfo("+quotePragmaArg(name)+")")
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite: probe replay key for %q: table_xinfo: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	cols = map[string]sqliteColumn{}
	type pkPart struct {
		pos  int
		name string
	}
	var pkParts []pkPart
	for rows.Next() {
		var (
			cid, notNull, pkPos, hidden int
			colName, declType           string
			dflt                        sql.NullString
		)
		if err := rows.Scan(&cid, &colName, &declType, &notNull, &dflt, &pkPos, &hidden); err != nil {
			return nil, nil, fmt.Errorf("sqlite: probe replay key for %q: table_xinfo: %w", name, err)
		}
		cols[strings.ToLower(colName)] = sqliteColumn{notNull: notNull == 1, generated: hidden == 2 || hidden == 3}
		if pkPos > 0 {
			pkParts = append(pkParts, pkPart{pos: pkPos, name: colName})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("sqlite: probe replay key for %q: table_xinfo: %w", name, err)
	}
	sort.Slice(pkParts, func(i, j int) bool { return pkParts[i].pos < pkParts[j].pos })
	pk = make([]string, len(pkParts))
	for i, p := range pkParts {
		pk[i] = p.name
	}
	return cols, pk, nil
}

// loadUniqueIndexKeys returns the key columns ("" for an expression entry)
// of every non-partial UNIQUE index on the table.
func (w *RowWriter) loadUniqueIndexKeys(ctx context.Context, name string) ([][]string, error) {
	names, err := w.uniqueIndexNames(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("sqlite: probe replay key for %q: index_list: %w", name, err)
	}
	keys := make([][]string, 0, len(names))
	for _, idx := range names {
		key, err := w.loadIndexKey(ctx, idx)
		if err != nil {
			return nil, fmt.Errorf("sqlite: probe replay key for %q: %w", name, err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// uniqueIndexNames reads PRAGMA index_list and returns the non-partial
// UNIQUE indexes. Split from loadUniqueIndexKeys so these rows close before
// the per-index index_xinfo queries open.
func (w *RowWriter) uniqueIndexNames(ctx context.Context, table string) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, "PRAGMA index_list("+quotePragmaArg(table)+")")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var (
			seq, unique, partial int
			idxName, origin      string
		)
		if err := rows.Scan(&seq, &idxName, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		if unique == 1 && partial == 0 {
			names = append(names, idxName)
		}
	}
	return names, rows.Err()
}

// loadIndexKey returns one index's key columns in order, "" standing for an
// expression entry. Auxiliary entries (key = 0) are skipped.
func (w *RowWriter) loadIndexKey(ctx context.Context, idx string) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, "PRAGMA index_xinfo("+quotePragmaArg(idx)+")")
	if err != nil {
		return nil, fmt.Errorf("index_xinfo(%q): %w", idx, err)
	}
	defer func() { _ = rows.Close() }()
	var key []string
	for rows.Next() {
		var (
			seqno, cid, desc, isKey int
			col                     sql.NullString
			coll                    string
		)
		if err := rows.Scan(&seqno, &cid, &col, &desc, &coll, &isKey); err != nil {
			return nil, fmt.Errorf("index_xinfo(%q): %w", idx, err)
		}
		if isKey == 1 {
			key = append(key, col.String)
		}
	}
	return key, rows.Err()
}
