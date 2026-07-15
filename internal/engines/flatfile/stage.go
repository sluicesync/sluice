// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// stageBatchRows bounds how many rows are inserted per staging transaction —
// large enough to amortise commit overhead, small enough to bound the
// transaction's memory/WAL footprint (the stage_d1.go constant's shape).
const stageBatchRows = 1000

// stager owns the staging write side: one temp SQLite database holding one
// all-TEXT table. Every value is bound as its exact source text (or nil),
// so the staged file is byte-faithful to the flat file — no affinity
// coercion can occur (TEXT affinity stores text verbatim). The column set
// may grow (NDJSON); SQLite's O(1) ADD COLUMN backfills existing rows with
// NULL, which is exactly the absent-key semantic.
type stager struct {
	db    *sql.DB
	table string

	cols     []string
	colIndex map[string]int

	tx      *sql.Tx
	stmt    *sql.Stmt
	pending int
}

// newStager opens the (fresh) temp database. The modernc driver is
// registered by this package's sqlite import.
func newStager(ctx context.Context, dbPath, table string) (*stager, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	// One connection: the stager is strictly sequential, and a single conn
	// keeps the tx/stmt lifecycle trivial.
	db.SetMaxOpenConns(1)
	return &stager{db: db, table: table, colIndex: map[string]int{}}, nil
}

// createTable creates the staged table with the given TEXT columns (CSV: the
// full set is known up front).
func (s *stager) createTable(ctx context.Context, cols []string) error {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = quoteIdent(c) + " TEXT"
		s.colIndex[c] = i
	}
	s.cols = append([]string(nil), cols...)
	ddl := "CREATE TABLE " + quoteIdent(s.table) + " (" + strings.Join(defs, ", ") + ")"
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("stage table %q: %w", s.table, err)
	}
	return nil
}

// columnCount reports how many columns are known so far (NDJSON growth).
func (s *stager) columnCount() int { return len(s.cols) }

// upsertColumns ensures every key exists as a column, creating the table on
// first sight and ALTERing new columns in later (NDJSON's evolving shape).
func (s *stager) upsertColumns(ctx context.Context, keys []string) error {
	if len(s.cols) == 0 {
		if len(keys) == 0 {
			return nil
		}
		return s.createTable(ctx, keys)
	}
	for _, k := range keys {
		if _, ok := s.colIndex[k]; ok {
			continue
		}
		// New column mid-file: rebuild the insert stmt on next use. ALTER is
		// fine inside the batch transaction (SQLite DDL is transactional).
		if err := s.closeStmt(); err != nil {
			return err
		}
		ddl := "ALTER TABLE " + quoteIdent(s.table) + " ADD COLUMN " + quoteIdent(k) + " TEXT"
		exec := s.db.ExecContext
		if s.tx != nil {
			exec = s.tx.ExecContext
		}
		if _, err := exec(ctx, ddl); err != nil {
			return fmt.Errorf("stage add column %q: %w", k, err)
		}
		s.colIndex[k] = len(s.cols)
		s.cols = append(s.cols, k)
	}
	return nil
}

// insert stages one row whose values align 1:1 with the current column set
// (the CSV path).
func (s *stager) insert(ctx context.Context, vals []any) error {
	if err := s.ensureBatch(ctx); err != nil {
		return err
	}
	if _, err := s.stmt.ExecContext(ctx, vals...); err != nil {
		return fmt.Errorf("stage insert into %q: %w", s.table, err)
	}
	s.pending++
	if s.pending < stageBatchRows {
		return nil
	}
	return s.commitBatch()
}

// insertByName stages one row from key/value pairs (the NDJSON path):
// columns absent from this row are bound NULL.
func (s *stager) insertByName(ctx context.Context, keys []string, vals []any) error {
	row := make([]any, len(s.cols))
	for i, k := range keys {
		row[s.colIndex[k]] = vals[i]
	}
	return s.insert(ctx, row)
}

// ensureBatch opens the transaction + prepared statement lazily (and after
// any column-set growth invalidated them).
func (s *stager) ensureBatch(ctx context.Context) error {
	if s.tx == nil {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("stage begin tx: %w", err)
		}
		s.tx = tx
	}
	if s.stmt == nil {
		quoted := make([]string, len(s.cols))
		for i, c := range s.cols {
			quoted[i] = quoteIdent(c)
		}
		ph := strings.TrimSuffix(strings.Repeat("?, ", len(s.cols)), ", ")
		stmt, err := s.tx.PrepareContext(ctx,
			"INSERT INTO "+quoteIdent(s.table)+" ("+strings.Join(quoted, ", ")+") VALUES ("+ph+")")
		if err != nil {
			return fmt.Errorf("stage prepare insert: %w", err)
		}
		s.stmt = stmt
	}
	return nil
}

// commitBatch commits the open transaction (if any), closing the stmt first.
func (s *stager) commitBatch() error {
	if err := s.closeStmt(); err != nil {
		return err
	}
	if s.tx == nil {
		return nil
	}
	if err := s.tx.Commit(); err != nil {
		s.tx = nil
		return fmt.Errorf("stage commit: %w", err)
	}
	s.tx = nil
	s.pending = 0
	return nil
}

// closeStmt closes the prepared statement (kept nil-safe; called before any
// DDL or commit).
func (s *stager) closeStmt() error {
	if s.stmt == nil {
		return nil
	}
	err := s.stmt.Close()
	s.stmt = nil
	if err != nil {
		return fmt.Errorf("stage close stmt: %w", err)
	}
	return nil
}

// finish commits the trailing partial batch.
func (s *stager) finish(context.Context) error {
	return s.commitBatch()
}

// close releases the database handle, rolling back any un-committed batch
// (only reached on error paths — finish committed the happy path).
func (s *stager) close() error {
	_ = s.closeStmt()
	if s.tx != nil {
		_ = s.tx.Rollback()
		s.tx = nil
	}
	return s.db.Close()
}

// quoteIdent renders a staged table/column name as a double-quoted SQL
// identifier, escaping embedded double-quotes by doubling. Column names come
// from operator files (headers / JSON keys), so arbitrary characters are
// expected and carried faithfully.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
