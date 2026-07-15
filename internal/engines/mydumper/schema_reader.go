// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"sluicesync.dev/sluice/internal/ir"
)

// SchemaReader builds an IR schema from a dump directory's per-table
// schema files. It implements [ir.SchemaReader] plus [ir.Verifier] (exact
// row counts by chunk re-scan — verify's count depth). Sample-depth verify
// is deliberately NOT implemented: file chunks have no cheap row
// addressing, so `sluice verify --depth sample` refuses with the
// orchestrator's clear "not supported" error (ADR-0161 §9).
type SchemaReader struct {
	dir *dumpDir
}

// ReadSchema parses every table's schema file into an IR schema. Table
// order is the sorted table-name order (deterministic for diffs/logs).
// Views/triggers/routines are not carried — their auxiliary files were
// WARN-skipped at open (see [openDumpDir]).
func (r *SchemaReader) ReadSchema(ctx context.Context) (*ir.Schema, error) {
	s := &ir.Schema{}
	for _, name := range r.dir.tableOrder {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tf := r.dir.tables[name]
		table, err := r.parseTableSchema(tf)
		if err != nil {
			return nil, err
		}
		s.Tables = append(s.Tables, table)
	}
	return s, nil
}

// parseTableSchema reads and parses one table's schema file, cross-
// checking the parsed table name against the filename (a mismatch means a
// mislabelled dump — refuse rather than migrate rows under the wrong name).
func (r *SchemaReader) parseTableSchema(tf *tableFiles) (*ir.Table, error) {
	base := filepath.Base(tf.schemaFile)
	content, err := readDumpFileAll(tf.schemaFile, schemaFileMaxBytes)
	if err != nil {
		return nil, err
	}
	table, err := parseSchemaFile(content, base)
	if err != nil {
		return nil, err
	}
	if table.Name != tf.name {
		return nil, fmt.Errorf("mydumper: schema file %s declares CREATE TABLE %s — filename and "+
			"statement disagree; corrupt or mislabelled dump", base, table.Name)
	}
	return table, nil
}

// ExactRowCount implements [ir.Verifier] for `sluice verify --depth count`:
// the authoritative count for a file source is a full re-scan of the
// table's chunks, counting extended-INSERT tuples without decoding values.
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("mydumper: ExactRowCount: table is nil")
	}
	return r.dir.countTableRows(ctx, table.Name)
}

// readDumpFileAll reads a whole (possibly compressed) dump file, refusing
// loudly past maxBytes — schema files are small; a giant one is not a
// mydumper schema file.
func readDumpFileAll(path string, maxBytes int64) (string, error) {
	f, err := openDumpFile(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return "", fmt.Errorf("mydumper: read %q: %w", path, err)
	}
	if int64(len(raw)) > maxBytes {
		return "", fmt.Errorf("mydumper: %q exceeds the %d MiB schema-file ceiling — not a mydumper "+
			"schema file", path, maxBytes>>20)
	}
	return string(raw), nil
}
