// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// This file is the flat-file staging shim (ADR-0163): the csv/tsv/ndjson
// source engines (internal/engines/flatfile) materialize a schema-less flat
// file into a temp SQLite database and read it through THIS package's
// readers — the same reader shape the ADR-0130 `.sql`-dump ingest path uses,
// so the staged source inherits the whole validated surface for free:
// value decode, the ir.InferredTypeValidator behind --infer-types, verify's
// ExactRowCount, and the tempPath ownership rules (removed on Close;
// within-table chunking disqualified, exactly like a materialized dump).
// Engine-to-engine reuse follows the mydumper→mysql shim precedent
// (mysql/flatfile_shim.go); the orchestrator still imports neither.

// OpenStagedSchemaReader opens a SchemaReader over an already-staged temp
// SQLite database. The reader OWNS stagedPath from this call on: it is
// removed on Close (and on an open failure here, so the caller never has to
// clean up after an error). displayPath is the operator's original flat
// file, used in error messages.
func OpenStagedSchemaReader(ctx context.Context, stagedPath, displayPath string) (*SchemaReader, error) {
	db, err := openStagedDB(ctx, stagedPath, displayPath)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, path: displayPath, tempPath: stagedPath}, nil
}

// OpenStagedRowReader is the RowReader counterpart of
// [OpenStagedSchemaReader]. The staged tables carry only TEXT columns, so
// the temporal date-encoding policy is pinned to ISO (nothing ever consults
// it — no staged column resolves to a temporal IR type until --infer-types
// promotes one, and promoted values decode through the override path).
func OpenStagedRowReader(ctx context.Context, stagedPath, displayPath string) (*RowReader, error) {
	db, err := openStagedDB(ctx, stagedPath, displayPath)
	if err != nil {
		return nil, err
	}
	return &RowReader{db: db, path: displayPath, dateEnc: dateEncodingISO, tempPath: stagedPath}, nil
}

// openStagedDB opens the staged temp database read-only and verifies it is
// reachable. On failure the staged file is removed (ownership transferred on
// call, per the constructors' contract).
func openStagedDB(ctx context.Context, stagedPath, displayPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", appendPragmas(stagedPath, readOnlyPragmas))
	if err != nil {
		cleanupTemp(stagedPath)
		return nil, fmt.Errorf("sqlite: open staged copy of %q: %w", displayPath, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		cleanupTemp(stagedPath)
		return nil, fmt.Errorf("sqlite: open staged copy of %q: %w", displayPath, err)
	}
	return db, nil
}
