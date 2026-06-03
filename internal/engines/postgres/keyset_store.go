// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/redact"
)

// PII Phase 4 — Postgres `db:` keyset store (ADR-0041).
//
// Mirrors the control-table pattern (control_table.go): schema-
// qualified, CREATE TABLE IF NOT EXISTS, idempotent. The redact
// package depends only on [redact.KeysetStore]; the engine-specific
// SQL stays here (IR-first tenet). Registered with the redact
// package in init() so `--keyset-source=db:postgres://...` resolves
// without redact importing this package.

const keysetTableName = "sluice_keysets"

func init() {
	redact.RegisterKeysetStoreOpener("postgres", openKeysetStore)
}

// pgKeysetStore is the Postgres implementation of
// [redact.KeysetStore].
type pgKeysetStore struct {
	db     *sql.DB
	schema string
}

// openKeysetStore opens a *sql.DB against the keyset DSN and pings
// it. The DSN's `schema` query parameter (default "public") selects
// the namespace, identically to the rest of the PG engine.
func openKeysetStore(ctx context.Context, dsn string) (redact.KeysetStore, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &pgKeysetStore{db: db, schema: cfg.schema}, nil
}

// EnsureKeysetTable creates sluice_keysets if absent. Schema per
// ADR-0041 §"Persistence shape": BYTEA bytes, TIMESTAMPTZ stamps,
// composite PK (name, generation), one active row per name.
// Idempotent.
func (s *pgKeysetStore) EnsureKeysetTable(ctx context.Context) error {
	tableRef := quoteIdent(s.schema) + "." + quoteIdent(keysetTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			name        TEXT        NOT NULL,
			generation  INTEGER     NOT NULL,
			bytes       BYTEA       NOT NULL,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			retired_at  TIMESTAMPTZ NULL,
			active      BOOLEAN     NOT NULL DEFAULT false,
			PRIMARY KEY (name, generation)
		)`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure keyset table: %w", err)
	}
	return nil
}

// LoadKeyset reads every row and resolves it via the shared
// [redact.KeysetFromRows] helper so the db: path produces
// byte-identical resolution to file:/env:.
func (s *pgKeysetStore) LoadKeyset(ctx context.Context) (*redact.Keyset, error) {
	tableRef := quoteIdent(s.schema) + "." + quoteIdent(keysetTableName)
	q := "SELECT name, generation, bytes, active FROM " + tableRef
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: query keyset: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var collected []redact.KeysetRow
	for rows.Next() {
		var r redact.KeysetRow
		if err := rows.Scan(&r.Name, &r.Generation, &r.Bytes, &r.Active); err != nil {
			return nil, fmt.Errorf("postgres: scan keyset row: %w", err)
		}
		collected = append(collected, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate keyset rows: %w", err)
	}
	return redact.KeysetFromRows(collected, "db:postgres", "")
}

// Close releases the connection pool.
func (s *pgKeysetStore) Close() error {
	return s.db.Close()
}
