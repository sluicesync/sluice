// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/redact"
)

// PII Phase 4 — MySQL `db:` keyset store (ADR-0041).
//
// Mirrors the control-table pattern (control_table.go): flat
// namespace (database implicit in the connection), CREATE TABLE IF
// NOT EXISTS, ENGINE=InnoDB DEFAULT CHARSET=utf8mb4. The redact
// package depends only on [redact.KeysetStore]; the engine-specific
// SQL stays here (IR-first tenet). Registered with the redact
// package in init() so `--keyset-source=db:<mysql-dsn>` resolves
// without redact importing this package.

const keysetTableName = "sluice_keysets"

func init() {
	redact.RegisterKeysetStoreOpener("mysql", openKeysetStore)
}

// mysqlKeysetStore is the MySQL implementation of
// [redact.KeysetStore].
type mysqlKeysetStore struct {
	db *sql.DB
}

// openKeysetStore parses the DSN, opens a *sql.DB, and pings it.
//
// Package-level parseDSN (not the flavor-aware parseDSNForFlavor): the
// keyset store is opened via the redact registry with no engine — and
// therefore no Flavor — in scope, so it conservatively keeps the driver's
// binary-protocol default. Its statements are single-row control-table
// reads/writes; the ADR-0153 RTT saving is immaterial here.
func openKeysetStore(ctx context.Context, dsn string) (redact.KeysetStore, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// nil sqlMode = the strict default: the keyset store is a control-table
	// connection (sluice_keysets), not a value-migration path, and it is opened
	// via the redact registry with no engine in scope, so it always uses the
	// strict-by-default sql_mode rather than a per-instance --mysql-sql-mode.
	db, err := openDB(ctx, cfg, nil)
	if err != nil {
		return nil, err
	}
	return &mysqlKeysetStore{db: db}, nil
}

// EnsureKeysetTable creates sluice_keysets if absent. Schema per
// ADR-0041 §"Persistence shape", MySQL-flavored: BLOB bytes,
// TIMESTAMP stamps, composite PK (name, generation), one active row
// per name. Idempotent.
func (s *mysqlKeysetStore) EnsureKeysetTable(ctx context.Context) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS ` + "`" + keysetTableName + "`" + ` (
			name        VARCHAR(255) NOT NULL,
			generation  INT          NOT NULL,
			bytes       BLOB         NOT NULL,
			created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			retired_at  TIMESTAMP    NULL,
			active      BOOLEAN      NOT NULL DEFAULT false,
			PRIMARY KEY (name, generation)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure keyset table: %w", wrapDDLError(err))
	}
	return nil
}

// LoadKeyset reads every row and resolves it via the shared
// [redact.KeysetFromRows] helper so the db: path produces
// byte-identical resolution to file:/env:.
func (s *mysqlKeysetStore) LoadKeyset(ctx context.Context) (*redact.Keyset, error) {
	const q = "SELECT name, generation, bytes, active FROM `" + keysetTableName + "`"
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("mysql: query keyset: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var collected []redact.KeysetRow
	for rows.Next() {
		var r redact.KeysetRow
		if err := rows.Scan(&r.Name, &r.Generation, &r.Bytes, &r.Active); err != nil {
			return nil, fmt.Errorf("mysql: scan keyset row: %w", err)
		}
		collected = append(collected, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: iterate keyset rows: %w", err)
	}
	return redact.KeysetFromRows(collected, "db:mysql", "")
}

// Close releases the connection pool.
func (s *mysqlKeysetStore) Close() error {
	return s.db.Close()
}
