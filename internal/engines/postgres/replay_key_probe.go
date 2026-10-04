// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ProbeReplayKey implements [ir.ReplayKeyProber]: it reports whether the
// target table exists and, if so, whether the change applier would key an
// INSERT on it.
//
// It does not re-derive the answer. [loadConflictKey] is the function the
// applier's INSERT dispatch calls ([ChangeApplier.conflictKeyFor]) to pick
// its ON CONFLICT arbiter, and an empty result from it IS the plain-INSERT
// fallback — so this asks that function and nothing else. Whatever it
// excludes (a nullable member, a partial or expression index, a DEFERRABLE
// key) this excludes, by construction. A table whose only keys are
// DEFERRABLE comes back as the applier's own coded refusal rather than as
// "keyless": the applier refuses such a table loudly, so it is not a
// silent-duplication case and the caller should surface the refusal.
//
// The writer's schema is the applier's schema (both come from the DSN's
// resolved schema, and [ir.SchemaSetter] moves both for --target-schema).
func (w *RowWriter) ProbeReplayKey(ctx context.Context, table *ir.Table) (exists, keyed bool, err error) {
	if table == nil {
		return false, false, errors.New("postgres: ProbeReplayKey: table is nil")
	}
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, false, fmt.Errorf("postgres: probe replay key for %q: begin tx: %w", table.Name, err)
	}
	defer func() { _ = tx.Rollback() }()

	const existsQ = `
		SELECT EXISTS (
			SELECT 1
			FROM   pg_catalog.pg_class c
			JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE  n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')
		)`
	if err := tx.QueryRowContext(ctx, existsQ, w.schema, table.Name).Scan(&exists); err != nil {
		return false, false, fmt.Errorf("postgres: probe replay key for %q: existence: %w", table.Name, err)
	}
	if !exists {
		return false, false, nil
	}
	key, err := loadConflictKey(ctx, tx, w.schema, table.Name)
	if err != nil {
		return true, false, err
	}
	return true, len(key) > 0, nil
}
