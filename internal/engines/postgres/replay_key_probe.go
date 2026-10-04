// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// ProbeReplayKey implements [ir.ReplayKeyProber]: it reports whether the
// target table exists and, if so, whether a replayed row of the recorded
// table collides on the key the change applier keys its INSERT on.
//
// Two questions, both answered from the applier's own rule rather than
// re-derived:
//
//  1. WHICH key. [loadConflictKey] is the function the applier's INSERT
//     dispatch calls ([ChangeApplier.conflictKeyFor]) to pick its single
//     ON CONFLICT arbiter, and an empty result from it IS the
//     plain-INSERT fallback. Whatever it excludes (a nullable member, a
//     partial or expression index, a DEFERRABLE key) this excludes. A
//     table whose only keys are DEFERRABLE comes back as the applier's
//     own coded refusal rather than as "keyless": the applier refuses
//     such a table loudly, so the caller surfaces that refusal.
//  2. Whether the replayed rows SUPPLY it. [buildInsertSQL] renders
//     `ON CONFLICT (<arbiter>)` whether or not the row carries the
//     arbiter's columns. When it does not — a bigserial, identity or
//     `DEFAULT gen_random_uuid()` surrogate the target was re-keyed on —
//     every re-applied row draws a fresh key and lands a second time
//     (measured: TestRowWriter_ProbeReplayKey_ShapeMatrix). So every
//     arbiter column must be a non-generated column of the recorded table
//     (exact name match: the applier quotes the row's keys verbatim) and
//     must not be generated on the target, where the applier drops it
//     from the INSERT. A generated arbiter derived only from supplied
//     columns does converge; it is refused anyway, conservatively,
//     because whether its expression reads only supplied columns is not
//     something this probe can see.
//
// The arbiter is the ONLY key judged, deliberately. A supplied UNIQUE
// index beside an unsupplied arbiter still collides, but as a 23505 from
// the applier rather than an upsert, so the broker could never get past
// it; refusing it here says so before anything is applied.
//
// The writer's schema is the applier's schema (both come from the DSN's
// resolved schema, and [ir.SchemaSetter] moves both for --target-schema).
func (w *RowWriter) ProbeReplayKey(ctx context.Context, table *ir.Table) (exists, keyed bool, err error) {
	if table == nil {
		return false, false, errors.New("postgres: ProbeReplayKey: table is nil")
	}
	supplied := irbackup.ReplaySuppliedColumns(table)
	if len(supplied) == 0 {
		return false, false, fmt.Errorf("postgres: ProbeReplayKey: recorded table %q carries no insertable columns; "+
			"cannot judge whether its rows supply a key", table.Name)
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
	if len(key) == 0 {
		return true, false, nil
	}
	generated, err := loadGeneratedColumns(ctx, tx, w.schema, table.Name)
	if err != nil {
		return true, false, fmt.Errorf("postgres: probe replay key for %q: generated columns: %w", table.Name, err)
	}
	carried := make(map[string]bool, len(supplied))
	for _, c := range supplied {
		carried[c] = true
	}
	for _, c := range key {
		if !carried[c] || generated[c] {
			return true, false, nil
		}
	}
	return true, true, nil
}

// loadGeneratedColumns returns the target table's generated (stored)
// columns — pg_attribute.attgenerated set — by name.
func loadGeneratedColumns(ctx context.Context, tx *sql.Tx, schema, table string) (map[string]bool, error) {
	const q = `
		SELECT a.attname
		FROM   pg_catalog.pg_attribute a
		JOIN   pg_catalog.pg_class     c ON c.oid = a.attrelid
		JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE  n.nspname = $1 AND c.relname = $2
		  AND  a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''`
	rows, err := tx.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}
