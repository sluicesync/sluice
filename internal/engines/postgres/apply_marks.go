// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// # ADR-0190 apply marks on a Postgres target
//
// The engine-neutral decision logic lives in internal/applymarks; this file
// is the Postgres half: the sluice_cdc_apply_marks table, its availability
// check, and the SQL each apply path runs INSIDE its own target transaction
// (a mark committed without its rows, or rows without their mark, is the one
// state the design cannot tolerate — so a mark write never rides a separate
// transaction).
//
// Which paths write (every apply path; see ADR-0190 "Implementation status"
// and amendment A):
//
//   - per-change (Apply / applyOneImpl) and the serial batch loop write marks
//     with their rows and delete a transaction's marks with the position
//     write that passes its commit;
//   - the lane barrier (applyBarrierNoPosition) writes marks — its
//     pre-barrier checkpoint has already persisted the position up to the
//     barrier's own transaction;
//   - lane batches write the marks of the transaction the coordinator's mark
//     fence cleared (amendment A: it drained the lanes and persisted the
//     position at that transaction's start first), and the frontier checkpoint
//     deletes the marks of every transaction it passes.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// applyMarksTableName aliases the shared ADR-0190 control-table name.
const applyMarksTableName = appliershared.ApplyMarksTableName

// applyMarksUpsertChunk bounds the rows of one mark upsert statement: seven
// bind parameters each keeps a statement far below PostgreSQL's 65,535.
const applyMarksUpsertChunk = 1000

// applyMarksTableRef is the schema-qualified, quoted mark-table reference.
func applyMarksTableRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(applyMarksTableName)
}

// applyMarksTableDDL renders the mark table. The primary key is the mark's
// identity — one row per (stream, target table, key) — so every write is an
// UPSERT and the table holds one row per marked key, not one per change.
func applyMarksTableDDL(schema string) string {
	return `CREATE TABLE IF NOT EXISTS ` + applyMarksTableRef(schema) + ` (
		stream_id     VARCHAR(255) NOT NULL,
		table_name    TEXT         NOT NULL,
		key_digest    VARCHAR(64)  NOT NULL,
		tx_id         TEXT         NOT NULL,
		seq           BIGINT       NOT NULL,
		change_digest VARCHAR(64)  NOT NULL,
		scope_digest  TEXT         NOT NULL,
		PRIMARY KEY (stream_id, table_name, key_digest)
	)`
}

// applyMarksTableExists reports whether the mark table is present.
func applyMarksTableExists(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT pg_catalog.to_regclass($1) IS NOT NULL`, applyMarksTableRef(schema)).Scan(&present); err != nil {
		return false, fmt.Errorf("postgres: detect %s: %w", applyMarksTableName, err)
	}
	return present, nil
}

// ensureApplyMarksTable creates the mark table when it is absent. Detect
// FIRST: PostgreSQL checks CREATE privilege on the schema before it
// evaluates IF NOT EXISTS, so a DML-only role would fail an unconditional
// CREATE on a table that is already there (the unforwarded_refusal lesson).
func ensureApplyMarksTable(ctx context.Context, db *sql.DB, schema string) error {
	present, err := applyMarksTableExists(ctx, db, schema)
	if err != nil || present {
		return err
	}
	if _, err := db.ExecContext(ctx, applyMarksTableDDL(schema)); err != nil {
		return fmt.Errorf("postgres: create %s: %w", applyMarksTableName, err)
	}
	return nil
}

// applyMarksUsable reports why the mark table cannot be used by this role,
// or nil. It checks, without writing anything, that the table exists and
// that the role holds every privilege the protocol needs: a mark that cannot
// be written would fail the apply transaction it rides in, which is exactly
// the new refusal operator decision 2 rules out.
func applyMarksUsable(ctx context.Context, db *sql.DB, schema string) error {
	present, err := applyMarksTableExists(ctx, db, schema)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("%s does not exist in schema %q", applyMarksTableName, schema)
	}
	var ok bool
	q := `SELECT pg_catalog.has_table_privilege($1, 'SELECT')
		AND pg_catalog.has_table_privilege($1, 'INSERT')
		AND pg_catalog.has_table_privilege($1, 'UPDATE')
		AND pg_catalog.has_table_privilege($1, 'DELETE')`
	if err := db.QueryRowContext(ctx, q, applyMarksTableRef(schema)).Scan(&ok); err != nil {
		return fmt.Errorf("postgres: check %s privileges: %w", applyMarksTableName, err)
	}
	if !ok {
		return fmt.Errorf("this role lacks SELECT, INSERT, UPDATE or DELETE on %s", applyMarksTableRef(schema))
	}
	return nil
}

// loadApplyMarks reads every mark of the stream.
func loadApplyMarks(ctx context.Context, db *sql.DB, schema, streamID string) ([]applymarks.Mark, error) {
	q := "SELECT table_name, key_digest, tx_id, seq, change_digest, scope_digest FROM " + applyMarksTableRef(schema) +
		" WHERE stream_id = $1"
	rows, err := db.QueryContext(ctx, q, streamID)
	if err != nil {
		return nil, fmt.Errorf("postgres: load apply marks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var marks []applymarks.Mark
	for rows.Next() {
		var (
			m   applymarks.Mark
			seq int64
		)
		if err := rows.Scan(&m.Table, &m.KeyDigest, &m.TxID, &seq, &m.ChangeDigest, &m.ScopeDigest); err != nil {
			return nil, fmt.Errorf("postgres: load apply marks: %w", err)
		}
		m.Seq = uint64(seq) //nolint:gosec // written from a uint64 the writer bounded to MaxInt64 (applyMarkStatements)
		marks = append(marks, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: load apply marks: %w", err)
	}
	return marks, nil
}

// markStatement is one SQL statement of an executed [applymarks.Plan].
type markStatement struct {
	sql  string
	args []any
}

// applyMarkStatements renders a plan: chunked multi-row upserts of its marks,
// then one DELETE of the passed transactions' marks.
func applyMarkStatements(schema, streamID string, pl applymarks.Plan) ([]markStatement, error) {
	ref := applyMarksTableRef(schema)
	var out []markStatement
	for start := 0; start < len(pl.Upserts); start += applyMarksUpsertChunk {
		end := min(start+applyMarksUpsertChunk, len(pl.Upserts))
		var b strings.Builder
		b.WriteString("INSERT INTO " + ref + " (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest) VALUES ")
		args := make([]any, 0, (end-start)*7)
		for i, m := range pl.Upserts[start:end] {
			if m.Seq > uint64(1<<63-1) {
				return nil, fmt.Errorf("postgres: apply mark ordinal %d for %s overflows BIGINT", m.Seq, m.Table)
			}
			if i > 0 {
				b.WriteString(", ")
			}
			n := len(args)
			b.WriteString("(")
			for j := 1; j <= 7; j++ {
				if j > 1 {
					b.WriteString(", ")
				}
				b.WriteString("$" + strconv.Itoa(n+j))
			}
			b.WriteString(")")
			args = append(args, streamID, m.Table, m.KeyDigest, m.TxID, int64(m.Seq), m.ChangeDigest, m.ScopeDigest)
		}
		b.WriteString(" ON CONFLICT (stream_id, table_name, key_digest) DO UPDATE SET " +
			"tx_id = EXCLUDED.tx_id, seq = EXCLUDED.seq, change_digest = EXCLUDED.change_digest, scope_digest = EXCLUDED.scope_digest")
		out = append(out, markStatement{sql: b.String(), args: args})
	}
	if len(pl.Deletes) > 0 {
		out = append(out, markStatement{
			sql:  "DELETE FROM " + ref + " WHERE stream_id = $1 AND tx_id = ANY($2)",
			args: []any{streamID, pl.Deletes},
		})
	}
	return out, nil
}

// clearApplyMarks deletes every mark of the stream. Tolerant of a missing
// table (nothing to clear).
func clearApplyMarks(ctx context.Context, db *sql.DB, schema, streamID string) error {
	q := "DELETE FROM " + applyMarksTableRef(schema) + " WHERE stream_id = $1"
	return appliershared.TolerantExec(ctx, db, controlCfg, "clear apply marks", q, streamID)
}

// startApplyMarks loads the stream's marks at the start of an apply run, or
// disables them — with the APPLY-MARKS-UNAVAILABLE WARN, and today's
// behaviour — when the table cannot be used. A load failure on a usable
// table is an ordinary (classified) apply error.
func (a *ChangeApplier) startApplyMarks(ctx context.Context, streamID string) error {
	unusable := applyMarksUsable(ctx, a.db, a.controlSchema)
	if unusable == nil {
		marks, err := loadApplyMarks(ctx, a.db, a.controlSchema, streamID)
		if err != nil {
			return classifyApplierError(err)
		}
		a.marks.Load(streamID, a.rowFilterHash, marks)
		return nil
	}
	a.marks.Disable()
	applymarks.WarnUnavailable(ctx, "postgres", streamID, errors.Join(a.applyMarksEnsureErr, unusable))
	return nil
}

// applyMarkSubject resolves the target-side facts the mark decision needs
// for a row change: the routed table, its primary key and whether it carries
// a secondary uniqueness constraint (the lane router's own probe, so a table
// is "marked" exactly when it is table-scoped).
func (a *ChangeApplier) applyMarkSubject(ctx context.Context, c ir.Change) (applymarks.Subject, error) {
	schema, table := laneapply.RowChangeSchemaTable(c)
	routed := a.routedSchema(schema)
	pk, err := a.pkForRedact(ctx, routed, table)
	if err != nil {
		return applymarks.Subject{}, err
	}
	return applymarks.Subject{
		Table:           schemaTableKey(routed, table),
		PK:              pk,
		SecondaryUnique: len(pk) > 0 && a.tableHasNonPKUniqueIndex(ctx, routed, table),
	}, nil
}

// decideApplyMarks is [applymarks.Tracker.Decide] for a change about to
// dispatch; a no-op for a change that carries no identity or when marks are
// not in force this run.
func (a *ChangeApplier) decideApplyMarks(ctx context.Context, c ir.Change) (applymarks.Decision, error) {
	if !a.marks.Enabled() || ir.ApplyIDOf(c).IsZero() {
		return applymarks.Decision{}, nil
	}
	s, err := a.applyMarkSubject(ctx, c)
	if err != nil {
		return applymarks.Decision{}, classifyApplierError(fmt.Errorf("postgres: applier: apply-mark subject: %w", err))
	}
	return a.marks.Decide(c, s)
}

// applyMarksSkip reports, without side effects, whether the marks prove c
// already applied — the lane coordinator's route-time rows_applied check.
func (a *ChangeApplier) applyMarksSkip(ctx context.Context, c ir.Change) bool {
	if !a.marks.Enabled() || ir.ApplyIDOf(c).IsZero() {
		return false
	}
	s, err := a.applyMarkSubject(ctx, c)
	return err == nil && a.marks.Skips(c, s)
}

// applyMarkFenceTx is the lane coordinator's route-time question (ADR-0190
// amendment A): the transaction whose apply mark c would write, or "" when it
// writes none. A subject probe that fails answers the transaction — fencing
// costs only a drain, and the apply path raises the probe error itself.
func (a *ChangeApplier) applyMarkFenceTx(ctx context.Context, c ir.Change) string {
	id := ir.ApplyIDOf(c)
	if !a.marks.Enabled() || id.IsZero() {
		return ""
	}
	if s, err := a.applyMarkSubject(ctx, c); err == nil && !a.marks.WouldMark(c, s) {
		return ""
	}
	return id.TxID
}

// execApplyMarksTx runs a plan on a *sql.Tx.
func (a *ChangeApplier) execApplyMarksTx(ctx context.Context, tx *sql.Tx, pl applymarks.Plan) error {
	if pl.Empty() {
		return nil
	}
	stmts, err := applyMarkStatements(a.controlSchema, a.marks.StreamID(), pl)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if _, err := a.txExec(ctx, tx, s.sql, s.args...); err != nil {
			return fmt.Errorf("postgres: applier: write apply marks: %w", err)
		}
	}
	return nil
}

// queueApplyMarks queues a plan onto a pipelined batch; it executes, in
// order, in the batch's single flush and commits with it.
func (a *ChangeApplier) queueApplyMarks(b *pgxBatchTx, pl applymarks.Plan) error {
	if pl.Empty() {
		return nil
	}
	stmts, err := applyMarkStatements(a.controlSchema, a.marks.StreamID(), pl)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		b.queue(s.sql, s.args, queuedStmt{schema: a.controlSchema, table: applyMarksTableName, kind: "apply-marks"})
	}
	return nil
}

// ClearApplyMarks implements [ir.ApplyMarksClearer]: every cold start
// deletes the stream's marks before its copy, because the copy re-seeds the
// target and no transaction a mark names will ever be re-delivered. A mark
// table this role cannot use is left alone — this role never consults it
// either (the apply run disables marks behind APPLY-MARKS-UNAVAILABLE), and
// refusing the cold start over it would be the new refusal operator
// decision 2 rules out.
func (a *ChangeApplier) ClearApplyMarks(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: ClearApplyMarks: streamID is empty")
	}
	if applyMarksUsable(ctx, a.db, a.controlSchema) == nil {
		return clearApplyMarks(ctx, a.db, a.controlSchema, streamID)
	}
	return nil
}

var _ ir.ApplyMarksClearer = (*ChangeApplier)(nil)
