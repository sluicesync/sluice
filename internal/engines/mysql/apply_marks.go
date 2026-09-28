// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # ADR-0190 apply marks on a MySQL-family target
//
// The engine-neutral decision logic lives in internal/applymarks; this file
// is the MySQL half: the sluice_cdc_apply_marks table, its availability
// check, and the SQL each apply path runs INSIDE its own target transaction
// (a mark committed without its rows, or rows without their mark, is the one
// state the design cannot tolerate — so a mark write never rides a separate
// transaction).
//
// Which paths write, which check (the delivered subset; see the ADR-0190
// implementation note):
//
//   - per-change (Apply / applyOneImpl) and the serial batch loop write marks
//     with their rows and delete a transaction's marks with the position
//     write that passes its commit;
//   - the lane barrier (applyBarrierNoPosition) writes marks — its
//     pre-barrier checkpoint has already persisted the position up to the
//     barrier's own transaction;
//   - lane batches CHECK marks but write none, and the frontier checkpoint
//     deletes the marks of every transaction it passes.
//
// The mark table is created only by EnsureControlTable and only when absent
// (detect-then-create, the safe-migrations constraint). A branch that refuses
// the CREATE, a role without the privileges, or a --schema-already-applied
// target that lacks it runs WITHOUT marks behind the APPLY-MARKS-UNAVAILABLE
// WARN: operator decision 2, never a new refusal.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// applyMarksTableName aliases the shared ADR-0190 control-table name.
const applyMarksTableName = appliershared.ApplyMarksTableName

// applyMarksUpsertChunk bounds the rows of one mark upsert statement: seven
// placeholders each keeps a statement far below MySQL's 65,535.
const applyMarksUpsertChunk = 1000

// applyMarksTableDDL renders the mark table. The primary key is the mark's
// identity — one row per (stream, target table, key) — so every write is an
// UPSERT and the table holds one row per marked key, not one per change.
// Every identifier column is binary-collated (the control-table rule:
// `Foo` and `foo` are two tables, and tx_id is an equality predicate of the
// garbage-collection DELETE).
func applyMarksTableDDL(controlKeyspace string) string {
	return `CREATE TABLE IF NOT EXISTS ` + controlTableRef(controlKeyspace, applyMarksTableName) + ` (
	stream_id     VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	table_name    VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	key_digest    VARCHAR(64)  ` + controlIdentifierCollateClause + ` NOT NULL,
	tx_id         VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	seq           BIGINT UNSIGNED NOT NULL,
	change_digest VARCHAR(64)  NOT NULL,
	scope_digest  VARCHAR(255) NOT NULL,
	PRIMARY KEY (stream_id, table_name, key_digest)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

// ensureApplyMarksTable creates the mark table when it is absent —
// detect-then-create, so a table that is already there costs no DDL (a
// PlanetScale safe-migrations branch refuses every direct DDL statement).
func ensureApplyMarksTable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, applyMarksTableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure %s: %w", applyMarksTableName, err)
	}
	if exists {
		return nil
	}
	ddl := applyMarksTableDDL(controlKeyspace)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure %s: %w", applyMarksTableName, wrapControlTableBootstrapError(wrapDDLError(err), ddl))
	}
	return nil
}

// applyMarksUsable reports why the mark table cannot be used by this role, or
// nil. It checks that the table exists, then runs each statement shape the
// protocol needs against no row at all — MySQL privilege-checks a statement
// whether or not it matches a row — so nothing is written. A mark that could
// not be written would fail the apply transaction it rides in, which is the
// new refusal operator decision 2 rules out.
func applyMarksUsable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, applyMarksTableName)
	if err != nil {
		return fmt.Errorf("mysql: detect %s: %w", applyMarksTableName, err)
	}
	if !exists {
		return fmt.Errorf("%s does not exist", controlTableRef(controlKeyspace, applyMarksTableName))
	}
	ref := controlTableRef(controlKeyspace, applyMarksTableName)
	for _, probe := range []string{
		"SELECT 1 FROM " + ref + " WHERE 1 = 0",
		"INSERT INTO " + ref + " (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest) " +
			"SELECT '', '', '', '', 0, '', '' FROM DUAL WHERE 1 = 0",
		"UPDATE " + ref + " SET seq = seq WHERE 1 = 0",
		"DELETE FROM " + ref + " WHERE 1 = 0",
	} {
		if _, err := db.ExecContext(ctx, probe); err != nil {
			return fmt.Errorf("this role cannot use %s: %w", ref, err)
		}
	}
	return nil
}

// loadApplyMarks reads every mark of the stream.
func loadApplyMarks(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) ([]applymarks.Mark, error) {
	q := "SELECT table_name, key_digest, tx_id, seq, change_digest, scope_digest FROM " +
		controlTableRef(controlKeyspace, applyMarksTableName) + " WHERE stream_id = ?"
	rows, err := db.QueryContext(ctx, q, streamID)
	if err != nil {
		return nil, fmt.Errorf("mysql: load apply marks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var marks []applymarks.Mark
	for rows.Next() {
		var m applymarks.Mark
		if err := rows.Scan(&m.Table, &m.KeyDigest, &m.TxID, &m.Seq, &m.ChangeDigest, &m.ScopeDigest); err != nil {
			return nil, fmt.Errorf("mysql: load apply marks: %w", err)
		}
		marks = append(marks, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: load apply marks: %w", err)
	}
	return marks, nil
}

// applyMarkStatements renders a plan: chunked multi-row upserts of its marks,
// then one DELETE of the passed transactions' marks.
func applyMarkStatements(controlKeyspace, streamID string, upsert upsertSpelling, pl applymarks.Plan) (stmts []string, args [][]any) {
	ref := controlTableRef(controlKeyspace, applyMarksTableName)
	for start := 0; start < len(pl.Upserts); start += applyMarksUpsertChunk {
		end := min(start+applyMarksUpsertChunk, len(pl.Upserts))
		rows := make([]string, 0, end-start)
		a := make([]any, 0, (end-start)*7)
		for _, m := range pl.Upserts[start:end] {
			rows = append(rows, "(?, ?, ?, ?, ?, ?, ?)")
			a = append(a, streamID, m.Table, m.KeyDigest, m.TxID, m.Seq, m.ChangeDigest, m.ScopeDigest)
		}
		stmts = append(stmts, "INSERT INTO "+ref+" (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest) VALUES "+
			strings.Join(rows, ", ")+upsert.clauseOpen()+
			"tx_id = "+upsert.newRowRef("tx_id")+", seq = "+upsert.newRowRef("seq")+", "+
			"change_digest = "+upsert.newRowRef("change_digest")+", scope_digest = "+upsert.newRowRef("scope_digest"))
		args = append(args, a)
	}
	if len(pl.Deletes) > 0 {
		a := make([]any, 0, 1+len(pl.Deletes))
		a = append(a, streamID)
		for _, tx := range pl.Deletes {
			a = append(a, tx)
		}
		stmts = append(stmts, "DELETE FROM "+ref+" WHERE stream_id = ? AND tx_id IN ("+
			strings.TrimSuffix(strings.Repeat("?, ", len(pl.Deletes)), ", ")+")")
		args = append(args, a)
	}
	return stmts, args
}

// clearApplyMarks deletes every mark of the stream. Tolerant of a missing
// table (nothing to clear).
func clearApplyMarks(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) error {
	q := "DELETE FROM " + controlTableRef(controlKeyspace, applyMarksTableName) + " WHERE stream_id = ?"
	return appliershared.TolerantExec(ctx, db, controlCfg, "clear apply marks", q, streamID)
}

// startApplyMarks loads the stream's marks at the start of an apply run, or
// disables them — with the APPLY-MARKS-UNAVAILABLE WARN, and today's
// behaviour — when the table cannot be used. A load failure on a usable
// table is an ordinary (classified) apply error.
func (a *ChangeApplier) startApplyMarks(ctx context.Context, streamID string) error {
	unusable := applyMarksUsable(ctx, a.db, a.controlKeyspace)
	if unusable == nil {
		marks, err := loadApplyMarks(ctx, a.db, a.controlKeyspace, streamID)
		if err != nil {
			return classifyApplierError(err)
		}
		a.marks.Load(streamID, a.rowFilterHash, marks)
		return nil
	}
	a.marks.Disable()
	applymarks.WarnUnavailable(ctx, "mysql", streamID, errors.Join(a.applyMarksEnsureErr, unusable))
	return nil
}

// applyMarkSubject resolves the target-side facts the mark decision needs
// for a row change: the routed table, its primary key and whether it carries
// a unique index besides the primary key (the lane router's own probe, so a
// table is "marked" exactly when it is table-scoped).
func (a *ChangeApplier) applyMarkSubject(ctx context.Context, c ir.Change) (applymarks.Subject, error) {
	schema, table := laneapply.RowChangeSchemaTable(c)
	routed := a.routedSchema(schema)
	pk, err := a.pkForRedact(ctx, schema, table)
	if err != nil {
		return applymarks.Subject{}, err
	}
	return applymarks.Subject{
		Table:           qualifiedName(routed, table),
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
		return applymarks.Decision{}, classifyApplierError(fmt.Errorf("mysql: applier: apply-mark subject: %w", err))
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

// execApplyMarksTx runs a plan on the apply transaction.
func (a *ChangeApplier) execApplyMarksTx(ctx context.Context, tx *sql.Tx, pl applymarks.Plan) error {
	if pl.Empty() {
		return nil
	}
	stmts, args := applyMarkStatements(a.controlKeyspace, a.marks.StreamID(), a.upsert, pl)
	for i, s := range stmts {
		if _, err := a.txExec(ctx, tx, s, args[i]...); err != nil {
			return fmt.Errorf("mysql: applier: write apply marks: %w", err)
		}
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
		return errors.New("mysql: applier: ClearApplyMarks: streamID is empty")
	}
	if applyMarksUsable(ctx, a.db, a.controlKeyspace) == nil {
		return clearApplyMarks(ctx, a.db, a.controlKeyspace, streamID)
	}
	return nil
}

var _ ir.ApplyMarksClearer = (*ChangeApplier)(nil)
