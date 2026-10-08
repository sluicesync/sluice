// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # ADR-0190 apply marks on a MySQL-family target
//
// The engine-neutral decision logic lives in internal/applymarks; this file
// is the MySQL half: the sluice_cdc_apply_marks table, its availability
// check, and the SQL each apply path runs INSIDE its own target transaction,
// so on a target whose commit is atomic a mark and its rows land together or
// not at all — a mark write never rides a separate transaction.
//
// One target does not commit atomically: vtgate in transaction_mode=MULTI
// with the marks in a --control-keyspace sidecar commits the data shard and
// the control shard one after the other (GC-41 (c)). There the two torn
// states are NOT equal. A mark committed without its rows is the one the
// design cannot tolerate — the replay would SKIP rows that never landed,
// silently — so every write core sends its rows before its marks
// (mysqlBatchTx.writeApplyMarks; TestWriteCoreStatementOrder), and a tear
// leaves the other state, rows without their mark. That one is tolerated by
// choice: the replay re-applies them with nothing to skip them, which is
// today's behaviour without marks — a loud collision on a secondary-unique or
// PK-changing change, a duplicate on a keyless table. So on that target a
// marked class is at-least-once (or loud), not exactly-once.
//
// Which paths write (every apply path; see ADR-0190 "Implementation status"
// and amendment A):
//
//   - per-change (Apply / applyOneImpl) and the serial batch loop write marks
//     with their rows and delete a transaction's marks with the position
//     write that passes its commit;
//   - the lane barrier (applyBarrier) writes marks — in the transaction that
//     also writes its pre-barrier checkpoint, the position at the barrier's
//     own transaction's start (ADR-0190 amendment E; a MySQL Truncate
//     barrier, which marks nothing, still writes it separately first);
//   - lane batches write the marks of the transaction the coordinator's mark
//     fence cleared (amendment A: it drained the lanes and persisted the
//     position at that transaction's start first), and the frontier checkpoint
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
	"unicode/utf8"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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
// table, and a TRANSIENT failure of the availability probe itself
// (applyMarksProbeTransient), is an ordinary classified apply error the
// retry loop rides out.
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
	if transient := applyMarksProbeTransient(unusable); transient != nil {
		return transient
	}
	if err := applymarks.Unavailable(ctx, "mysql", streamID, a.requireMarks, errors.Join(a.applyMarksEnsureErr, unusable)); err != nil {
		return err
	}
	a.marks.Disable()
	return nil
}

// RequireApplyMarks implements [ir.ApplyMarksRequirer]: with on, an apply
// that finds the mark table unusable refuses instead of applying without
// marks (ADR-0191 review).
func (a *ChangeApplier) RequireApplyMarks(on bool) { a.requireMarks = on }

// applyMarksProbeTransient returns unusable classified when it is a transient
// failure of the availability probe (a lost connection, a timeout, a
// reparent — [applymarks.Transient] over classifyApplierError), and nil when
// it is a definite verdict: the table absent, a privilege or unsupported
// statement refused, or anything else the classifier calls terminal. The
// classifier's schema-drift arm (1146 no such table, 1054 unknown column) is
// definite HERE even though it is retriable for a user table: a mark table
// that vanished between the existence check and the probe, or lacks a column,
// cannot be used, and waiting for an operator to fix it would be the new
// refusal operator decision 2 rules out (GC-41 (f)).
func applyMarksProbeTransient(unusable error) error {
	var me *gomysql.MySQLError
	if errors.As(unusable, &me) && (me.Number == 1146 || me.Number == 1054) {
		return nil
	}
	if c := classifyApplierError(unusable); applymarks.Transient(c) {
		return c
	}
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

// markFieldsFit is the defensive half of the v0.157.0 review's HIGH: every
// identifier column of the MySQL mark table is VARCHAR(255), and a value
// longer than that fails the apply with 1406 under the strict sql_mode sluice
// sets — or, under a relaxed one, is TRUNCATED, so the reloaded mark stops
// matching the change it vouches for and that change re-applies (a keyless
// duplicate). [applymarks.MarkTxKey] makes tx_id fit by construction; this
// refuses, before any statement, a plan whose stream id, table, tx_id or
// scope would still not fit, so no sql_mode can turn it into a silent
// truncation. Counted in characters, as the column is.
func markFieldsFit(streamID string, pl applymarks.Plan) error {
	check := func(column, v string) error {
		if n := utf8.RuneCountInString(v); n > applymarks.MarkTxIDMaxLen {
			return fmt.Errorf("mysql: applier: an apply mark's %s is %d characters, longer than the mark table's VARCHAR(%d); "+
				"refusing to write it (a truncated mark would stop matching its change): %.80q…",
				column, n, applymarks.MarkTxIDMaxLen, v)
		}
		return nil
	}
	if err := check("stream_id", streamID); err != nil {
		return err
	}
	for _, m := range pl.Upserts {
		for _, f := range [][2]string{{"table_name", m.Table}, {"tx_id", m.TxID}, {"scope_digest", m.ScopeDigest}} {
			if err := check(f[0], f[1]); err != nil {
				return err
			}
		}
	}
	for _, tx := range pl.Deletes {
		if err := check("tx_id", tx); err != nil {
			return err
		}
	}
	return nil
}

// execApplyMarksTx runs a plan on the apply transaction.
func (a *ChangeApplier) execApplyMarksTx(ctx context.Context, tx *sql.Tx, pl applymarks.Plan) error {
	if pl.Empty() {
		return nil
	}
	if err := markFieldsFit(a.marks.StreamID(), pl); err != nil {
		return err
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

// MarksCoverReason implements [ir.ApplyMarksCoverageProber] (ADR-0191 §3.5)
// for a MySQL-family target: "" when this applier's marks would key every
// change of table and commit with its rows, otherwise why not.
//
// A `--control-keyspace` sidecar is refused as a whole: under vtgate's
// transaction_mode=MULTI the marks and the rows commit on different shards,
// and the write cores order the rows first so that a tear leaves rows WITHOUT
// their marks (GC-41 (c)) — the tolerable direction for a loud collision, and
// exactly the duplicate a keyless replay must not risk. Today's only caller,
// `sync from-backup`, has no --control-keyspace flag, so this arm is not
// reachable from it (ADR-0191 review); it is kept so the answer stays true
// for an applier that does carry a sidecar, rather than silently "covered".
func (a *ChangeApplier) MarksCoverReason(ctx context.Context, table *ir.Table) (string, error) {
	if a.controlKeyspace != "" {
		return fmt.Sprintf("the target keeps its control tables in the `--control-keyspace` sidecar %q, where vtgate's MULTI "+
			"commit can land rows without their apply marks (GC-41 (c))", a.controlKeyspace), nil
	}
	if unusable := applyMarksUsable(ctx, a.db, a.controlKeyspace); unusable != nil {
		if transient := applyMarksProbeTransient(unusable); transient != nil {
			return "", transient
		}
		return applymarks.UnavailableMarker + ": " + unusable.Error(), nil
	}
	if table == nil {
		return "", nil
	}
	pk, err := a.targetKeyForCoverage(ctx, table)
	if err != nil {
		return "", err
	}
	return irbackup.MarkKeyUnsupplied(pk, table), nil
}

// targetKeyForCoverage is the primary key the applier's mark subject would
// use for table, read without touching the applier's PK cache (a table the
// target does not hold yet must not cache "keyless"), and the recorded key for
// a table the target does not hold (it is created with that key).
func (a *ChangeApplier) targetKeyForCoverage(ctx context.Context, table *ir.Table) ([]string, error) {
	schema := a.routedSchema(table.Schema)
	const existsQ = `SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ?`
	var n int
	if err := a.db.QueryRowContext(ctx, existsQ, schema, table.Name).Scan(&n); err != nil {
		return nil, fmt.Errorf("mysql: apply-marks coverage: probe %s: %w", table.Name, err)
	}
	if n == 0 {
		return irbackup.RecordedKeyColumns(table), nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql: apply-marks coverage: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	pk, err := loadPrimaryKey(ctx, tx, schema, table.Name)
	if err != nil {
		return nil, fmt.Errorf("mysql: apply-marks coverage: %w", err)
	}
	return pk, nil
}

var (
	_ ir.ApplyMarksCoverageProber = (*ChangeApplier)(nil)
	_ ir.ApplyMarksRequirer       = (*ChangeApplier)(nil)
)
