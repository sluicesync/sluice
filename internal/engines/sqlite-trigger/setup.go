// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// Standard names for the engine's source-side artifacts (ADR-0135 §2). The
// change-log + meta table names are the canonical spellings owned by the sqlite
// package (so the cold-start schema reader's skip set and this installer can
// never drift); the trigger names live here. Exported where the CLI dry-run
// output and tests refer to them.
const (
	// ChangeLogTable, ChangeLogMetaTable, and ChangeLogColumnsTable mirror
	// pgtrigger's names for operator familiarity; the literals live in the sqlite
	// package because its schema reader must skip them (ADR-0135).
	ChangeLogTable        = sqlite.ChangeLogTable
	ChangeLogMetaTable    = sqlite.ChangeLogMetaTable
	ChangeLogColumnsTable = sqlite.ChangeLogColumnsTable

	// CaptureTriggerPrefix prefixes every per-table capture trigger name; the
	// teardown discovery scans for it. SQLite triggers are per-operation (no
	// "AFTER INSERT OR UPDATE OR DELETE" form), so each table gets three.
	CaptureTriggerPrefix = "sluice_capture_"

	// ChangeLogSchemaVer is the schema-version pin recorded in the meta table.
	ChangeLogSchemaVer = 1
)

// triggerOp is one of the three captured DML operations, carrying the SQLite
// trigger event keyword, the change-log op code, and the trigger-name suffix.
type triggerOp struct {
	event  string // INSERT / UPDATE / DELETE
	opCode string // I / U / D (the change-log `op` column)
	suffix string // ins / upd / del (per-table trigger-name suffix)
}

// triggerOps is the fixed set of captured operations, in install order.
var triggerOps = []triggerOp{
	{event: "INSERT", opCode: "I", suffix: "ins"},
	{event: "UPDATE", opCode: "U", suffix: "upd"},
	{event: "DELETE", opCode: "D", suffix: "del"},
}

// SetupOptions controls [Setup]. Zero values are the safe defaults; the CLI
// threads operator flags through. The SQLite engine's surface is deliberately
// simpler than pgtrigger's: a flat namespace (no Schema), no event-trigger
// permission probe (SQLite has no DDL triggers — schema-change forwarding is a
// deferred follow-up), and a single full-payload capture mode in Phase 1.
type SetupOptions struct {
	// Tables names the per-table triggers to install. Required for Phase 1
	// (empty-list discovery is a follow-up). Each must exist and carry a PRIMARY
	// KEY (refused loudly otherwise — the applier identifies rows by PK).
	Tables []string

	// DryRun returns the DDL without applying it (surfaced via [Plan]).
	DryRun bool
}

// TeardownOptions controls [Teardown].
type TeardownOptions struct {
	// Tables names the per-table triggers to drop. Empty discovers every
	// sluice-installed capture trigger in the file.
	Tables []string

	// KeepData retains `sluice_change_log` (and the meta table) for forensics.
	// Default drops them — the engine's promise is to remove every trace.
	KeepData bool

	// DryRun returns the DDL without applying it.
	DryRun bool
}

// Plan is the result of a dry-run [Setup] / [Teardown]: the ordered DDL the
// operator would apply, plus any per-table refusals (Setup only).
type Plan struct {
	Statements []string
	Refusals   []TableRefusal
}

// TableRefusal is one operator-actionable refusal from the Setup preflight.
type TableRefusal struct {
	Table  string
	Reason string
	Hint   string
}

// Error renders a one-line operator-facing string.
func (r TableRefusal) Error() string {
	return fmt.Sprintf("sqlite-trigger: refuse-loudly %s on %s — %s", r.Reason, r.Table, r.Hint)
}

// Setup installs the engine's source-side state: the change-log table, the meta
// (schema-version) table, and one per-table AFTER INSERT/UPDATE/DELETE trigger
// trio whose body captures the faithful `(typeof, text/hex)` before/after image
// (§crux). Idempotent: re-running drops and recreates each trigger (so a column
// add/remove re-renders the captured image) and refreshes the meta row.
//
// Setup runs a refuse-loudly preflight BEFORE touching any source-side state: a
// requested table that is missing or lacks a PRIMARY KEY blocks the whole run
// (the applier identifies CDC rows by PK). When opts.DryRun is true, no DDL is
// applied; the returned Plan carries the statements that would have run.
func Setup(ctx context.Context, dsn string, opts SetupOptions) (*Plan, error) {
	return setup(ctx, localBackend(dsn), opts)
}

// setup is the transport-neutral installer used by both the local file engine
// ([Setup]) and the D1 engine ([SetupD1], ADR-0136). The backend supplies the
// cold-start schema reader (table-shape resolution) and the executor (DDL).
func setup(ctx context.Context, b backend, opts SetupOptions) (*Plan, error) {
	if len(opts.Tables) == 0 {
		return nil, errors.New("sqlite-trigger: setup: no tables specified; pass --tables=t1,t2,…")
	}

	// Never install capture triggers on the engine's OWN internal tables: a
	// trigger on sluice_change_log would re-fire on every captured insert. The
	// schema reader already skips them so they can't be discovered, but a caller
	// passing them explicitly is filtered here too (defense in depth).
	opts.Tables = filterInternal(opts.Tables)
	if len(opts.Tables) == 0 {
		return nil, errors.New("sqlite-trigger: setup: no user tables remain after excluding the engine's own internal tables")
	}

	// Resolve the requested tables' shapes from the validated cold-start schema
	// reader (so column lists, PKs, and generated-column flags match exactly what
	// the cold-start reader sees — ADR-0135 reuse).
	tables, err := resolveTables(ctx, b.coldStart, b.dsn, opts.Tables)
	if err != nil {
		return nil, err
	}

	refusals := preflight(tables)
	plan := &Plan{
		Refusals:   refusals,
		Statements: renderSetupDDL(tables),
	}
	if len(refusals) > 0 {
		// Refusals block the run even on dry-run — the operator sees them first.
		return plan, fmt.Errorf("sqlite-trigger: setup: %d table(s) refused (see plan.Refusals)", len(refusals))
	}
	if opts.DryRun {
		return plan, nil
	}

	exec, err := b.openExec(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: setup: open: %w", err)
	}
	defer func() { _ = exec.close() }()
	if err := execAll(ctx, exec, plan.Statements); err != nil {
		return plan, fmt.Errorf("sqlite-trigger: setup: %w", err)
	}
	return plan, nil
}

// Teardown removes the engine's source-side state. Idempotent — every DROP uses
// IF EXISTS so re-running on a partially-uninstalled source proceeds cleanly.
func Teardown(ctx context.Context, dsn string, opts TeardownOptions) (*Plan, error) {
	return teardown(ctx, localBackend(dsn), opts)
}

// teardown is the transport-neutral remover used by both the local file engine
// ([Teardown]) and the D1 engine ([TeardownD1], ADR-0136). The backend supplies
// the executor (trigger discovery + DROP DDL).
func teardown(ctx context.Context, b backend, opts TeardownOptions) (*Plan, error) {
	exec, err := b.openExec(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: teardown: open: %w", err)
	}
	defer func() { _ = exec.close() }()

	var triggers []string
	if len(opts.Tables) > 0 {
		triggers = triggersForTables(opts.Tables)
	} else {
		triggers, err = exec.discoverTriggers(ctx)
		if err != nil {
			return nil, fmt.Errorf("sqlite-trigger: teardown: discover triggers: %w", err)
		}
	}

	plan := &Plan{Statements: renderTeardownDDL(triggers, opts.KeepData)}
	if opts.DryRun {
		return plan, nil
	}
	if err := execAll(ctx, exec, plan.Statements); err != nil {
		return plan, fmt.Errorf("sqlite-trigger: teardown: %w", err)
	}
	return plan, nil
}

// resolveTables reads the source schema and returns the requested tables in
// request order, erroring if any requested table is absent. Reuses the validated
// cold-start schema reader (the `sqlite` file reader or the `d1` HTTP reader) so
// the captured column set matches the cold-start reader's exactly (incl.
// generated-column detection).
func resolveTables(ctx context.Context, coldStart ir.Engine, dsn string, requested []string) ([]*ir.Table, error) {
	sr, err := coldStart.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: setup: open schema reader: %w", err)
	}
	defer func() { _ = closeReader(sr) }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: setup: read schema: %w", err)
	}
	byName := make(map[string]*ir.Table, len(schema.Tables))
	for _, t := range schema.Tables {
		byName[t.Name] = t
	}
	out := make([]*ir.Table, 0, len(requested))
	for _, name := range requested {
		t, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf(
				"sqlite-trigger: setup: table %q not found in the SQLite source (check the name; the change-log/meta tables are not eligible)", name,
			)
		}
		out = append(out, t)
	}
	return out, nil
}

// preflight runs the per-table refuse-loudly checks: a table must have a PRIMARY
// KEY (the CDC applier identifies rows by PK; a PK-less table's UPDATE/DELETE
// could not be applied faithfully). Returns nil on a clean preflight.
func preflight(tables []*ir.Table) []TableRefusal {
	var refusals []TableRefusal
	for _, t := range tables {
		if t.PrimaryKey == nil || len(t.PrimaryKey.Columns) == 0 {
			refusals = append(refusals, TableRefusal{
				Table:  t.Name,
				Reason: "no-primary-key",
				Hint:   "add a PRIMARY KEY to " + t.Name + " before including it in the trigger engine's replication set (the CDC applier identifies rows by PK)",
			})
		}
	}
	return refusals
}

// renderSetupDDL produces the ordered DDL that installs the engine. Order
// matters: the change-log table must exist before the triggers reference it.
func renderSetupDDL(tables []*ir.Table) []string {
	// 4 base statements + 7 per table (DROP+CREATE×3 triggers + 1 fingerprint upsert).
	out := make([]string, 0, 4+len(tables)*(2*len(triggerOps)+1))
	out = append(
		out,
		// id is INTEGER PRIMARY KEY AUTOINCREMENT — the monotonic, never-reused
		// watermark. SQLite serialises writers, so the id is allocated in commit
		// order and the reader needs no safety-lag predicate (cdc_reader.go).
		"CREATE TABLE IF NOT EXISTS "+quoteIdent(ChangeLogTable)+` (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    op           TEXT NOT NULL,
    tbl          TEXT NOT NULL,
    before       TEXT,
    after        TEXT,
    captured_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
)`,
		"CREATE TABLE IF NOT EXISTS "+quoteIdent(ChangeLogMetaTable)+` (
    singleton_pk   INTEGER PRIMARY KEY CHECK (singleton_pk = 1),
    schema_version INTEGER NOT NULL,
    installed_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
)`,
		// Per-table captured-column fingerprint: the EXACT ordered non-generated
		// column set the triggers below were built against. The CDC reader compares
		// it to the live schema at stream start so an un-re-setup ADD/DROP/RENAME
		// COLUMN is REFUSED LOUDLY rather than silently dropping the drifted
		// column's values (ADR-0135; SQLite has no DDL triggers in Phase 1).
		"CREATE TABLE IF NOT EXISTS "+quoteIdent(ChangeLogColumnsTable)+` (
    tbl     TEXT PRIMARY KEY,
    columns TEXT NOT NULL
)`,
		fmt.Sprintf(
			"INSERT INTO %s (singleton_pk, schema_version) VALUES (1, %d) "+
				"ON CONFLICT (singleton_pk) DO UPDATE SET schema_version = excluded.schema_version",
			quoteIdent(ChangeLogMetaTable), ChangeLogSchemaVer,
		),
	)
	for _, t := range tables {
		out = append(out, renderTableTriggers(t)...)
	}
	return out
}

// renderTableTriggers renders the DROP+CREATE for the three per-table capture
// triggers (INSERT/UPDATE/DELETE). The DROP makes Setup idempotent and re-renders
// the captured image if the column set changed since the last setup.
func renderTableTriggers(t *ir.Table) []string {
	cols := nonGeneratedColumnNames(t)
	afterImg := captureImageExpr("NEW", cols)
	beforeImg := captureImageExpr("OLD", cols)
	fqTable := quoteIdent(t.Name)

	out := make([]string, 0, 2*len(triggerOps)+1)
	// Record the EXACT captured column set this trigger set is built against, so
	// the CDC reader can detect an un-re-setup schema change at stream start
	// (ADR-0135). Upsert so re-running setup for one table doesn't touch others'.
	out = append(out, fmt.Sprintf(
		"INSERT INTO %s (tbl, columns) VALUES (%s, %s) "+
			"ON CONFLICT (tbl) DO UPDATE SET columns = excluded.columns",
		quoteIdent(ChangeLogColumnsTable), quoteSQLString(t.Name), quoteSQLString(columnFingerprint(cols)),
	))
	for _, op := range triggerOps {
		name := quoteIdent(triggerName(t.Name, op.suffix))
		before, after := "NULL", "NULL"
		switch op.opCode {
		case "I":
			after = afterImg
		case "U":
			before, after = beforeImg, afterImg
		case "D":
			before = beforeImg
		}
		out = append(
			out,
			"DROP TRIGGER IF EXISTS "+name,
			fmt.Sprintf(
				"CREATE TRIGGER %s AFTER %s ON %s FOR EACH ROW\nBEGIN\n"+
					"  INSERT INTO %s (op, tbl, before, after)\n  VALUES (%s, %s, %s, %s);\nEND",
				name, op.event, fqTable,
				quoteIdent(ChangeLogTable),
				quoteSQLString(op.opCode), quoteSQLString(t.Name), before, after,
			),
		)
	}
	return out
}

// captureImageExpr builds the per-row before/after image as a json_object of
// per-column {t, v} entries, each VALUE captured via the SHARED faithful
// (typeof, text/hex) encoding ([sqlite.CapturedValueExpr] — the §crux load-bearing
// reuse) so big integers (> 2^53) and blobs round-trip EXACT, never through a
// lossy JSON number. rowVar is "NEW" or "OLD". An empty column list (a table with
// only generated columns — impossible since a PK column is stored) yields an
// empty json_object.
func captureImageExpr(rowVar string, cols []string) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		ref := rowVar + "." + quoteIdent(c)
		parts = append(parts,
			quoteSQLString(c)+", json_object('t', "+sqlite.CapturedTypeofExpr(ref)+", 'v', "+sqlite.CapturedValueExpr(ref)+")")
	}
	return "json_object(" + strings.Join(parts, ", ") + ")"
}

// nonGeneratedColumnNames returns the names of the columns the capture image
// records: every NON-generated column (a generated column is re-derived by the
// target's GENERATED clause, exactly as the cold-start reader omits it — keeping
// the captured shape in lockstep with the snapshot).
func nonGeneratedColumnNames(t *ir.Table) []string {
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c.IsGenerated() {
			continue
		}
		out = append(out, c.Name)
	}
	return out
}

// columnFingerprint canonicalises an ordered non-generated column-name list into
// the string stored in [ChangeLogColumnsTable] and compared by the CDC reader at
// stream start. A JSON array preserves order and is unambiguous under any column
// name (commas, quotes); Setup and the reader call this SAME function so the
// stored and live fingerprints are byte-comparable. Marshal of a []string never
// errors, but the error is checked rather than ignored (errcheck).
func columnFingerprint(cols []string) string {
	b, err := json.Marshal(cols)
	if err != nil {
		// Unreachable for []string; fall back to a join so a bug surfaces as a
		// mismatch refusal, never a panic.
		return strings.Join(cols, "\x00")
	}
	return string(b)
}

// renderTeardownDDL returns the ordered DROP statements. Triggers drop before
// the change-log table; KeepData retains the change-log + meta + columns tables
// for inspection (all three kept together so the "change-log exists ⟹ columns
// exists" invariant the reader's drift check relies on always holds).
func renderTeardownDDL(triggers []string, keepData bool) []string {
	out := make([]string, 0, len(triggers)+3)
	for _, name := range triggers {
		out = append(out, "DROP TRIGGER IF EXISTS "+quoteIdent(name))
	}
	if !keepData {
		out = append(
			out,
			"DROP TABLE IF EXISTS "+quoteIdent(ChangeLogTable),
			"DROP TABLE IF EXISTS "+quoteIdent(ChangeLogMetaTable),
			"DROP TABLE IF EXISTS "+quoteIdent(ChangeLogColumnsTable),
		)
	}
	return out
}

// triggersForTables returns the three trigger names for each table (used when the
// operator scopes teardown with --tables).
func triggersForTables(tables []string) []string {
	out := make([]string, 0, len(tables)*len(triggerOps))
	for _, t := range tables {
		for _, op := range triggerOps {
			out = append(out, triggerName(t, op.suffix))
		}
	}
	sort.Strings(out)
	return out
}

// triggerName builds the deterministic per-table, per-op trigger name.
func triggerName(table, suffix string) string {
	return CaptureTriggerPrefix + table + "_" + suffix
}

// filterInternal drops the engine's own change-log/meta names from a table list,
// order-preserving.
func filterInternal(tables []string) []string {
	out := tables[:0:0]
	for _, t := range tables {
		if t == ChangeLogTable || t == ChangeLogMetaTable {
			continue
		}
		out = append(out, t)
	}
	return out
}

// execAll runs each statement in order over the executor, wrapping a failure
// with its first line.
func execAll(ctx context.Context, exec executor, stmts []string) error {
	for _, stmt := range stmts {
		if err := exec.execDDL(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(stmt), err)
		}
	}
	return nil
}

// firstLine returns s up to the first newline (keeps exec-failure messages short).
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// quoteIdent double-quotes a SQLite identifier, escaping embedded double quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteSQLString single-quotes a SQL string literal, doubling embedded quotes.
func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
