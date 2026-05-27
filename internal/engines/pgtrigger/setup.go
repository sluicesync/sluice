// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	// stdlib registers pgx as a database/sql driver under "pgx".
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Standard names for the engine's source-side artifacts. ADR-0066 §2,
// §3, §7. Exported so the CLI's dry-run output and integration tests
// can refer to them.
const (
	ChangeLogTable     = "sluice_change_log"
	ChangeLogMetaTable = "sluice_change_log_meta"
	CaptureFunctionRow = "sluice_capture_change"
	CaptureFunctionDDL = "sluice_capture_ddl"
	CapturePrefixRow   = "sluice_capture_" // for per-table CREATE TRIGGER names
	CaptureTriggerRow  = "sluice_capture"  // per-table row-trigger name
	CaptureTriggerDDL  = "sluice_capture_ddl_trg"
	ChangeLogSchemaVer = 1 // schema-version pin recorded in the meta table
)

// SetupOptions controls the behaviour of [Setup]. Zero values are the
// safe defaults; the CLI threads operator flags through.
type SetupOptions struct {
	// Tables names the per-table triggers to install. Empty means
	// "every PRIMARY-KEY-bearing user table in the active schema"
	// (discovered via the schema reader). Phase 1 only supports the
	// explicit form; the empty-list discovery shape is reserved for a
	// follow-up phase that needs to integrate with the existing
	// include/exclude filter.
	Tables []string

	// Schema is the source-side PG schema (namespace) the change-log
	// + capture function + per-table triggers live in. Defaults to
	// "public" via parseDSN's fallback.
	Schema string

	// DryRun returns the DDL without applying it. The DDL string is
	// surfaced via [Plan] for inspection.
	DryRun bool

	// AllowPolledFingerprint opts in to the polled schema-fingerprint
	// fallback (§7) when the source denies event-trigger creation
	// (Heroku Essential is the known case). When false (default),
	// Setup refuses-loudly on tiers that grant neither superuser nor
	// `pg_create_event_trigger`. Phase 1 only records the operator's
	// intent — the polled-fingerprint loop itself is a follow-up.
	AllowPolledFingerprint bool
}

// Plan is the result of a dry-run [Setup]. Holds the DDL statements
// the operator would apply, the per-table refusal list (if any), and
// a flag indicating whether the source denies event-trigger creation.
type Plan struct {
	// Statements is the ordered DDL the engine would apply, joined
	// with blank lines for operator readability.
	Statements []string

	// Refusals lists per-table preflight refusals (§14). When
	// non-empty, Setup refuses regardless of DryRun — the operator
	// must address each before re-running.
	Refusals []TableRefusal

	// EventTriggerSupported reports whether the connecting role can
	// create event triggers (PG 14+ via the pg_create_event_trigger
	// role, OR superuser on pre-14). False signals the §7 fallback
	// path; the polled-fingerprint loop is enabled by
	// SetupOptions.AllowPolledFingerprint.
	EventTriggerSupported bool

	// PGVersionNum is the server's PG_VERSION_NUM (e.g. 160001 for
	// 16.1). Captured for the §14 PG < 9.4 refusal.
	PGVersionNum int
}

// TableRefusal is one operator-actionable refusal from the §14
// preflight. The Hint string is intentionally verbose — operators
// reading it on a CLI run should not need to consult the ADR to know
// what to do next.
type TableRefusal struct {
	Schema string
	Table  string
	Reason string
	Hint   string
}

// Error renders a one-line operator-facing string. The Table is left
// unqualified when Schema is empty so the message reads naturally on
// flat-namespace error wrappers.
func (r TableRefusal) Error() string {
	name := r.Table
	if r.Schema != "" {
		name = r.Schema + "." + r.Table
	}
	return fmt.Sprintf("pgtrigger: refuse-loudly %s on %s: %s — %s",
		r.Reason, name, r.Reason, r.Hint)
}

// Setup installs the engine's source-side state: the change-log
// table, the meta table, the shared capture function, the DDL
// capture function + event trigger (when permitted), and one
// per-table row trigger for every table in opts.Tables.
//
// Idempotent: re-running Setup against an already-set-up source
// applies the DDL with IF NOT EXISTS / CREATE OR REPLACE semantics
// and refreshes the meta table's schema-version row.
//
// When opts.DryRun is true, no DDL is applied; the returned Plan
// carries the statements that would have been applied so the
// operator can inspect them.
//
// Setup runs the §14 refuse-loudly preflight BEFORE touching any
// source-side state. A non-empty Plan.Refusals means the engine
// did not run any DDL — the operator must address each refusal and
// re-run.
func Setup(ctx context.Context, dsn string, opts SetupOptions) (*Plan, error) {
	if len(opts.Tables) == 0 {
		return nil, errors.New("pgtrigger: setup: no tables specified; pass --tables=t1,t2,…")
	}

	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}
	if opts.Schema == "" {
		opts.Schema = cfg.schema
	}

	db, err := sql.Open("pgx", cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: ping: %w", err)
	}

	// Refuse loudly on PG < 9.4 (JSONB unavailable, §14).
	pgver, err := readPGVersionNum(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: read PG version: %w", err)
	}
	if pgver < 90400 {
		return nil, fmt.Errorf(
			"pgtrigger: setup: source PG version_num = %d (< 9.4); the trigger engine requires JSONB — %s",
			pgver,
			"upgrade the source server to PG 9.4 or later",
		)
	}

	// §14 per-table preflight: no-PK, UNLOGGED, generated columns,
	// custom domain-over-UDT.
	refusals, err := preflightTables(ctx, db, opts.Schema, opts.Tables)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: preflight: %w", err)
	}

	// Event-trigger permissions probe. Doesn't grant; only checks.
	canEventTrigger, err := canCreateEventTrigger(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: probe event-trigger permission: %w", err)
	}
	if !canEventTrigger && !opts.AllowPolledFingerprint {
		// §14 last bullet — refuse with the flag suggestion.
		return nil, fmt.Errorf(
			"pgtrigger: setup: connecting role lacks pg_create_event_trigger membership and is not a superuser; %s",
			"the trigger engine requires event-trigger creation to detect source-side DDL — re-run with --allow-polled-fingerprint to opt in to the polled-fingerprint fallback (§7) or grant the role pg_create_event_trigger",
		)
	}

	plan := &Plan{
		Refusals:              refusals,
		EventTriggerSupported: canEventTrigger,
		PGVersionNum:          pgver,
	}
	plan.Statements = renderSetupDDL(opts.Schema, opts.Tables, canEventTrigger)

	if len(refusals) > 0 {
		// Refusals block the run even on dry-run — the operator
		// should see the refusals first, not the DDL.
		return plan, fmt.Errorf("pgtrigger: setup: %d table(s) refused (see plan.Refusals)", len(refusals))
	}
	if opts.DryRun {
		return plan, nil
	}

	for _, stmt := range plan.Statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return plan, fmt.Errorf("pgtrigger: setup: exec %q: %w", firstLine(stmt), err)
		}
	}
	return plan, nil
}

// TeardownOptions controls [Teardown].
type TeardownOptions struct {
	// Tables names the per-table triggers to drop. Empty means
	// "every per-table trigger sluice manages in the active schema"
	// (discovered via information_schema.triggers).
	Tables []string

	// Schema is the source-side PG schema. Defaults to the DSN's
	// `schema` query parameter (typically "public").
	Schema string

	// KeepData retains `sluice_change_log` (and the meta table) for
	// forensics. Default behaviour drops them — the engine's whole
	// point is being able to remove every trace from the source. The
	// per-table triggers + capture function + event trigger are
	// always dropped.
	KeepData bool

	// DryRun returns the DDL without applying it.
	DryRun bool
}

// Teardown removes the engine's source-side state. Idempotent —
// every DROP uses IF EXISTS so re-running on a partially-uninstalled
// source proceeds cleanly.
func Teardown(ctx context.Context, dsn string, opts TeardownOptions) (*Plan, error) {
	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}
	if opts.Schema == "" {
		opts.Schema = cfg.schema
	}

	db, err := sql.Open("pgx", cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: teardown: open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pgtrigger: teardown: ping: %w", err)
	}

	tables := opts.Tables
	if len(tables) == 0 {
		discovered, err := discoverTriggeredTables(ctx, db, opts.Schema)
		if err != nil {
			return nil, fmt.Errorf("pgtrigger: teardown: discover tables: %w", err)
		}
		tables = discovered
	}
	sort.Strings(tables)

	plan := &Plan{
		Statements: renderTeardownDDL(opts.Schema, tables, opts.KeepData),
	}
	if opts.DryRun {
		return plan, nil
	}

	for _, stmt := range plan.Statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return plan, fmt.Errorf("pgtrigger: teardown: exec %q: %w", firstLine(stmt), err)
		}
	}
	return plan, nil
}

// renderSetupDDL produces the ordered DDL statements that install the
// engine. Order matters: the change-log table must exist before the
// capture function references it; the function must exist before the
// per-table triggers reference it.
func renderSetupDDL(schema string, tables []string, canEventTrigger bool) []string {
	tableRef := func(name string) string {
		return quoteIdent(schema) + "." + quoteIdent(name)
	}
	out := []string{
		"CREATE TABLE IF NOT EXISTS " + tableRef(ChangeLogTable) + ` (
    id            BIGSERIAL PRIMARY KEY,
    txid          BIGINT NOT NULL,
    committed_at  TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    schema_name   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    op            CHAR(1) NOT NULL,
    pk_jsonb      JSONB NOT NULL,
    before_jsonb  JSONB,
    after_jsonb   JSONB
)`,
		"CREATE INDEX IF NOT EXISTS sluice_change_log_id_idx ON " + tableRef(ChangeLogTable) + " (id)",
		"CREATE INDEX IF NOT EXISTS sluice_change_log_table_idx ON " + tableRef(ChangeLogTable) + " (schema_name, table_name, id)",

		"CREATE TABLE IF NOT EXISTS " + tableRef(ChangeLogMetaTable) + ` (
    singleton_pk   BOOLEAN PRIMARY KEY DEFAULT TRUE,
    schema_version INT NOT NULL,
    installed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sluice_change_log_meta_singleton CHECK (singleton_pk = TRUE)
)`,
		fmt.Sprintf(
			"INSERT INTO %s (singleton_pk, schema_version) VALUES (TRUE, %d) ON CONFLICT (singleton_pk) DO UPDATE SET schema_version = EXCLUDED.schema_version",
			tableRef(ChangeLogMetaTable), ChangeLogSchemaVer,
		),

		// Row-event capture function. TG_RELID drives a catalog
		// lookup at fire time to discover the source table's PK
		// column list; jsonb_object_agg projects pk_jsonb out of
		// OLD/NEW.
		renderCaptureRowFunction(schema, tableRef(ChangeLogTable)),

		// TRUNCATE companion — separate function because TRUNCATE
		// triggers are FOR EACH STATEMENT, not FOR EACH ROW.
		renderCaptureTruncateFunction(schema, tableRef(ChangeLogTable)),
	}

	for _, t := range tables {
		// Drop any pre-existing trigger with the canonical name so
		// re-running Setup with a different PK list refreshes the
		// TG_ARGV payload. PG does not have CREATE OR REPLACE TRIGGER
		// on row triggers (it does on PG 14+, but we target 9.4+); a
		// DROP IF EXISTS + CREATE is the portable shape.
		fqTable := quoteIdent(schema) + "." + quoteIdent(t)
		out = append(
			out,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent(CaptureTriggerRow), fqTable),
			fmt.Sprintf(
				"CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %s(%s)",
				quoteIdent(CaptureTriggerRow),
				fqTable,
				tableRef(CaptureFunctionRow),
				quoteSQLString(t),
			),
			// TRUNCATE trigger.
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent("sluice_capture_truncate"), fqTable),
			fmt.Sprintf(
				"CREATE TRIGGER %s AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION %s()",
				quoteIdent("sluice_capture_truncate"),
				fqTable,
				tableRef("sluice_capture_truncate_fn"),
			),
		)
	}

	if canEventTrigger {
		out = append(
			out,
			renderCaptureDDLFunction(schema, tableRef(ChangeLogTable)),
			"DROP EVENT TRIGGER IF EXISTS "+quoteIdent(CaptureTriggerDDL),
			fmt.Sprintf(
				"CREATE EVENT TRIGGER %s ON ddl_command_end WHEN TAG IN ('ALTER TABLE','CREATE TABLE','DROP TABLE','CREATE INDEX','DROP INDEX') EXECUTE FUNCTION %s()",
				quoteIdent(CaptureTriggerDDL),
				quoteIdent(schema)+"."+quoteIdent(CaptureFunctionDDL),
			),
		)
	}

	return out
}

// rowFunctionRef / truncateFunctionRef / ddlFunctionRef are
// schema-qualified function references used by render helpers and the
// teardown DROP path.
func rowFunctionRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(CaptureFunctionRow)
}

func truncateFnRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent("sluice_capture_truncate_fn")
}

func ddlFnRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(CaptureFunctionDDL)
}

// renderCaptureRowFunction returns the CREATE OR REPLACE FUNCTION
// statement for the shared row-event capture function. ADR-0066 §3.
//
// TG_ARGV[0] carries the source-table-qualified name; the function
// reads pg_attribute through information_schema to derive the PK
// column list at trigger fire time. Storing the PK list in TG_ARGV
// at CREATE TRIGGER time is the §3-described shape, but for v1 the
// simpler form is to look the PK up via to_jsonb-mediated catalog
// access. The catalog hit per row is fine for Phase 1's design
// ceiling (§11) — the §11 5000/sec ceiling is on the change-log
// write path, not the function's complexity.
func renderCaptureRowFunction(schema, changeLogTableRef string) string {
	// Hand-written SQL — the source string is operator-readable and
	// avoids the per-engine identifier-quoting tangle of building
	// the function body programmatically. SECURITY DEFINER lets a
	// non-table-owning role drive the engine as long as the
	// function-owning role has INSERT on sluice_change_log.
	return `CREATE OR REPLACE FUNCTION ` + rowFunctionRef(schema) + `()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $sluice$
DECLARE
    v_pk_cols  TEXT[];
    v_pk       JSONB;
    v_before   JSONB;
    v_after    JSONB;
    v_op       CHAR(1);
BEGIN
    -- Discover the source table's PK column list at fire time.
    SELECT array_agg(att.attname::text ORDER BY array_position(con.conkey, att.attnum))
      INTO v_pk_cols
      FROM pg_constraint con
      JOIN pg_attribute  att
        ON att.attrelid = con.conrelid
       AND att.attnum   = ANY(con.conkey)
     WHERE con.conrelid = TG_RELID
       AND con.contype  = 'p';

    IF v_pk_cols IS NULL OR array_length(v_pk_cols, 1) IS NULL THEN
        -- No PK on the source table. The setup preflight refuses this
        -- shape (§14), but a defensive guard here keeps a manually-
        -- attached trigger from silently producing pk_jsonb=NULL rows
        -- that the applier can't dispatch.
        RAISE EXCEPTION 'sluice_capture_change: table %.% has no PRIMARY KEY; refuse-loudly per ADR-0066 §14',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    IF TG_OP = 'INSERT' THEN
        v_op     := 'I';
        v_after  := to_jsonb(NEW);
        v_before := NULL;
        v_pk     := (SELECT jsonb_object_agg(key, value) FROM jsonb_each(v_after) WHERE key = ANY(v_pk_cols));
    ELSIF TG_OP = 'UPDATE' THEN
        v_op     := 'U';
        v_before := to_jsonb(OLD);
        v_after  := to_jsonb(NEW);
        v_pk     := (SELECT jsonb_object_agg(key, value) FROM jsonb_each(v_after) WHERE key = ANY(v_pk_cols));
    ELSIF TG_OP = 'DELETE' THEN
        v_op     := 'D';
        v_before := to_jsonb(OLD);
        v_after  := NULL;
        v_pk     := (SELECT jsonb_object_agg(key, value) FROM jsonb_each(v_before) WHERE key = ANY(v_pk_cols));
    ELSE
        RAISE EXCEPTION 'sluice_capture_change: unexpected TG_OP %', TG_OP;
    END IF;

    INSERT INTO ` + changeLogTableRef + `
        (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
    VALUES
        (pg_current_xact_id()::text::bigint,
         TG_TABLE_SCHEMA,
         TG_TABLE_NAME,
         v_op,
         v_pk,
         v_before,
         v_after);

    RETURN NULL;  -- AFTER triggers ignore the return value
END
$sluice$;`
}

// renderCaptureTruncateFunction returns the CREATE OR REPLACE
// FUNCTION statement for the TRUNCATE companion. ADR-0066 §3 — the
// row function can't double-up because TRUNCATE triggers are FOR
// EACH STATEMENT, not FOR EACH ROW (no OLD/NEW).
func renderCaptureTruncateFunction(schema, changeLogTableRef string) string {
	return `CREATE OR REPLACE FUNCTION ` + truncateFnRef(schema) + `()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $sluice$
BEGIN
    INSERT INTO ` + changeLogTableRef + `
        (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
    VALUES
        (pg_current_xact_id()::text::bigint,
         TG_TABLE_SCHEMA,
         TG_TABLE_NAME,
         'T',
         '{}'::jsonb,
         NULL,
         NULL);
    RETURN NULL;
END
$sluice$;`
}

// renderCaptureDDLFunction returns the CREATE OR REPLACE FUNCTION
// statement for the DDL event-trigger handler. ADR-0066 §7. The
// event trigger emits a marker row with op='X' for every recognised
// DDL command tag; the polling reader translates these into a
// refuse-loudly error with the drained-model recovery hint.
func renderCaptureDDLFunction(schema, changeLogTableRef string) string {
	return `CREATE OR REPLACE FUNCTION ` + ddlFnRef(schema) + `()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $sluice$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.object_identity IS NULL THEN
            CONTINUE;
        END IF;
        INSERT INTO ` + changeLogTableRef + `
            (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
        VALUES
            (pg_current_xact_id()::text::bigint,
             COALESCE(r.schema_name, 'public'),
             COALESCE(r.object_identity, 'unknown'),
             'X',
             jsonb_build_object('command_tag', r.command_tag, 'object_type', r.object_type),
             NULL,
             NULL);
    END LOOP;
END
$sluice$;`
}

// renderTeardownDDL returns the ordered DROP statements that remove
// the engine. Order matters: drop per-table triggers BEFORE the
// shared capture function (else DROP FUNCTION CASCADE would have to
// be used, which is louder than necessary). KeepData retains the
// change-log table for post-mortem inspection.
func renderTeardownDDL(schema string, tables []string, keepData bool) []string {
	out := []string{}
	for _, t := range tables {
		fqTable := quoteIdent(schema) + "." + quoteIdent(t)
		out = append(
			out,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent(CaptureTriggerRow), fqTable),
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent("sluice_capture_truncate"), fqTable),
		)
	}
	out = append(
		out,
		// Event trigger (idempotent — IF EXISTS handles the
		// permissions-denied / polled-fingerprint-mode case).
		"DROP EVENT TRIGGER IF EXISTS "+quoteIdent(CaptureTriggerDDL),
		"DROP FUNCTION IF EXISTS "+ddlFnRef(schema)+"()",
		"DROP FUNCTION IF EXISTS "+truncateFnRef(schema)+"()",
		"DROP FUNCTION IF EXISTS "+rowFunctionRef(schema)+"()",
	)
	if !keepData {
		out = append(
			out,
			"DROP TABLE IF EXISTS "+quoteIdent(schema)+"."+quoteIdent(ChangeLogTable),
			"DROP TABLE IF EXISTS "+quoteIdent(schema)+"."+quoteIdent(ChangeLogMetaTable),
		)
	}
	return out
}

// preflightTables runs the §14 per-table refuse-loudly checks. Each
// refusal carries an operator-actionable Hint string. Returns nil
// (not an empty slice) on a clean preflight so callers can distinguish
// "nothing to refuse" from "preflight ran".
func preflightTables(ctx context.Context, db *sql.DB, schema string, tables []string) ([]TableRefusal, error) {
	var refusals []TableRefusal
	for _, t := range tables {
		hasPK, isUnlogged, hasGenerated, hasUnrecognisedDomain, err := loadTableShape(ctx, db, schema, t)
		if err != nil {
			return nil, fmt.Errorf("load table shape %s.%s: %w", schema, t, err)
		}
		if !hasPK {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "no-primary-key",
				Hint:   "add a PRIMARY KEY to " + schema + "." + t + " before including it in the trigger engine's replication set",
			})
		}
		if isUnlogged {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "unlogged-table",
				Hint:   "exclude UNLOGGED tables explicitly via --exclude-table, or convert them to LOGGED",
			})
		}
		if hasGenerated {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "generated-stored-column",
				Hint:   "the trigger engine does not replicate GENERATED ALWAYS AS ... STORED columns; use the `postgres` engine or exclude the column via --exclude-column",
			})
		}
		if hasUnrecognisedDomain {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "custom-domain-over-udt",
				Hint:   "the trigger engine refuses custom domains whose underlying type is also user-defined; remap the column with --type-override or use the `postgres` engine",
			})
		}
	}
	return refusals, nil
}

// loadTableShape returns the per-table boolean flags the preflight
// classifies on. A missing relation (the table doesn't exist) is
// returned as hasPK=false; the no-primary-key refusal then fires
// downstream, which is the right operator-facing message ("add a PK
// to a table that doesn't exist" reads weird but it's better than a
// raw catalog error).
func loadTableShape(ctx context.Context, db *sql.DB, schema, table string) (hasPK, isUnlogged, hasGenerated, hasUnrecognisedDomain bool, err error) {
	const q = `
SELECT
    EXISTS (
        SELECT 1
          FROM pg_constraint c
         WHERE c.conrelid = (SELECT oid FROM pg_class WHERE relname = $2 AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1))
           AND c.contype = 'p'
    ) AS has_pk,
    COALESCE(
        (SELECT relpersistence = 'u'
           FROM pg_class
          WHERE relname = $2
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)),
        false
    ) AS is_unlogged,
    EXISTS (
        SELECT 1
          FROM pg_attribute a
          JOIN pg_class c     ON c.oid = a.attrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = $2 AND n.nspname = $1
           AND a.attnum > 0 AND NOT a.attisdropped
           AND a.attgenerated = 's'
    ) AS has_generated,
    EXISTS (
        SELECT 1
          FROM pg_attribute a
          JOIN pg_class    c ON c.oid = a.attrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_type     t ON t.oid = a.atttypid
          JOIN pg_type     bt ON bt.oid = t.typbasetype
         WHERE c.relname = $2 AND n.nspname = $1
           AND a.attnum > 0 AND NOT a.attisdropped
           AND t.typtype = 'd'                       -- domain
           AND bt.typtype IN ('c', 'e', 'd', 'p')    -- composite/enum/domain/pseudo: refuse
    ) AS has_unrecognised_domain
`
	row := db.QueryRowContext(ctx, q, schema, table)
	if err := row.Scan(&hasPK, &isUnlogged, &hasGenerated, &hasUnrecognisedDomain); err != nil {
		return false, false, false, false, err
	}
	return hasPK, isUnlogged, hasGenerated, hasUnrecognisedDomain, nil
}

// canCreateEventTrigger reports whether the connecting role is a
// superuser OR has membership in pg_create_event_trigger (PG 14+).
// Either grant is sufficient to run CREATE EVENT TRIGGER.
func canCreateEventTrigger(ctx context.Context, db *sql.DB) (bool, error) {
	const q = `
SELECT
    bool_or(rolsuper) AS is_super,
    bool_or(
        pg_has_role(current_user, 'pg_create_event_trigger', 'MEMBER')
    ) AS has_role_member
  FROM pg_roles
 WHERE rolname = current_user`
	var isSuper, hasRoleMember sql.NullBool
	if err := db.QueryRowContext(ctx, q).Scan(&isSuper, &hasRoleMember); err != nil {
		// pg_create_event_trigger doesn't exist on PG < 14 — the
		// pg_has_role call fails with "role does not exist". Fall
		// back to checking just superuser.
		var ok bool
		const fb = `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`
		if err2 := db.QueryRowContext(ctx, fb).Scan(&ok); err2 != nil {
			return false, fmt.Errorf("probe event-trigger permission: %w", err2)
		}
		return ok, nil
	}
	return isSuper.Valid && isSuper.Bool || hasRoleMember.Valid && hasRoleMember.Bool, nil
}

// readPGVersionNum reads the server's PG_VERSION_NUM. Used for the
// §14 PG < 9.4 refusal.
func readPGVersionNum(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, `SHOW server_version_num`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// discoverTriggeredTables lists user tables in `schema` that have a
// trigger named `sluice_capture` installed. Used by Teardown when the
// operator doesn't pass --tables explicitly.
func discoverTriggeredTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const q = `
SELECT c.relname
  FROM pg_trigger t
  JOIN pg_class    c ON c.oid = t.tgrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1
   AND t.tgname  = $2
   AND NOT t.tgisinternal`
	rows, err := db.QueryContext(ctx, q, schema, CaptureTriggerRow)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// firstLine returns s up to the first newline. Used to keep the
// "exec failed: %q" error message short — the full DDL body is
// useful but unwieldy in error wrappers.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// quoteIdent quotes a SQL identifier per PG's standard rules: wrap in
// double quotes, doubling any embedded double-quote. Mirror of the
// vanilla-PG engine's same-named helper (not exported, so we redeclare
// it here rather than reach into a sibling package).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteSQLString quotes a SQL string literal per PG's standard rules:
// wrap in single quotes, doubling any embedded single-quote.
func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
