// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/engines/postgres"
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
	// CaptureTriggerTruncate / CaptureFunctionTruncate are the per-table
	// TRUNCATE companion trigger and its FOR EACH STATEMENT function.
	// Named (rather than the literals renderSetupDDL used to repeat) so
	// the capture-shape door (cdc_capture_shape.go) grades against the
	// same identifiers setup installs.
	CaptureTriggerTruncate  = "sluice_capture_truncate"
	CaptureFunctionTruncate = "sluice_capture_truncate_fn"
	// ChangeLogSchemaVer is the schema-version pin recorded in the meta
	// table. v1 was the original change-log + meta pair; v2 (roadmap item
	// 115) adds [ChangeLogConsumersTable], the source-side registry the
	// auto-prune cuts against; v3 (ADR-0185) adds the meta table's
	// capture_replicated_writes posture column; v4 (audit 2026-08-31 SEC-2)
	// adds the three setup-evidence columns the DDL suppression is bound to
	// ([metaSetupPIDCol] / [metaSetupNonceCol] / [metaSetupAtCol]). Setup is
	// idempotent, so re-running it against an older install IS the
	// migration: the CREATE TABLE IF NOT EXISTS adds the registry, the ADD
	// COLUMN IF NOT EXISTS statements add the posture and evidence columns,
	// and the meta upsert lifts the version. No reader gates on v3 or v4 —
	// the posture read tolerates the column's absence
	// (readCaptureReplicatedWritesPosture) and the capture function's
	// evidence read tolerates it too (the bootstrap arm) — so the lift is
	// bookkeeping that makes an install's vintage readable, not a door.
	ChangeLogSchemaVer = 4

	// metaCaptureReplicatedCol is the meta-table column recording whether
	// this install's capture triggers are meant to be ENABLE ALWAYS
	// (`trigger setup --capture-replicated-writes`, ADR-0185). Written by
	// the setup upsert; read at every CDC open, where it selects the F2
	// door's expected tgenabled ('A' vs 'O') and arms the echo-loop
	// refusal. Deliberately NOT in [internalTableColumnFloor]: pre-v3
	// installs lack it, the ADD COLUMN IF NOT EXISTS in renderSetupDDL is
	// the migration, and the posture read defaults an absent column to
	// false (origin-only), which is exactly what a pre-v3 install is.
	metaCaptureReplicatedCol = "capture_replicated_writes"

	// setupSessionGUC is the transaction-scoped marker Setup's own plan sets
	// and the DDL capture function returns early on (Bug 257). Without it,
	// every `trigger setup` RE-RUN over an existing event-trigger install
	// records sluice's OWN idempotent DDL — the ADR-0185 meta ADD COLUMN
	// migration fires ddl_command_end even as an IF-NOT-EXISTS no-op
	// (ground-truthed on PG 16.14: a no-op ADD COLUMN IF NOT EXISTS yields
	// one pg_event_trigger_ddl_commands() row; a no-op CREATE TABLE IF NOT
	// EXISTS and a no-op DROP INDEX IF EXISTS yield none), and the opt-in's
	// ENABLE ALWAYS ALTERs carry the ALTER TABLE tag — as op='X' rows, so
	// the next warm resume refuses "observed source-side DDL" and steers a
	// full re-copy for statements no operator ran.
	//
	// # The GUC is a PRIVILEGE BOUNDARY, not an annoyance filter (SEC-2)
	//
	// PostgreSQL puts no privilege on `SET`ting a dotted placeholder GUC, so
	// v0.133.1's `current_setting(...) = 'on'` check handed every writer on
	// the source an off-switch for the ONLY DDL-detection tier this engine
	// has (the polled-fingerprint fallback was never implemented — see
	// preflight_ddl_detection.go). Observed on PG 16.14: an unprivileged
	// role SET the GUC, ran ADD COLUMN / DROP COLUMN / CREATE INDEX, and
	// recorded ZERO op='X' rows while identical control ALTERs before and
	// after recorded normally. The suppression is therefore bound to
	// evidence an ordinary writer cannot produce — see
	// [captureDDLSuppressionCheck] for the two arms and their reasoning.
	//
	// Transaction-scoped rather than session-scoped (audit C-1): the plan
	// opens with BEGIN, sets the marker with SET LOCAL, and closes with
	// COMMIT, so PostgreSQL reverts the marker at BOTH commit and rollback.
	// The off-state is structural instead of a trailing `RESET` statement
	// that a mid-plan error, a cancel, or an operator abandoning a
	// hand-pasted plan never reaches.
	setupSessionGUC = "sluice.setup_in_progress"

	// setupBootstrapMarker is the [setupSessionGUC] value the plan's SET
	// LOCAL installs before it can arm the real evidence, and the only
	// value the bootstrap arm of [captureDDLSuppressionCheck] accepts. It
	// is deliberately the SAME literal v0.133.1–v0.134.x used, because on
	// the first re-setup over such an install the OLD function body — which
	// grades this literal and nothing else — is what covers the plan's meta
	// migration until the new body is armed.
	setupBootstrapMarker = "on"

	// metaSetupPIDCol / metaSetupNonceCol / metaSetupAtCol are the v4
	// setup-evidence columns (SEC-2). Setup ARMS them inside its own
	// transaction (`pg_backend_pid()` of the executing session + a fresh
	// gen_random_uuid() + clock_timestamp()) and DISARMS them before COMMIT;
	// the capture function suppresses only for a firing backend whose PID,
	// marker value and freshness all match the armed row. An ordinary
	// writer cannot forge any of the three: setup renders no GRANTs, so the
	// meta table is owner-only (observed: `permission denied for table` on
	// both SELECT and UPDATE as an unprivileged role), and no role can
	// choose its own backend PID.
	//
	// Deliberately NOT in [internalTableColumnFloor], for the same reason
	// [metaCaptureReplicatedCol] is not: pre-v4 installs lack them, the ADD
	// COLUMN IF NOT EXISTS in renderSetupDDL is the migration, and the
	// capture function's evidence read tolerates their absence.
	metaSetupPIDCol   = "setup_backend_pid"
	metaSetupNonceCol = "setup_nonce"
	metaSetupAtCol    = "setup_at"

	// setupEvidenceFreshness bounds how long an ARMED evidence row can
	// authorize suppression. The arm/disarm pair lives inside the plan's own
	// transaction, so a completed plan leaves the row disarmed and an
	// aborted one rolls the arm back — which is what actually bounds PID
	// reuse. This window is the belt for the one residue an operator can
	// still produce by hand: pasting the plan into psql and typing their own
	// COMMIT after the arm but before the disarm. An hour is far longer than
	// any real setup and far shorter than "until the next reboot".
	setupEvidenceFreshness = "1 hour"

	// setupDisarmedNonce is what the disarm writes into [metaSetupNonceCol].
	// It must be non-NULL and match no marker: NULL is reserved as the
	// "these columns exist but were never armed" signal that opens the
	// bootstrap arm, and that signal has to be unreachable once any v4-aware
	// setup has completed.
	setupDisarmedNonce = ""
)

// CapturePayload selects how much of each changed row the capture
// trigger writes into `sluice_change_log` (ADR-0068). The three modes
// are points on a single axis of decreasing payload — they change ONLY
// the source-side trigger body; the CDC reader and applier are
// payload-shape-agnostic (they build the apply SET from whatever is in
// `after` and the WHERE from whatever is in `before`), so the mode is
// entirely a property of the installed trigger.
type CapturePayload string

const (
	// CapturePayloadFull writes the full before-image AND full
	// after-image on every UPDATE (and the full old row on DELETE).
	// Today's behaviour, byte-identical. Conservative default per the
	// loud-failure / validate-end-to-end tenets.
	CapturePayloadFull CapturePayload = "full"

	// CapturePayloadChanged trims only the UPDATE `after` image to
	// `PK ∪ {changed columns}`; the full `before` image is retained so
	// the apply WHERE still does optimistic divergence detection.
	CapturePayloadChanged CapturePayload = "changed"

	// CapturePayloadMinimal trims the UPDATE `after` image (as
	// `changed`) AND trims the UPDATE/DELETE `before` image to the PK
	// only, so the apply WHERE becomes a PK match (last-write-wins CDC).
	// Reaches toward Bucardo's ~2x source-write overhead; trades the
	// divergence-detecting WHERE, acceptable for one-way CDC.
	CapturePayloadMinimal CapturePayload = "minimal"
)

// normalizePayload returns the effective mode (empty → full) and an
// error for any unrecognised value. The empty-default keeps a
// zero-value SetupOptions on today's byte-identical behaviour.
func normalizePayload(p CapturePayload) (CapturePayload, error) {
	switch p {
	case "", CapturePayloadFull:
		return CapturePayloadFull, nil
	case CapturePayloadChanged:
		return CapturePayloadChanged, nil
	case CapturePayloadMinimal:
		return CapturePayloadMinimal, nil
	default:
		return "", fmt.Errorf(
			"pgtrigger: setup: unknown --capture-payload %q; valid modes are full, changed, minimal (ADR-0068)",
			string(p),
		)
	}
}

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
	// Setup refuses-loudly when the attempt-based probe shows the
	// role cannot CREATE EVENT TRIGGER (stock PG: superuser-only;
	// managed providers may grant it to their master role). Phase 1
	// only records the operator's intent — the polled-fingerprint
	// loop itself is a follow-up.
	AllowPolledFingerprint bool

	// CapturePayload selects how much of each changed row the capture
	// trigger writes (ADR-0068). Empty defaults to [CapturePayloadFull]
	// (today's byte-identical behaviour). Validated by Setup, which
	// refuses-loudly on an unrecognised value. The mode is a property
	// of the installed trigger body only — the reader and applier are
	// unaffected.
	CapturePayload CapturePayload

	// CaptureReplicatedWrites installs the per-table capture triggers
	// (row AND truncate) ENABLE ALWAYS so they fire for DML applied
	// under session_replication_role=replica too — a source that is a
	// native logical-replication SUBSCRIBER, whose apply workers the
	// plain triggers are blind to (ADR-0185; audit 2026-08-26 F1). The
	// zero value keeps today's plain triggers (origin-only capture +
	// the replica-role WARN). Setup REFUSES this opt-in when the source
	// carries sluice's own apply bookkeeping (the echo-loop shape —
	// see checkReplicaRoleCaptureShapes), and the recorded posture is
	// re-verified against the installed triggers at every CDC open.
	CaptureReplicatedWrites bool
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
	// create event triggers, established by an attempt-based probe
	// (a rolled-back CREATE EVENT TRIGGER — stock PG gates this on
	// superuser; managed providers like AWS RDS grant it to their
	// master role). False signals the §7 fallback path; the
	// polled-fingerprint loop is enabled by
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

	// Never install capture triggers on the engine's OWN internal tables. The
	// capture function INSERTs into sluice_change_log, so a capture trigger on
	// that table re-fires on every insert → unbounded recursion → PostgreSQL
	// "stack depth limit exceeded", which fails EVERY write on EVERY triggered
	// table (a source-wide write outage, not a localized error). Both internal
	// tables carry a PRIMARY KEY, so any caller that enumerates "all tables with
	// a PK" (the common shape) sweeps them in on a re-run once they exist. Guard
	// here so the engine is self-protecting regardless of caller hygiene; the
	// matching caller-side filter is defense-in-depth, not the load-bearing fix.
	keptTables, excludedInternal := filterEngineInternalTables(opts.Tables)
	if len(excludedInternal) > 0 {
		slog.Warn(
			"pgtrigger: setup: excluded the engine's own internal tables from trigger installation; a capture trigger on these recurses infinitely and would block all source writes",
			"excluded", excludedInternal,
			"see", "ADR-0066",
		)
	}
	opts.Tables = keptTables
	if len(opts.Tables) == 0 {
		return nil, errors.New("pgtrigger: setup: no user tables remain after excluding the engine's own internal tables (sluice_change_log / sluice_change_log_meta); pass actual user tables via --tables")
	}

	// Validate (and default) the capture-payload mode before touching
	// any source-side state, so an unknown value refuses loudly upfront.
	payload, err := normalizePayload(opts.CapturePayload)
	if err != nil {
		return nil, err
	}
	opts.CapturePayload = payload

	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}
	if opts.Schema == "" {
		opts.Schema = cfg.schema
	}

	// One-shot CLI operation with no stream-id in play; the empty
	// label gets the `sluice/control/-` fallback application_name.
	db, err := postgres.OpenPgxDB(cfg.dsn, "")
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: ping: %w", err)
	}

	// Refuse loudly on PG < 13. The §14 floor was written as 9.4
	// (JSONB), but the engine has REQUIRED PG 13 since its first
	// release: the capture trigger records pg_current_xact_id() (the
	// 64-bit epoch-carrying xid8 the safety-lag hold-back compares in —
	// see pollQuery) and the poller calls pg_current_snapshot() /
	// pg_snapshot_xmin() — all PG 13+. Refusing here replaces the
	// catastrophic late failure a PG 12 source would otherwise hit:
	// Setup succeeds (plpgsql bodies are only syntax-checked at CREATE
	// time) and then EVERY write to EVERY triggered table fails at its
	// first DML — a source-wide write outage, not a setup error.
	pgver, err := readPGVersionNum(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: read PG version: %w", err)
	}
	if pgver < 130000 {
		return nil, fmt.Errorf(
			"pgtrigger: setup: source PG version_num = %d (< 13); the trigger engine requires PG 13+ (pg_current_xact_id / pg_current_snapshot for the gap-free safety lag) — %s",
			pgver,
			"upgrade the source server to PG 13 or later, or use the slot-based `postgres` engine if the source tier allows replication slots",
		)
	}

	// The replica-role capture shapes (audit 2026-08-26 F1, ADR-0185).
	// Default posture: WARN (never refuse) when the source's writes can
	// arrive under session_replication_role=replica — a logical-replication
	// subscriber source or an all-sluice relay — which the plain capture
	// triggers installed below do NOT fire for. Under
	// --capture-replicated-writes the subscriber shape is the SUPPORTED
	// case (the ENABLE ALWAYS triggers capture it) and the relay shape is
	// REFUSED instead (the echo loop; SLUICE-E-CDC-TRIGGER-ECHO-LOOP).
	// Runs on dry-run too: the operator should see the risk — or the
	// refusal — while planning, not after installing.
	if err := checkReplicaRoleCaptureShapes(ctx, db, opts.Schema, opts.CaptureReplicatedWrites); err != nil {
		return nil, err
	}

	// §14 per-table preflight: no-PK, UNLOGGED, generated columns,
	// custom domain-over-UDT. Also loads each table's PK column list —
	// renderSetupDDL bakes it into the capture trigger's TG_ARGV (N-16).
	refusals, pkColsByTable, err := preflightTables(ctx, db, opts.Schema, opts.Tables)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: preflight: %w", err)
	}

	// The engine's own tables are created with CREATE TABLE IF NOT EXISTS, so a
	// pre-existing relation at one of those names would be ADOPTED rather than
	// refused (item 149b). Graded before the event-trigger probe and before the
	// dry-run return, so a plan surfaces it and nothing — not even the probe's
	// rolled-back transaction — runs against a source we are about to refuse.
	if err := preflightInternalTableShapes(ctx, db, opts.Schema); err != nil {
		return nil, err
	}

	// Event-trigger permissions probe (attempt-based, rolled back —
	// zero residue; see canCreateEventTrigger).
	canEventTrigger, err := canCreateEventTrigger(ctx, db, opts.Schema)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: setup: probe event-trigger permission: %w", err)
	}
	if !canEventTrigger && !opts.AllowPolledFingerprint {
		// §14 last bullet — refuse with the flag suggestion. Stock PG
		// gates CREATE EVENT TRIGGER on superuser (there is NO
		// predefined role for it); managed providers grant it to their
		// master role (AWS RDS via rds_superuser).
		return nil, fmt.Errorf(
			"pgtrigger: setup: connecting role cannot create event triggers (probe CREATE EVENT TRIGGER was refused); %s",
			"the trigger engine uses an event trigger to detect source-side DDL — re-run as a superuser (or your managed provider's master role, e.g. the RDS master user), or re-run with --allow-polled-fingerprint to opt in to the polled-fingerprint fallback (§7)",
		)
	}

	plan := &Plan{
		Refusals:              refusals,
		EventTriggerSupported: canEventTrigger,
		PGVersionNum:          pgver,
	}
	specs := make([]tableTriggerSpec, len(opts.Tables))
	for i, t := range opts.Tables {
		specs[i] = tableTriggerSpec{Name: t, PKCols: pkColsByTable[t]}
	}
	plan.Statements = renderSetupDDL(opts.Schema, specs, canEventTrigger, opts.CapturePayload, opts.CaptureReplicatedWrites)

	if len(refusals) > 0 {
		// Refusals block the run even on dry-run — the operator
		// should see the refusals first, not the DDL.
		return plan, fmt.Errorf("pgtrigger: setup: %d table(s) refused (see plan.Refusals)", len(refusals))
	}
	if opts.DryRun {
		return plan, nil
	}

	// Every statement rides ONE session: the plan is a single transaction
	// whose BEGIN/COMMIT are statements 0 and N, it sets [setupSessionGUC]
	// with SET LOCAL, and it arms the suppression evidence against this
	// backend's PID — all three of which a pooled db.ExecContext could
	// silently break by hopping connections between statements, re-opening
	// exactly the self-recorded op='X' poisoning the marker exists to
	// prevent (Bug 257). Running the plan VERBATIM is deliberate: what
	// `--dry-run` prints is exactly what an apply executes, so an operator
	// applying it by hand through psql gets the same property.
	conn, err := db.Conn(ctx)
	if err != nil {
		return plan, fmt.Errorf("pgtrigger: setup: acquire session: %w", err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range plan.Statements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			// Roll back explicitly rather than relying on the pooled
			// connection being discarded (audit C-1 / reconciler item 2):
			// pgx-stdlib's ResetSession is a liveness check only, so a
			// pinned connection returned mid-transaction would carry both
			// the aborted transaction and the SET LOCAL marker into its
			// next acquisition. WithoutCancel so a cancelled ctx still
			// gets the rollback out.
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
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

	// One-shot CLI operation with no stream-id in play; the empty
	// label gets the `sluice/control/-` fallback application_name.
	db, err := postgres.OpenPgxDB(cfg.dsn, "")
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

// isEngineInternalTable reports whether name is one of the source-side tables
// the engine creates for its own bookkeeping (the change-log and its meta
// companion). These must NEVER receive a capture trigger: the capture function
// writes into sluice_change_log, so a trigger there recurses to a stack-depth
// failure, and sluice_change_log_meta is engine state, not user data.
func isEngineInternalTable(name string) bool {
	return name == ChangeLogTable || name == ChangeLogMetaTable || name == ChangeLogConsumersTable
}

// filterEngineInternalTables splits a caller-supplied table list into the
// tables to set up (kept) and the engine-internal names to exclude. It is
// order-preserving so the rendered DDL order is stable for tests and dry-run.
func filterEngineInternalTables(tables []string) (kept, excluded []string) {
	for _, t := range tables {
		if isEngineInternalTable(t) {
			excluded = append(excluded, t)
			continue
		}
		kept = append(kept, t)
	}
	return kept, excluded
}

// internalTableColumnFloor is, per engine-internal table, the column set every
// sluice-created copy of that table has carried since the table was introduced
// — the evidence [preflightInternalTableShapes] grades an ALREADY-PRESENT
// relation against (roadmap item 149b). The twin of sqlite-trigger's map of the
// same name; the two column sets differ because the two change logs do.
//
// # Why a floor and not "the columns the current DDL emits"
//
// The probe runs on the CURRENT binary against a source an OLDER binary may
// have set up, so the expected set must be what the oldest shipped installer
// wrote. Ground-truthed from the DDL's own history (`git log -p` over this
// file): no column of these three tables has ever been renamed or removed — the
// schema evolutions have been a whole NEW table (the consumer registry at
// schema_version 2) and an ADDED meta column ([metaCaptureReplicatedCol],
// schema_version 3, ADR-0185) that is deliberately NOT in this floor: pre-v3
// installs lack it, setup's ADD COLUMN IF NOT EXISTS is the migration, and the
// posture read treats absence as false. An absent table is created rather than
// graded. So the floor and the rendered CREATE TABLE bodies agree exactly
// today, which is what [TestInternalTableColumnFloorMatchesTheRenderedDDL]
// pins; a future release that ADDS a column to a CREATE body must not add it
// here unless it also migrates existing installs, and that gate forces the
// decision rather than letting an upgrade start refusing every source it used
// to accept.
//
// # Why column NAMES and not the indexes
//
// The indexes are actively wrong as evidence: this engine's index set DID
// change (the N-16 diet DROPs `sluice_change_log_id_idx` and
// `sluice_change_log_table_idx`, which every pre-diet install still carries), so
// an index-based probe would refuse exactly the older installs it exists to
// leave alone. The column names have been stable across the same span.
var internalTableColumnFloor = map[string][]string{
	ChangeLogTable: {
		"id", "txid", "committed_at", "schema_name", "table_name",
		"op", "pk_jsonb", "before_jsonb", "after_jsonb",
	},
	ChangeLogMetaTable:      {"singleton_pk", "schema_version", "installed_at"},
	ChangeLogConsumersTable: {"consumer_id", "applied_id", "updated_at"},
}

// preflightInternalTableShapes refuses loudly when `schema` already carries a
// relation at one of the engine's own names whose shape is not sluice's — the
// silent-adoption half of item 149b.
//
// Existence alone proves nothing: a legitimate re-`setup` finds every one of
// these relations there. What distinguishes them is the column set, so an
// existing relation passes exactly when it carries every column in its floor. A
// relation that is absent (no columns) is skipped — the setup DDL creates it.
func preflightInternalTableShapes(ctx context.Context, db *sql.DB, schema string) error {
	names := make([]string, 0, len(internalTableColumnFloor))
	for name := range internalTableColumnFloor {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		cols, err := loadRelationColumns(ctx, db, schema, name)
		if err != nil {
			return fmt.Errorf("pgtrigger: setup: read the shape of %s.%s: %w", schema, name, err)
		}
		if len(cols) == 0 {
			continue // absent; the setup DDL creates it
		}
		missing := missingFloorColumns(internalTableColumnFloor[name], cols)
		if len(missing) == 0 {
			continue
		}
		return fmt.Errorf(
			"pgtrigger: setup: refuse-loudly foreign-table on %s.%s — the source already has a relation by that name "+
				"and it is NOT sluice's change-log bookkeeping: it is missing the column(s) %v (it has %v). "+
				"Setup creates its own tables with CREATE TABLE IF NOT EXISTS, so continuing would have ADOPTED this relation silently "+
				"and the first captured change would then fail on YOUR OWN write path, after the capture triggers were installed. "+
				"Rename or move the conflicting relation and re-run setup — its rows are yours, so `sluice trigger teardown` is not the fix "+
				"(teardown DROPs tables by these names)",
			schema, name, missing, cols,
		)
	}
	return nil
}

// loadRelationColumns returns the live column names of `schema.relation` in
// attnum order, or an empty slice when no such relation exists. Deliberately
// NOT restricted by relkind: a VIEW or foreign table sitting at one of the
// engine's names is just as much a collision, and reading its columns lets the
// refusal say which ones are missing instead of failing later on the CREATE.
func loadRelationColumns(ctx context.Context, db *sql.DB, schema, relation string) ([]string, error) {
	const q = `
SELECT a.attname::text
  FROM pg_attribute  a
  JOIN pg_class      c ON c.oid = a.attrelid
  JOIN pg_namespace  n ON n.oid = c.relnamespace
 WHERE n.nspname = $1
   AND c.relname = $2
   AND a.attnum  > 0
   AND NOT a.attisdropped
 ORDER BY a.attnum`
	rows, err := db.QueryContext(ctx, q, schema, relation)
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

// missingFloorColumns returns the floor columns absent from have, in floor
// order (a stable, operator-readable list).
func missingFloorColumns(floor, have []string) []string {
	present := make(map[string]bool, len(have))
	for _, c := range have {
		present[c] = true
	}
	var missing []string
	for _, c := range floor {
		if !present[c] {
			missing = append(missing, c)
		}
	}
	return missing
}

// tableTriggerSpec pairs one replicated table with the PK column list
// [renderSetupDDL] bakes into its capture trigger's TG_ARGV (ADR-0066
// §3, N-16). PKCols is in PK-constraint (conkey) order; it is empty
// only for the §14-refused no-PK shape, which setup never applies.
type tableTriggerSpec struct {
	Name   string
	PKCols []string
}

// pkColsJSON renders a trigger's TG_ARGV[0] payload: the PK column list
// as a JSON array (`["tenant_id","order_id"]`). An empty list renders
// `[]` — reachable only in a dry-run plan for a table the §14 preflight
// refused; the trigger body refuses it too, defensively.
func pkColsJSON(cols []string) string {
	if len(cols) == 0 {
		return "[]"
	}
	// json.Marshal of a []string cannot fail (no unsupported types).
	b, _ := json.Marshal(cols)
	return string(b)
}

// renderSetupDDL produces the ordered DDL statements that install the
// engine. Order matters: the change-log table must exist before the
// capture function references it; the function must exist before the
// per-table triggers reference it.
//
// The table creates below are `CREATE TABLE IF NOT EXISTS`, so on their own
// they would ADOPT a pre-existing relation at one of these names rather than
// refuse it (roadmap item 149b, the twin of the note on
// sqlite-trigger.renderSetupDDL). What keeps that from happening is NOT this
// render — it is [preflightInternalTableShapes], which Setup runs before any of
// this is applied and which refuses a relation whose column set is not
// sluice's. The IF NOT EXISTS itself stays, because setup is idempotent by
// design: a re-run against a healthy install must be silent.
//
// The trigger DDL is not a member of the class: it emits
// `DROP TRIGGER IF EXISTS` plus a plain `CREATE TRIGGER`, which is loud.
//
// captureReplicated (ADR-0185) adds, per table, an
// `ALTER TABLE … ENABLE ALWAYS TRIGGER` for the row and truncate triggers
// — PostgreSQL has no ENABLE ALWAYS clause on CREATE TRIGGER, so the
// posture is a separate ALTER after each create — and records the posture
// in the meta upsert so every CDC open can grade the installed enablement
// against the recorded intent (the F2 door's posture match). A re-run
// WITHOUT the flag converges an opt-in install back to plain: the
// DROP + CREATE yields fresh 'O' triggers and the upsert records false.
//
// # The plan is ONE transaction (Bug 257 + audit SEC-2 / C-1)
//
// It opens with BEGIN, sets [setupSessionGUC] with SET LOCAL, arms the
// suppression evidence in the meta table, and closes by disarming that
// evidence and COMMITting — so a re-run's own DDL is never recorded as
// op='X' by the install's already-present event trigger, and PostgreSQL
// reverts BOTH the marker and (on abort) the armed evidence at the end of
// the transaction. Before v0.135 the off-state rode a trailing `RESET`
// statement, which a mid-plan error, a cancel, or an operator abandoning a
// hand-pasted plan simply never reached — leaving the suppression on for
// the rest of that psql session and silently swallowing their OPERATOR DDL
// (audit C-1, observed). Every statement here is transaction-safe: PG DDL
// is transactional, `CREATE EVENT TRIGGER` included (verified), and the
// plan emits no CONCURRENTLY.
//
// Ordering is load-bearing in three places:
//
//   - The capture-DDL function's CREATE OR REPLACE is the plan's FIRST
//     statement, ahead of every statement the event trigger's TAG filter
//     watches (Bug 257): on the first re-setup over an install whose body
//     predates the suppression check, the old body ignores the marker and
//     must be replaced before the meta ALTER it would record. CREATE
//     FUNCTION is not a watched tag, so this statement records nothing at
//     any install vintage.
//   - The evidence ARM runs twice. The early arm covers a v4 install's
//     re-run, whose meta ADD COLUMN IF NOT EXISTS no-ops still fire
//     ddl_command_end; it is a no-op itself on a pre-v4 install (the
//     columns do not exist yet), where the bootstrap arm carries the
//     window instead. The second arm, after the meta migration, is strict
//     on every install vintage — everything from there on is covered by
//     the unforgeable evidence rather than by the bootstrap literal.
//   - The consumer registry is still created before the meta upsert lifts
//     schema_version, so the version can never claim a registry that isn't
//     there. (The transaction now makes that atomic as well, but the
//     ordering is kept: a hand-applied plan can be stopped anywhere.)
func renderSetupDDL(schema string, tables []tableTriggerSpec, canEventTrigger bool, payload CapturePayload, captureReplicated bool) []string {
	tableRef := func(name string) string {
		return quoteIdent(schema) + "." + quoteIdent(name)
	}
	metaRef := tableRef(ChangeLogMetaTable)
	out := []string{"BEGIN"}
	if canEventTrigger {
		// Re-created FIRST, before any TAG-watched statement (Bug 257): a
		// re-setup over an install whose function body predates the
		// suppression check must replace that body before the first
		// statement the old body would record (the meta ADD COLUMN below).
		// Safe on a fresh install too — plpgsql bodies are only
		// syntax-checked at CREATE time, so the reference to the
		// not-yet-created change-log table resolves at first execution.
		out = append(out, renderCaptureDDLFunction(schema, tableRef(ChangeLogTable), metaRef))
	}
	out = append(
		out,
		"SET LOCAL "+setupSessionGUC+" = "+quoteSQLString(setupBootstrapMarker),
		renderArmSetupEvidence(metaRef),

		"CREATE TABLE IF NOT EXISTS "+metaRef+` (
    singleton_pk   BOOLEAN PRIMARY KEY DEFAULT TRUE,
    schema_version INT NOT NULL,
    installed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sluice_change_log_meta_singleton CHECK (singleton_pk = TRUE)
)`,
		// The v3 capture-posture column (ADR-0185), added by ALTER rather
		// than in the CREATE body above so ONE statement migrates every
		// install — a fresh create and a pre-v3 re-run alike — and the
		// column-floor gate's CREATE-derived roster (setup_adoption_test)
		// keeps grading pre-v3 installs against the columns they actually
		// have. Readers default an absent column to false, so an install
		// that never re-runs setup keeps its origin-only posture.
		"ALTER TABLE "+metaRef+" ADD COLUMN IF NOT EXISTS "+metaCaptureReplicatedCol+" BOOLEAN NOT NULL DEFAULT FALSE",
		// The v4 setup-evidence columns (SEC-2), by the same argument. One
		// statement so a pre-v4 install fires ddl_command_end once, inside
		// the bootstrap arm's window, rather than three times.
		fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s INT, ADD COLUMN IF NOT EXISTS %s TEXT, ADD COLUMN IF NOT EXISTS %s TIMESTAMPTZ",
			metaRef, metaSetupPIDCol, metaSetupNonceCol, metaSetupAtCol,
		),
		// The source-side CONSUMER REGISTRY (roadmap item 115). Every
		// trigger-CDC stream reading this database records its own
		// durably-applied frontier here and the auto-prune cuts at the MIN
		// across all of them, so a slower peer's unread rows are never
		// reaped. Created before the meta upsert below lifts
		// schema_version to 2, so the version can never claim a registry
		// that isn't there.
		"CREATE TABLE IF NOT EXISTS "+tableRef(ChangeLogConsumersTable)+` (
    consumer_id  TEXT PRIMARY KEY,
    applied_id   BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)`,

		fmt.Sprintf(
			"INSERT INTO %s (singleton_pk, schema_version, %s) VALUES (TRUE, %d, %t) ON CONFLICT (singleton_pk) DO UPDATE SET schema_version = EXCLUDED.schema_version, %s = EXCLUDED.%s",
			metaRef, metaCaptureReplicatedCol, ChangeLogSchemaVer, captureReplicated,
			metaCaptureReplicatedCol, metaCaptureReplicatedCol,
		),

		// The meta table, its evidence columns and its singleton row all
		// exist now, so this arm is STRICT on every install vintage — the
		// bootstrap literal stops carrying the plan here.
		renderArmSetupEvidence(metaRef),

		"CREATE TABLE IF NOT EXISTS "+tableRef(ChangeLogTable)+` (
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

		// N-16 (change-log index diet): earlier releases also
		// created sluice_change_log_id_idx ON (id) — an exact duplicate
		// of the BIGSERIAL PK's implicit index — and
		// sluice_change_log_table_idx ON (schema_name, table_name, id),
		// which no engine query has ever read (poll / anchor /
		// settle-clamp / prune / stats all key on id and txid alone).
		// Both were maintained on EVERY captured source DML for zero
		// read benefit — pure write amplification on tiers where the
		// operator pays IOPS. The idempotent DROPs converge any
		// pre-existing install to the lean shape; on a fresh install
		// they are no-ops.
		"DROP INDEX IF EXISTS "+tableRef("sluice_change_log_id_idx"),
		"DROP INDEX IF EXISTS "+tableRef("sluice_change_log_table_idx"),

		// Row-event capture function. TG_ARGV[0] carries the table's
		// PK column list, baked in per-trigger below (N-16);
		// jsonb_object_agg projects pk_jsonb out of OLD/NEW. The
		// capture-payload mode (ADR-0068) selects the per-op
		// v_before / v_after assignment block.
		renderCaptureRowFunction(schema, tableRef(ChangeLogTable), payload),

		// TRUNCATE companion — separate function because TRUNCATE
		// triggers are FOR EACH STATEMENT, not FOR EACH ROW.
		renderCaptureTruncateFunction(schema, tableRef(ChangeLogTable)),
	)

	for _, t := range tables {
		// Drop any pre-existing trigger with the canonical name so
		// re-running Setup with a different PK list refreshes the
		// TG_ARGV payload. PG does not have CREATE OR REPLACE TRIGGER
		// on row triggers (it does on PG 14+, but the engine's floor
		// is PG 13); a DROP IF EXISTS + CREATE is the portable shape.
		fqTable := quoteIdent(schema) + "." + quoteIdent(t.Name)
		out = append(
			out,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent(CaptureTriggerRow), fqTable),
			fmt.Sprintf(
				"CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %s(%s)",
				quoteIdent(CaptureTriggerRow),
				fqTable,
				tableRef(CaptureFunctionRow),
				quoteSQLString(pkColsJSON(t.PKCols)),
			),
			// TRUNCATE trigger.
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent(CaptureTriggerTruncate), fqTable),
			fmt.Sprintf(
				"CREATE TRIGGER %s AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION %s()",
				quoteIdent(CaptureTriggerTruncate),
				fqTable,
				tableRef(CaptureFunctionTruncate),
			),
		)
		if captureReplicated {
			// ADR-0185: fire under session_replication_role=replica too
			// (native subscription apply workers, privileged appliers).
			// Both members of the pair, so replicated TRUNCATE is graded
			// the same as replicated DML.
			out = append(
				out,
				fmt.Sprintf("ALTER TABLE %s ENABLE ALWAYS TRIGGER %s", fqTable, quoteIdent(CaptureTriggerRow)),
				fmt.Sprintf("ALTER TABLE %s ENABLE ALWAYS TRIGGER %s", fqTable, quoteIdent(CaptureTriggerTruncate)),
			)
		}
	}

	if canEventTrigger {
		// The DDL capture FUNCTION is the plan's first DDL statement (see
		// the Bug 257 ordering note above); only the event trigger itself
		// is (re)created here.
		out = append(
			out,
			"DROP EVENT TRIGGER IF EXISTS "+quoteIdent(CaptureTriggerDDL),
			fmt.Sprintf(
				"CREATE EVENT TRIGGER %s ON ddl_command_end WHEN TAG IN ('ALTER TABLE','CREATE TABLE','DROP TABLE','CREATE INDEX','DROP INDEX') EXECUTE FUNCTION %s()",
				quoteIdent(CaptureTriggerDDL),
				quoteIdent(schema)+"."+quoteIdent(CaptureFunctionDDL),
			),
		)
	}

	out = append(out, renderDisarmSetupEvidence(metaRef), "COMMIT")
	return out
}

// renderArmSetupEvidence renders the statement that ARMS the DDL-suppression
// evidence for the executing session and adopts the resulting nonce as the
// [setupSessionGUC] value (SEC-2).
//
// Everything it records is evaluated IN THE EXECUTING SESSION —
// `pg_backend_pid()`, `gen_random_uuid()` (core since PG 13, the engine's
// floor), `clock_timestamp()` — never baked in at render time. That is what
// keeps a hand-applied `--dry-run` plan working: the operator's own psql
// backend arms its own PID, and the privilege boundary is the UPDATE on the
// meta table (which setup grants to nobody), not knowledge of a secret in
// the plan text.
//
// `set_config(..., is_local => true)` is the function spelling of SET LOCAL:
// verified on PG 16.14 that a set_config issued inside a DO block persists to
// the end of the transaction and is reverted at COMMIT, which is exactly the
// scoping audit C-1 asks for.
//
// The exception handler is what makes the statement safe to emit BEFORE the
// meta migration: on a fresh install the table does not exist yet, and on a
// pre-v4 install the columns do not — in both cases the plan's own migration
// follows and the second arm is the strict one. Failing to arm leaves the
// bootstrap marker in place, which is the conservative direction (it demands
// session_user evidence instead of nonce evidence; it never widens anything).
func renderArmSetupEvidence(metaTableRef string) string {
	return `DO $sluice$
DECLARE
    v_nonce TEXT;
BEGIN
    -- Arm the DDL-suppression evidence for THIS backend (audit SEC-2). The
    -- meta table is owner-only, so no ordinary writer can produce this row.
    UPDATE ` + metaTableRef + `
       SET ` + metaSetupPIDCol + `   = pg_catalog.pg_backend_pid(),
           ` + metaSetupNonceCol + ` = pg_catalog.gen_random_uuid()::text,
           ` + metaSetupAtCol + `    = pg_catalog.clock_timestamp()
     WHERE singleton_pk
 RETURNING ` + metaSetupNonceCol + ` INTO v_nonce;
    IF v_nonce IS NOT NULL THEN
        PERFORM pg_catalog.set_config('` + setupSessionGUC + `', v_nonce, true);
    END IF;
EXCEPTION
    WHEN undefined_table OR undefined_column THEN
        -- Fresh install, or a pre-v4 install this plan is about to migrate.
        -- The bootstrap marker set above covers the window; the plan arms
        -- again once the columns and the singleton row exist.
        NULL;
END
$sluice$`
}

// renderDisarmSetupEvidence renders the plan's last statement before COMMIT:
// it clears the armed evidence so nothing outside this transaction can ever
// present it. [setupDisarmedNonce] (not NULL) is written deliberately — NULL
// is the "never armed" signal that opens the bootstrap arm, and that arm must
// be unreachable once a v4-aware setup has completed. A plan that ABORTS
// instead needs no disarm: the rollback takes the arm with it.
func renderDisarmSetupEvidence(metaTableRef string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = NULL, %s = %s, %s = NULL WHERE singleton_pk",
		metaTableRef, metaSetupPIDCol, metaSetupNonceCol, quoteSQLString(setupDisarmedNonce), metaSetupAtCol,
	)
}

// rowFunctionRef / truncateFunctionRef / ddlFunctionRef are
// schema-qualified function references used by render helpers and the
// teardown DROP path.
func rowFunctionRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(CaptureFunctionRow)
}

func truncateFnRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(CaptureFunctionTruncate)
}

func ddlFnRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(CaptureFunctionDDL)
}

// renderCaptureRowFunction returns the CREATE OR REPLACE FUNCTION
// statement for the shared row-event capture function. ADR-0066 §3,
// ADR-0068 (the capture-payload modes).
//
// TG_ARGV[0] carries the table's PK column list as a JSON array, baked
// in at setup time by [renderSetupDDL] — the §3-described shape (N-16).
// v1 instead re-derived the list from a pg_constraint/pg_attribute join
// at trigger fire time: a per-fired-row catalog join for a list that
// only changes on ALTER TABLE, i.e. pure source-write amplification on
// every captured DML. The baked list can go stale if the table's PK is
// ALTERed after setup; the projection guard in the body refuses such
// writes loudly (see its comment for the tier-by-tier posture), and
// re-running Setup re-bakes it (the per-table DROP + CREATE TRIGGER
// refreshes TG_ARGV).
//
// The PK-list block, the INSERT branch, the INSERT INTO scaffolding,
// SECURITY DEFINER, and SET search_path are SHARED across all three
// payload modes — only the UPDATE/DELETE v_before/v_after assignment
// block differs (ADR-0068). The per-mode block is produced by
// captureUpdateDeleteBlock so the shared scaffold lives in exactly
// one place.
func renderCaptureRowFunction(schema, changeLogTableRef string, payload CapturePayload) string {
	// Hand-written SQL — the source string is operator-readable and
	// avoids the per-engine identifier-quoting tangle of building
	// the function body programmatically. SECURITY DEFINER lets a
	// non-table-owning role drive the engine as long as the
	// function-owning role has INSERT on sluice_change_log.
	// Every built-in call is pg_catalog-qualified: this function has
	// carried the SET search_path pin since the engine's first commit
	// (so it was never the SEC-1 shape [renderCaptureDDLFunction] was),
	// but a SECURITY DEFINER body should not rest on the pin alone —
	// see that function's SEC-1 block for the resolution rule that
	// makes an unpinned definer exploitable.
	// SET extra_float_digits = 3 (Bug 194's trigger-capture face): the
	// capture format is to_jsonb(), and PG converts float4/float8 into a
	// jsonb numeric THROUGH the type's text output function — which
	// honors the FIRING session's extra_float_digits, i.e. the
	// application's session, which sluice can never pin. A
	// server/database/role default < 1 (Supabase ships 0 server-wide)
	// silently rounds every captured float (ground-truthed on PG 17:
	// to_jsonb(pi()) at efd=0 → 3.14159265358979). The per-function SET
	// clause pins the GUC for exactly the trigger's execution — the same
	// class fix as the raw-copy/walsender statement-level pins, at the
	// only layer that survives arbitrary application sessions. Existing
	// installs pick it up on the next setup re-run (CREATE OR REPLACE).
	//
	// SET bytea_output = hex, by the SAME argument on the same clause (audit
	// 2026-08-05 B-1's sibling sweep). to_jsonb() renders a bytea through the
	// type's text output function too, so it honors the FIRING session's
	// bytea_output exactly as it does extra_float_digits. Ground-truthed on
	// real PG: under `escape`, to_jsonb of a 2-byte 0xDEAD yields
	// "\\336\\255", the downstream writer's hex-sniff correctly declines it,
	// and the escape ASCII is bound to the target VARBINARY/BLOB verbatim —
	// silent corruption of every captured bytea on the pgtrigger→MySQL lane.
	// Same one-line remedy, same layer, same reason it must not rely on the
	// application's session.
	return `CREATE OR REPLACE FUNCTION ` + rowFunctionRef(schema) + `()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET extra_float_digits = 3
SET bytea_output = hex
AS $sluice$
DECLARE
    v_pk_cols  TEXT[];
    v_pk       JSONB;
    v_before   JSONB;
    v_after    JSONB;
    v_op       CHAR(1);
    -- v_new_json / v_old_json cache to_jsonb(NEW) / to_jsonb(OLD) so the
    -- changed/minimal UPDATE branches compute each ONCE per row instead of
    -- 2x and 1+Nx respectively (N = column count). Unused (NULL) in the
    -- INSERT/DELETE branches and in the full mode. See ADR-0068 follow-ups.
    v_new_json JSONB;
    v_old_json JSONB;
BEGIN
    -- The PK column list is baked into TG_ARGV[0] at setup time as a
    -- JSON array (ADR-0066 §3, N-16). Earlier releases re-derived it
    -- from the PK catalogs on EVERY fired row. Staleness (PK ALTERed
    -- after setup) fails loudly at the projection guard before the
    -- INSERT below.
    IF TG_NARGS = 0 OR TG_ARGV[0] IS NULL THEN
        RAISE EXCEPTION 'sluice_capture_change: trigger on %.% carries no baked PK column list (TG_ARGV[0]); re-run sluice trigger setup to reinstall the trigger',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;
    v_pk_cols := ARRAY(SELECT pg_catalog.jsonb_array_elements_text(TG_ARGV[0]::jsonb));

    IF pg_catalog.cardinality(v_pk_cols) = 0 THEN
        -- No PK on the source table. The setup preflight refuses this
        -- shape (§14) and only ever renders an empty list into a
        -- dry-run plan it refuses to apply, but a defensive guard here
        -- keeps a manually-attached trigger from silently producing
        -- pk_jsonb=NULL rows that the applier can't dispatch.
        RAISE EXCEPTION 'sluice_capture_change: table %.% has no PRIMARY KEY; refuse-loudly per ADR-0066 §14',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    IF TG_OP = 'INSERT' THEN
        -- INSERT is identical in all three modes: the after image is
        -- all-new data, so there is nothing to trim (ADR-0068).
        v_op     := 'I';
        v_after  := pg_catalog.to_jsonb(NEW);
        v_before := NULL;
        v_pk     := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_after) WHERE key = ANY(v_pk_cols));
` + captureUpdateDeleteBlock(payload) + `    ELSE
        RAISE EXCEPTION 'sluice_capture_change: unexpected TG_OP %', TG_OP;
    END IF;

    -- Stale-baked-list guard (loud failure beats silent corruption):
    -- every baked PK column must project out of the row image, and PK
    -- columns are NOT NULL, so a missing key can only mean the table's
    -- PK no longer matches what setup baked (ALTERed / column renamed
    -- after setup). On the event-trigger tier the ALTER independently
    -- refuses the stream at its op='X' row BEFORE any post-ALTER DML
    -- row is consumed (ALTER TABLE's ACCESS EXCLUSIVE lock orders the
    -- X row ahead of them); this guard is the net for the
    -- polled-fingerprint tier and for manual trigger surgery, where a
    -- stale list would otherwise capture rows keyed on the wrong
    -- columns. Recovery is a setup re-run, which re-bakes TG_ARGV.
    IF v_pk IS NULL OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(v_pk)) <> pg_catalog.cardinality(v_pk_cols) THEN
        RAISE EXCEPTION 'sluice_capture_change: baked PK column list % no longer matches the row image of %.% (PRIMARY KEY altered after setup?); re-run sluice trigger setup to re-bake the capture trigger',
            v_pk_cols, TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    INSERT INTO ` + changeLogTableRef + `
        (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
    VALUES
        (pg_catalog.pg_current_xact_id()::text::bigint,
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

// captureUpdateDeleteBlock returns the per-mode UPDATE + DELETE branch
// of the capture function (the only part that differs across the three
// ADR-0068 payload modes). The returned snippet slots in directly after
// the shared INSERT branch's closing line; it begins with `    ELSIF`
// and ends with a trailing newline so the shared scaffold reads as one
// IF/ELSIF chain.
//
//   - full:    UPDATE before = full OLD, after = full NEW; DELETE before = full OLD.
//   - changed: UPDATE before = full OLD, after = PK ∪ changed cols;  DELETE before = full OLD.
//   - minimal: UPDATE before = PK only,  after = PK ∪ changed cols;  DELETE before = PK only.
//
// The changed-set `after` always unions the PK (key = ANY(v_pk_cols))
// so the applier's WHERE and SET both have the key. jsonb_object_agg
// over zero rows returns NULL, but the PK union guarantees ≥1 row, so
// v_after is never NULL for a PK'd table (the engine refuses no-PK
// tables, §14). A zero-non-PK-column UPDATE (SET a=a) yields a PK-only
// after; that is a harmless idempotent no-op on apply and is NOT
// suppressed (per-stream change counts stay faithful).
func captureUpdateDeleteBlock(payload CapturePayload) string {
	// Shared changed-set computation for the UPDATE after image
	// (changed + minimal). PK union + IS DISTINCT FROM diff (NULL-safe,
	// type-exact on jsonb).
	// The `->` operator is left unqualified: OPERATOR(pg_catalog.->) is
	// the only qualified spelling and it makes the body unreadable, and
	// the function's own SET search_path pin already excludes every
	// attacker-writable schema from operator resolution.
	const changedAfter = `        v_after  := (
            SELECT pg_catalog.jsonb_object_agg(n.key, n.value)
            FROM pg_catalog.jsonb_each(v_new_json) n
            WHERE n.key = ANY(v_pk_cols)
               OR (v_old_json -> n.key) IS DISTINCT FROM n.value
        );`

	switch payload {
	case CapturePayloadChanged:
		return `    ELSIF TG_OP = 'UPDATE' THEN
        v_op       := 'U';
        v_new_json := pg_catalog.to_jsonb(NEW);
        v_old_json := pg_catalog.to_jsonb(OLD);
        v_before   := v_old_json;  -- full before-image (divergence WHERE)
` + changedAfter + `
        v_pk     := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_new_json) WHERE key = ANY(v_pk_cols));
    ELSIF TG_OP = 'DELETE' THEN
        v_op     := 'D';
        v_before := pg_catalog.to_jsonb(OLD);
        v_after  := NULL;
        v_pk     := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_before) WHERE key = ANY(v_pk_cols));
`
	case CapturePayloadMinimal:
		return `    ELSIF TG_OP = 'UPDATE' THEN
        v_op       := 'U';
        v_new_json := pg_catalog.to_jsonb(NEW);
        v_old_json := pg_catalog.to_jsonb(OLD);
        v_pk       := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_new_json) WHERE key = ANY(v_pk_cols));
        -- before drives the apply WHERE, which must locate the row by its
        -- identity BEFORE the change. Use the OLD PK (not v_pk, which is the
        -- NEW PK) so a PK-changing UPDATE still finds the existing target
        -- row; pk_jsonb stays the NEW PK (metadata, consistent w/ full/changed).
        v_before   := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_old_json) WHERE key = ANY(v_pk_cols));  -- OLD PK (PK-scoped WHERE)
` + changedAfter + `
    ELSIF TG_OP = 'DELETE' THEN
        v_op     := 'D';
        v_pk     := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(pg_catalog.to_jsonb(OLD)) WHERE key = ANY(v_pk_cols));
        v_before := v_pk;  -- PK only (OLD PK — DELETE WHERE targets the existing row)
        v_after  := NULL;
`
	default: // CapturePayloadFull
		return `    ELSIF TG_OP = 'UPDATE' THEN
        v_op     := 'U';
        v_before := pg_catalog.to_jsonb(OLD);
        v_after  := pg_catalog.to_jsonb(NEW);
        v_pk     := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_after) WHERE key = ANY(v_pk_cols));
    ELSIF TG_OP = 'DELETE' THEN
        v_op     := 'D';
        v_before := pg_catalog.to_jsonb(OLD);
        v_after  := NULL;
        v_pk     := (SELECT pg_catalog.jsonb_object_agg(key, value) FROM pg_catalog.jsonb_each(v_before) WHERE key = ANY(v_pk_cols));
`
	}
}

// renderCaptureTruncateFunction returns the CREATE OR REPLACE
// FUNCTION statement for the TRUNCATE companion. ADR-0066 §3 — the
// row function can't double-up because TRUNCATE triggers are FOR
// EACH STATEMENT, not FOR EACH ROW (no OLD/NEW).
//
// The search_path pin has been here since the engine's first commit
// (unlike [renderCaptureDDLFunction]'s — SEC-1); the pg_catalog
// qualification of the body's one call is the belt added alongside
// that fix, so no member of the trio relies on the pin alone.
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
        (pg_catalog.pg_current_xact_id()::text::bigint,
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

// captureDDLSuppressionCheck is the Bug 257 early-return the DDL capture
// function opens with: when the firing backend is sluice's own `trigger
// setup` transaction, record nothing. A function (rather than inline body
// text) so the integration pin can reconstruct the PRE-fix function body by
// stripping exactly this block, exercising the old-install re-setup
// ordering; metaTableRef is pre-quoted and schema-qualified by the caller,
// because the function's pinned `search_path = pg_catalog, pg_temp` (SEC-1)
// reaches no user schema.
//
// # Two arms, and why the marker alone is not one of them
//
// v0.133.1 graded `current_setting(guc, true) = 'on'` and nothing else.
// PostgreSQL lets ANY session SET a dotted placeholder GUC, so that was an
// off-switch for the only DDL-detection tier this engine has (SEC-2,
// observed on PG 16.14). Both arms below therefore require evidence the
// firing session cannot manufacture:
//
//   - STRICT arm — the meta row is ARMED for exactly this backend: the
//     marker equals a random nonce Setup wrote, `pg_backend_pid()` equals
//     the PID Setup recorded, and the arm is fresher than
//     [setupEvidenceFreshness]. Writing that row needs UPDATE on a table
//     setup grants to nobody, and no role can choose its own backend PID.
//     This is the steady-state arm; every statement of a v4-install re-run
//     takes it.
//
//   - BOOTSTRAP arm — the evidence columns are absent (a pre-v4 install)
//     or present-but-never-armed (the instant between this plan's own ADD
//     COLUMN and its arm), the marker is [setupBootstrapMarker], AND the
//     firing session is authenticated as the install's own role. Inside a
//     SECURITY DEFINER body `current_user` is the owner while
//     `session_user` stays the caller's authenticated role (ground-truthed
//     on PG 16.14: `session_user=app current_user=postgres`), and changing
//     `session_user` needs SET SESSION AUTHORIZATION, i.e. superuser — so
//     an ordinary writer fails this arm too. Its window is the plan's own
//     transaction: the disarm writes [setupDisarmedNonce] rather than NULL
//     precisely so this arm cannot be re-entered afterwards.
//
// `session_user` / `current_user` are SQL keywords, not schema-resolved
// functions, so they are not shadowable (same reasoning as the COALESCE
// note in [renderCaptureDDLFunction]).
//
// # Fail-safe direction: an unreadable evidence read RECORDS
//
// Suppression is the privilege, so failing closed means capturing.
// `SELECT ... INTO` over zero rows leaves the flag NULL (not false) and
// `IF v_suppress IS TRUE` rejects NULL; an unexpected error on the read
// sets it FALSE explicitly. The only exception routed to the bootstrap arm
// is undefined_table / undefined_column, which is the pre-v4 shape and
// still demands the session_user evidence.
func captureDDLSuppressionCheck(metaTableRef string) string {
	return `    -- Bug 257: sluice's own setup transaction emits idempotent DDL (the
    -- meta ADD COLUMN migrations, the opt-in's trigger-enablement ALTERs)
    -- on every re-run; recording it as op='X' would make the next warm
    -- resume refuse sluice's own statements as operator DDL. Audit SEC-2:
    -- the marker alone is settable by ANY session, so suppression is bound
    -- to evidence an ordinary writer cannot produce.
    v_marker := pg_catalog.current_setting('` + setupSessionGUC + `', true);
    IF v_marker IS NOT NULL THEN
        BEGIN
            -- STRICT arm: this backend holds the armed evidence.
            SELECT TRUE INTO v_suppress
              FROM ` + metaTableRef + ` m
             WHERE m.singleton_pk
               AND m.` + metaSetupPIDCol + ` = pg_catalog.pg_backend_pid()
               AND m.` + metaSetupNonceCol + ` = v_marker
               AND m.` + metaSetupAtCol + ` > pg_catalog.clock_timestamp() - '` + setupEvidenceFreshness + `'::interval;
            IF v_suppress IS NOT TRUE THEN
                -- BOOTSTRAP arm: the columns exist but were never armed —
                -- reachable only inside the plan that adds them.
                SELECT (v_marker = '` + setupBootstrapMarker + `' AND session_user = current_user) INTO v_suppress
                  FROM ` + metaTableRef + ` m
                 WHERE m.singleton_pk AND m.` + metaSetupNonceCol + ` IS NULL;
            END IF;
        EXCEPTION
            WHEN undefined_table OR undefined_column THEN
                -- Pre-v4 install: the evidence columns arrive later in this
                -- same plan. Same bootstrap evidence, no row to read.
                v_suppress := (v_marker = '` + setupBootstrapMarker + `' AND session_user = current_user);
            WHEN OTHERS THEN
                v_suppress := FALSE;  -- fail-safe: an unreadable evidence read RECORDS
        END;
        IF v_suppress IS TRUE THEN
            RETURN;
        END IF;
    END IF;
`
}

// renderCaptureDDLFunction returns the CREATE OR REPLACE FUNCTION
// statement for the DDL event-trigger handler. ADR-0066 §7. The
// event trigger emits a marker row with op='X' for every recognised
// DDL command tag; the polling reader translates these into a
// refuse-loudly error with the drained-model recovery hint.
//
// Bug 101 (v0.92.0): the function wraps the INSERT in an EXCEPTION
// handler for `undefined_table` so an operator who manually dropped
// `sluice_change_log` without running `sluice trigger teardown` sees
// a clear recovery message instead of PG's default "relation does
// not exist" / function-body dump. Pre-fix, every subsequent DDL on
// the source was blocked with the raw PG error, leaving the operator
// to grep for the right recovery command — that's a catastrophic
// blast-radius for what should be a five-second operator fix.
//
// # SEC-1 (audit 2026-08-31): SET search_path is load-bearing HERE, not cosmetic
//
// This function is necessarily owned by a SUPERUSER — `CREATE EVENT
// TRIGGER` requires superuser, so `trigger setup` installs it as one —
// and `SECURITY DEFINER` means its body runs with that superuser's
// privileges. From v0.85.0 through v0.134.0 it shipped WITHOUT the
// search_path pin its two siblings ([renderCaptureRowFunction],
// [renderCaptureTruncateFunction]) have carried since the engine's
// first commit, so the body resolved unqualified names against the
// FIRING session's search_path — and the firing session belongs to
// whoever ran the DDL, i.e. to any unprivileged user of the database.
//
// `jsonb_build_object`'s built-in signature is `VARIADIC "any"`, which
// scores ZERO exact argument-type matches in PostgreSQL's function
// resolution. An attacker-created exact-typed overload
// (`jsonb_build_object(text,text,text,text)`) in any schema their
// search_path reaches scores two, and **a better match beats schema
// order** — including pg_catalog's implicit first position — so the
// attacker's function wins resolution and executes as the superuser
// owner. One `CREATE TABLE` by that user is then arbitrary superuser
// code execution. Reproduced end-to-end on real PG by
// TestDDLCaptureFunctionSearchPath_ShadowedBuiltin.
//
// The fix is both halves, deliberately: the `SET search_path =
// pg_catalog, pg_temp` clause (the PostgreSQL-documented secure
// arrangement, matching the siblings' shape exactly) AND full
// pg_catalog qualification of every call in the body, so neither
// alone is load-bearing. COALESCE is a reserved keyword, not a
// shadowable function; the change-log INSERT target arrives
// pre-qualified via changeLogTableRef.
//
// Existing installs keep the vulnerable function until a
// `sluice trigger setup` re-run replaces it (CREATE OR REPLACE resets
// proconfig). [warnInsecureCaptureFunctions] detects that shape at
// every CDC open and WARNs with the remedy.
//
// metaTableRef is the pre-quoted, schema-qualified meta table the
// suppression check reads its evidence from (SEC-2) — see
// [captureDDLSuppressionCheck].
func renderCaptureDDLFunction(schema, changeLogTableRef, metaTableRef string) string {
	return `CREATE OR REPLACE FUNCTION ` + ddlFnRef(schema) + `()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $sluice$
DECLARE
    r          RECORD;
    v_marker   TEXT;
    v_suppress BOOLEAN;
BEGIN
` + captureDDLSuppressionCheck(metaTableRef) + `    FOR r IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands() LOOP
        IF r.object_identity IS NULL THEN
            CONTINUE;
        END IF;
        BEGIN
            INSERT INTO ` + changeLogTableRef + `
                (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
            VALUES
                (pg_catalog.pg_current_xact_id()::text::bigint,
                 COALESCE(r.schema_name, 'public'),
                 COALESCE(r.object_identity, 'unknown'),
                 'X',
                 pg_catalog.jsonb_build_object('command_tag', r.command_tag, 'object_type', r.object_type),
                 NULL,
                 NULL);
        EXCEPTION
            WHEN undefined_table THEN
                -- Bug 101 (v0.92.0): the change-log table was dropped
                -- without running ` + "`sluice trigger teardown`" + `, leaving this
                -- event trigger orphaned. Block the DDL with a clear
                -- recovery message instead of PG's raw error dump.
                RAISE EXCEPTION USING
                    ERRCODE = 'object_not_in_prerequisite_state',
                    MESSAGE = 'sluice trigger engine is partially uninstalled (` + changeLogTableRef + ` missing); DDL blocked by orphaned event trigger',
                    HINT    = 'To fully remove the sluice trigger engine, run: sluice trigger teardown --dsn=<source-dsn> --yes. To restore CDC capture, re-run: sluice trigger setup --dsn=<source-dsn> --tables=<...>.';
        END;
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
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdent(CaptureTriggerTruncate), fqTable),
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
			"DROP TABLE IF EXISTS "+quoteIdent(schema)+"."+quoteIdent(ChangeLogConsumersTable),
		)
	}
	return out
}

// preflightTables runs the §14 per-table refuse-loudly checks. Each
// refusal carries an operator-actionable Hint string. Returns nil
// (not an empty slice) on a clean preflight so callers can distinguish
// "nothing to refuse" from "preflight ran". The second return maps each
// table to its PK column list (conkey order) — the setup-time source of
// the trigger's baked TG_ARGV payload (N-16).
func preflightTables(ctx context.Context, db *sql.DB, schema string, tables []string) ([]TableRefusal, map[string][]string, error) {
	var refusals []TableRefusal
	pkColsByTable := make(map[string][]string, len(tables))
	for _, t := range tables {
		shape, err := loadTableShape(ctx, db, schema, t)
		if err != nil {
			return nil, nil, fmt.Errorf("load table shape %s.%s: %w", schema, t, err)
		}
		pkColsByTable[t] = shape.pkCols
		if len(shape.pkCols) == 0 {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "no-primary-key",
				Hint:   "add a PRIMARY KEY to " + schema + "." + t + " before including it in the trigger engine's replication set",
			})
		}
		if shape.isUnlogged {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "unlogged-table",
				Hint:   "exclude UNLOGGED tables explicitly via --exclude-table, or convert them to LOGGED",
			})
		}
		if shape.hasGenerated {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "generated-stored-column",
				// The `postgres` alternative is real but NARROWER than the
				// original wording ("logical replication carries them"),
				// which was false as stated: pgoutput does not publish a
				// generated column at all before PG 18. What the postgres
				// engine actually does is create the TARGET column as
				// GENERATED so it recomputes — which works for an ordinary
				// computed column and does NOT work when the column is part
				// of the row identity, where that engine refuses too
				// (SLUICE-E-CDC-GENERATED-PRIMARY-KEY, 2026-08-08).
				Hint: "the trigger engine does not replicate GENERATED ALWAYS AS ... STORED columns; use the `postgres` engine (its target keeps the GENERATED clause, so the column is recomputed rather than carried — unless it is part of the PRIMARY KEY or replica identity, which that engine refuses too), or take the whole table out of scope with --exclude-table — sluice has no column-scope filter (ADR-0177)",
			})
		}
		if shape.hasUnrecognisedDomain {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "custom-domain-over-udt",
				Hint:   "the trigger engine refuses custom domains whose underlying type is also user-defined; remap the column with --type-override or use the `postgres` engine",
			})
		}
		// Roadmap item 145's other half. Every other column type survives the
		// to_jsonb capture because its rendering is a JSON leaf; a json[] /
		// jsonb[] column does not, and it is the ONLY family where that is
		// true. to_jsonb embeds each element AS JSON rather than as a string,
		// so after the payload decode a JSON `null` element is byte-identical
		// to a SQL NULL element and an array-valued element `[1,2]` is
		// byte-identical to a nested array dimension. Neither is recoverable
		// downstream — the postgres writer's jsonArrayLeaf refuses every
		// payload-decoded shape it CAN see for exactly that reason — so the
		// column is refused HERE, before any trigger is installed, rather than
		// as a mid-stream apply error on the first array-bearing change.
		if shape.hasJSONArrayColumn {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "json-array-column",
				Hint:   "the trigger engine's to_jsonb capture cannot distinguish a JSON `null` element from a SQL NULL element, nor an array-valued element from a nested array dimension, in a json[]/jsonb[] column; remap the column with --type-override, take the whole table out of scope with --exclude-table (sluice has no column-scope filter — ADR-0177), or use the `postgres` engine (logical replication carries this family faithfully)",
			})
		}
		// Audit 2026-08-11 SPAT-4. The delegated postgres reader CARRIES
		// geometry/geography (ADR-0035), so cold copy works — but the
		// trigger capture is to_jsonb, and PostGIS's registered jsonb
		// cast renders a spatial value as a GeoJSON OBJECT: lossy in
		// itself (GeoJSON cannot carry M coordinates or non-EPSG SRIDs)
		// and undecodable by the shared apply path, which demands []byte
		// for a Geometry column. Before this refusal, setup + cold copy
		// succeeded and the FIRST spatial DML wedged the stream
		// mid-incident. Refused here instead, where the operator can
		// act. An ST_AsEWKB capture that would carry spatial columns is
		// the recorded alternative (it needs schema-qualified PostGIS
		// calls under the capture function's pinned search_path — see
		// the SPAT-4 decision memo in the audit backlog).
		if shape.hasSpatialColumn {
			refusals = append(refusals, TableRefusal{
				Schema: schema, Table: t,
				Reason: "postgis-spatial-column",
				Hint:   "the trigger engine's to_jsonb capture renders a PostGIS geometry/geography value as a GeoJSON object (lossy for M coordinates and non-EPSG SRIDs, and the apply path cannot decode it), so the first spatial change after cold start would wedge the stream; use the `postgres` engine (logical replication carries spatial values as EWKB faithfully), or take the whole table out of scope with --exclude-table (sluice has no column-scope filter — ADR-0177)",
			})
		}
	}
	return refusals, pkColsByTable, nil
}

// tableShape is what [loadTableShape] reads out of the catalog: the
// per-table facts the §14 preflight classifies on, in one value so a new
// classification axis is a field rather than another return slot.
type tableShape struct {
	// pkCols is the PK column list in PK-constraint (conkey) order.
	pkCols []string

	isUnlogged            bool
	hasGenerated          bool
	hasUnrecognisedDomain bool
	hasJSONArrayColumn    bool
	hasSpatialColumn      bool
}

// loadTableShape returns the per-table flags the preflight classifies
// on, plus the table's PK column list in PK-constraint (conkey) order —
// the same catalog derivation the capture trigger ran per fired row
// before N-16 baked it into TG_ARGV at setup time. A missing relation
// (the table doesn't exist) returns an empty pkCols; the no-primary-key
// refusal then fires downstream, which is the right operator-facing
// message ("add a PK to a table that doesn't exist" reads weird but
// it's better than a raw catalog error). The list crosses the wire as
// a JSON array (`to_jsonb(...)::text`) because database/sql has no
// portable text[] scan; encoding/json decodes it exactly.
func loadTableShape(ctx context.Context, db *sql.DB, schema, table string) (tableShape, error) {
	const q = `
SELECT
    COALESCE((
        SELECT to_jsonb(array_agg(att.attname::text ORDER BY array_position(con.conkey, att.attnum)))::text
          FROM pg_constraint con
          JOIN pg_attribute  att
            ON att.attrelid = con.conrelid
           AND att.attnum   = ANY(con.conkey)
         WHERE con.conrelid = (SELECT oid FROM pg_class WHERE relname = $2 AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1))
           AND con.contype  = 'p'
    ), '[]') AS pk_cols,
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
    ) AS has_unrecognised_domain,
    EXISTS (
        SELECT 1
          FROM pg_attribute a
          JOIN pg_class     c ON c.oid = a.attrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_type      t ON t.oid = a.atttypid
          JOIN pg_type     et ON et.oid = t.typelem
         WHERE c.relname = $2 AND n.nspname = $1
           AND a.attnum > 0 AND NOT a.attisdropped
           AND t.typcategory = 'A'                   -- an array type…
           AND et.typname IN ('json', 'jsonb')       -- …of json/jsonb: refuse (item 145)
    ) AS has_json_array_column,
    EXISTS (
        SELECT 1
          FROM pg_attribute a
          JOIN pg_class      c ON c.oid = a.attrelid
          JOIN pg_namespace  n ON n.oid = c.relnamespace
          JOIN pg_type       t ON t.oid = a.atttypid
          LEFT JOIN pg_type et ON et.oid = t.typelem      -- array element
          LEFT JOIN pg_type bt ON bt.oid = t.typbasetype  -- domain base
          LEFT JOIN pg_type bet ON bet.oid = bt.typelem   -- domain over array
         WHERE c.relname = $2 AND n.nspname = $1
           AND a.attnum > 0 AND NOT a.attisdropped
           -- PostGIS spatial columns, at every wrapping the capture can
           -- meet: direct, array element, domain base, domain-over-array
           -- (audit 2026-08-11 SPAT-4). Name-keyed on typname, the same
           -- key PostGIS's own geometry_columns view joins on and the
           -- postgres reader's udt_name dispatch uses — a hand-rolled
           -- type NAMED geometry also refuses, which is the loud
           -- direction (to_jsonb renders any such value as a JSON
           -- object the apply path cannot decode either way).
           AND (t.typname   IN ('geometry', 'geography')
             OR et.typname  IN ('geometry', 'geography')
             OR bt.typname  IN ('geometry', 'geography')
             OR bet.typname IN ('geometry', 'geography'))
    ) AS has_spatial_column
`
	var (
		shape          tableShape
		pkColsJSONText string
	)
	row := db.QueryRowContext(ctx, q, schema, table)
	if err := row.Scan(
		&pkColsJSONText,
		&shape.isUnlogged,
		&shape.hasGenerated,
		&shape.hasUnrecognisedDomain,
		&shape.hasJSONArrayColumn,
		&shape.hasSpatialColumn,
	); err != nil {
		return tableShape{}, err
	}
	if err := json.Unmarshal([]byte(pkColsJSONText), &shape.pkCols); err != nil {
		return tableShape{}, fmt.Errorf("decode PK column list %q: %w", pkColsJSONText, err)
	}
	return shape, nil
}

// canCreateEventTrigger reports whether the connecting role can run
// CREATE EVENT TRIGGER, by ATTEMPTING one inside a transaction that is
// always rolled back (event-trigger DDL is transactional, so the probe
// is side-effect-free — nothing survives, not even on a crash, because
// an un-committed transaction self-rolls-back).
//
// Attempt-based on purpose (RDS validation F2, 2026-07-16): the
// previous probe checked membership in a `pg_create_event_trigger`
// predefined role that DOES NOT EXIST in any stock PostgreSQL release
// (the only `pg_create*` predefined role is `pg_create_subscription`),
// so the capability read as superuser-only everywhere — and doubly
// false on AWS RDS, where the master user CAN create event triggers via
// `rds_superuser` (live-proven) despite `rolsuper=f`. Provider
// permission models here are patched server-side and enumerable only by
// drifting; the attempt is the one probe that can't lie. Contrast the
// replication-capability preflight, which stays catalog-based because
// slot creation is NOT transactional (see
// postgres/replication_preflight.go for that tradeoff).
//
// The probe function is created in `schema` (rolled back with the rest);
// a role that can't even create a function there can't run Setup's DDL
// at all, so that failure propagates as a loud error rather than being
// folded into "no event-trigger capability". Only SQLSTATE 42501
// (insufficient_privilege) from CREATE EVENT TRIGGER itself maps to
// (false, nil) — the §7 polled-fingerprint fallback signal.
func canCreateEventTrigger(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("probe event-trigger capability: begin: %w", err)
	}
	// Always roll back — the probe must leave zero residue whether the
	// CREATE succeeded or not.
	defer func() { _ = tx.Rollback() }()

	probeFn := quoteIdent(schema) + "." + quoteIdent("sluice_evtrig_probe")
	createFn := "CREATE FUNCTION " + probeFn +
		"() RETURNS event_trigger LANGUAGE plpgsql AS 'BEGIN NULL; END'"
	if _, err := tx.ExecContext(ctx, createFn); err != nil {
		return false, fmt.Errorf("probe event-trigger capability: create probe function: %w", err)
	}
	createTrig := "CREATE EVENT TRIGGER " + quoteIdent("sluice_evtrig_probe") +
		" ON ddl_command_end EXECUTE FUNCTION " + probeFn + "()"
	if _, err := tx.ExecContext(ctx, createTrig); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			return false, nil
		}
		return false, fmt.Errorf("probe event-trigger capability: %w", err)
	}
	return true, nil
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
