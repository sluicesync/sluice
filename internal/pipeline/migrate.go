// Package pipeline implements the simple-mode orchestrator: a one-shot
// schema-and-data migration from a source database to a target. It is
// the layer that wires the IR's reader and writer interfaces into an
// end-to-end migration, given two engines.
//
// The simple-mode flow:
//
//  1. Read the source schema.
//  2. Translate (currently identity; the dedicated translator layer
//     lands in a future commit when cross-engine type rewriting needs
//     to be policy-driven rather than rejected with a clear error).
//  3. Apply schema phase 1: tables without indexes or constraints.
//  4. Bulk-copy data, table by table.
//  5. Apply schema phase 2: indexes.
//  6. Apply schema phase 3: foreign keys.
//
// The package does not depend on any specific engine package; engines
// are passed in as [ir.Engine] values, typically resolved by the CLI
// from the engines registry.
//
// Output goes through [log/slog]. The CLI configures the default
// handler (level, destination) in cmd/sluice/main.go; this package
// emits structured key/value lines via slog.Default(). Tests that
// want to assert on log output swap the default handler with a
// buffer-backed one for the duration of the test.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// progressInterval is how often the bulk-copy progress ticker emits a
// line for a table that is actively receiving rows. Two seconds is the
// shortest cadence that still feels alive to a human watching tail -f
// without spamming aggregators on a many-table migration.
const progressInterval = 2 * time.Second

// Migrator runs a single simple-mode migration from Source/SourceDSN to
// Target/TargetDSN. Construct the value, then call Run with a context.
//
// Migrator does not retain state between Run calls — call it once per
// migration. Concurrent calls on the same value are not supported; if
// you want to run two migrations in parallel, instantiate two values.
type Migrator struct {
	// Source is the engine the source DSN belongs to (e.g. mysql,
	// postgres). Required.
	Source ir.Engine

	// Target is the engine the target DSN belongs to. May be the
	// same as Source for same-engine migrations. Required.
	Target ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// DryRun, when true, reads the source schema and prints what
	// would be applied without actually writing anything to the
	// target. Useful for verifying connectivity and previewing the
	// migration plan.
	DryRun bool

	// Mappings is the per-column type-override list from sluice.yaml.
	// Applied after ReadSchema and before the schema-write phase, so
	// the named columns reach the target with the requested IR type.
	// nil/empty disables the override step entirely.
	Mappings []config.Mapping

	// Filter selects which source tables participate in the
	// migration. Empty filter (zero value) keeps the previous
	// behaviour of migrating every table the source schema reader
	// returns. The filter is applied immediately after ReadSchema
	// and before any subsequent phase (schema apply, bulk copy,
	// indexes, constraints) so each phase consumes the pruned
	// schema implicitly.
	Filter TableFilter

	// Resume, when true, picks up where a previously-failed
	// migration with the same MigrationID left off. Reads the
	// per-target sluice_migrate_state row, branches on phase, and
	// skips work already recorded as complete. See
	// internal/pipeline/resume.go for the full design rationale.
	//
	// Default false preserves the v0.2.x semantics: a fresh run
	// against a target with no state row writes a new row and
	// runs every phase. A run against a target with an existing
	// non-complete state row errors out — operators must
	// explicitly opt into resume rather than silently overwriting.
	Resume bool

	// MigrationID is the stable identifier under which state is
	// persisted on the target. When empty, an ID is auto-derived
	// from source/target engine names plus DSN host info — same
	// shape Streamer.resolveStreamID uses for stream IDs. Operators
	// who need stable identity across DSN changes (DNS shifts, host
	// renames) should pass --migration-id explicitly.
	MigrationID string
}

// Run executes the migration. Returns nil on success or a wrapped
// error pointing at the phase that failed.
//
// Run honours ctx cancellation: if ctx is cancelled mid-migration,
// the underlying database operations return ctx.Err() and Run
// surfaces it. Partially-applied state on the target is the user's
// responsibility — for v1 there is no automatic rollback, since DDL
// in MySQL is implicit-commit and rollback after partial application
// is engine-dependent.
func (m *Migrator) Run(ctx context.Context) error {
	if err := m.validate(); err != nil {
		return err
	}

	// ---- 1. Open and read source schema ----
	sr, err := m.Source.OpenSchemaReader(ctx, m.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	defer closeIf(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "source schema has no tables; nothing to migrate")
		return nil
	}

	// ---- 1.25. Prune schema by table filter ----
	// Pruning here means every downstream phase (schema apply, bulk
	// copy, indexes, constraints) operates on the filtered set
	// implicitly — engines stay agnostic to the filter spec.
	if err := applyTableFilter(ctx, schema, m.Filter); err != nil {
		return err
	}

	// ---- 1.5. Apply per-column type-mapping overrides ----
	schema, err = translate.ApplyMappings(schema, m.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply mappings: %w", err)
	}

	if m.DryRun {
		return m.logPlan(ctx, schema)
	}

	// ---- 1.75. Open the migration-state store (if the target engine
	// supports it) and resolve the migration_id. ----
	rc, state, exitClean, err := m.openResumeContext(ctx)
	if err != nil {
		return err
	}
	if exitClean {
		// already-complete with --resume → log handled in
		// loadOrInitState; nothing else to do.
		return nil
	}
	if rc.enabled {
		defer closeIf(rc.store)
	}
	resuming := m.Resume && rc.enabled

	// ---- 2. Open target writers ----
	sw, err := m.Target.OpenSchemaWriter(ctx, m.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open target schema writer: %w", err)))
	}
	defer closeIf(sw)

	rw, err := m.Target.OpenRowWriter(ctx, m.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open target row writer: %w", err)))
	}
	defer closeIf(rw)

	// ---- 3-6. Schema apply (phase 1) → bulk copy → indexes → constraints.
	rr, err := m.Source.OpenRowReader(ctx, m.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open source row reader: %w", err)))
	}
	defer closeIf(rr)

	if resuming {
		logResumeStart(ctx, state, schema)
	}

	if err := runBulkCopyPhases(ctx, rc, &state, schema, rr, sw, rw, resuming); err != nil {
		return err
	}

	markComplete(ctx, rc, state)
	slog.InfoContext(ctx, "migration complete", slog.Int("tables", len(schema.Tables)))
	return nil
}

// openResumeContext resolves the migration_id, opens the per-target
// state store (when the engine supports it), and decides whether to
// short-circuit (already-complete + --resume) or proceed. The
// returned (resumeContext, state, exitClean) tuple carries every
// piece of state the rest of Run threads through phase boundaries.
//
// The only error path here is the "row found, refuse to overwrite"
// branch and the "store open / table ensure" failures; both surface
// before any target writers open so the operator gets a clean
// refusal rather than a half-open connection set.
func (m *Migrator) openResumeContext(ctx context.Context) (resumeContext, ir.MigrationState, bool, error) {
	store, err := openMigrationStateStore(ctx, m.Target, m.TargetDSN)
	if err != nil {
		return resumeContext{}, ir.MigrationState{}, false, wrapWithHint(PhaseConnect, err)
	}
	rc := resumeContext{
		store:       store,
		migrationID: m.resolveMigrationID(),
		enabled:     store != nil,
	}
	state, exitClean, err := loadOrInitState(ctx, rc, m.Resume)
	if err != nil {
		if rc.enabled {
			closeIf(rc.store)
		}
		return resumeContext{}, ir.MigrationState{}, false, err
	}
	return rc, state, exitClean, nil
}

// resolveMigrationID returns the operator-supplied MigrationID when
// non-empty, else an auto-derived value. Mirrors
// Streamer.resolveStreamID's contract; see [deriveMigrationID] for
// the hashing rationale.
func (m *Migrator) resolveMigrationID() string {
	if m.MigrationID != "" {
		return m.MigrationID
	}
	return deriveMigrationID(m.Source.Name(), m.SourceDSN, m.Target.Name(), m.TargetDSN)
}

// runBulkCopy applies the shared phases that follow target-writer
// open with no resume awareness: schema phase 1 (tables without
// constraints) → bulk-copy of every table → identity-sequence sync →
// schema phase 2 (indexes) → schema phase 3 (foreign keys). Used by
// the Streamer's cold-start path (which pre-dates the resume
// feature). [Migrator] uses [runBulkCopyPhases] instead so the
// per-phase boundaries can persist resume state.
//
// Phase 3.5 (identity-sequence sync) runs between bulk-copy and
// indexes so the next user-initiated INSERT against an identity
// column doesn't collide with bulk-copied IDs. Engines whose
// identity mechanism auto-bumps on direct INSERT (MySQL InnoDB)
// implement this as a no-op; the call costs nothing on those
// engines.
//
// Errors from any phase are wrapped with the phase name so the
// caller can pinpoint which step failed without parsing strings.
func runBulkCopy(
	ctx context.Context,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
) error {
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: create tables: %w", err))
	}
	for _, table := range schema.Tables {
		if err := copyTable(ctx, rows, rw, table); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", table.Name, err))
		}
	}
	if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: sync identity sequences: %w", err))
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		return wrapWithHint(PhaseIndexes, fmt.Errorf("pipeline: create indexes: %w", err))
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
	}
	return nil
}

// runBulkCopyPhases is the resume-aware variant of [runBulkCopy].
// Each of the five phases is a state-update boundary: state.Phase
// flips before the work runs, and on success the next iteration
// inherits the updated phase. On failure, [markFailed] persists the
// in-flight phase plus a truncated error message; the caller surfaces
// the original error.
//
// Resume semantics per phase:
//
//   - tables:       re-run unconditionally (idempotent CREATE TABLE).
//   - bulk_copy:    per-table classification (skip / truncate-redo /
//     fresh) keyed off state.TableProgress.
//   - identity_sync, indexes, constraints: re-run unconditionally.
//     Idempotency is best-effort here; a CREATE INDEX with a clashing
//     name will fail. Future iterations can pre-query catalog tables
//     and skip pre-existing entries.
func runBulkCopyPhases(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
	resuming bool,
) error {
	// Phase 1: tables.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseTables); err != nil {
		// Phase mark is non-fatal; continue with the data work.
		_ = err
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create tables: %w", err)
		return wrapWithHint(PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseTables, err))
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseTables)))

	// Phase 2: bulk-copy. Per-table state-row updates here so a mid-
	// phase failure preserves the partial progress map.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseBulkCopy); err != nil {
		_ = err
	}
	if state.TableProgress == nil {
		state.TableProgress = map[string]ir.TableProgressState{}
	}
	for _, table := range schema.Tables {
		action := classifyTableForResume(*state, table.Name, resuming)
		switch action {
		case resumeActionSkip:
			slog.InfoContext(ctx, "migration: skipping completed table",
				slog.String("table", table.Name))
			continue
		case resumeActionTruncate:
			slog.InfoContext(ctx, "migration: truncating in-progress table for resume",
				slog.String("table", table.Name))
			if err := truncateForResume(ctx, rw, table); err != nil {
				wrapped := fmt.Errorf("pipeline: truncate before resume: %w", err)
				return wrapWithHint(PhaseBulkCopy, markFailed(ctx, rc, *state, ir.MigrationPhaseBulkCopy, wrapped))
			}
		}

		// Mark this table in-progress before the copy starts so a
		// subsequent failure leaves the right breadcrumb.
		state.TableProgress[table.Name] = ir.TableProgressInProgress
		if err := writeState(ctx, rc, *state); err != nil {
			slog.WarnContext(ctx, "migration: per-table state write failed; continuing",
				slog.String("table", table.Name),
				slog.String("err", err.Error()))
		}

		if err := copyTable(ctx, rows, rw, table); err != nil {
			wrapped := fmt.Errorf("pipeline: copy table %q: %w", table.Name, err)
			return wrapWithHint(PhaseBulkCopy, markFailed(ctx, rc, *state, ir.MigrationPhaseBulkCopy, wrapped))
		}

		state.TableProgress[table.Name] = ir.TableProgressComplete
		if err := writeState(ctx, rc, *state); err != nil {
			slog.WarnContext(ctx, "migration: per-table state write failed; continuing",
				slog.String("table", table.Name),
				slog.String("err", err.Error()))
		}
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseBulkCopy)))

	// Phase 3.5: identity sync.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseIdentitySync); err != nil {
		_ = err
	}
	if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: sync identity sequences: %w", err)
		return wrapWithHint(PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseIdentitySync, err))
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseIdentitySync)))

	// Phase 4: indexes.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseIndexes); err != nil {
		_ = err
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create indexes: %w", err)
		return wrapWithHint(PhaseIndexes, markFailed(ctx, rc, *state, ir.MigrationPhaseIndexes, err))
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseIndexes)))

	// Phase 5: constraints.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseConstraints); err != nil {
		_ = err
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create constraints: %w", err)
		return wrapWithHint(PhaseConstraints, markFailed(ctx, rc, *state, ir.MigrationPhaseConstraints, err))
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseConstraints)))

	return nil
}

// validate checks that all required fields are populated. Errors here
// indicate caller bugs; surface them clearly before any I/O happens.
func (m *Migrator) validate() error {
	switch {
	case m.Source == nil:
		return errors.New("pipeline: Source engine is nil")
	case m.Target == nil:
		return errors.New("pipeline: Target engine is nil")
	case m.SourceDSN == "":
		return errors.New("pipeline: SourceDSN is empty")
	case m.TargetDSN == "":
		return errors.New("pipeline: TargetDSN is empty")
	}
	return nil
}

// copyTable opens the source-side row stream, hands it off to the
// target writer, and waits for completion. The reader's lifetime
// covers exactly one table; the writer is reused across tables.
//
// A [progressTicker] sits in the pipe between reader and writer: it
// counts every row the orchestrator hands to the writer and emits a
// slog line every [progressInterval]. Stop is called via defer so
// progress reporting terminates even on writer error.
func copyTable(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(ctx, progressInterval, table.Name)
	defer pt.Stop(ctx)

	teed := teeRows(ctx, rows, pt.inc)
	if err := rw.WriteRows(ctx, table, teed); err != nil {
		return fmt.Errorf("write rows: %w", err)
	}
	return nil
}

// logPlan writes a human-readable summary of what Run would do via
// slog, without performing any writes. Used when DryRun is true.
//
// The plan is logged at Info level so it surfaces under the default
// handler. The header line is a single message; the per-table lines
// follow with structured attributes so an aggregator can pick out
// individual table summaries without parsing prose.
func (m *Migrator) logPlan(ctx context.Context, schema *ir.Schema) error {
	slog.InfoContext(ctx, "dry run: migration plan",
		slog.String("source", m.Source.Name()),
		slog.String("target", m.Target.Name()),
		slog.Int("tables", len(schema.Tables)),
	)
	for _, t := range schema.Tables {
		// Field naming note: secondary_indexes excludes the primary
		// key (which is reported separately via primary_key) — the IR
		// stores PK on its own field, and operators comparing against
		// psql / SHOW INDEX output have been confused by a bare
		// "indexes" count that didn't include PK.
		slog.InfoContext(ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", len(t.Columns)),
			slog.Bool("primary_key", t.PrimaryKey != nil),
			slog.Int("secondary_indexes", len(t.Indexes)),
			slog.Int("foreign_keys", len(t.ForeignKeys)),
		)
	}
	return nil
}

// closeIf calls Close on v if it implements io.Closer. Used to clean
// up the *sql.DB handles the engine readers/writers wrap.
func closeIf(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}
