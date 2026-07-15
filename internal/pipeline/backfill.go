// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// `sluice backfill` orchestration (ADR-0159): the expand-contract
// "migrate" step. A same-database, keyset-chunked, resumable, online-
// safe in-place UPDATE — walk the table's primary key in bounded
// batches and issue one `UPDATE ... WHERE (pk-range) [AND (where)]`
// per batch, persisting the cursor after each committed chunk so a
// re-invocation with the same spec resumes where it left off.
//
// Engine-neutral: every per-engine operation goes through the
// [ir.BackfillExecutor] optional surface, type-asserted from the
// engine. Engines without it (SQLite/D1 today) are refused loudly
// with SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE.

package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Backfill pretty-view phases (ADR-0155). Backfill is a new command
// with no historical slog contract; the non-TTY path gets the
// summary slog lines this file emits, and this checklist drives only
// the interactive view.
var (
	backfillPhaseSchema = progress.Phase{Key: "schema", Label: "Schema"}
	backfillPhaseUpdate = progress.Phase{Key: "backfill", Label: "Backfill"}
)

// BackfillProgressSpec is the pretty-view [progress.Spec] for `sluice
// backfill`. The CLI hands it to [progress.NewTTYSink].
var BackfillProgressSpec = progress.Spec{
	Title:       "sluice backfill",
	Phases:      []progress.Phase{backfillPhaseSchema, backfillPhaseUpdate},
	ProgressKey: backfillPhaseUpdate.Key,
	LabelWidth:  12,
}

// backfillPhaseRunning is the migrate-state header phase while the
// chunk walk is in flight. It shares the header table's phase column
// with the migrate phases but never collides with a migrate run: the
// backfill migration-id namespace ("backfill:<table>:<spec-hash>") is
// disjoint from migrate's operator-chosen ids. Terminal states reuse
// [ir.MigrationPhaseComplete] / [ir.MigrationPhaseFailed] so ad-hoc
// inspection of the control table reads uniformly.
const backfillPhaseRunning ir.MigrationPhase = "backfill"

// Backfiller runs a single in-place backfill against one database.
// Same shape as [Verifier]: hold config, call Run.
type Backfiller struct {
	// Engine is the database's engine. Required.
	Engine ir.Engine

	// DSN is the ONE database the backfill runs inside (same-DB by
	// design: no cross-dialect translation, the --expr-override
	// posture). Required.
	DSN string

	// Table is the unqualified table name to backfill. Required.
	Table string

	// Sets are the parsed `--set` clauses. Required (at least one).
	Sets []ir.BackfillSet

	// Where is the optional verbatim native-SQL predicate scoping which
	// rows are backfilled. A predicate that self-describes doneness
	// (e.g. `new_col IS NULL`) is what makes re-runs and crash-replays
	// idempotent — see the ADR-0159 idempotency contract.
	Where string

	// BatchSize bounds each UPDATE to at most this many PK-walk rows.
	// Zero falls back to [migcore.DefaultBulkBatchSize] (zero-value-
	// safe: every construction that doesn't set it gets the default).
	BatchSize int

	// DryRun prints the generated chunk-UPDATE template plus the
	// CountRemaining estimate to Out and returns without writing
	// anything (no rows, no control table).
	DryRun bool

	// Restart clears the stored cursor/state for this exact spec and
	// starts the walk over from the beginning of the table.
	Restart bool

	// Verify runs the completion post-pass AFTER the walk (or after the
	// completed-spec no-op): one whole-table CountRemaining on Where.
	// Zero remaining is the explicit safe-to-contract signal; a nonzero
	// count is the coded SLUICE-E-BACKFILL-INCOMPLETE error. Requires a
	// non-empty Where — without a self-describing guard the count is
	// the whole table and the signal is meaningless.
	Verify bool

	// VerifyOnly skips the walk entirely — no UPDATEs, no control-table
	// reads/writes — and just runs the Verify count with the same exit
	// contract: the scriptable post-migration gate. Requires a
	// non-empty Where; Sets are optional (any given are still checked
	// against the schema). Contradicts DryRun and Restart.
	VerifyOnly bool

	// Out receives the --dry-run preview, the completed-no-op notice,
	// and the verify completion report. nil falls back to [io.Discard].
	Out io.Writer

	// Progress is the ADR-0155 presentation sink. nil is the
	// [progress.Nop] default.
	Progress progress.Sink
}

// BackfillResult is the structured outcome of a backfill run.
type BackfillResult struct {
	// Table is the backfilled table's name.
	Table string
	// RowsUpdated is the total affected-row count across every chunk
	// UPDATE this run (plus, on resume, the rows recorded by earlier
	// runs of the same spec).
	RowsUpdated int64
	// Chunks is the number of bounded UPDATE statements this run
	// executed.
	Chunks int
	// Remaining is the CountRemaining estimate taken BEFORE the walk —
	// the rows still matching Where at start (all rows when Where is
	// empty).
	Remaining int64
	// Resumed reports the walk continued from a persisted cursor.
	Resumed bool
	// AlreadyComplete reports the spec's stored state was terminal
	// `complete`: the run was a no-op and touched no rows.
	AlreadyComplete bool
	// Statement is the generated mid-walk chunk UPDATE (placeholders
	// symbolic). Always populated except under VerifyOnly, which
	// renders no UPDATE; the --dry-run preview prints it.
	Statement string

	// Verified reports the Verify/VerifyOnly post-pass ran and found
	// zero rows still matching Where — the safe-to-contract signal.
	Verified bool
	// VerifiedRemaining is the post-pass count. Always 0 on a returned
	// result by construction: a nonzero verify count surfaces as the
	// coded SLUICE-E-BACKFILL-INCOMPLETE error, not a result.
	VerifiedRemaining int64
}

// sink returns the presentation sink, defaulting a nil Progress to the
// no-op sink so every call-site is nil-free.
func (b *Backfiller) sink() progress.Sink {
	if b.Progress == nil {
		return progress.Nop{}
	}
	return b.Progress
}

// out returns the report writer, defaulting nil to io.Discard.
func (b *Backfiller) out() io.Writer {
	if b.Out == nil {
		return io.Discard
	}
	return b.Out
}

// ParseBackfillSets parses the CLI's repeatable `--set 'col = expr'`
// clauses. The split is at the FIRST '=' — everything after it is the
// verbatim native-SQL expression, so exprs containing '=' (CASE WHEN
// x = 1 ...) pass through intact. Column and expression must both be
// non-empty after trimming, and a column may appear only once (two
// assignments to one column is always an operator mistake — the second
// would silently win).
func ParseBackfillSets(specs []string) ([]ir.BackfillSet, error) {
	if len(specs) == 0 {
		return nil, errors.New("backfill: at least one --set 'col = expr' is required")
	}
	seen := make(map[string]struct{}, len(specs))
	out := make([]ir.BackfillSet, 0, len(specs))
	for _, spec := range specs {
		idx := strings.Index(spec, "=")
		if idx < 0 {
			return nil, fmt.Errorf("backfill: --set %q has no '=' (want 'col = <native SQL expr>')", spec)
		}
		col := strings.TrimSpace(spec[:idx])
		expr := strings.TrimSpace(spec[idx+1:])
		if col == "" {
			return nil, fmt.Errorf("backfill: --set %q has an empty column name", spec)
		}
		if expr == "" {
			return nil, fmt.Errorf("backfill: --set %q has an empty expression", spec)
		}
		if _, dup := seen[col]; dup {
			return nil, fmt.Errorf("backfill: column %q appears in more than one --set", col)
		}
		seen[col] = struct{}{}
		out = append(out, ir.BackfillSet{Column: col, Expr: expr})
	}
	return out, nil
}

// BackfillMigrationID derives the control-table migration id for a
// backfill spec: "backfill:<table>:<12-hex hash of the set + where
// clauses>". A re-invocation with the same spec resumes its cursor; a
// different spec (different sets or where) hashes to a different id
// and starts fresh. BatchSize is deliberately excluded — retuning the
// batch must not orphan an in-flight cursor.
func BackfillMigrationID(table string, sets []ir.BackfillSet, where string) string {
	h := sha256.New()
	for _, s := range sets {
		h.Write([]byte(s.Column))
		h.Write([]byte{0})
		h.Write([]byte(s.Expr))
		h.Write([]byte{0})
	}
	h.Write([]byte{0})
	h.Write([]byte(where))
	return "backfill:" + table + ":" + hex.EncodeToString(h.Sum(nil))[:12]
}

// Run executes the backfill. On operational failure or refusal it
// returns (nil, error); refusals carry a [sluicecode.CodedError]. On
// success the result reports what happened (including the
// AlreadyComplete no-op shape).
func (b *Backfiller) Run(ctx context.Context) (*BackfillResult, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	batch := b.BatchSize
	if batch <= 0 {
		batch = migcore.DefaultBulkBatchSize
	}
	sink := b.sink()
	sink.PhaseStarted(backfillPhaseSchema)

	// The unsupported-engine check runs BEFORE the table resolves, so
	// an engine with no backfill surface always gets the coded refusal
	// — even when the table is also missing (a schema read would
	// otherwise err first with an uncoded table-not-found).
	opener, ok := b.Engine.(ir.BackfillExecutorOpener)
	if !ok {
		return nil, sluicecode.Wrap(
			sluicecode.CodeBackfillUnsupportedEngine,
			"use --driver mysql, planetscale, vitess, or postgres",
			fmt.Errorf("backfill: engine %q does not support in-place backfill (no ir.BackfillExecutor implementation)", b.Engine.Name()),
		)
	}
	table, err := b.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	ex, err := opener.OpenBackfillExecutor(ctx, b.DSN)
	if err != nil {
		return nil, fmt.Errorf("backfill: open executor: %w", err)
	}
	defer func() { _ = ex.Close() }()
	sink.PhaseCompleted(backfillPhaseSchema)

	if b.VerifyOnly {
		// The standalone post-migration gate: no walk, no control-table
		// reads/writes — just the whole-table remaining-count on the
		// guard and the 0-clean / >0-coded-error exit contract.
		result := &BackfillResult{Table: b.Table}
		if err := b.verifyComplete(ctx, ex, table, result); err != nil {
			return nil, err
		}
		sink.Summary(progress.Result{Fields: []progress.Field{
			{Label: "Table", Value: result.Table},
			{Label: "Verified", Value: "0 rows match the --where guard"},
		}})
		return result, nil
	}

	// CountRemaining doubles as the where-predicate preflight: an
	// unparsable --where fails HERE, before any UPDATE runs.
	remaining, err := ex.CountRemaining(ctx, table, b.Where)
	if err != nil {
		return nil, fmt.Errorf("backfill: count remaining (is --where valid SQL for this engine?): %w", err)
	}
	stmt, err := ex.BackfillStatement(table, b.Sets, b.Where)
	if err != nil {
		return nil, fmt.Errorf("backfill: render statement: %w", err)
	}
	result := &BackfillResult{Table: b.Table, Remaining: remaining, Statement: stmt}

	if b.DryRun {
		fmt.Fprintf(b.out(),
			"-- sluice backfill --dry-run (nothing written)\n"+
				"-- per-chunk statement (PK bounds bind per batch; the first chunk omits the lower bound):\n"+
				"%s\n"+
				"-- estimated rows matching: %d\n",
			stmt, remaining)
		return result, nil
	}

	sink.PhaseStarted(backfillPhaseUpdate)
	if err := b.runWalk(ctx, ex, table, batch, remaining, result); err != nil {
		return nil, err
	}
	sink.PhaseCompleted(backfillPhaseUpdate)
	if b.Verify {
		// The post-pass runs AFTER the walk (and after the completed-
		// spec no-op) so it sees the WHOLE table, not just the walked
		// range. A failed verify does NOT mark the migration state
		// failed: the walk itself succeeded and its persisted work
		// stands — the count is the online catch-up signal.
		if err := b.verifyComplete(ctx, ex, table, result); err != nil {
			return nil, err
		}
	}
	fields := []progress.Field{
		{Label: "Table", Value: result.Table},
		{Label: "Rows updated", Value: progress.HumanCount(result.RowsUpdated)},
		{Label: "Chunks", Value: progress.HumanCount(int64(result.Chunks))},
	}
	if result.Verified {
		fields = append(fields, progress.Field{Label: "Verified", Value: "0 rows match the --where guard"})
	}
	sink.Summary(progress.Result{Fields: fields})
	return result, nil
}

// verifyComplete is the --verify/--verify-only completion gate: one
// whole-table CountRemaining on the Where guard. Zero rows is the
// explicit "safe to run the contract step" signal; a nonzero count is
// the coded SLUICE-E-BACKFILL-INCOMPLETE error — rows written behind
// the walk's cursor during an online run (or since a completed run)
// still need a catch-up pass, and on a quiesced database a nonzero
// count after a clean walk means the guard does not self-describe
// doneness. Runtime class, not a refusal: the check ran truthfully and
// found incomplete work (the SLUICE-E-BACKUP-INCOMPLETE analogue).
func (b *Backfiller) verifyComplete(ctx context.Context, ex ir.BackfillExecutor, table *ir.Table, result *BackfillResult) error {
	n, err := ex.CountRemaining(ctx, table, b.Where)
	if err != nil {
		return fmt.Errorf("backfill: verify count remaining (is --where valid SQL for this engine?): %w", err)
	}
	if n > 0 {
		return sluicecode.Wrap(
			sluicecode.CodeBackfillIncomplete,
			"re-run the backfill to pick up the stragglers (a completed spec needs --restart), then verify again; on a quiesced database, fix the --where guard so it self-describes doneness",
			fmt.Errorf("backfill verify: %d row(s) of %q still match the --where guard (%s) — rows written behind the walk's cursor (or since a completed run) need a catch-up pass",
				n, b.Table, b.Where),
		)
	}
	result.Verified = true
	result.VerifiedRemaining = 0
	fmt.Fprintf(b.out(),
		"backfill verified complete: 0 rows of %q match the --where guard (%s) — safe to run the contract step (drop/rename the old column)\n",
		b.Table, b.Where)
	slog.InfoContext(ctx, "backfill verified complete",
		"table", b.Table, "where", b.Where, "remaining", int64(0))
	return nil
}

// runWalk owns the resume-state lifecycle plus the chunk loop.
func (b *Backfiller) runWalk(ctx context.Context, ex ir.BackfillExecutor, table *ir.Table, batch int, remaining int64, result *BackfillResult) error {
	opener, ok := b.Engine.(ir.MigrationStateStoreOpener)
	if !ok {
		// Both shipping executors' engines expose the store; an engine
		// growing BackfillExecutor without it is a development error,
		// not an operator-remediable refusal.
		return fmt.Errorf("backfill: engine %q implements ir.BackfillExecutor but not ir.MigrationStateStoreOpener; the resumable cursor requires both", b.Engine.Name())
	}
	store, err := opener.OpenMigrationStateStore(ctx, b.DSN)
	if err != nil {
		return fmt.Errorf("backfill: open migrate-state store: %w", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	migID := BackfillMigrationID(b.Table, b.Sets, b.Where)
	if b.Restart {
		if err := store.ClearMigration(ctx, migID); err != nil {
			return fmt.Errorf("backfill: --restart clear state: %w", err)
		}
	}
	state, found, err := store.Read(ctx, migID)
	if err != nil {
		return fmt.Errorf("backfill: read state: %w", err)
	}

	var cursor []any
	var rowsUpdated int64
	if found {
		if state.Phase == ir.MigrationPhaseComplete {
			// A completed spec re-run is a no-op by contract: report and
			// touch no rows. --restart is the explicit start-over.
			result.AlreadyComplete = true
			fmt.Fprintf(b.out(),
				"backfill of %q with this exact spec already completed (state %s); nothing to do — pass --restart to run it again\n",
				b.Table, migID)
			slog.InfoContext(ctx, "backfill: already complete; no-op",
				"table", b.Table, "migration_id", migID)
			return nil
		}
		if tp, ok := state.TableProgress[b.Table]; ok && len(tp.LastPK) > 0 {
			cursor = tp.LastPK
			rowsUpdated = tp.RowsCopied
			result.Resumed = true
			slog.InfoContext(ctx, "backfill: resuming from persisted cursor",
				"table", b.Table, "migration_id", migID,
				"rows_updated_previously", rowsUpdated)
		}
	}
	if err := store.Write(ctx, ir.MigrationState{MigrationID: migID, Phase: backfillPhaseRunning}); err != nil {
		return fmt.Errorf("backfill: write state header: %w", err)
	}

	sink := b.sink()
	for {
		if err := ctx.Err(); err != nil {
			return b.failWalk(ctx, store, migID, err)
		}
		upper, more, err := ex.NextChunkUpperBound(ctx, table, cursor, batch)
		if err != nil {
			return b.failWalk(ctx, store, migID, fmt.Errorf("next chunk bound: %w", err))
		}
		if !more {
			break
		}
		n, err := ex.ExecBackfillChunk(ctx, table, b.Sets, b.Where, cursor, upper)
		if err != nil {
			return b.failWalk(ctx, store, migID, fmt.Errorf("chunk update: %w", err))
		}
		rowsUpdated += n
		result.Chunks++
		// Persist the cursor only AFTER the chunk committed. A crash in
		// the gap re-executes one chunk on resume; the operator's
		// self-describing --where guard is what makes that replay a
		// no-op (the ADR-0159 idempotency contract).
		if err := store.WriteTableProgress(ctx, migID, b.Table, ir.TableProgress{
			State:      ir.TableProgressInProgress,
			LastPK:     upper,
			RowsCopied: rowsUpdated,
		}); err != nil {
			return b.failWalk(ctx, store, migID, fmt.Errorf("persist cursor: %w", err))
		}
		cursor = upper
		sink.TableProgress(b.Table, rowsUpdated, remaining)
		slog.DebugContext(ctx, "backfill: chunk applied",
			"table", b.Table, "rows_updated", n, "rows_updated_total", rowsUpdated)
	}

	if err := store.WriteTableProgress(ctx, migID, b.Table, ir.TableProgress{
		State:      ir.TableProgressComplete,
		RowsCopied: rowsUpdated,
	}); err != nil {
		return b.failWalk(ctx, store, migID, fmt.Errorf("persist completion: %w", err))
	}
	if err := store.Write(ctx, ir.MigrationState{MigrationID: migID, Phase: ir.MigrationPhaseComplete}); err != nil {
		return fmt.Errorf("backfill: mark complete: %w", err)
	}
	result.RowsUpdated = rowsUpdated
	slog.InfoContext(ctx, "backfill complete",
		"table", b.Table, "migration_id", migID,
		"rows_updated", rowsUpdated, "chunks", result.Chunks)
	return nil
}

// failWalk marks the header failed (best-effort, on a cancel-immune
// context so a Ctrl-C still records why the run stopped) and returns
// the original error.
func (b *Backfiller) failWalk(ctx context.Context, store ir.MigrationStateStore, migID string, cause error) error {
	writeCtx := context.WithoutCancel(ctx)
	if err := store.Write(writeCtx, ir.MigrationState{
		MigrationID: migID,
		Phase:       ir.MigrationPhaseFailed,
		LastError:   truncateLastError(cause.Error()),
	}); err != nil {
		slog.WarnContext(writeCtx, "backfill: could not record failure state", "err", err)
	}
	return fmt.Errorf("backfill: %w", cause)
}

// resolveTable reads the schema, locates Table, and runs the coded
// refusals: unknown --set column, and no/non-orderable primary key.
func (b *Backfiller) resolveTable(ctx context.Context) (*ir.Table, error) {
	table, err := lookupBackfillTable(ctx, b.Engine, b.DSN, b.Table)
	if err != nil {
		return nil, err
	}
	for _, s := range b.Sets {
		if migcore.LookupColumn(table, s.Column) == nil {
			return nil, sluicecode.Wrap(
				sluicecode.CodeBackfillUnknownColumn,
				"check the --set column name against the table's columns",
				fmt.Errorf("backfill: table %q has no column %q (columns: %s)",
					b.Table, s.Column, strings.Join(columnNamesOf(table), ", ")),
			)
		}
	}
	// VerifyOnly issues no bounded UPDATEs, so the keyset walk's PK
	// requirements don't apply: a no-PK table is still verifiable.
	if !b.VerifyOnly {
		if err := validateBackfillPK(table); err != nil {
			return nil, err
		}
	}
	return table, nil
}

// lookupBackfillTable reads the schema at dsn and locates table.
func lookupBackfillTable(ctx context.Context, engine ir.Engine, dsn, table string) (*ir.Table, error) {
	sr, err := engine.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("backfill: open schema reader: %w", err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("backfill: read schema: %w", err)
	}
	for _, t := range schema.Tables {
		if t.Name == table {
			return t, nil
		}
	}
	return nil, fmt.Errorf("backfill: table %q not found in the database (%d table(s) visible)", table, len(schema.Tables))
}

// ResolveBackfillTable is the walk-eligibility preflight as a
// standalone: table exists + a usable orderable primary key — the
// coded refusals a keyset backfill needs answered BEFORE anything
// irreversible happens. `sluice expand-contract` (ADR-0162) runs it
// up front so a doomed run refuses before any branch is created; it
// deliberately does NOT check --set column existence, because the
// expand leg is what creates those columns.
func ResolveBackfillTable(ctx context.Context, engine ir.Engine, dsn, table string) (*ir.Table, error) {
	t, err := lookupBackfillTable(ctx, engine, dsn, table)
	if err != nil {
		return nil, err
	}
	if err := validateBackfillPK(t); err != nil {
		return nil, err
	}
	return t, nil
}

// validateBackfillPK refuses tables whose primary key cannot drive the
// keyset walk: absent, sluice-injected, or non-orderable — the same
// eligibility reasoning [migcore.CanParallelChunkTable] applies to the
// chunked copy, minus the boundary precompute (the walk discovers
// bounds as it goes).
func validateBackfillPK(table *ir.Table) error {
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return sluicecode.Wrap(
			sluicecode.CodeBackfillNoPrimaryKey,
			"add a primary key, or run the transform manually — a keyset backfill cannot bound its UPDATEs without one",
			fmt.Errorf("backfill: table %q has no primary key — a keyset-chunked backfill is unsafe without one", table.Name),
		)
	}
	for _, pkc := range table.PrimaryKey.Columns {
		col := migcore.LookupColumn(table, pkc.Column)
		if col == nil {
			return fmt.Errorf("backfill: table %q primary-key column %q not found in column list", table.Name, pkc.Column)
		}
		if col.SluiceInjected || !migcore.IsOrderablePKType(col.Type) {
			return sluicecode.Wrap(
				sluicecode.CodeBackfillNoPrimaryKey,
				"the PK must be orderable (integer/string/binary/decimal/temporal/UUID) to drive the keyset walk",
				fmt.Errorf("backfill: table %q primary-key column %q (%s) cannot drive an ordered keyset walk",
					table.Name, pkc.Column, col.Type.String()),
			)
		}
	}
	return nil
}

// columnNamesOf lists a table's column names for the unknown-column
// refusal message.
func columnNamesOf(table *ir.Table) []string {
	out := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		out[i] = c.Name
	}
	return out
}

func (b *Backfiller) validate() error {
	switch {
	case b.Engine == nil:
		return errors.New("backfill: Engine is required")
	case b.DSN == "":
		return errors.New("backfill: DSN is required")
	case b.Table == "":
		return errors.New("backfill: Table is required")
	case len(b.Sets) == 0 && !b.VerifyOnly:
		return errors.New("backfill: at least one Set is required")
	case (b.Verify || b.VerifyOnly) && b.Where == "":
		return errors.New("backfill: --verify/--verify-only require --where — without a self-describing guard the remaining-count is the whole table and the completion signal is meaningless")
	case b.VerifyOnly && b.DryRun:
		return errors.New("backfill: --verify-only and --dry-run are contradictory (a verify-only run already writes nothing)")
	case b.VerifyOnly && b.Restart:
		return errors.New("backfill: --verify-only and --restart are contradictory (--verify-only never walks; run the backfill with --restart, then verify)")
	case b.Verify && b.DryRun:
		return errors.New("backfill: --verify and --dry-run are contradictory (a dry run writes nothing, so there is nothing to verify)")
	}
	return nil
}
