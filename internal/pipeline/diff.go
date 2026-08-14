// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Schema-diff orchestration for the `sluice schema diff` CLI
// (ADR-0029). Reads the source schema, applies the translation
// pipeline (filter + per-column type-mapping overrides) to produce
// the *expected* shape on the target, reads the *actual* shape from
// the target's SchemaReader, then runs an IR-level diff and renders
// the result — text (with copy-paste DDL suggestions) or JSON.
//
// Engine-neutral: every engine-specific operation goes through
// ir.Engine. Identifier-quoting differences are handled by an engine-
// name switch in the text renderer; this is intentional (the diff is
// a read-only inspection tool, not a migration writer) and avoids
// growing a new ir surface for the same job.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"
)

// Differ runs a single schema-diff against the configured source/
// target pair. Same shape as [Previewer]: hold config, call Run.
type Differ struct {
	// Source / Target are the engines the source/target DSNs belong
	// to. Required.
	Source ir.Engine
	Target ir.Engine

	// SourceDSN is the source-engine-native DSN. ReadSchema is run
	// against it to derive the expected target shape (after
	// translation). Required.
	SourceDSN string

	// TargetDSN is the target-engine-native DSN. ReadSchema is run
	// against it to capture the actual on-target shape. Required.
	TargetDSN string

	// Mappings is the per-column type-override list, applied to the
	// source schema before computing the diff. Empty disables the
	// override step.
	Mappings []config.Mapping

	// ExpressionMappings is the per-column generated-expression
	// override list. Applied alongside Mappings so the diff compares
	// what migrate would actually emit (overridden bodies and all).
	ExpressionMappings []config.ExpressionMapping

	// Filter selects which source tables participate. Empty (zero
	// value) keeps every source table the reader returns.
	Filter migcore.TableFilter

	// ViewFilter selects which source views participate in the
	// diff. Empty keeps every view; SkipViews=true drops them all.
	ViewFilter ViewFilter

	// SkipViews drops every source view before computing the diff.
	// Useful when the operator manages views out-of-band and
	// considers any target-side view drift as not-sluice's-concern.
	SkipViews bool

	// Format is "text" (default) or "json". Empty defaults to "text".
	Format string

	// IgnoreCharsetCollation suppresses MySQL-specific charset/
	// collation diffs. Reserved for the v1.x extension when those
	// fields land in the IR; today's IR doesn't compare them at the
	// diff layer, so the flag is plumbed for forward compatibility
	// and surfaced in the text output's preamble.
	IgnoreCharsetCollation bool

	// IgnoreExtras suppresses "extra on target" diffs (tables and
	// columns/indexes present on actual but absent from expected).
	// Useful when the target hosts other applications' tables.
	IgnoreExtras bool

	// Out is the destination for the rendered diff. Required.
	Out io.Writer

	// TargetSchema is the per-source target-schema namespace
	// (ADR-0031). When set, the diff's target-side schema reader is
	// pinned to this schema, the missing-on-target DDL suggestions
	// render schema-qualified, and the comparison runs against the
	// target's per-source namespace rather than its DSN default.
	// PG-only.
	TargetSchema string

	// EnabledPGExtensions is the operator's `--enable-pg-extension`
	// allowlist (ADR-0032). PG → PG only. Threaded through both the
	// source and target SchemaReaders so the diff sees extension-
	// owned types as IR ExtensionType on both sides; the comparison
	// then matches target-side `vector(384)` against the expected
	// shape correctly. Empty preserves the pre-v0.26.0 behaviour.
	EnabledPGExtensions []string

	// InjectShardColumn, when engaged, applies the ADR-0048 Shape A
	// IR pass to the source-side expected schema BEFORE the diff
	// comparison runs. Combined with the [ir.Column.SluiceInjected]
	// suppression in [irdiff.Schemas], this lets `schema diff`
	// against a consolidated Shape-A target report "in sync" rather
	// than surface the discriminator as drift on every run.
	InjectShardColumn ShardColumnSpec
}

// DiffJSON is the JSON-format diff output. The shape is stable for
// tooling consumers (CI gates, dashboards). Adding fields is
// backward-compatible; renaming or removing them is not.
type DiffJSON struct {
	SourceEngine string         `json:"source_engine"`
	TargetEngine string         `json:"target_engine"`
	Summary      DiffJSONCounts `json:"summary"`
	irdiff.SchemaDiff
}

// DiffJSONCounts is the high-level rollup the CI consumer looks at
// first. Per-table breakdowns live in the embedded SchemaDiff.
// Every count here is derived in [summarise] and pinned against the
// renderer by TestDiffSurfaceRosterEveryTableDiffFieldIsRenderedAndCounted
// — a TableDiff/SchemaDiff field that reaches neither this rollup nor the
// text output fails that gate rather than silently reading zero.
type DiffJSONCounts struct {
	TablesMissing     int `json:"tables_missing"`
	TablesExtra       int `json:"tables_extra"`
	TablesMismatched  int `json:"tables_mismatched"`
	ColumnsMissing    int `json:"columns_missing"`
	ColumnsExtra      int `json:"columns_extra"`
	ColumnsMismatched int `json:"columns_mismatched"`
	IndexesMissing    int `json:"indexes_missing"`
	IndexesExtra      int `json:"indexes_extra"`
	IndexesMismatched int `json:"indexes_mismatched"`
	ChecksMissing     int `json:"checks_missing"`
	ChecksExtra       int `json:"checks_extra"`
	ChecksMismatched  int `json:"checks_mismatched"`

	// Foreign-key counts (audit 2026-08-04 C-7 / Bug 227). The v0.112.0
	// FK work wired the comparison and HasChanges but neither the text
	// renderer nor this rollup, so a CI consumer keying off `summary` read
	// zeros while the command exited 1.
	ForeignKeysMissing    int `json:"foreign_keys_missing"`
	ForeignKeysExtra      int `json:"foreign_keys_extra"`
	ForeignKeysMismatched int `json:"foreign_keys_mismatched"`

	// ForeignKeysUnnamed is a COVERAGE figure, not a drift figure: the
	// number of foreign keys the comparison could not match on either side
	// because they carry no constraint name. Surfaced so a zero-drift
	// report cannot quietly mean "nothing was compared". Both shipping
	// engines auto-name FK constraints, so this is normally zero.
	ForeignKeysUnnamed int `json:"foreign_keys_unnamed"`

	// EXCLUDE counts. Same omission as the FK block above and found by the
	// same sweep — ADR-0053 added the comparison, no render, no count.
	ExcludesMissing    int `json:"excludes_missing"`
	ExcludesExtra      int `json:"excludes_extra"`
	ExcludesMismatched int `json:"excludes_mismatched"`

	// Row-level-security counts (audit 2026-08-04 B-10). RLSMismatched is
	// a count of TABLES whose ENABLE/FORCE flags disagree, not of flags.
	RLSMismatched      int `json:"rls_mismatched"`
	PoliciesMissing    int `json:"policies_missing"`
	PoliciesExtra      int `json:"policies_extra"`
	PoliciesMismatched int `json:"policies_mismatched"`

	ViewsMissing    int `json:"views_missing"`
	ViewsExtra      int `json:"views_extra"`
	ViewsMismatched int `json:"views_mismatched"`

	// Standalone-sequence counts (audit 2026-08-04 B-10).
	SequencesMissing    int `json:"sequences_missing"`
	SequencesExtra      int `json:"sequences_extra"`
	SequencesMismatched int `json:"sequences_mismatched"`
}

// Run executes the diff. Returns the computed diff plus an error.
// On success the diff is non-nil; on failure (couldn't read either
// schema, render error) the diff is nil and err describes the
// failure. The caller's CLI layer maps the (diff, err) tuple onto
// the ADR-0029 exit codes.
func (d *Differ) Run(ctx context.Context) (*irdiff.SchemaDiff, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	if err := migcore.ValidateTargetSchema(d.Target, d.TargetSchema); err != nil {
		return nil, err
	}
	if err := validateEnabledPGExtensions(d.Source, d.Target, d.EnabledPGExtensions); err != nil {
		return nil, err
	}

	// ---- 1. Read source schema ----
	sr, err := d.Source.OpenSchemaReader(ctx, d.SourceDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: open source schema reader: %w", err))
	}
	defer migcore.CloseIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, d.EnabledPGExtensions); err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: enable PG extensions on source: %w", err))
	}

	// Engine-default exclusions (Bug 22): same shape as Migrator and
	// Streamer — merge engine-supplied patterns (e.g. PlanetScale's
	// `_vt_*`) when the operator is in exclude-or-no-filter mode.
	// Computed before the read so the Bug-76 scope push-down (below)
	// matches the authoritative post-read prune.
	if eff, added := migcore.EffectiveTableFilter(d.Filter, d.Source, d.SourceDSN); len(added) > 0 {
		slog.InfoContext(
			ctx, "applying engine-default table exclusions",
			slog.String("engine", d.Source.Name()),
			slog.Any("patterns", added),
		)
		d.Filter = eff
	}

	// catalog Bug 76: scope per-column type validation to the filtered
	// table set before the schema scan.
	migcore.ApplyTableScope(sr, d.Filter)

	srcSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: read source schema: %w", err))
	}
	if len(srcSchema.Tables) == 0 {
		return nil, errors.New("diff: source schema has no tables")
	}

	if err := migcore.ApplyTableFilter(ctx, srcSchema, d.Filter); err != nil {
		return nil, err
	}
	if err := migcore.PreflightTableReads(sr, srcSchema); err != nil {
		return nil, err
	}
	applyViewFilter(ctx, srcSchema, d.ViewFilter, d.SkipViews)

	expected, err := translate.ApplyMappings(srcSchema, d.Mappings)
	if err != nil {
		return nil, fmt.Errorf("diff: apply mappings: %w", err)
	}
	expected, err = translate.ApplyExpressionOverrides(expected, d.ExpressionMappings)
	if err != nil {
		return nil, fmt.Errorf("diff: apply expression overrides: %w", err)
	}
	// ADR-0048 Shape A: when the operator's diff targets a
	// consolidated target, run the IR pass on the source-side
	// expected schema so the discriminator + composite PK are part
	// of the comparison. Without this, the diff would report the
	// discriminator as "extra on target" and the composite PK
	// shape as "PK mismatch" on every run.
	if d.InjectShardColumn.Engaged() {
		expected, err = translate.InjectShardColumn(expected, d.InjectShardColumn.Name, ir.Varchar{Length: 64})
		if err != nil {
			return nil, fmt.Errorf("diff: inject shard column: %w", err)
		}
	}

	// Cross-engine retarget: rewrite source-native IR types to their
	// target-engine emit equivalents (PG uuid → CHAR(36), inet →
	// VARCHAR(45), etc.) so the IR comparison below sees the actual
	// target storage shape, not the un-translated source type. Same-
	// engine pairs are identity. Mappings already ran above, so any
	// operator-supplied --type-override has already replaced the IR
	// type and the retarget pattern match doesn't fire.
	//
	// TWO retargets, because this command does two different things with
	// the result (roadmap item 153). The COMPARISON side reads a PG
	// `CREATE DOMAIN` wrapper through its storage type, since that is
	// what a MySQL catalog reads back — without it the diff reports
	// phantom drift on every domain column of a target sluice itself
	// created. The DDL-SUGGESTION side (step 4) keeps the wrapper: the
	// target engine's emitter translates a domain's CHECKs into inline
	// table CHECKs from Column.Type, so a flattened schema would suggest
	// a CREATE TABLE missing constraints the migrate path does emit.
	expectedDDL := translate.RetargetForEngine(expected, d.Source.Name(), d.Target.Name())
	sourceShaped := expected
	expected = translate.RetargetForShapeCompare(expected, d.Source.Name(), d.Target.Name())

	// ---- 2. Read target's actual schema via the same SchemaReader
	// surface (ADR-0029). The reader doesn't care whether a DSN points
	// at a "source" or a "target".
	tr, err := d.Target.OpenSchemaReader(ctx, d.TargetDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: open target schema reader: %w", err))
	}
	migcore.ApplyTargetSchema(tr, d.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, tr, d.EnabledPGExtensions); err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: enable PG extensions on target: %w", err))
	}
	defer migcore.CloseIf(tr)

	actual, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: read target schema: %w", err))
	}

	// ---- 2b. Teach the expected side about the CHECK constraints the
	// TARGET's own emitter synthesizes (Bug 237(b) / roadmap item 156).
	//
	// A MySQL `SET` column lands on Postgres as TEXT[] plus a membership
	// CHECK; a PG DOMAIN's translatable CHECKs land on MySQL as inline
	// table CHECKs. Neither is on the source, so the expected side had
	// nowhere to put them and the diff reported every one as extra on a
	// target `migrate` itself created.
	//
	// The predictor is asked with the SOURCE-shaped schema, before the
	// shape-compare retarget: it dispatches on the source construct
	// (`ir.Set`, `ir.Domain`, a generated `ir.Enum`) and the retarget has
	// by then replaced exactly those types with the storage the target
	// holds.
	//
	// A failure to OPEN the writer is deliberately not fatal here, and
	// that is a decision rather than laxity: before this step the writer
	// was opened LAZILY, only when there was drift to render DDL for, so
	// a target the operator can read but not open a writer against ran a
	// clean `schema diff` fine. Making the open fatal would break that
	// working configuration to add a report improvement. The error is
	// carried to [previewMissingDDL], which is where it WAS fatal and
	// still is — so no previously-failing case quietly starts passing
	// either.
	sw, swErr := d.openTargetSchemaWriter(ctx)
	if swErr != nil {
		slog.WarnContext(ctx,
			"diff: could not open a target schema writer; CHECK constraints sluice's own DDL emitter "+
				"synthesizes (a MySQL SET column's membership CHECK, a PG DOMAIN's inlined CHECKs) will be "+
				"reported as extra on target because the expected side cannot know about them",
			slog.String("engine", d.Target.Name()),
			slog.String("error", swErr.Error()))
	} else {
		defer migcore.CloseIf(sw)
		expected = attachEmittedChecks(expected, sourceShaped, sw)
	}

	// ---- 3. Compute the diff. ----
	//
	// Charset/collation joins the comparison only for a same-storage-
	// family pair, mirroring the migrate pre-create shape gate's rule
	// ("cross-engine pairs keep charset out — translation noise, not
	// drift", migrate_existing_tables.go). [irdiff.diffColumn]'s
	// "empty-on-source means no opinion" rule already covered the
	// postgres→mysql direction; its mirror — a MySQL source's `utf8mb4`
	// against a Postgres or SQLite target, which have no per-column
	// charset to expose — was never written, so every text column on
	// every mysql→postgres pair reported permanent drift (Bug 234's
	// sibling sweep). Same-family pairs still compare, so a genuine
	// `latin1` vs `utf8mb4` or a PG `COLLATE "C"` change surfaces.
	//
	// Row-level security joins the comparison only when the TARGET can
	// hold it. RLS is Postgres-only — the PG reader is the only one that
	// populates the flags and the policies, and MySQL's SchemaWriter WARNs
	// once and creates the table without them — so a PG→MySQL pair
	// `migrate` itself created reported `RLSMismatched` plus every policy
	// as missing, forever, with no target-side action able to close it
	// (Bug 234's deferred list). Keyed on the already-declared
	// [ir.Capabilities.PostgresBackend] rather than an engine name, so a
	// future PG-family flavor inherits the comparison by declaration.
	sameFamily := translate.SameStorageShapeFamily(d.Source.Name(), d.Target.Name())
	// DIFF-2 translator pairs: when the target's schema writer exposes
	// its own dialect translation (the same rewrite migrate applied),
	// thread it into the CHECK comparison so a rewritten construct
	// (json_extract → ->, date_format → to_char) compares as what the
	// target actually holds. A writer-less degraded diff (sw == nil
	// after an open refusal) leaves it nil — those pairs then
	// phantom-report, the stated degraded mode.
	var translateExpected func(expr, dialect string) (string, bool)
	if xlat, ok := sw.(ir.CheckExprDialectTranslator); ok {
		translateExpected = xlat.TranslateCheckExprFromDialect
	}
	diff := irdiff.Schemas(expected, actual, irdiff.Options{
		IgnoreExtras:                     d.IgnoreExtras,
		IgnoreCharsetCollation:           d.IgnoreCharsetCollation || !sameFamily,
		TargetCannotHoldRowLevelSecurity: !d.Target.Capabilities().PostgresBackend,
		TranslateExpectedCheckExpr:       translateExpected,
	})

	// ---- 4. Resolve missing-table DDL via the target engine's
	// PreviewDDL surface so the text renderer can include real CREATE
	// TABLE suggestions (MySQL/PG syntax) rather than a generic
	// placeholder. PreviewDDL is optional; engines without it fall
	// through to a simple comment.
	missingDDL, missingColDDL, err := previewMissingDDL(ctx, sw, swErr, expectedDDL, diff)
	if err != nil {
		return nil, err
	}

	// ---- 5. Render. ----
	switch strings.ToLower(strings.TrimSpace(d.Format)) {
	case "", "text":
		if err := renderDiffText(d.Out, diffBundle{
			srcEngine:     d.Source.Name(),
			tgtEngine:     d.Target.Name(),
			tgtDialect:    d.Target.Capabilities().DDLDialect,
			diff:          diff,
			missingDDL:    missingDDL,
			missingColDDL: missingColDDL,
			expected:      expected,
			actual:        actual,
			opts:          diffRenderOpts{IgnoreCharsetCollation: d.IgnoreCharsetCollation, IgnoreExtras: d.IgnoreExtras},
		}); err != nil {
			return nil, err
		}
	case "json":
		if err := renderDiffJSON(d.Out, d.Source.Name(), d.Target.Name(), diff); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("diff: unknown --format %q (recognised: text, json)", d.Format)
	}
	return &diff, nil
}

func (d *Differ) validate() error {
	switch {
	case d.Source == nil:
		return errors.New("diff: Source engine is nil")
	case d.Target == nil:
		return errors.New("diff: Target engine is nil")
	case d.SourceDSN == "":
		return errors.New("diff: SourceDSN is empty")
	case d.TargetDSN == "":
		return errors.New("diff: TargetDSN is empty")
	case d.Out == nil:
		return errors.New("diff: Out writer is nil")
	}
	return nil
}

// diffBundle gathers the data the text renderer consumes. Mirrors
// previewBundle's shape so the formatters read alike.
type diffBundle struct {
	srcEngine  string
	tgtEngine  string
	tgtDialect ir.DDLDialect // target's declared DDL-suggestion dialect
	diff       irdiff.SchemaDiff
	missingDDL map[string][]ir.DDLStatement // table name -> CREATE TABLE / CREATE INDEX statements

	// missingColDDL maps "<table>.<column>" -> the target engine's
	// rendered column-def fragment (e.g. `"created_at" TIMESTAMP(6)
	// NOT NULL`) for use in the ALTER TABLE ADD COLUMN suggestion.
	// nil when no missing-on-target column was rendered (engine
	// didn't expose ir.ColumnDDLPreviewer or the emit failed for a
	// specific column — in that case the renderer falls back to the
	// `-- TYPE` placeholder).
	missingColDDL map[string]string

	expected *ir.Schema
	actual   *ir.Schema
	opts     diffRenderOpts
}

type diffRenderOpts struct {
	IgnoreCharsetCollation bool
	IgnoreExtras           bool
}

// openTargetSchemaWriter opens the target engine's schema writer for
// the diff's two write-side-knowledge needs: the emitted-CHECK
// prediction ([ir.EmittedCheckPredictor]) and the missing-table /
// missing-column DDL suggestions.
//
// It is opened UNCONDITIONALLY, where the suggestion path used to open
// it lazily only when there was drift to render. That is the cost of the
// prediction and it is deliberate: whether MySQL inlines a DOMAIN's
// CHECKs at all depends on the live server version, which the writer
// probes at open (see mysql.SchemaWriter.PredictEmittedChecks). Guessing
// instead would invent a permanent "missing on target" line on an older
// server that no action could close — and the clean-diff case is exactly
// the one the prediction exists to produce, so a lazy open would never
// fire for it.
//
// Both engines' OpenSchemaWriter are side-effect-free against the
// TARGET (connect, plus version / PostGIS / flavor probes); this adds a
// connection to a command that already opens two, and creates nothing.
// The open can still REFUSE — the MySQL writer doors a sharded
// Vitess/PlanetScale keyspace (SLUICE-E-SCHEMA-TARGET-KEYSPACE-SHARDED)
// and a flavor mismatch at open — which is why the caller treats a
// failed open as degradation, not failure: the diff still runs, and
// only the DDL-suggestion path (previewMissingDDL) surfaces the
// refusal, exactly where the suggested DDL could not have run anyway.
func (d *Differ) openTargetSchemaWriter(ctx context.Context) (ir.SchemaWriter, error) {
	sw, err := d.Target.OpenSchemaWriter(ctx, d.TargetDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: open target schema writer: %w", err))
	}
	migcore.ApplyTargetSchema(sw, d.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, sw, d.EnabledPGExtensions); err != nil {
		migcore.CloseIf(sw)
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("diff: enable PG extensions on target: %w", err))
	}
	return sw, nil
}

// attachEmittedChecks returns compared with each table's
// CheckConstraints extended by the constraints the target's writer would
// SYNTHESIZE for the corresponding sourceShaped table, marked
// [ir.CheckConstraint.SluiceEmitted].
//
// Two schemas because the two questions need different shapes: the
// comparison runs against `compared` (retargeted to the target's storage
// shapes), while the predictor dispatches on the SOURCE construct that
// retarget just replaced. They are the same tables, by name.
//
// Returns compared unchanged when the engine doesn't expose the optional
// surface or predicts nothing — the pre-existing behaviour, in which
// those constraints report as extra on target.
func attachEmittedChecks(compared, sourceShaped *ir.Schema, sw ir.SchemaWriter) *ir.Schema {
	predictor, ok := sw.(ir.EmittedCheckPredictor)
	if !ok || compared == nil || sourceShaped == nil {
		return compared
	}
	srcTables := make(map[string]*ir.Table, len(sourceShaped.Tables))
	for _, t := range sourceShaped.Tables {
		if t != nil {
			srcTables[t.Name] = t
		}
	}
	out := *compared
	out.Tables = make([]*ir.Table, len(compared.Tables))
	copy(out.Tables, compared.Tables)
	changed := false
	for i, t := range compared.Tables {
		if t == nil {
			continue
		}
		emitted := predictor.PredictEmittedChecks(srcTables[t.Name])
		if len(emitted) == 0 {
			continue
		}
		tableCopy := *t
		tableCopy.CheckConstraints = make([]*ir.CheckConstraint, 0, len(t.CheckConstraints)+len(emitted))
		tableCopy.CheckConstraints = append(tableCopy.CheckConstraints, t.CheckConstraints...)
		tableCopy.CheckConstraints = append(tableCopy.CheckConstraints, emitted...)
		out.Tables[i] = &tableCopy
		changed = true
	}
	if !changed {
		return compared
	}
	return &out
}

// previewMissingDDL asks the already-open target schema writer for two
// flavours of "render the DDL you would emit" material: full CREATE
// TABLE statements for tables missing from the target, and per-column-def
// fragments for individual columns missing from a present-on-both-sides
// table.
//
// The returned maps may be nil when there's nothing to preview or the
// engine doesn't expose the relevant optional surface
// ([ir.DDLPreviewer] / [ir.ColumnDDLPreviewer]); the renderer falls
// back to placeholder output in those cases. Errors from the
// underlying preview calls are returned verbatim.
//
// openErr carries a failure from the writer open, which the caller now
// performs up front for the emitted-CHECK prediction. It is surfaced
// HERE and only here, because here is where it was fatal before the
// open moved: a diff with nothing to render never needed a writer and
// must not start failing for want of one.
func previewMissingDDL(ctx context.Context, sw ir.SchemaWriter, openErr error, expected *ir.Schema, diff irdiff.SchemaDiff) (tableDDL map[string][]ir.DDLStatement, columnDDL map[string]string, err error) {
	missingTables := diff.TablesMissing
	missingCols := collectMissingColumns(diff)
	if len(missingTables) == 0 && len(missingCols) == 0 {
		return nil, nil, nil
	}
	if openErr != nil {
		return nil, nil, openErr
	}

	tableDDL, err = previewDDLForTables(ctx, sw, expected, missingTables)
	if err != nil {
		return nil, nil, err
	}

	columnDDL, err = previewDDLForColumns(ctx, sw, expected, missingCols)
	if err != nil {
		return nil, nil, err
	}
	return tableDDL, columnDDL, nil
}

// collectMissingColumns returns the per-table list of columns absent
// from the target. Map key is table name, value is the slice of
// missing column names (in the same alphabetic order irdiff.Schemas
// returned them).
func collectMissingColumns(diff irdiff.SchemaDiff) map[string][]string {
	out := make(map[string][]string, len(diff.TablesMismatched))
	for _, td := range diff.TablesMismatched {
		if len(td.ColumnsMissing) == 0 {
			continue
		}
		out[td.Name] = td.ColumnsMissing
	}
	return out
}

// previewDDLForTables asks the target engine for the DDL it would
// emit for the listed tables. Used to render CREATE TABLE suggestions
// for "missing on target" entries. Returns an empty map (nil) when
// missing is empty or the target doesn't expose DDLPreviewer — the
// renderer falls back to a plain "-- CREATE TABLE x (missing)" hint.
func previewDDLForTables(ctx context.Context, sw ir.SchemaWriter, expected *ir.Schema, missing []string) (map[string][]ir.DDLStatement, error) {
	if len(missing) == 0 {
		return nil, nil
	}
	missingSet := make(map[string]struct{}, len(missing))
	for _, n := range missing {
		missingSet[n] = struct{}{}
	}
	subset := &ir.Schema{Tables: make([]*ir.Table, 0, len(missing))}
	for _, t := range expected.Tables {
		if _, ok := missingSet[t.Name]; ok {
			subset.Tables = append(subset.Tables, t)
		}
	}
	if len(subset.Tables) == 0 {
		return nil, nil
	}
	previewer, ok := sw.(ir.DDLPreviewer)
	if !ok {
		return nil, nil
	}
	stmts, err := previewer.PreviewDDL(ctx, subset)
	if err != nil {
		return nil, fmt.Errorf("diff: emit DDL for missing tables: %w", err)
	}
	out := make(map[string][]ir.DDLStatement, len(missing))
	for _, s := range stmts {
		if s.Table == "" {
			continue
		}
		out[s.Table] = append(out[s.Table], s)
	}
	return out, nil
}

// previewDDLForColumns asks the target engine for the column-def
// fragment of every (table, column) pair missing on the target.
// Returns nil when there's nothing to render or the engine doesn't
// expose ir.ColumnDDLPreviewer — the diff renderer falls back to the
// `-- TYPE` placeholder in either case.
//
// Per-column emit failures (e.g. PG GEOMETRY without PostGIS) are
// silently skipped — the renderer falls through to the placeholder
// for that column and the operator sees the same diagnostic loop the
// migration would surface. Aborting the whole diff over one column
// would be worse UX than partial rendering with a placeholder for the
// problem cases.
func previewDDLForColumns(ctx context.Context, sw ir.SchemaWriter, expected *ir.Schema, missing map[string][]string) (map[string]string, error) {
	if len(missing) == 0 {
		return nil, nil
	}
	previewer, ok := sw.(ir.ColumnDDLPreviewer)
	if !ok {
		return nil, nil
	}
	tablesByName := make(map[string]*ir.Table, len(expected.Tables))
	for _, t := range expected.Tables {
		tablesByName[t.Name] = t
	}
	out := make(map[string]string, totalColumns(missing))
	for tableName, cols := range missing {
		table, ok := tablesByName[tableName]
		if !ok {
			continue
		}
		colsByName := make(map[string]*ir.Column, len(table.Columns))
		for _, c := range table.Columns {
			colsByName[c.Name] = c
		}
		for _, colName := range cols {
			col, ok := colsByName[colName]
			if !ok {
				continue
			}
			frag, err := previewer.EmitColumnDef(ctx, table, col)
			if err != nil {
				// Skip; renderer falls back to placeholder. Column
				// emit errors are recoverable at the diff layer —
				// the renderer's job is to surface drift, not to
				// produce a fully-validated migration script.
				continue
			}
			out[tableName+"."+colName] = frag
		}
	}
	return out, nil
}

func totalColumns(m map[string][]string) int {
	n := 0
	for _, cols := range m {
		n += len(cols)
	}
	return n
}

// renderDiffText writes the human-readable diff to w. Format follows
// ADR-0029 §"Output format" — header summary, per-table sections with
// DDL suggestions for closing the diff.
func renderDiffText(w io.Writer, b diffBundle) error {
	var sb strings.Builder

	sb.WriteString("-- sluice schema diff\n")
	fmt.Fprintf(&sb, "-- source: %s (%d tables expected after translation)\n", b.srcEngine, countTables(b.expected))
	fmt.Fprintf(&sb, "-- target: %s (%d tables found)\n", b.tgtEngine, countTables(b.actual))
	fmt.Fprintf(&sb, "-- result: %s\n", b.diff.Summary())
	if b.opts.IgnoreCharsetCollation {
		sb.WriteString("-- (charset/collation comparisons suppressed via --ignore-charset-collation)\n")
	}
	if b.opts.IgnoreExtras {
		sb.WriteString("-- (extra-on-target entries suppressed via --ignore-extras)\n")
	}
	if n := b.diff.ForeignKeysUnnamed; n > 0 {
		// A coverage caveat on the WHOLE report, so it belongs in the
		// preamble next to the suppression notices rather than buried in a
		// per-table section the reader may never reach — the tables whose
		// FKs went uncompared are frequently the ones with no other drift.
		fmt.Fprintf(&sb, "-- (COVERAGE: %d foreign key(s) carry no constraint name and were NOT compared)\n", n)
	}
	sb.WriteString("--\n")
	sb.WriteString("-- The ALTER/DROP statements below are starting points for manual\n")
	sb.WriteString("-- reconciliation. sluice does not execute them. Review carefully\n")
	sb.WriteString("-- before running them against the target.\n")
	sb.WriteByte('\n')

	if !b.diff.HasChanges() {
		sb.WriteString("-- No drift detected. Target schema matches the expected shape.\n")
		_, err := io.WriteString(w, sb.String())
		return err
	}

	quote := identifierQuoter(b.tgtDialect)

	// Tables missing on target — render the engine's CREATE TABLE
	// (and CREATE INDEX, FK) when available, otherwise a placeholder.
	for _, name := range b.diff.TablesMissing {
		fmt.Fprintf(&sb, "-- ──────────── %s (missing on target) ────────────\n", name)
		stmts := b.missingDDL[name]
		if len(stmts) == 0 {
			fmt.Fprintf(&sb, "-- target engine does not expose CREATE-DDL preview; manually create %s\n", quote(name))
		}
		for _, s := range stmts {
			sb.WriteString(s.SQL)
			sb.WriteString(";\n")
		}
		sb.WriteByte('\n')
	}

	// Tables extra on target.
	for _, name := range b.diff.TablesExtra {
		fmt.Fprintf(&sb, "-- ──────────── %s (extra on target) ────────────\n", name)
		fmt.Fprintf(&sb, "DROP TABLE %s;\n", quote(name))
		fmt.Fprintf(&sb, "-- ^ not in source schema; sluice would not create it\n\n")
	}

	// Views missing on target.
	for _, name := range b.diff.ViewsMissing {
		fmt.Fprintf(&sb, "-- ──────────── view %s (missing on target) ────────────\n", name)
		// Look up the expected definition so the operator gets a
		// copy-paste-ready CREATE VIEW. Materialized views emit
		// `CREATE MATERIALIZED VIEW ... WITH DATA`; regular views
		// emit `CREATE VIEW`. Cross-engine view-body translation is
		// Phase 3 — for now the body is verbatim.
		expView := lookupView(b.expected, name)
		if expView != nil {
			kw := "CREATE VIEW"
			suffix := ""
			if expView.Materialized {
				kw = "CREATE MATERIALIZED VIEW"
				suffix = " WITH DATA"
			}
			fmt.Fprintf(&sb, "%s %s AS %s%s;\n", kw, quote(name), expView.Definition, suffix)
		} else {
			fmt.Fprintf(&sb, "-- expected view definition not available; manually create %s\n", quote(name))
		}
		sb.WriteByte('\n')
	}

	// Views extra on target.
	for _, name := range b.diff.ViewsExtra {
		fmt.Fprintf(&sb, "-- ──────────── view %s (extra on target) ────────────\n", name)
		// Pick the right DROP keyword based on what the actual side
		// reports. Falls back to DROP VIEW (most common case).
		actView := lookupView(b.actual, name)
		if actView != nil && actView.Materialized {
			fmt.Fprintf(&sb, "DROP MATERIALIZED VIEW %s;\n", quote(name))
		} else {
			fmt.Fprintf(&sb, "DROP VIEW %s;\n", quote(name))
		}
		fmt.Fprintf(&sb, "-- ^ not in source schema; sluice would not create it\n\n")
	}

	// Views mismatched (definition drift).
	for _, vd := range b.diff.ViewsMismatched {
		fmt.Fprintf(&sb, "-- ──────────── view %s (mismatched) ────────────\n", vd.Name)
		if vd.ExpectedMaterialized != nil && vd.ActualMaterialized != nil &&
			*vd.ExpectedMaterialized != *vd.ActualMaterialized {
			fmt.Fprintf(&sb, "-- materialized flag differs: target=%v expected=%v\n",
				*vd.ActualMaterialized, *vd.ExpectedMaterialized)
		}
		if vd.ExpectedDefinition != "" || vd.ActualDefinition != "" {
			fmt.Fprintf(&sb, "-- (cross-engine view-definition comparison is high-noise; verify before applying)\n")
			fmt.Fprintf(&sb, "-- target  : %s\n", oneLine(vd.ActualDefinition))
			fmt.Fprintf(&sb, "-- expected: %s\n", oneLine(vd.ExpectedDefinition))
		}
		expView := lookupView(b.expected, vd.Name)
		if expView != nil {
			if expView.Materialized {
				// PG won't accept CREATE OR REPLACE on a matview;
				// the operator has to drop and recreate.
				fmt.Fprintf(&sb, "DROP MATERIALIZED VIEW %s;\n", quote(vd.Name))
				fmt.Fprintf(&sb, "CREATE MATERIALIZED VIEW %s AS %s WITH DATA;\n", quote(vd.Name), expView.Definition)
			} else {
				fmt.Fprintf(&sb, "CREATE OR REPLACE VIEW %s AS %s;\n", quote(vd.Name), expView.Definition)
			}
		}
		sb.WriteByte('\n')
	}

	renderSequenceSections(&sb, b.diff, quote, b.expected)

	// Per-table mismatched sections.
	for _, td := range b.diff.TablesMismatched {
		fmt.Fprintf(&sb, "-- ──────────── %s (mismatched) ────────────\n", td.Name)
		for _, col := range td.ColumnsMissing {
			renderMissingColumn(&sb, td.Name, col, quote, b.missingColDDL)
		}
		for _, col := range td.ColumnsExtra {
			fmt.Fprintf(&sb, "ALTER TABLE %s DROP COLUMN %s;\n", quote(td.Name), quote(col))
			fmt.Fprintln(&sb, "-- ^ column not in source schema; sluice would not create it")
		}
		for _, cd := range td.ColumnsMismatched {
			renderColumnMismatch(&sb, td.Name, cd, quote, b.tgtDialect)
		}
		for _, idx := range td.IndexesMissing {
			if renderMissingPrimaryKey(&sb, td.Name, idx, quote, b.expected) {
				continue
			}
			fmt.Fprintf(&sb, "-- index %s missing on target; CREATE INDEX %s ON %s (...);\n",
				quote(idx), quote(idx), quote(td.Name))
		}
		for _, idx := range td.IndexesExtra {
			if renderExtraPrimaryKey(&sb, td.Name, idx, quote, b.actual, b.tgtDialect) {
				continue
			}
			fmt.Fprintf(&sb, "DROP INDEX %s; -- not in source schema\n", quote(idx))
		}
		for _, id := range td.IndexesMismatched {
			renderIndexMismatch(&sb, td.Name, id, quote)
		}
		for _, name := range td.ChecksMissing {
			renderMissingCheck(&sb, td.Name, name, quote, b.expected)
		}
		for _, name := range td.ChecksExtra {
			fmt.Fprintf(&sb, "ALTER TABLE %s DROP CONSTRAINT %s; -- CHECK not in source schema\n",
				quote(td.Name), quote(name))
		}
		for _, ck := range td.ChecksMismatched {
			fmt.Fprintf(&sb, "-- CHECK %s mismatched: target has %q; expected %q\n",
				quote(ck.Name), ck.ActualExpr, ck.ExpectedExpr)
			fmt.Fprintf(&sb, "ALTER TABLE %s DROP CONSTRAINT %s;\n", quote(td.Name), quote(ck.Name))
			fmt.Fprintf(&sb, "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);\n",
				quote(td.Name), quote(ck.Name), ck.ExpectedExpr)
		}
		renderForeignKeySection(&sb, td, quote, b.expected)
		renderExcludeSection(&sb, td, quote, b.expected)
		renderRowLevelSecuritySection(&sb, td, quote)
		sb.WriteByte('\n')
	}

	_, err := io.WriteString(w, sb.String())
	return err
}

// renderMissingColumn writes the ALTER TABLE ADD COLUMN suggestion
// for a column missing on target. When the target engine emitted a
// concrete column-def fragment via ir.ColumnDDLPreviewer the renderer
// inlines it (operators get a copy-paste-ready ALTER); otherwise we
// fall back to the v0.7.0 `-- TYPE` placeholder shape.
func renderMissingColumn(sb *strings.Builder, table, col string, quote func(string) string, ddl map[string]string) {
	if frag, ok := ddl[table+"."+col]; ok && frag != "" {
		fmt.Fprintf(sb, "ALTER TABLE %s ADD COLUMN %s; -- column missing on target\n",
			quote(table), frag)
		return
	}
	fmt.Fprintf(sb, "ALTER TABLE %s ADD COLUMN %s; -- TYPE; column missing on target\n",
		quote(table), quote(col))
}

// renderMissingPrimaryKey / renderExtraPrimaryKey write the suggestion
// for a table whose PRIMARY KEY is genuinely absent from one side, and
// report whether they handled the entry.
//
// The generic index lines they pre-empt would suggest `CREATE INDEX
// <name> ON <table> (...)`, which no engine accepts for a primary key —
// and since Bug 234 matched the primary key by ROLE rather than by name,
// the entry can arrive under the placeholder display name `PRIMARY KEY`
// (a SQLite side leaves its primary-key index unnamed), where the
// generic line reads as outright nonsense. A missing primary key is the
// drift these two must keep reporting loudly, so its suggestion has to
// be one the operator can actually run.
//
// Detection is by IDENTITY against the schema in hand — the entry is a
// primary key iff that side's table declares a primary key rendering to
// this display name — so an ordinary index that happens to be named
// `PRIMARY KEY` still takes the generic path.
func renderMissingPrimaryKey(sb *strings.Builder, table, name string, quote func(string) string, expected *ir.Schema) bool {
	pk := lookupPrimaryKeyByDisplayName(expected, table, name)
	if pk == nil {
		return false
	}
	fmt.Fprintf(sb, "ALTER TABLE %s ADD PRIMARY KEY %s; -- PRIMARY KEY missing on target\n",
		quote(table), renderPrimaryKeyColumns(pk, quote))
	return true
}

func renderExtraPrimaryKey(sb *strings.Builder, table, name string, quote func(string) string, actual *ir.Schema, dialect ir.DDLDialect) bool {
	pk := lookupPrimaryKeyByDisplayName(actual, table, name)
	if pk == nil {
		return false
	}
	// MySQL spells the drop `DROP PRIMARY KEY`; ANSI/Postgres needs the
	// backing constraint's name, which a PG catalog always supplies.
	if dialect == ir.DDLDialectMySQL || pk.Name == "" {
		fmt.Fprintf(sb, "ALTER TABLE %s DROP PRIMARY KEY; -- PRIMARY KEY %s not in source schema\n",
			quote(table), renderPrimaryKeyColumns(pk, quote))
		return true
	}
	fmt.Fprintf(sb, "ALTER TABLE %s DROP CONSTRAINT %s; -- PRIMARY KEY %s not in source schema\n",
		quote(table), quote(pk.Name), renderPrimaryKeyColumns(pk, quote))
	return true
}

// lookupPrimaryKeyByDisplayName returns s's primary key for the named
// table when it is the one the diff reported under displayName, and nil
// otherwise. The empty-name fallback mirrors internal/ir/diff's
// indexDisplayName.
func lookupPrimaryKeyByDisplayName(s *ir.Schema, tableName, displayName string) *ir.Index {
	if s == nil {
		return nil
	}
	for _, t := range s.Tables {
		if t == nil || t.Name != tableName || t.PrimaryKey == nil {
			continue
		}
		pkName := t.PrimaryKey.Name
		if pkName == "" {
			pkName = "PRIMARY KEY"
		}
		if pkName == displayName {
			return t.PrimaryKey
		}
		return nil
	}
	return nil
}

// renderPrimaryKeyColumns renders a primary key's column list for the
// suggestion lines: `("a", "b")`. Prefix lengths and DESC are dropped —
// a primary key carrying either is vanishingly rare and the line is a
// starting point the operator edits, not a migration.
func renderPrimaryKeyColumns(pk *ir.Index, quote func(string) string) string {
	if pk == nil || len(pk.Columns) == 0 {
		return "(...)"
	}
	parts := make([]string, 0, len(pk.Columns))
	for _, c := range pk.Columns {
		if c.Column == "" {
			return "(...)"
		}
		parts = append(parts, quote(c.Column))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// renderMissingCheck writes the ADD CONSTRAINT ... CHECK suggestion
// for a CHECK constraint missing on target, looking up the expected
// expression in the expected-side schema. Surfaces a placeholder
// when the constraint name resolves but the schema is malformed (no
// expression text) — the operator can still see the name and chase
// it down by hand.
func renderMissingCheck(sb *strings.Builder, table, name string, quote func(string) string, expected *ir.Schema) {
	expr := lookupCheckExpr(expected, table, name)
	if expr == "" {
		fmt.Fprintf(sb, "-- CHECK %s missing on target; expression unavailable for ADD CONSTRAINT suggestion\n",
			quote(name))
		return
	}
	fmt.Fprintf(sb, "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s); -- CHECK missing on target\n",
		quote(table), quote(name), expr)
}

// lookupCheckExpr returns the expression text for the named CHECK
// constraint on the named table within s, or "" when the schema is
// nil / table absent / constraint absent. Used by the missing-CHECK
// renderer; schemas should be populated with the constraint's text
// from the source-side reader.
func lookupCheckExpr(s *ir.Schema, tableName, checkName string) string {
	if s == nil {
		return ""
	}
	for _, t := range s.Tables {
		if t.Name != tableName {
			continue
		}
		for _, c := range t.CheckConstraints {
			if c != nil && c.Name == checkName {
				return c.Expr
			}
		}
	}
	return ""
}

// renderColumnMismatch emits one ALTER suggestion per column-level
// drift. The exact MODIFY syntax varies between MySQL (MODIFY COLUMN)
// and PG (ALTER COLUMN ... TYPE / SET NOT NULL); we write the form
// the target engine's declared [ir.Capabilities.DDLDialect] asks for.
// Operators copy-paste these as a starting point — they're not
// guaranteed verified migration scripts.
func renderColumnMismatch(sb *strings.Builder, table string, cd irdiff.ColumnDiff, quote func(string) string, dialect ir.DDLDialect) {
	switch dialect {
	case ir.DDLDialectMySQL:
		if cd.ExpectedType != "" {
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedType, cd.ActualType)
		}
		if cd.ExpectedNullable != nil {
			null := "NOT NULL"
			if *cd.ExpectedNullable {
				null = "NULL"
			}
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... %s; -- nullable on target: %v -> expected: %v\n",
				quote(table), quote(cd.Name), null, *cd.ActualNullable, *cd.ExpectedNullable)
		}
		if cd.ExpectedDefault != "" || cd.ActualDefault != "" {
			renderDefaultMismatchMySQL(sb, table, cd, quote)
		}
		if cd.ExpectedGeneratedExpr != cd.ActualGeneratedExpr {
			renderGeneratedExprMismatch(sb, table, cd, quote)
		}
		renderCharsetCollationMismatch(sb, table, cd, quote, ir.DDLDialectMySQL)
	default:
		if cd.ExpectedType != "" {
			fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s TYPE %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedType, cd.ActualType)
		}
		if cd.ExpectedNullable != nil {
			action := "SET NOT NULL"
			if *cd.ExpectedNullable {
				action = "DROP NOT NULL"
			}
			fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s %s; -- nullable on target: %v -> expected: %v\n",
				quote(table), quote(cd.Name), action, *cd.ActualNullable, *cd.ExpectedNullable)
		}
		if cd.ExpectedDefault != "" || cd.ActualDefault != "" {
			renderDefaultMismatchPG(sb, table, cd, quote)
		}
		if cd.ExpectedGeneratedExpr != cd.ActualGeneratedExpr {
			renderGeneratedExprMismatch(sb, table, cd, quote)
		}
		renderCharsetCollationMismatch(sb, table, cd, quote, ir.DDLDialectANSI)
	}
}

// renderCharsetCollationMismatch emits ALTER suggestions for charset
// or collation drift. Empty fields are skipped, so a ColumnDiff that
// passed `--ignore-charset-collation` (which clears these via
// stripCharsetCollation at compare time) renders nothing here.
//
// MySQL syntax uses `MODIFY COLUMN ... CHARACTER SET ... COLLATE ...`;
// PG uses `ALTER COLUMN ... TYPE ... COLLATE "..."`. Suggestions are
// hint comments — the precise type still needs filling in by the
// operator.
func renderCharsetCollationMismatch(sb *strings.Builder, table string, cd irdiff.ColumnDiff, quote func(string) string, dialect ir.DDLDialect) {
	if cd.ExpectedCharset == "" && cd.ActualCharset == "" &&
		cd.ExpectedCollation == "" && cd.ActualCollation == "" {
		return
	}
	switch dialect {
	case ir.DDLDialectMySQL:
		switch {
		case cd.ExpectedCharset != cd.ActualCharset && cd.ExpectedCollation != cd.ActualCollation:
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... CHARACTER SET %s COLLATE %s; -- on target: charset=%s collation=%s\n",
				quote(table), quote(cd.Name), cd.ExpectedCharset, cd.ExpectedCollation, cd.ActualCharset, cd.ActualCollation)
		case cd.ExpectedCharset != cd.ActualCharset:
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... CHARACTER SET %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedCharset, cd.ActualCharset)
		case cd.ExpectedCollation != cd.ActualCollation:
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... COLLATE %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedCollation, cd.ActualCollation)
		}
	default:
		// PG has no per-column charset; only collation surfaces here.
		if cd.ExpectedCollation != cd.ActualCollation {
			fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE ... COLLATE %q; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedCollation, cd.ActualCollation)
		}
	}
}

// renderDefaultMismatchPG renders an ALTER TABLE ... ALTER COLUMN ...
// SET DEFAULT / DROP DEFAULT suggestion for a PG-style target. When
// the diff carries DefaultLowConfidence=true the suggestion is
// preceded by a `-- (default may differ across engines)` hint so the
// operator knows to verify the rendering against the actual source-
// side spelling before applying.
func renderDefaultMismatchPG(sb *strings.Builder, table string, cd irdiff.ColumnDiff, quote func(string) string) {
	if cd.DefaultLowConfidence {
		fmt.Fprintf(sb, "-- (default on %s may differ across engines; verify before applying)\n",
			quote(cd.Name))
	}
	switch {
	case cd.ExpectedDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT; -- on target: %s -> expected: <none>\n",
			quote(table), quote(cd.Name), cd.ActualDefault)
	case cd.ActualDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: <none>\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault))
	default:
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: %s\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault), cd.ActualDefault)
	}
}

// renderDefaultMismatchMySQL renders the MySQL-style ALTER for a
// default-clause drift. MySQL uses MODIFY COLUMN ... DEFAULT (or
// ALTER COLUMN ... SET/DROP DEFAULT in 8.0+); we use the latter form
// because it's narrower (doesn't require the operator to retype the
// column type) and works on both 5.7+ and 8.0+.
func renderDefaultMismatchMySQL(sb *strings.Builder, table string, cd irdiff.ColumnDiff, quote func(string) string) {
	if cd.DefaultLowConfidence {
		fmt.Fprintf(sb, "-- (default on %s may differ across engines; verify before applying)\n",
			quote(cd.Name))
	}
	switch {
	case cd.ExpectedDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT; -- on target: %s -> expected: <none>\n",
			quote(table), quote(cd.Name), cd.ActualDefault)
	case cd.ActualDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: <none>\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault))
	default:
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: %s\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault), cd.ActualDefault)
	}
}

// unwrapDefaultLiteral converts the diff's rendered-default string
// back into a SQL fragment suitable for inlining after `SET DEFAULT
// `. The diff renders literal defaults as `'value'` (with the
// surrounding quotes) and expression defaults verbatim; the SQL
// emitter wants both forms passed through unchanged. Today the two
// shapes happen to be identical at the surface, so this function is
// a no-op — it exists as a single point to evolve later if the IR's
// default rendering grows new shapes (e.g. typed literals).
func unwrapDefaultLiteral(rendered string) string {
	return rendered
}

// lookupView returns the named view from s, or nil when s is nil or
// the view is absent. Used by the diff renderer to fetch the
// expected definition for missing-on-target / mismatched-view
// suggestions.
func lookupView(s *ir.Schema, name string) *ir.View {
	if s == nil {
		return nil
	}
	for _, v := range s.Views {
		if v != nil && v.Name == name {
			return v
		}
	}
	return nil
}

// oneLine collapses internal whitespace runs in s to single spaces
// and trims leading/trailing whitespace, so a multi-line view
// definition fits on a single comment line in the diff output.
func oneLine(s string) string {
	s = strings.TrimSpace(s)
	var sb strings.Builder
	sb.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !prevSpace {
				sb.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		sb.WriteRune(r)
	}
	return sb.String()
}

// renderGeneratedExprMismatch emits a comment describing the
// generated-column expression drift. We don't try to ALTER the
// expression: PG/MySQL both require dropping and re-adding the
// column to change a STORED generated expression, which is
// destructive enough that the operator should run the migration
// hand-edited rather than copy-pasting from a diff suggestion.
func renderGeneratedExprMismatch(sb *strings.Builder, table string, cd irdiff.ColumnDiff, quote func(string) string) {
	fmt.Fprintf(sb, "-- generated expression drift on %s.%s: target=%q expected=%q\n",
		quote(table), quote(cd.Name), cd.ActualGeneratedExpr, cd.ExpectedGeneratedExpr)
	fmt.Fprintln(sb, "-- ^ engines require DROP + ADD COLUMN to change a generated expression; review carefully")
}

// identifierQuoter returns a function that quotes a SQL identifier in
// the target engine's declared [ir.Capabilities.DDLDialect] —
// backticks for the MySQL family, double quotes for everything else
// (PostgreSQL today, ANSI SQL idiom for future engines). The renderer
// is the only thing that cares about dialect-specific identifier
// syntax in the diff path.
func identifierQuoter(dialect ir.DDLDialect) func(string) string {
	switch dialect {
	case ir.DDLDialectMySQL:
		return func(s string) string { return "`" + s + "`" }
	default:
		return func(s string) string { return `"` + s + `"` }
	}
}

func countTables(s *ir.Schema) int {
	if s == nil {
		return 0
	}
	return len(s.Tables)
}

// summarise rolls per-table counts up into the header summary line.
func summarise(d irdiff.SchemaDiff) DiffJSONCounts {
	c := DiffJSONCounts{
		TablesMissing:       len(d.TablesMissing),
		TablesExtra:         len(d.TablesExtra),
		TablesMismatched:    len(d.TablesMismatched),
		ViewsMissing:        len(d.ViewsMissing),
		ViewsExtra:          len(d.ViewsExtra),
		ViewsMismatched:     len(d.ViewsMismatched),
		SequencesMissing:    len(d.SequencesMissing),
		SequencesExtra:      len(d.SequencesExtra),
		SequencesMismatched: len(d.SequencesMismatched),
		// Read from the SCHEMA-level total, not summed from
		// TablesMismatched: a table whose only notable property is an
		// uncompared FK never reaches TablesMismatched at all.
		ForeignKeysUnnamed: d.ForeignKeysUnnamed,
	}
	for _, td := range d.TablesMismatched {
		c.ColumnsMissing += len(td.ColumnsMissing)
		c.ColumnsExtra += len(td.ColumnsExtra)
		c.ColumnsMismatched += len(td.ColumnsMismatched)
		c.IndexesMissing += len(td.IndexesMissing)
		c.IndexesMismatched += len(td.IndexesMismatched)
		c.IndexesExtra += len(td.IndexesExtra)
		c.ChecksMissing += len(td.ChecksMissing)
		c.ChecksExtra += len(td.ChecksExtra)
		c.ChecksMismatched += len(td.ChecksMismatched)
		c.ForeignKeysMissing += len(td.ForeignKeysMissing)
		c.ForeignKeysExtra += len(td.ForeignKeysExtra)
		c.ForeignKeysMismatched += len(td.ForeignKeysMismatched)
		c.ExcludesMissing += len(td.ExcludesMissing)
		c.ExcludesExtra += len(td.ExcludesExtra)
		c.ExcludesMismatched += len(td.ExcludesMismatched)
		c.PoliciesMissing += len(td.PoliciesMissing)
		c.PoliciesExtra += len(td.PoliciesExtra)
		c.PoliciesMismatched += len(td.PoliciesMismatched)
		if td.RLSMismatched {
			c.RLSMismatched++
		}
	}
	return c
}

// renderDiffJSON writes the structured diff to w. The shape mirrors
// irdiff.SchemaDiff with a summary block prepended and the engine names
// recorded alongside.
func renderDiffJSON(w io.Writer, srcEngine, tgtEngine string, diff irdiff.SchemaDiff) error {
	out := DiffJSON{
		SourceEngine: srcEngine,
		TargetEngine: tgtEngine,
		Summary:      summarise(diff),
		SchemaDiff:   diff,
	}
	// Stable nested ordering: the fields inside SchemaDiff are already
	// sorted by irdiff.Schemas; this defensive sort is a no-op today but
	// keeps the JSON renderer's output deterministic if a future caller
	// constructs SchemaDiff some other way.
	sort.Strings(out.TablesMissing)
	sort.Strings(out.TablesExtra)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// renderIndexMismatch writes the human-readable suggestion for an index that
// exists on both sides under one name but enforces something different
// (roadmap item 125).
//
// It leads with the CONSEQUENCE rather than the attribute, because that is
// what an operator needs to decide urgency: a target that admits rows the
// source rejects is a live data-integrity gap, while one that admits fewer is
// a availability problem. The suggestion is a comment rather than executable
// DDL — rebuilding a unique index on a live target is not something to hand
// someone as a paste-ready statement without them choosing the moment.
func renderIndexMismatch(sb *strings.Builder, table string, id irdiff.IndexDiff, quote func(string) string) {
	fmt.Fprintf(sb, "-- index %s on %s differs between source and target:\n",
		quote(id.Name), quote(table))
	if id.ExpectedColumns != "" || id.ActualColumns != "" {
		fmt.Fprintf(sb, "--   columns:   source %s   target %s\n", id.ExpectedColumns, id.ActualColumns)
		if strings.Contains(id.ExpectedColumns, "(") && !strings.Contains(id.ActualColumns, "(") {
			fmt.Fprintln(sb, "--   ^ the source constrains a PREFIX of the column and the target does not, so the "+
				"target ACCEPTS ROWS THE SOURCE REJECTS")
		}
	}
	if id.UniqueMismatched {
		fmt.Fprintf(sb, "--   unique:    source %v   target %v\n", id.ExpectedUnique, id.ActualUnique)
		if id.ExpectedUnique && !id.ActualUnique {
			fmt.Fprintln(sb, "--   ^ the target no longer enforces uniqueness, so it ACCEPTS ROWS THE SOURCE REJECTS")
		}
	}
	if id.ExpectedPredicate != "" || id.ActualPredicate != "" {
		fmt.Fprintf(sb, "--   predicate: source %q   target %q\n", id.ExpectedPredicate, id.ActualPredicate)
		if id.ExpectedPredicate != "" && id.ActualPredicate == "" {
			fmt.Fprintln(sb, "--   ^ the source's index is PARTIAL and the target's is not, so the target "+
				"REJECTS ROWS THE SOURCE HOLDS")
		}
	}
	fmt.Fprintf(sb, "-- review before acting; rebuilding %s on a live target takes a lock\n", quote(id.Name))
}
