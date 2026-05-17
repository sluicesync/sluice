// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Schema-preview orchestration for the `sluice schema preview` CLI
// (ADR-0024). Reads the source schema, applies the translation
// pipeline (filter + per-column type-mapping overrides), asks the
// target engine for the DDL it would emit, and renders the result —
// either as human-readable text with inline cross-engine notes and
// advisory hints, or as JSON for tooling.
//
// The package stays engine-neutral: every engine-specific operation
// goes through ir.Engine and the optional ir.DDLPreviewer surface.
// Importing internal/engines/postgres or internal/engines/mysql here
// would couple the orchestrator to specific engines and break the
// "engines slot in via the registry" invariant.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
	"github.com/orware/sluice/internal/translate"
)

// Previewer runs a single schema-preview against the configured
// source/target pair. Construct, set fields, call Run.
//
// Same shape as [Migrator] / [Streamer]: the type holds enough config
// to drive one preview; concurrent calls on the same value are not
// supported.
type Previewer struct {
	// Source / Target are the engines the source/target DSNs belong
	// to. Required.
	Source ir.Engine
	Target ir.Engine

	// SourceDSN / TargetDSN are the engine-native DSNs. The target
	// DSN is consulted only to construct the schema writer the
	// preview emits through; the preview never modifies the target.
	// Required.
	SourceDSN string
	TargetDSN string

	// Mappings is the per-column type-override list to apply before
	// asking the target writer for DDL. Empty disables the override
	// step. Mirrors the migrate/streamer field of the same name.
	Mappings []config.Mapping

	// ExpressionMappings is the per-column generated-expression
	// override list. Mirrors the migrate/streamer field of the same
	// name. Applied alongside Mappings so previewed DDL reflects
	// what migrate / sync start would produce.
	ExpressionMappings []config.ExpressionMapping

	// Filter selects which source tables participate in the preview.
	// Empty (zero value) keeps every table the source schema reader
	// returns.
	Filter TableFilter

	// ViewFilter selects which source views participate in the
	// preview. Empty zero-value keeps every view; SkipViews=true
	// drops them all regardless of filter.
	ViewFilter ViewFilter

	// SkipViews drops every view before preview rendering.
	SkipViews bool

	// Format is "text" (default human-readable form) or "json"
	// (machine-readable for tooling). Empty defaults to "text".
	Format string

	// Out is the destination for the rendered preview. Required.
	// `--output FILE` is a CLI concern; the orchestrator writes to
	// the supplied io.Writer regardless.
	Out io.Writer

	// TargetSchema is the per-source target-schema namespace
	// (ADR-0031). When set, preview output renders DDL prefixed with
	// the schema name so operators see exactly what `migrate` /
	// `sync start` would emit. Mirrors the Migrator/Streamer field
	// of the same name; PG-only.
	TargetSchema string

	// EnabledPGExtensions is the operator's `--enable-pg-extension`
	// allowlist (ADR-0032). PG → PG only. Threaded through the
	// freshly-opened source SchemaReader and target SchemaWriter
	// so the preview's DDL renders the same shape `migrate` /
	// `sync start` would emit. Empty preserves the pre-v0.26.0
	// behaviour.
	EnabledPGExtensions []string

	// Redactor, when non-nil and non-empty, annotates each redacted
	// column in the rendered CREATE TABLE DDL with a trailing
	// `-- REDACTED via <strategy>` comment so operators can SEE
	// what `sluice migrate` / `sync start` would redact before
	// committing. PII Phase 1.5 (v0.55.0), roadmap item 15a
	// follow-on. nil/empty: no annotations rendered (the
	// pre-v0.55.0 shape).
	Redactor *redact.Registry
}

// PreviewJSON is the JSON-format preview output. The shape is stable
// for tooling consumers — schema-diff scripts, CI gates that flag bad
// translations, etc. Adding fields is a backward-compatible change;
// renaming or removing them is not.
type PreviewJSON struct {
	SourceEngine string             `json:"source_engine"`
	TargetEngine string             `json:"target_engine"`
	Tables       []PreviewJSONTable `json:"tables"`
	// TranslatorGaps is the v0.39.0 MySQL → PG translator-gap scan
	// result. Omitted for non-cross-engine pairs and when the scan
	// detected nothing.
	TranslatorGaps []PreviewJSONTranslatorGap `json:"translator_gaps,omitempty"`
	// UnsignedBigintNarrowings is the Bug 11 MySQL `bigint unsigned`
	// → PG `bigint` range-narrowing advisory list. Omitted for
	// non-MySQL→PG pairs and when no unsigned-bigint column is present.
	// Advisory only — tooling that wants to flag the (2^63, 2^64) loss
	// can gate on a non-empty list, but sluice itself does not refuse.
	UnsignedBigintNarrowings []PreviewJSONUnsignedBigint `json:"unsigned_bigint_narrowings,omitempty"`
}

// PreviewJSONUnsignedBigint is one MySQL `bigint unsigned` → PG
// `bigint` narrowing. Stable shape for tooling consumers.
type PreviewJSONUnsignedBigint struct {
	Table         string `json:"table"`
	Column        string `json:"column"`
	AutoIncrement bool   `json:"auto_increment"`
}

// PreviewJSONTranslatorGap is one detected MySQL → PG translator
// gap. Stable shape for tooling consumers (CI gates, schema-diff
// scripts) that want to fail the migration plan on any "loud"
// severity entry. v0.39.0.
type PreviewJSONTranslatorGap struct {
	Table      string `json:"table"`
	Column     string `json:"column,omitempty"`
	Constraint string `json:"constraint,omitempty"`
	Field      string `json:"field"` // "DEFAULT" / "GENERATED" / "CHECK"
	Pattern    string `json:"pattern"`
	RuleNum    int    `json:"rule"`
	Severity   string `json:"severity"` // "loud" / "silent"
	Expression string `json:"expression"`
	Note       string `json:"note"`
}

// PreviewJSONTable is one table's worth of preview output.
type PreviewJSONTable struct {
	Name         string                   `json:"name"`
	DDL          []string                 `json:"ddl"`
	Translations []PreviewJSONTranslation `json:"translations,omitempty"`
	Hints        []PreviewJSONHint        `json:"hints,omitempty"`
}

// PreviewJSONTranslation is a single column-level translation note.
type PreviewJSONTranslation struct {
	Column     string `json:"column"`
	SourceType string `json:"source_type"`
	TargetType string `json:"target_type"`
	Note       string `json:"note,omitempty"`
}

// PreviewJSONHint is a single advisory hint.
type PreviewJSONHint struct {
	Column            string `json:"column"`
	Message           string `json:"message"`
	SuggestedOverride string `json:"suggested_override"`
}

// Run executes the preview. Returns nil on success or a wrapped error
// pointing at the phase that failed.
func (p *Previewer) Run(ctx context.Context) error {
	if err := p.validate(); err != nil {
		return err
	}
	if err := validateTargetSchema(p.Target, p.TargetSchema); err != nil {
		return err
	}
	if err := validateEnabledPGExtensions(p.Source, p.Target, p.EnabledPGExtensions); err != nil {
		return err
	}

	// ---- 1. Read source schema ----
	sr, err := p.Source.OpenSchemaReader(ctx, p.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, p.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: enable PG extensions on source: %w", err))
	}

	srcSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: read source schema: %w", err))
	}
	if len(srcSchema.Tables) == 0 {
		return errors.New("preview: source schema has no tables")
	}

	// ---- 2. Filter ----
	// Engine-default exclusions (Bug 22): same shape as Migrator and
	// Streamer — merge engine-supplied patterns (e.g. PlanetScale's
	// `_vt_*`) when the operator is in exclude-or-no-filter mode.
	// Replace the field in-place; Previewer is single-shot per Run.
	if eff, added := effectiveTableFilter(p.Filter, p.Source, p.SourceDSN); len(added) > 0 {
		slog.InfoContext(ctx, "applying engine-default table exclusions",
			slog.String("engine", p.Source.Name()),
			slog.Any("patterns", added),
		)
		p.Filter = eff
	}
	if err := applyTableFilter(ctx, srcSchema, p.Filter); err != nil {
		return err
	}
	applyViewFilter(ctx, srcSchema, p.ViewFilter, p.SkipViews)

	// ---- 3. Apply mappings ----
	tgtSchema, err := translate.ApplyMappings(srcSchema, p.Mappings)
	if err != nil {
		return fmt.Errorf("preview: apply mappings: %w", err)
	}
	tgtSchema, err = translate.ApplyExpressionOverrides(tgtSchema, p.ExpressionMappings)
	if err != nil {
		return fmt.Errorf("preview: apply expression overrides: %w", err)
	}

	// ---- 3.5. Untranslatable-expression refusal (Bug 8 backstop) ----
	// v0.68.1: preview must refuse — not silently emit invalid PG —
	// when a MySQL-only construct that falls through the translator
	// verbatim has no portable PostgreSQL form. Before this, `schema
	// preview` exited 0 and printed valid-looking DDL while `migrate`
	// hard-aborted mid-pipeline on the same schema (a structural
	// false-green). The refusal fires here, BEFORE any DDL is
	// rendered, with the same diagnostic `migrate` pre-flight emits
	// so the two surfaces are consistent. SeveritySilent gaps stay
	// advisory (rendered in the translator-gaps section below); only
	// the loud, parse-fatal tail refuses. Scoped to MySQL→PG by
	// RefuseOnLoudGaps; other pairs short-circuit to nil.
	// Scan the post-override schema (tgtSchema is srcSchema after
	// ApplyMappings + ApplyExpressionOverrides) so an operator's
	// `--expr-override` escape hatch — which retags the expression
	// dialect off "mysql" — correctly suppresses the refusal.
	if err := translate.RefuseOnLoudGaps(
		tgtSchema, p.Source.Name(), p.Target.Name(), "schema preview",
		enabledExtensionSet(p.EnabledPGExtensions),
	); err != nil {
		return err
	}
	// Bug 14 GENERAL backstop: the curated RefuseOnLoudGaps above
	// catches only KNOWN MySQL-only constructs (a denylist). Any
	// MySQL-only function OUTSIDE that set (SOUNDEX(), ELT(),
	// CAST(... AS UNSIGNED), POINT(x,y), …) would still fall through
	// the PG translator verbatim and emit invalid PG (a structural
	// false-green at preview; a partial-target abort at migrate). This
	// allowlist gate refuses any function-call identifier with no
	// provable PG-valid form, AFTER the curated layer so the curated
	// construct-specific messages win when they apply. Same post-
	// override schema (tgtSchema) so `--expr-override` suppresses it.
	if err := translate.RefuseOnUntranslatableExprs(
		tgtSchema, p.Source.Name(), p.Target.Name(), "schema preview",
		enabledExtensionSet(p.EnabledPGExtensions),
	); err != nil {
		return err
	}

	// ---- 3.6. Unsigned-bigint range-narrowing notice (Bug 11) ----
	// Advisory — NOT a refusal (it must still migrate the universal
	// ORM schema). Scanned on the post-override schema so an operator's
	// `--type-override TABLE.COL=numeric` escape hatch suppresses the
	// notice for that column. Rendered in a dedicated preview section
	// so it's loud and visible alongside the translator-gaps block,
	// matching the wording `migrate` preflight logs.
	unsignedBigintNotices := translate.ScanUnsignedBigintNotices(
		tgtSchema, p.Source.Name(), p.Target.Name(),
	)

	// ---- 4. Open the target schema writer; type-assert for preview. ----
	sw, err := p.Target.OpenSchemaWriter(ctx, p.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: open target schema writer: %w", err))
	}
	applyTargetSchema(sw, p.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, sw, p.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: enable PG extensions on target: %w", err))
	}
	defer closeIf(sw)

	previewer, ok := sw.(ir.DDLPreviewer)
	if !ok {
		return fmt.Errorf("preview: target engine %q does not support schema preview (no DDLPreviewer surface)", p.Target.Name())
	}

	stmts, err := previewer.PreviewDDL(ctx, tgtSchema)
	if err != nil {
		return fmt.Errorf("preview: emit DDL: %w", err)
	}

	// ---- 5. Compute notes/hints per column. ----
	srcByTable := tableMap(srcSchema)
	tgtByTable := tableMap(tgtSchema)

	tables := previewTables(stmts, tgtSchema)

	bundle := previewBundle{
		srcEngine:    p.Source.Name(),
		tgtEngine:    p.Target.Name(),
		tables:       tables,
		srcByTable:   srcByTable,
		tgtByTable:   tgtByTable,
		appliedAlias: appliedOverrideAliases(p.Mappings),
		// Translator-gap scanner (v0.39.0): surface MySQL → PG
		// patterns the catalog deliberately doesn't auto-translate so
		// the operator gets an advisory before migrate rather than a
		// surprise at apply-time or a silent runtime divergence.
		// Returns nil for non-MySQL → PG pairs.
		translatorGaps: translate.ScanMySQLToPGGaps(
			srcSchema, p.Source.Name(), p.Target.Name(),
			enabledExtensionSet(p.EnabledPGExtensions),
		),
		unsignedBigintNotices: unsignedBigintNotices,
		redactor:              p.Redactor,
	}

	switch strings.ToLower(strings.TrimSpace(p.Format)) {
	case "", "text":
		return renderPreviewText(p.Out, bundle)
	case "json":
		return renderPreviewJSON(p.Out, bundle)
	default:
		return fmt.Errorf("preview: unknown --format %q (recognised: text, json)", p.Format)
	}
}

func (p *Previewer) validate() error {
	switch {
	case p.Source == nil:
		return errors.New("preview: Source engine is nil")
	case p.Target == nil:
		return errors.New("preview: Target engine is nil")
	case p.SourceDSN == "":
		return errors.New("preview: SourceDSN is empty")
	case p.TargetDSN == "":
		return errors.New("preview: TargetDSN is empty")
	case p.Out == nil:
		return errors.New("preview: Out writer is nil")
	}
	return nil
}

// previewBundle bundles the data renderPreviewText / renderPreviewJSON
// consume so the formatters don't take a half-dozen parameters.
type previewBundle struct {
	srcEngine      string
	tgtEngine      string
	tables         []previewTable
	srcByTable     map[string]*ir.Table
	tgtByTable     map[string]*ir.Table
	appliedAlias   map[string]bool // table.column → true when operator already applied an override
	translatorGaps []translate.Gap // v0.39.0 MySQL → PG translator-gap scan results (nil for non-cross-engine pairs)
	// unsignedBigintNotices is the Bug 11 MySQL `bigint unsigned` → PG
	// `bigint` range-narrowing advisory list (nil for non-MySQL→PG
	// pairs or when no unsigned-bigint column is present).
	unsignedBigintNotices []translate.UnsignedBigintNotice
	redactor              *redact.Registry // PII Phase 1.5 (v0.55.0) — nil/empty disables annotation
}

// enabledExtensionSet converts the operator's `--enable-pg-extension`
// flag values (a string slice) into the set shape sluice's scanner
// + translator helpers consume.
func enabledExtensionSet(flags []string) map[string]bool {
	if len(flags) == 0 {
		return nil
	}
	out := make(map[string]bool, len(flags))
	for _, f := range flags {
		out[f] = true
	}
	return out
}

// previewTable groups DDL statements + computed notes/hints per
// table, sorted by table name for deterministic output.
type previewTable struct {
	name  string
	stmts []ir.DDLStatement
}

// previewTables groups stmts by their Table field and returns them in
// schema declaration order (matching tgtSchema.Tables, which the
// engine's PreviewDDL emits in alphabetical order). Statements whose
// Table is empty are dropped — they have no per-column context to
// produce notes against, and the preview's group-by-table layout
// assumes a non-empty group key.
func previewTables(stmts []ir.DDLStatement, tgtSchema *ir.Schema) []previewTable {
	byName := make(map[string][]ir.DDLStatement, len(stmts))
	for _, s := range stmts {
		if s.Table == "" {
			continue
		}
		byName[s.Table] = append(byName[s.Table], s)
	}
	out := make([]previewTable, 0, len(byName))
	// Walk tgtSchema.Tables for declaration order; PreviewDDL itself
	// emits in alphabetical order, but the formatter doesn't need to
	// re-sort because tgtSchema.Tables is the same shape.
	seen := make(map[string]bool, len(byName))
	for _, t := range tgtSchema.Tables {
		if stmts, ok := byName[t.Name]; ok {
			out = append(out, previewTable{name: t.Name, stmts: stmts})
			seen[t.Name] = true
		}
	}
	// Tables in the statement list but not in tgtSchema (defensive;
	// shouldn't happen on a well-formed PreviewDDL result) get
	// appended in alphabetical order.
	var leftover []string
	for name := range byName {
		if !seen[name] {
			leftover = append(leftover, name)
		}
	}
	sort.Strings(leftover)
	for _, name := range leftover {
		out = append(out, previewTable{name: name, stmts: byName[name]})
	}
	return out
}

// appliedOverrideAliases returns a "table.column" set of mappings the
// operator already supplied. The text formatter uses this to suppress
// hints whose suggested override the operator has already applied —
// the hint would only be noise at that point.
func appliedOverrideAliases(mappings []config.Mapping) map[string]bool {
	out := make(map[string]bool, len(mappings))
	for _, m := range mappings {
		out[m.Table+"."+m.Column] = true
	}
	return out
}

// tableMap indexes schema.Tables by name for cheap lookup. Returns
// nil for a nil schema.
func tableMap(s *ir.Schema) map[string]*ir.Table {
	if s == nil {
		return nil
	}
	out := make(map[string]*ir.Table, len(s.Tables))
	for _, t := range s.Tables {
		out[t.Name] = t
	}
	return out
}

// computeNotes returns the cross-engine translation notes for every
// column in the table, in column declaration order. Notes whose
// source/target rendering is identical are dropped by translate.NotesFor.
func computeNotes(table string, bundle previewBundle) []translate.Note {
	src := bundle.srcByTable[table]
	tgt := bundle.tgtByTable[table]
	if src == nil || tgt == nil {
		return nil
	}
	tgtCols := make(map[string]*ir.Column, len(tgt.Columns))
	for _, c := range tgt.Columns {
		tgtCols[c.Name] = c
	}
	var out []translate.Note
	for _, sc := range src.Columns {
		tc, ok := tgtCols[sc.Name]
		if !ok {
			continue
		}
		out = append(out, translate.NotesFor(sc, tc, bundle.srcEngine, bundle.tgtEngine)...)
	}
	return out
}

// computeHints returns the advisory hints for every column in the
// table. Hints for columns the operator has already overridden are
// dropped — the hint would only be noise at that point.
func computeHints(table string, bundle previewBundle) []translate.Hint {
	src := bundle.srcByTable[table]
	tgt := bundle.tgtByTable[table]
	if src == nil || tgt == nil {
		return nil
	}
	tgtCols := make(map[string]*ir.Column, len(tgt.Columns))
	for _, c := range tgt.Columns {
		tgtCols[c.Name] = c
	}
	var out []translate.Hint
	for _, sc := range src.Columns {
		tc, ok := tgtCols[sc.Name]
		if !ok {
			continue
		}
		raw := translate.HintsFor(table, sc, tc, bundle.srcEngine, bundle.tgtEngine)
		for _, h := range raw {
			if bundle.appliedAlias[table+"."+sc.Name] {
				continue
			}
			out = append(out, h)
		}
	}
	return out
}

// renderPreviewText writes the human-readable preview to w. Format
// matches ADR-0024 §"Output structure (text format)".
func renderPreviewText(w io.Writer, bundle previewBundle) error {
	totalColumns := 0
	totalHints := 0
	tableNotes := make(map[string][]translate.Note, len(bundle.tables))
	tableHints := make(map[string][]translate.Hint, len(bundle.tables))
	for _, t := range bundle.tables {
		notes := computeNotes(t.name, bundle)
		hints := computeHints(t.name, bundle)
		tableNotes[t.name] = notes
		tableHints[t.name] = hints
		totalHints += len(hints)
		if src := bundle.srcByTable[t.name]; src != nil {
			totalColumns += len(src.Columns)
		}
	}

	var sb strings.Builder
	sb.WriteString("-- sluice schema preview\n")
	fmt.Fprintf(&sb, "-- source: %s (%d tables, %d columns)\n", bundle.srcEngine, len(bundle.tables), totalColumns)
	fmt.Fprintf(&sb, "-- target: %s\n", bundle.tgtEngine)
	fmt.Fprintf(&sb, "-- mappings applied: %d\n", len(bundle.appliedAlias))
	fmt.Fprintf(&sb, "-- advisory hints: %d\n", totalHints)
	if n := len(bundle.translatorGaps); n > 0 {
		fmt.Fprintf(&sb, "-- translator gaps: %d (see section below)\n", n)
	}
	if n := len(bundle.unsignedBigintNotices); n > 0 {
		fmt.Fprintf(&sb, "-- unsigned-bigint narrowings: %d (see section below)\n", n)
	}
	sb.WriteByte('\n')

	if len(bundle.translatorGaps) > 0 {
		writeTranslatorGapsSection(&sb, bundle.translatorGaps)
	}

	if len(bundle.unsignedBigintNotices) > 0 {
		writeUnsignedBigintSection(&sb, bundle.unsignedBigintNotices)
	}

	for _, t := range bundle.tables {
		notes := tableNotes[t.name]
		hints := tableHints[t.name]
		writeTableSection(&sb, t, notes, hints, bundle)
	}

	_, err := io.WriteString(w, sb.String())
	return err
}

// writeTranslatorGapsSection appends the MySQL → PG translator-gap
// advisory block to sb. Each gap names the catalog rule number,
// severity (loud → PG parse-fails at apply; silent → PG accepts but
// diverges), source location (table.column or constraint), and
// operator-actionable next step. v0.39.0.
func writeTranslatorGapsSection(sb *strings.Builder, gaps []translate.Gap) {
	sb.WriteString("-- ============================================================\n")
	sb.WriteString("-- Translator gaps (MySQL → Postgres)\n")
	sb.WriteString("-- ============================================================\n")
	sb.WriteString("-- sluice's translator catalog does not auto-rewrite the patterns\n")
	sb.WriteString("-- below. Each entry names the catalog rule, the severity, the\n")
	sb.WriteString("-- source location, and an actionable workaround. See\n")
	sb.WriteString("-- docs/dev/translator-coverage.md for the per-rule analysis.\n")
	sb.WriteString("--\n")
	sb.WriteString("-- Severity:\n")
	sb.WriteString("--   loud   → PG parse-fails at apply time (visible immediately)\n")
	sb.WriteString("--   silent → PG accepts but produces different output than MySQL\n")
	sb.WriteString("--            (no error; divergence surfaces in row data later)\n")
	sb.WriteString("--\n")
	for _, g := range gaps {
		fmt.Fprintf(sb, "-- [%s] rule #%d %s — ", g.Severity, g.RuleNum, g.Pattern)
		if g.Constraint != "" {
			fmt.Fprintf(sb, "%s CHECK constraint %q", g.Table, g.Constraint)
		} else {
			fmt.Fprintf(sb, "%s.%s %s", g.Table, g.Column, g.Field)
		}
		sb.WriteByte('\n')
		fmt.Fprintf(sb, "--     expr: %s\n", g.Expression)
		fmt.Fprintf(sb, "--     note: %s\n", g.Note)
	}
	sb.WriteByte('\n')
}

// writeUnsignedBigintSection appends the Bug 11 MySQL `bigint
// unsigned` → PG `bigint` range-narrowing advisory block to sb. This
// is a NOTICE, not a refusal — `schema preview` still exits 0 and
// `migrate` still proceeds. It is rendered loudly (its own section,
// same shape as the translator-gaps block) so the deliberate
// (2^63, 2^64) range loss is never silent. Wording mirrors what
// `migrate` preflight logs at WARN.
func writeUnsignedBigintSection(sb *strings.Builder, notices []translate.UnsignedBigintNotice) {
	sb.WriteString("-- ============================================================\n")
	sb.WriteString("-- Unsigned-bigint range narrowing (MySQL -> Postgres)\n")
	sb.WriteString("-- ============================================================\n")
	sb.WriteString("-- MySQL `bigint unsigned` maps uniformly to PostgreSQL `bigint`\n")
	sb.WriteString("-- (PostgreSQL has no unsigned 64-bit type). This keeps PRIMARY\n")
	sb.WriteString("-- KEY and FOREIGN KEY column types consistent so FKs to\n")
	sb.WriteString("-- AUTO_INCREMENT keys are created successfully. The tradeoff:\n")
	sb.WriteString("-- values > 2^63-1 (9223372036854775807) are NOT representable\n")
	sb.WriteString("-- on the target. This is advisory; the migration proceeds.\n")
	sb.WriteString("-- Override a column that genuinely exceeds 2^63-1 with\n")
	sb.WriteString("-- `--type-override TABLE.COL=numeric` (numeric(20,0) keeps the\n")
	sb.WriteString("-- full unsigned 64-bit range; it cannot also be an IDENTITY key).\n")
	sb.WriteString("--\n")
	for _, n := range notices {
		fmt.Fprintf(sb, "-- %s.%s: bigint unsigned -> bigint", n.Table, n.Column)
		if n.AutoIncrement {
			sb.WriteString(" (AUTO_INCREMENT — IDs in practice never reach 2^63)")
		}
		sb.WriteByte('\n')
	}
	sb.WriteByte('\n')
}

// writeTableSection appends one table's preview block to sb. The
// block is structured as: section header → DDL with inline notes →
// advisory hints (if any).
func writeTableSection(sb *strings.Builder, t previewTable, notes []translate.Note, hints []translate.Hint, bundle previewBundle) {
	noteByCol := make(map[string]translate.Note, len(notes))
	for _, n := range notes {
		// First note wins per column; the registry is small enough
		// that a column getting two notes is unusual today.
		if _, exists := noteByCol[n.Column]; !exists {
			noteByCol[n.Column] = n
		}
	}

	cols := 0
	if src := bundle.srcByTable[t.name]; src != nil {
		cols = len(src.Columns)
	}
	noteCount := countTranslations(notes)
	fmt.Fprintf(sb, "-- ──────────── %s ────────────\n", t.name)
	fmt.Fprintf(sb, "-- %d columns; %d cross-engine translation%s; %d hint%s\n\n",
		cols, noteCount, plural(noteCount), len(hints), plural(len(hints)))

	// Build per-column redact strategy-name map (PII Phase 1.5).
	// Empty registry produces a nil map; the annotator short-
	// circuits.
	redactByCol := redactNotesForTable(bundle.redactor, t.name, bundle)

	// DDL: emit each statement, attaching inline notes on
	// CREATE TABLE column lines that match a noted column or a
	// redacted column.
	for i, stmt := range t.stmts {
		if stmt.Kind == "CREATE TABLE" && (len(noteByCol) > 0 || len(redactByCol) > 0) {
			sb.WriteString(annotateCreateTable(stmt.SQL, noteByCol, redactByCol))
		} else {
			sb.WriteString(stmt.SQL)
		}
		sb.WriteString(";\n")
		if i < len(t.stmts)-1 {
			sb.WriteByte('\n')
		}
	}

	if len(hints) > 0 {
		sb.WriteByte('\n')
		for _, h := range hints {
			fmt.Fprintf(sb, "-- hint: %s\n", h.Message)
			fmt.Fprintf(sb, "--       %s\n", h.SuggestedOverride)
		}
	}
	sb.WriteByte('\n')
}

// annotateCreateTable returns ddl with inline `-- source: X → target:
// Y` comments appended to lines whose first quoted/backticked
// identifier matches a noted column. The matcher is deliberately
// shallow — the goal is best-effort annotation on the column lines a
// human would expect to see annotated, not a full DDL parser. Lines
// that don't match a noted column pass through unchanged.
func annotateCreateTable(ddl string, notes map[string]translate.Note, redactByCol map[string]string) string {
	lines := strings.Split(ddl, "\n")
	for i, line := range lines {
		colName := firstIdentifier(line)
		if colName == "" {
			continue
		}
		annotated := line
		if note, ok := notes[colName]; ok {
			annotated = appendNoteComment(annotated, note)
		}
		if strategyName, ok := redactByCol[colName]; ok {
			annotated = appendRedactComment(annotated, strategyName)
		}
		lines[i] = annotated
	}
	return strings.Join(lines, "\n")
}

// appendRedactComment appends a `-- REDACTED via <strategy>` comment
// to line. PII Phase 1.5 (v0.55.0). Mirrors [appendNoteComment]'s
// trailing-comma handling so the line stays parseable as DDL if
// anyone copies it from the preview output.
func appendRedactComment(line, strategyName string) string {
	body := "REDACTED via " + strategyName
	if strings.Contains(line, "--") {
		return line + "; " + body
	}
	trimmed := strings.TrimRight(line, " \t")
	if strings.HasSuffix(trimmed, ",") {
		return strings.TrimSuffix(trimmed, ",") + ", -- " + body
	}
	return trimmed + "  -- " + body
}

// redactNotesForTable returns a column-name → strategy-name map for
// the given table, drawn from the registry's rules. Returns nil
// when the registry is empty.
//
// Lookup honours the same bare-schema fallback as
// [Registry.Get]: rules registered for the empty schema apply
// to every table's columns; rules registered for a specific
// schema apply only to that schema's table. The bundle's
// srcByTable supplies the source schema name; if the table isn't
// found in srcByTable, we fall back to checking the bare-schema
// form alone.
func redactNotesForTable(r *redact.Registry, tableName string, bundle previewBundle) map[string]string {
	if r.Empty() {
		return nil
	}
	srcTable, ok := bundle.srcByTable[tableName]
	schema := ""
	if ok && srcTable != nil {
		schema = srcTable.Schema
	}
	out := map[string]string{}
	for _, rule := range r.Rules() {
		// Schema mismatch: skip unless the rule's schema is bare
		// (empty), in which case it applies to all schemas.
		if rule.Schema != "" && rule.Schema != schema {
			continue
		}
		if rule.Table != tableName {
			continue
		}
		out[rule.Column] = rule.Strategy.Name()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// firstIdentifier returns the first quoted identifier on a line —
// either backtick-quoted (`name`, MySQL) or double-quote-quoted
// ("name", Postgres). Returns "" when no identifier is present.
// Helper for annotating CREATE TABLE column lines.
func firstIdentifier(line string) string {
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == "" {
		return ""
	}
	var quote byte
	switch trimmed[0] {
	case '`', '"':
		quote = trimmed[0]
	default:
		return ""
	}
	rest := trimmed[1:]
	end := strings.IndexByte(rest, quote)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// appendNoteComment appends a `-- source: X → target: Y[; message]`
// comment to line. Preserves any trailing comma or whitespace by
// inserting before them.
func appendNoteComment(line string, note translate.Note) string {
	// Build comment body.
	body := fmt.Sprintf("source: %s -> target: %s", note.SourceType, note.TargetType)
	if note.Message != "" {
		body += "; " + note.Message
	}
	// If line already has a comment, we don't try to merge — append
	// a separator. Today's emitters don't emit comments on column
	// lines so this branch is defensive.
	if strings.Contains(line, "--") {
		return line + "  -- " + body
	}
	// Insert before any trailing comma so the line stays parseable
	// as DDL if anyone copies it.
	trimmed := strings.TrimRight(line, " \t")
	if strings.HasSuffix(trimmed, ",") {
		return strings.TrimSuffix(trimmed, ",") + ", -- " + body
	}
	return trimmed + "  -- " + body
}

// countTranslations returns the count of cross-engine translation
// notes — both bare type-change notes and message-bearing notes.
// Surfaced in the per-table header; matches the inline-comment count
// the operator sees in the DDL block.
func countTranslations(notes []translate.Note) int {
	return len(notes)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// renderPreviewJSON writes the JSON-format preview to w.
func renderPreviewJSON(w io.Writer, bundle previewBundle) error {
	out := PreviewJSON{
		SourceEngine: bundle.srcEngine,
		TargetEngine: bundle.tgtEngine,
		Tables:       make([]PreviewJSONTable, 0, len(bundle.tables)),
	}
	for _, t := range bundle.tables {
		ddl := make([]string, len(t.stmts))
		for i, s := range t.stmts {
			ddl[i] = s.SQL + ";"
		}
		notes := computeNotes(t.name, bundle)
		hints := computeHints(t.name, bundle)
		jt := PreviewJSONTable{
			Name: t.name,
			DDL:  ddl,
		}
		for _, n := range notes {
			// Every note (bare or message-carrying) lands in the
			// JSON output so tooling can flag any cross-engine type
			// change without reparsing the DDL strings. The text
			// renderer suppresses redundant header lines for bare
			// notes; tooling consumers don't have that constraint.
			jt.Translations = append(jt.Translations, PreviewJSONTranslation{
				Column:     n.Column,
				SourceType: n.SourceType,
				TargetType: n.TargetType,
				Note:       n.Message,
			})
		}
		for _, h := range hints {
			jt.Hints = append(jt.Hints, PreviewJSONHint{
				Column:            h.Column,
				Message:           h.Message,
				SuggestedOverride: h.SuggestedOverride,
			})
		}
		out.Tables = append(out.Tables, jt)
	}
	for _, g := range bundle.translatorGaps {
		out.TranslatorGaps = append(out.TranslatorGaps, PreviewJSONTranslatorGap{
			Table:      g.Table,
			Column:     g.Column,
			Constraint: g.Constraint,
			Field:      g.Field,
			Pattern:    g.Pattern,
			RuleNum:    g.RuleNum,
			Severity:   g.Severity.String(),
			Expression: g.Expression,
			Note:       g.Note,
		})
	}
	for _, n := range bundle.unsignedBigintNotices {
		out.UnsignedBigintNarrowings = append(out.UnsignedBigintNarrowings, PreviewJSONUnsignedBigint{
			Table:         n.Table,
			Column:        n.Column,
			AutoIncrement: n.AutoIncrement,
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
