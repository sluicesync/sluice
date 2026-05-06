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

	// Filter selects which source tables participate in the preview.
	// Empty (zero value) keeps every table the source schema reader
	// returns.
	Filter TableFilter

	// Format is "text" (default human-readable form) or "json"
	// (machine-readable for tooling). Empty defaults to "text".
	Format string

	// Out is the destination for the rendered preview. Required.
	// `--output FILE` is a CLI concern; the orchestrator writes to
	// the supplied io.Writer regardless.
	Out io.Writer
}

// PreviewJSON is the JSON-format preview output. The shape is stable
// for tooling consumers — schema-diff scripts, CI gates that flag bad
// translations, etc. Adding fields is a backward-compatible change;
// renaming or removing them is not.
type PreviewJSON struct {
	SourceEngine string             `json:"source_engine"`
	TargetEngine string             `json:"target_engine"`
	Tables       []PreviewJSONTable `json:"tables"`
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

	// ---- 1. Read source schema ----
	sr, err := p.Source.OpenSchemaReader(ctx, p.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: open source schema reader: %w", err))
	}
	defer closeIf(sr)

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

	// ---- 3. Apply mappings ----
	tgtSchema, err := translate.ApplyMappings(srcSchema, p.Mappings)
	if err != nil {
		return fmt.Errorf("preview: apply mappings: %w", err)
	}

	// ---- 4. Open the target schema writer; type-assert for preview. ----
	sw, err := p.Target.OpenSchemaWriter(ctx, p.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("preview: open target schema writer: %w", err))
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
	srcEngine    string
	tgtEngine    string
	tables       []previewTable
	srcByTable   map[string]*ir.Table
	tgtByTable   map[string]*ir.Table
	appliedAlias map[string]bool // table.column → true when operator already applied an override
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
	sb.WriteByte('\n')

	for _, t := range bundle.tables {
		notes := tableNotes[t.name]
		hints := tableHints[t.name]
		writeTableSection(&sb, t, notes, hints, bundle)
	}

	_, err := io.WriteString(w, sb.String())
	return err
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

	// DDL: emit each statement, attaching inline notes on
	// CREATE TABLE column lines that match a noted column.
	for i, stmt := range t.stmts {
		if stmt.Kind == "CREATE TABLE" && len(noteByCol) > 0 {
			sb.WriteString(annotateCreateTable(stmt.SQL, noteByCol))
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
func annotateCreateTable(ddl string, notes map[string]translate.Note) string {
	lines := strings.Split(ddl, "\n")
	for i, line := range lines {
		colName := firstIdentifier(line)
		if colName == "" {
			continue
		}
		note, ok := notes[colName]
		if !ok {
			continue
		}
		lines[i] = appendNoteComment(line, note)
	}
	return strings.Join(lines, "\n")
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
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
