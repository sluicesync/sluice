package main

import (
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/pipeline"
)

// SchemaDiffCmd implements `sluice schema diff` (ADR-0029). Reads the
// source schema, applies translation (mappings + cross-engine type
// policy), reads the actual target schema via the same SchemaReader
// surface, and renders the structural delta to stdout (or --output
// FILE).
//
// Read-only: the suggested ALTER/DROP DDL in the text output is for
// operator-driven reconciliation. No `--apply` flag exists by design
// (see ADR-0029 §"Why not auto-converge").
type SchemaDiffCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN. The diff never modifies the target; the DSN is used only to read the actual schema for comparison." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only diff these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Diff every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	IncludeView []string `help:"Only diff these views (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-view." sep:"," placeholder:"VIEW"`
	ExcludeView []string `help:"Diff every view except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-view." sep:"," placeholder:"VIEW"`
	SkipViews   bool     `help:"Skip view comparison entirely. Useful when views are managed out-of-band and target-side view drift isn't sluice's concern."`

	TypeOverride []string `help:"Force a specific target type for a column (repeatable). Format: 'TABLE.COLUMN=TYPE', e.g. 'users.id=binary_uuid'. CLI form of the YAML 'mappings:' config; for target-type options, use the YAML form." placeholder:"TABLE.COLUMN=TYPE" sep:"none"`

	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. Emitted verbatim; ADR-0016 translator skips overridden columns. CLI form of the YAML 'expression_mappings:' config." placeholder:"TABLE.COLUMN=EXPRESSION" sep:"none"`

	Format string `help:"Output format: 'text' (human-readable, default) or 'json' (machine-readable for CI gates)." default:"text" enum:"text,json" placeholder:"FORMAT"`

	Output string `help:"Write to FILE instead of stdout. Atomic: written to a sibling temp file in the destination directory, then renamed into place." short:"o" placeholder:"FILE"`

	IgnoreCharsetCollation bool `help:"Suppress MySQL-specific charset/collation diffs (operators often manage these out-of-band via server defaults)."`
	IgnoreExtras           bool `help:"Suppress 'extra on target' diffs (tables/columns/indexes present on the target but absent from the source). Useful when the target hosts other applications' tables."`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, the diff reads the target schema from this namespace rather than the DSN's default, and renders DDL suggestions prefixed with the schema name. ADR-0031. MySQL operators use a different --target DSN database instead." placeholder:"NAME"`

	EnablePGExtension []string `help:"Enable passthrough for a Postgres extension type (repeatable). Same-engine PG → PG passthrough; hstore and citext additionally have built-in cross-engine translators on MySQL targets. Recognised: vector (pgvector), pg_trgm, hstore, citext. See ADR-0032." placeholder:"EXT"`

	InjectShardColumn string `help:"ADR-0048 Shape A — diff a consolidated Shape-A target against the per-shard source. Format: NAME=VALUE (matches the value used at migrate / sync start time). The diff applies the same IR-pass the migrate / sync paths apply, then compares; the sluice-injected discriminator column is suppressed from 'extra column on target' drift via the IR's SluiceInjected provenance marker. Off when empty (default)." placeholder:"NAME=VALUE"`
}

// Run implements `sluice schema diff`. Returns:
//   - nil error on a clean (no-drift) run → kong exits 0.
//   - driftError when drift is detected → kong exits 1 (CI gate fails).
//   - operationalError on read/render failure → kong exits 2.
func (s *SchemaDiffCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return operationalError{err: err}
	}

	source, err := resolveEngine(s.SourceDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--source-driver: %w", err)}
	}
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}

	if len(s.IncludeTable) > 0 && len(s.ExcludeTable) > 0 {
		return operationalError{err: errors.New("--include-table and --exclude-table are mutually exclusive")}
	}
	if len(s.IncludeView) > 0 && len(s.ExcludeView) > 0 {
		return operationalError{err: errors.New("--include-view and --exclude-view are mutually exclusive")}
	}
	include, exclude := resolveTableFilterArgs(s.IncludeTable, s.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return operationalError{err: err}
	}
	viewFilter, err := pipeline.NewViewFilter(s.IncludeView, s.ExcludeView)
	if err != nil {
		return operationalError{err: err}
	}

	mappings, err := resolveMappings(s.TypeOverride, cfg)
	if err != nil {
		return operationalError{err: err}
	}
	exprMappings, err := resolveExpressionMappings(s.ExprOverride, cfg)
	if err != nil {
		return operationalError{err: err}
	}

	writer, finalize, err := openPreviewOutput(s.Output)
	if err != nil {
		return operationalError{err: err}
	}
	// finalize: on success commit (rename temp into place), on error
	// discard (remove temp). Stdout has a no-op finalize.
	var runErr error
	defer func() { _ = finalize(runErr) }()

	shardSpec, err := parseInjectShardColumn(s.InjectShardColumn)
	if err != nil {
		return operationalError{err: err}
	}

	differ := &pipeline.Differ{
		Source:                 source,
		Target:                 target,
		SourceDSN:              s.Source,
		TargetDSN:              s.Target,
		Mappings:               mappings,
		ExpressionMappings:     exprMappings,
		Filter:                 filter,
		ViewFilter:             viewFilter,
		SkipViews:              s.SkipViews,
		Format:                 s.Format,
		IgnoreCharsetCollation: s.IgnoreCharsetCollation,
		IgnoreExtras:           s.IgnoreExtras,
		Out:                    writer,
		TargetSchema:           s.TargetSchema,
		EnabledPGExtensions:    s.EnablePGExtension,
		InjectShardColumn:      shardSpec,
	}
	diff, err := differ.Run(kongContext())
	if err != nil {
		runErr = err
		return operationalError{err: err}
	}
	if diff != nil && diff.HasChanges() {
		// Diff was rendered to writer; signal CI-gating exit code 1
		// without re-printing the body. The error message is one
		// line on stderr, useful for log aggregators.
		return driftError{summary: diff.Summary()}
	}
	return nil
}

// driftError signals "schemas differ" with kong exit code 1. Kong's
// FatalIfErrorf prints the message via Errorf to stderr and calls
// Exit(1). The message stays short — the diff body is on stdout (or
// --output FILE), and a CI log prefers a single-line summary.
type driftError struct{ summary string }

func (driftError) ExitCode() int { return 1 }

func (d driftError) Error() string {
	if d.summary != "" {
		return "drift detected: " + d.summary
	}
	return "drift detected"
}

// _ enforces that ir.SchemaDiff exposes the Summary() string used
// above; a removed/renamed Summary method would fail to build here,
// catching the API drift at compile time rather than at runtime.
var _ = (ir.SchemaDiff{}).Summary

// operationalError signals "we couldn't run the gate" with kong exit
// code 2 — distinct from drift so CI scripts don't conflate "the
// schemas differ" with "we couldn't connect to the target". The
// underlying error is unwrapped intact for any caller that wants to
// errors.Is / errors.As against it.
type operationalError struct{ err error }

func (operationalError) ExitCode() int { return 2 }
func (o operationalError) Error() string {
	if o.err == nil {
		return "operational error"
	}
	return o.err.Error()
}
func (o operationalError) Unwrap() error { return o.err }
