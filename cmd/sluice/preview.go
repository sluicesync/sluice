package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/pipeline"
)

// SchemaCmd groups subcommands that inspect or describe schemas
// without modifying them — plus the read-mostly `add-table`
// command that brings a new source table into an active stream's
// scope (Phase 1 MVP, see `docs/dev/design-mid-stream-add-table.md`).
//
// `add-table` lives here rather than under `sync` because it is a
// schema-shape mutation that the operator drives explicitly,
// alongside the source-side `CREATE TABLE` they just ran. The flow
// is: drain the stream (`sluice sync stop --wait`), run
// `sluice schema add-table SOURCE.NAME`, then resume
// (`sluice sync start --resume`).
type SchemaCmd struct {
	Preview  SchemaPreviewCmd  `cmd:"" help:"Render the target DDL sluice would emit, with cross-engine translation notes and advisory hints."`
	Diff     SchemaDiffCmd     `cmd:"" help:"Compare the expected target DDL (source -> translation) against the actual on-target schema; report drift with copy-paste DDL suggestions."`
	AddTable SchemaAddTableCmd `cmd:"add-table" help:"Bring a new source table into an active stream's scope without a destructive --reset-target-data cycle. Drain the stream first via 'sluice sync stop --wait'."`
}

// SchemaPreviewCmd implements `sluice schema preview` (ADR-0024).
// Reads the source schema, applies translation (mappings + cross-
// engine type policy), asks the target engine for the DDL it would
// emit, and writes the result to stdout (or --output FILE).
type SchemaPreviewCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN. The preview never writes to the target; the DSN is only used to construct the schema writer that emits the DDL." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only preview these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Preview every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	IncludeView []string `help:"Only preview these views (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-view." sep:"," placeholder:"VIEW"`
	ExcludeView []string `help:"Preview every view except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-view." sep:"," placeholder:"VIEW"`
	SkipViews   bool     `help:"Skip views entirely in preview output."`

	TypeOverride []string `help:"Force a specific target type for a column (repeatable). Format: 'TABLE.COLUMN=TYPE', e.g. 'users.id=binary_uuid'. CLI form of the YAML 'mappings:' config; for target-type options, use the YAML form." placeholder:"TABLE.COLUMN=TYPE"`

	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. Emitted verbatim; ADR-0016 translator skips overridden columns. CLI form of the YAML 'expression_mappings:' config." placeholder:"TABLE.COLUMN=EXPRESSION"`

	Format string `help:"Output format: 'text' (human-readable, default) or 'json' (machine-readable for tooling)." default:"text" enum:"text,json" placeholder:"FORMAT"`

	Output string `help:"Write to FILE instead of stdout. Atomic: written to a sibling temp file in the destination directory, then renamed into place." short:"o" placeholder:"FILE"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). Renders preview DDL prefixed with this schema so operators see exactly what 'sluice migrate' / 'sync start' would emit under --target-schema (ADR-0031). MySQL operators use a different --target DSN database instead." placeholder:"NAME"`

	EnablePGExtension []string `help:"Enable passthrough for a Postgres extension type (repeatable). Same-engine PG → PG passthrough; hstore and citext additionally have built-in cross-engine translators on MySQL targets. Recognised: vector (pgvector), pg_trgm, hstore, citext. See ADR-0032." placeholder:"EXT"`

	Redact          []string `help:"Annotate redacted columns in the preview DDL (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies match 'sluice migrate --redact' (null, static:<v>, hash:sha256, hash:hmac-sha256, truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid, randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid). PII Phase 1.5 (v0.55.0+): operator can SEE what 'sluice migrate' / 'sync start' would redact before committing. Each redacted column's CREATE TABLE line gets a trailing '-- REDACTED via <strategy>' comment. The DDL itself is unchanged — preview only annotates." placeholder:"RULE" sep:"none"`
	RedactKeySource string   `help:"Source for the HMAC keyset when --redact rules use 'hash:hmac-sha256'. Same forms as 'sluice migrate --redact-key-source'." placeholder:"SRC"`
}

// Run implements `sluice schema preview`.
func (s *SchemaPreviewCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(s.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	if len(s.IncludeTable) > 0 && len(s.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	if len(s.IncludeView) > 0 && len(s.ExcludeView) > 0 {
		return errors.New("--include-view and --exclude-view are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(s.IncludeTable, s.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}
	viewFilter, err := pipeline.NewViewFilter(s.IncludeView, s.ExcludeView)
	if err != nil {
		return err
	}

	mappings, err := resolveMappings(s.TypeOverride, cfg)
	if err != nil {
		return err
	}
	exprMappings, err := resolveExpressionMappings(s.ExprOverride, cfg)
	if err != nil {
		return err
	}

	writer, finalize, err := openPreviewOutput(s.Output)
	if err != nil {
		return err
	}
	// Always finalize: on success, the temp file is renamed into
	// place; on error, the temp file is removed. finalize is a
	// no-op for the stdout path.
	defer func() { _ = finalize(err) }()

	keySource := s.RedactKeySource
	if keySource == "" {
		keySource = cfg.RedactKeySource
	}
	redactor, err := parseRedactFlags(s.Redact, keySource, "")
	if err != nil {
		return err
	}
	redactor, err = mergeYAMLRedactions(redactor, cfg.Redactions, keySource, "")
	if err != nil {
		return fmt.Errorf("redactions (YAML): %w", err)
	}
	prev := &pipeline.Previewer{
		Source:              source,
		Target:              target,
		SourceDSN:           s.Source,
		TargetDSN:           s.Target,
		Mappings:            mappings,
		ExpressionMappings:  exprMappings,
		Filter:              filter,
		ViewFilter:          viewFilter,
		SkipViews:           s.SkipViews,
		Format:              s.Format,
		Out:                 writer,
		TargetSchema:        s.TargetSchema,
		EnabledPGExtensions: s.EnablePGExtension,
		Redactor:            redactor,
	}
	err = prev.Run(kongContext())
	return err
}

// openPreviewOutput resolves `--output FILE` (or stdout when empty)
// to an io.Writer plus a finalize callback. The callback's err
// argument is the Run error: nil → commit (rename temp into place),
// non-nil → discard (remove temp). Stdout has a no-op finalize.
//
// Atomic-write semantics: the temp file lives in the same directory
// as the destination so the os.Rename stays on the same volume
// (POSIX guarantees atomicity in that case; Windows ReplaceFile
// guarantees the same on NTFS). A Ctrl-C mid-write leaves the temp
// file behind for the operator to discover, but never corrupts the
// destination.
func openPreviewOutput(path string) (io.Writer, func(err error) error, error) {
	if path == "" {
		return os.Stdout, func(error) error { return nil }, nil
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, base+".tmp.*")
	if err != nil {
		return nil, nil, fmt.Errorf("atomic output: create temp file in %q: %w", dir, err)
	}

	finalize := func(runErr error) error {
		// Close first; rename or remove afterward.
		closeErr := tmp.Close()
		if runErr != nil {
			// runErr already surfaced from Run; discard the temp
			// file silently and return nil so the deferred call
			// doesn't shadow the upstream error. The caller
			// inspects runErr directly.
			_ = os.Remove(tmp.Name())
			return nil //nolint:nilerr // intentional: don't shadow upstream Run error
		}
		if closeErr != nil {
			_ = os.Remove(tmp.Name())
			return fmt.Errorf("atomic output: close temp file: %w", closeErr)
		}
		if err := os.Rename(tmp.Name(), path); err != nil {
			_ = os.Remove(tmp.Name())
			return fmt.Errorf("atomic output: rename temp file into %q: %w", path, err)
		}
		return nil
	}

	return tmp, finalize, nil
}
