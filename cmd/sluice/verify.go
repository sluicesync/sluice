// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/pipeline"
)

// VerifyCmd implements `sluice verify`. v0.12.0 ships count-mode
// only — row-count comparison per table. Sample mode and full mode
// follow per the proto-ADR (docs/dev/design/sluice-verify.md).
//
// Exit codes mirror `schema diff`:
//   - 0 on clean (every table's source count == target count).
//   - 1 on mismatch (at least one table differs).
//   - 2 on operational error (couldn't connect, engine unsupported,
//     etc.).
type VerifyCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN. Verify never modifies the target." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only verify these tables (comma-separated, repeatable). Glob patterns allowed (e.g. 'audit_*'). Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Verify every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	Depth string `help:"Verification depth. 'count' (default; row-count comparison) or 'sample' (count + per-table sampled-row content hashes; ~99% confidence on 5%+ corruption). 'full' planned per proto-ADR." default:"count" enum:"count,sample" placeholder:"DEPTH"`

	SampleRowsPerTable int   `help:"Per-table sample size when --depth=sample. Default 100 gives ~99% confidence of detecting a 5%+ corruption rate; raise for stronger guarantees on tables with rare anomalies." default:"100" placeholder:"N"`
	SampleSeed         int64 `help:"Seed for deterministic sampling when --depth=sample. Same seed → same sample row set on source and target. Default 42; change to 'reshuffle' the sample." default:"42" placeholder:"N"`
	StrictHash         bool  `help:"Use SHA-256 instead of MD5 for sample-mode row hashing. MD5 is statistically sufficient for honest-data scenarios at any practical row count (see docs/verify-vs-vitess-vdiff.md for the collision math); --strict-hash gives operators an extra confidence margin and matches compliance postures that require SHA-256. Costs ~2× the server-side hashing time; difference is sub-second at typical sample sizes."`

	Format string `help:"Output format: 'text' (default) or 'json' (machine-readable for CI gates / alertmanager pipes)." default:"text" enum:"text,json" placeholder:"FORMAT"`
	Output string `help:"Write to FILE instead of stdout. Atomic." short:"o" placeholder:"FILE"`
}

// Run implements `sluice verify`.
func (v *VerifyCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return operationalError{err: err}
	}

	source, err := resolveEngine(v.SourceDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--source-driver: %w", err)}
	}
	target, err := resolveEngine(v.TargetDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}
	// Value-fidelity flags (task 2.5): verify reads source + target values, so
	// its readers honor --zero-date / --sqlite-date-encoding / --mysql-sql-mode.
	if source, err = applyEngineOptions(source, g); err != nil {
		return operationalError{err: err}
	}
	if target, err = applyEngineOptions(target, g); err != nil {
		return operationalError{err: err}
	}

	if len(v.IncludeTable) > 0 && len(v.ExcludeTable) > 0 {
		return operationalError{err: errors.New("--include-table and --exclude-table are mutually exclusive")}
	}
	include, exclude := resolveTableFilterArgs(v.IncludeTable, v.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return operationalError{err: err}
	}

	writer, finalize, err := openVerifyOutput(v.Output)
	if err != nil {
		return operationalError{err: err}
	}
	var runErr error
	defer func() { _ = finalize(runErr) }()

	verifier := &pipeline.Verifier{
		Source:             source,
		Target:             target,
		SourceDSN:          v.Source,
		TargetDSN:          v.Target,
		Depth:              pipeline.VerifyDepth(v.Depth),
		SampleRowsPerTable: v.SampleRowsPerTable,
		SampleSeed:         v.SampleSeed,
		StrictHash:         v.StrictHash,
		Filter:             filter,
		Format:             v.Format,
		Out:                writer,
	}
	result, err := verifier.Run(kongContext())
	if err != nil {
		runErr = err
		return operationalError{err: err}
	}
	if result != nil && result.HasMismatch() {
		return driftError{summary: fmt.Sprintf("%d table(s) with row-count mismatch", result.Summary.TablesMismatch)}
	}
	return nil
}

// openVerifyOutput mirrors openPreviewOutput's atomic-write shape but
// kept verify-local to avoid forcing the diff/preview helpers to grow
// new responsibilities. When path == "" returns os.Stdout with a no-op
// finalizer.
func openVerifyOutput(path string) (io.Writer, func(error) error, error) {
	if path == "" {
		return os.Stdout, func(error) error { return nil }, nil
	}
	return openPreviewOutput(path)
}
