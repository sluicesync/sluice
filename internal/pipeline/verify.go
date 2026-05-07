// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// `sluice verify` orchestration. v0.12.0 ships count-mode only —
// row-count comparison per table — per the proto-ADR's MVP slice
// (docs/dev/design-sluice-verify.md). Sample mode and full mode follow.
//
// Engine-neutral: every per-engine operation goes through the
// [ir.Verifier] optional interface, type-asserted from the engine's
// SchemaReader. Engines without Verifier surface a clear "not
// supported" operational error.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// VerifyDepth selects which depth of verification to run. Count is the
// MVP and the only supported depth in v0.12.0; sample and full are
// reserved for the proto-ADR's later phases.
type VerifyDepth string

// Recognised VerifyDepth values.
const (
	VerifyDepthCount VerifyDepth = "count"
	// VerifyDepthSample VerifyDepth = "sample" // post-MVP
	// VerifyDepthFull   VerifyDepth = "full"   // post-MVP
)

// Verifier runs a single verify pass against the configured source/
// target pair. Same shape as [Differ]: hold config, call Run.
type Verifier struct {
	// Source / Target are the engines for the source/target DSNs.
	// Required.
	Source ir.Engine
	Target ir.Engine

	// SourceDSN / TargetDSN — required.
	SourceDSN string
	TargetDSN string

	// Depth selects the verification mode. Empty defaults to count.
	Depth VerifyDepth

	// Filter selects which source tables participate. Empty (zero
	// value) keeps every source table.
	Filter TableFilter

	// Format is "text" (default) or "json".
	Format string

	// Out is the destination for the rendered report. Required.
	Out io.Writer
}

// VerifyResult is the structured outcome of a verify run. The renderer
// consumes this; CLI callers also inspect HasMismatch() to set exit
// codes.
type VerifyResult struct {
	SourceEngine string              `json:"source_engine"`
	TargetEngine string              `json:"target_engine"`
	Depth        VerifyDepth         `json:"depth"`
	Tables       []VerifyTableResult `json:"tables"`
	// ExtraOnTarget lists table names present on the target but
	// absent from the source. Sorted alphabetically. Empty when no
	// extras exist or when the source-only set is unknown.
	ExtraOnTarget []string      `json:"extra_on_target,omitempty"`
	Summary       VerifySummary `json:"summary"`
}

// VerifyTableResult is the per-table outcome.
type VerifyTableResult struct {
	Name           string `json:"name"`
	SourceRowCount int64  `json:"source_row_count"`
	TargetRowCount int64  `json:"target_row_count"`
	CountMismatch  bool   `json:"count_mismatch"`
	// Reason is non-empty when this table couldn't be verified
	// (e.g. table not on target side, engine doesn't support
	// Verifier). The Source/Target counts are 0 when Reason is set.
	Reason string `json:"reason,omitempty"`
}

// VerifySummary is the high-level rollup the operator reads first.
type VerifySummary struct {
	TablesChecked  int `json:"tables_checked"`
	TablesClean    int `json:"tables_clean"`
	TablesMismatch int `json:"tables_mismatch"`
	TablesSkipped  int `json:"tables_skipped"`
	// TablesExtraOnTarget is the count of tables present on the
	// target but absent from the source. Reported informationally
	// (the names land in [VerifyResult.ExtraOnTarget]); not flagged
	// as mismatches since extra tables don't imply data loss — they
	// could be operator-managed tables outside sluice's scope. The
	// existing `sluice schema diff` is the right tool when they are
	// drift; verify keeps its lane focused on row-data concerns.
	TablesExtraOnTarget int `json:"tables_extra_on_target,omitempty"`
}

// HasMismatch reports whether any table failed verification. Used by
// the CLI to set exit code 1 (vs 0 for clean, 2 for op error).
func (r *VerifyResult) HasMismatch() bool {
	return r.Summary.TablesMismatch > 0
}

// Run executes the verify pass. Returns the result + a possibly-nil
// error. On operational failure (couldn't open a connection, engine
// doesn't implement Verifier, etc.) returns (nil, error). On
// successful execution returns a non-nil result whose HasMismatch
// distinguishes clean from drift.
func (v *Verifier) Run(ctx context.Context) (*VerifyResult, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	depth := v.Depth
	if depth == "" {
		depth = VerifyDepthCount
	}
	if depth != VerifyDepthCount {
		return nil, fmt.Errorf("verify: depth %q not supported in v0.12.0 MVP (count only)", depth)
	}

	sr, err := v.Source.OpenSchemaReader(ctx, v.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("verify: open source schema reader: %w", err)
	}
	defer closeIf(sr)

	tr, err := v.Target.OpenSchemaReader(ctx, v.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("verify: open target schema reader: %w", err)
	}
	defer closeIf(tr)

	srcVerifier, ok := sr.(ir.Verifier)
	if !ok {
		return nil, fmt.Errorf("verify: source engine %q does not support data verification (no ir.Verifier implementation)", v.Source.Name())
	}
	tgtVerifier, ok := tr.(ir.Verifier)
	if !ok {
		return nil, fmt.Errorf("verify: target engine %q does not support data verification (no ir.Verifier implementation)", v.Target.Name())
	}

	srcSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: read source schema: %w", err)
	}
	if len(srcSchema.Tables) == 0 {
		return nil, errors.New("verify: source schema has no tables")
	}
	if err := applyTableFilter(ctx, srcSchema, v.Filter); err != nil {
		return nil, err
	}

	tgtSchema, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: read target schema: %w", err)
	}
	tgtTables := make(map[string]*ir.Table, len(tgtSchema.Tables))
	for _, t := range tgtSchema.Tables {
		tgtTables[t.Name] = t
	}
	srcNames := make(map[string]struct{}, len(srcSchema.Tables))
	for _, t := range srcSchema.Tables {
		srcNames[t.Name] = struct{}{}
	}

	result := &VerifyResult{
		SourceEngine: v.Source.Name(),
		TargetEngine: v.Target.Name(),
		Depth:        depth,
	}
	for name := range tgtTables {
		if _, present := srcNames[name]; !present {
			result.ExtraOnTarget = append(result.ExtraOnTarget, name)
		}
	}
	sort.Strings(result.ExtraOnTarget)
	result.Summary.TablesExtraOnTarget = len(result.ExtraOnTarget)
	tables := make([]*ir.Table, 0, len(srcSchema.Tables))
	tables = append(tables, srcSchema.Tables...)
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })

	for _, srcTable := range tables {
		tr := VerifyTableResult{Name: srcTable.Name}
		tgtTable, present := tgtTables[srcTable.Name]
		if !present {
			tr.Reason = "table not present on target"
			result.Tables = append(result.Tables, tr)
			result.Summary.TablesSkipped++
			continue
		}
		srcCount, err := srcVerifier.ExactRowCount(ctx, srcTable)
		if err != nil {
			tr.Reason = fmt.Sprintf("source count error: %v", err)
			result.Tables = append(result.Tables, tr)
			result.Summary.TablesSkipped++
			continue
		}
		tgtCount, err := tgtVerifier.ExactRowCount(ctx, tgtTable)
		if err != nil {
			tr.Reason = fmt.Sprintf("target count error: %v", err)
			result.Tables = append(result.Tables, tr)
			result.Summary.TablesSkipped++
			continue
		}
		tr.SourceRowCount = srcCount
		tr.TargetRowCount = tgtCount
		tr.CountMismatch = srcCount != tgtCount
		result.Tables = append(result.Tables, tr)
		if tr.CountMismatch {
			result.Summary.TablesMismatch++
		} else {
			result.Summary.TablesClean++
		}
	}
	result.Summary.TablesChecked = len(result.Tables)

	if err := v.render(result); err != nil {
		return result, fmt.Errorf("verify: render: %w", err)
	}
	return result, nil
}

func (v *Verifier) validate() error {
	switch {
	case v.Source == nil:
		return errors.New("verify: Source engine is required")
	case v.Target == nil:
		return errors.New("verify: Target engine is required")
	case v.SourceDSN == "":
		return errors.New("verify: SourceDSN is required")
	case v.TargetDSN == "":
		return errors.New("verify: TargetDSN is required")
	case v.Out == nil:
		return errors.New("verify: Out writer is required")
	}
	return nil
}

func (v *Verifier) render(r *VerifyResult) error {
	switch strings.ToLower(strings.TrimSpace(v.Format)) {
	case "json":
		enc := json.NewEncoder(v.Out)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	case "", "text":
		return v.renderText(r)
	}
	return fmt.Errorf("verify: unknown format %q (want 'text' or 'json')", v.Format)
}

func (v *Verifier) renderText(r *VerifyResult) error {
	var sb strings.Builder
	sb.WriteString("-- sluice verify (depth=")
	sb.WriteString(string(r.Depth))
	sb.WriteString(")\n")
	fmt.Fprintf(&sb, "-- source: %s\n-- target: %s\n", r.SourceEngine, r.TargetEngine)
	fmt.Fprintf(&sb, "-- result: %d table(s) checked, %d clean, %d mismatched, %d skipped\n",
		r.Summary.TablesChecked, r.Summary.TablesClean, r.Summary.TablesMismatch, r.Summary.TablesSkipped)
	sb.WriteString("\n")

	for _, t := range r.Tables {
		switch {
		case t.Reason != "":
			fmt.Fprintf(&sb, "%-40s SKIPPED (%s)\n", t.Name, t.Reason)
		case t.CountMismatch:
			fmt.Fprintf(&sb, "%-40s MISMATCH source=%d target=%d (delta=%+d)\n",
				t.Name, t.SourceRowCount, t.TargetRowCount, t.TargetRowCount-t.SourceRowCount)
		default:
			fmt.Fprintf(&sb, "%-40s OK rows=%d\n", t.Name, t.SourceRowCount)
		}
	}
	if len(r.ExtraOnTarget) > 0 {
		sb.WriteString("\n-- tables present on target but absent on source (informational; not counted as mismatches):\n")
		for _, n := range r.ExtraOnTarget {
			fmt.Fprintf(&sb, "   %s\n", n)
		}
		sb.WriteString("-- run `sluice schema diff` if you need to reconcile structural drift.\n")
	}
	if r.Summary.TablesMismatch > 0 {
		sb.WriteString("\n-- non-zero exit code follows; re-run with --depth=sample (post-MVP) to compare row content for drift root-cause.\n")
	}
	_, err := io.WriteString(v.Out, sb.String())
	return err
}
