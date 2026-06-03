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

	"sluicesync.dev/sluice/internal/ir"
)

// VerifyDepth selects which depth of verification to run. Count is the
// MVP and the only supported depth in v0.12.0; sample and full are
// reserved for the proto-ADR's later phases.
type VerifyDepth string

// Recognised VerifyDepth values.
const (
	VerifyDepthCount  VerifyDepth = "count"
	VerifyDepthSample VerifyDepth = "sample" // v0.14.0
	// VerifyDepthFull   VerifyDepth = "full"   // post-sample
)

// DefaultSampleRowsPerTable is the per-table sample size when --depth
// sample is requested without an explicit --sample-rows-per-table.
// 100 gives ~99% confidence of detecting a 5%+ corruption rate per
// the proto-ADR (`docs/dev/design-sluice-verify.md`).
const DefaultSampleRowsPerTable = 100

// DefaultSampleSeed is the seed used when sample mode runs without an
// explicit --sample-seed flag. Deterministic across calls so a given
// table's sample rows are stable run-to-run; the operator overrides
// for "reshuffle the sample" workflows.
const DefaultSampleSeed = 42

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

	// SampleRowsPerTable controls the per-table sample size when
	// Depth == sample. Zero falls back to [DefaultSampleRowsPerTable].
	SampleRowsPerTable int

	// SampleSeed makes sampling deterministic across calls. Same seed
	// → same row subset on source + target. Zero falls back to
	// [DefaultSampleSeed].
	SampleSeed int64

	// StrictHash, when true, switches sample-mode hashing from MD5
	// (default) to SHA-256. v0.14.2 operator opt-in. See
	// `docs/verify-vs-vitess-vdiff.md` for the collision-math
	// rationale; MD5 is statistically sufficient for honest-data
	// scenarios at any practical row count, but SHA-256 gives
	// operators an extra confidence margin and matches compliance
	// postures that require it.
	StrictHash bool

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
	// SampleSize is the number of rows actually sampled (may be
	// less than the requested SampleRowsPerTable on small tables).
	// Zero when Depth != sample.
	SampleSize int `json:"sample_size,omitempty"`
	// SampleMismatch is the number of sampled rows whose source
	// hash differs from the target hash (or whose PK isn't present
	// on both sides). Non-zero implies row-content drift.
	SampleMismatch int `json:"sample_mismatch,omitempty"`
	// SampleMismatchPKs lists the primary keys of mismatched rows
	// for forensic drill-in. Capped at the first 25 entries to
	// keep reports readable; the full count is in SampleMismatch.
	SampleMismatchPKs []string `json:"sample_mismatch_pks,omitempty"`
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
	if depth != VerifyDepthCount && depth != VerifyDepthSample {
		return nil, fmt.Errorf("verify: depth %q not supported (recognised: count, sample); full mode pending future release", depth)
	}
	// Sample-mode currently requires same source + target engine —
	// server-side hashing produces engine-specific text rendering of
	// values, so cross-engine sample comparison would yield silent
	// false-positive mismatches. Cross-engine sample is deferred to a
	// future phase that adds client-side canonicalization.
	if depth == VerifyDepthSample && v.Source.Name() != v.Target.Name() {
		return nil, fmt.Errorf("verify: depth=sample requires same source and target engine (got source=%q target=%q); cross-engine sample is planned but not yet implemented — use --depth=count for cross-engine verification",
			v.Source.Name(), v.Target.Name())
	}
	sampleRows := v.SampleRowsPerTable
	if sampleRows == 0 {
		sampleRows = DefaultSampleRowsPerTable
	}
	sampleSeed := v.SampleSeed
	if sampleSeed == 0 {
		sampleSeed = DefaultSampleSeed
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

		// Sample mode: also compare row content hashes for the
		// requested sample size. Skip on count-mode runs and on tables
		// where the engine's SampleVerifier surface isn't usable
		// (no PK, etc.) — those land as SKIPPED with a clear reason.
		if depth == VerifyDepthSample {
			srcSV, srcOK := srcVerifier.(ir.SampleVerifier)
			tgtSV, tgtOK := tgtVerifier.(ir.SampleVerifier)
			switch {
			case !srcOK || !tgtOK:
				tr.Reason = "sample mode not supported on this engine (no ir.SampleVerifier implementation)"
				result.Tables = append(result.Tables, tr)
				result.Summary.TablesSkipped++
				continue
			default:
				algo := ir.HashMD5
				if v.StrictHash {
					algo = ir.HashSHA256
				}
				if err := compareSampleHashes(ctx, srcSV, tgtSV, srcTable, tgtTable, sampleRows, sampleSeed, algo, &tr); err != nil {
					tr.Reason = fmt.Sprintf("sample-hash error: %v", err)
					result.Tables = append(result.Tables, tr)
					result.Summary.TablesSkipped++
					continue
				}
			}
		}

		result.Tables = append(result.Tables, tr)
		switch {
		case tr.CountMismatch || tr.SampleMismatch > 0:
			result.Summary.TablesMismatch++
		default:
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
		case t.SampleMismatch > 0:
			fmt.Fprintf(&sb, "%-40s SAMPLE-MISMATCH counts=%d/%d sampled=%d mismatch=%d\n",
				t.Name, t.SourceRowCount, t.TargetRowCount, t.SampleSize, t.SampleMismatch)
			for _, pk := range t.SampleMismatchPKs {
				fmt.Fprintf(&sb, "   pk=%s\n", pk)
			}
			if t.SampleMismatch > len(t.SampleMismatchPKs) {
				fmt.Fprintf(&sb, "   ... and %d more (capped for readability; see JSON output for full list)\n",
					t.SampleMismatch-len(t.SampleMismatchPKs))
			}
		case t.SampleSize > 0:
			fmt.Fprintf(&sb, "%-40s OK rows=%d sampled=%d clean\n", t.Name, t.SourceRowCount, t.SampleSize)
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
	if r.Summary.TablesMismatch > 0 && r.Depth == VerifyDepthCount {
		sb.WriteString("\n-- non-zero exit code follows; re-run with --depth=sample to compare row content for drift root-cause.\n")
	}
	_, err := io.WriteString(v.Out, sb.String())
	return err
}

// compareSampleHashes runs SampleRowHashes on both sides with the
// same n + seed (so both engines select the same row subset), then
// walks the two sorted lists side-by-side to detect mismatches:
//   - PK present on source only (target is missing the row).
//   - PK present on target only (target has an extra row).
//   - PK present on both but hashes differ (row content drift).
//
// Mismatches populate tr.SampleMismatch + tr.SampleMismatchPKs (cap
// 25). The function returns nil on success (including the all-clean
// case); operational errors propagate via the err return so the
// orchestrator can mark the table SKIPPED with a clear reason.
//
// Const for the per-table mismatch-PK cap; full count remains in
// SampleMismatch even when the slice is truncated.
func compareSampleHashes(ctx context.Context, src, tgt ir.SampleVerifier, srcTable, tgtTable *ir.Table, n int, seed int64, algo ir.HashAlgorithm, tr *VerifyTableResult) error {
	srcSamples, err := src.SampleRowHashes(ctx, srcTable, n, seed, algo)
	if err != nil {
		return fmt.Errorf("source-side: %w", err)
	}
	tgtSamples, err := tgt.SampleRowHashes(ctx, tgtTable, n, seed, algo)
	if err != nil {
		return fmt.Errorf("target-side: %w", err)
	}
	tr.SampleSize = len(srcSamples)
	const maxPKsListed = 25

	// Walk both sorted lists in parallel. Each side is sorted by PK
	// (engines guarantee this); merge-walk catches all three kinds
	// of mismatch in a single O(n+m) pass.
	i, j := 0, 0
	addMismatch := func(pk string) {
		tr.SampleMismatch++
		if len(tr.SampleMismatchPKs) < maxPKsListed {
			tr.SampleMismatchPKs = append(tr.SampleMismatchPKs, pk)
		}
	}
	for i < len(srcSamples) && j < len(tgtSamples) {
		switch {
		case srcSamples[i].PrimaryKey < tgtSamples[j].PrimaryKey:
			addMismatch(srcSamples[i].PrimaryKey)
			i++
		case srcSamples[i].PrimaryKey > tgtSamples[j].PrimaryKey:
			addMismatch(tgtSamples[j].PrimaryKey)
			j++
		default:
			if srcSamples[i].Hash != tgtSamples[j].Hash {
				addMismatch(srcSamples[i].PrimaryKey)
			}
			i++
			j++
		}
	}
	// Trailing tail on either side.
	for ; i < len(srcSamples); i++ {
		addMismatch(srcSamples[i].PrimaryKey)
	}
	for ; j < len(tgtSamples); j++ {
		addMismatch(tgtSamples[j].PrimaryKey)
	}
	return nil
}
