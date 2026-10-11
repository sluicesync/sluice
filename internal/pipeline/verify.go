// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// `sluice verify` orchestration. v0.12.0 ships count-mode only —
// row-count comparison per table — per the proto-ADR's MVP slice
// (docs/dev/design/sluice-verify.md). Sample mode and full mode follow.
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
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
)

// Verify pretty-view phases (ADR-0155 phase 2). Verify has no historical
// slog output — it renders a text/JSON report to [Verifier.Out] — so on
// the non-TTY path the sink is [progress.Nop] and this checklist drives
// only the interactive view.
var (
	verifyPhaseSchema  = progress.Phase{Key: "schema", Label: "Schema"}
	verifyPhaseCompare = progress.Phase{Key: "compare", Label: "Compare"}
)

// VerifyProgressSpec is the pretty-view [progress.Spec] for `sluice
// verify`. The CLI hands it to [progress.NewTTYSink].
var VerifyProgressSpec = progress.Spec{
	Title:      "sluice verify",
	Phases:     []progress.Phase{verifyPhaseSchema, verifyPhaseCompare},
	LabelWidth: 12,
}

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
// the proto-ADR (`docs/dev/design/sluice-verify.md`).
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
	Filter migcore.TableFilter

	// RowFilters holds the operator's per-table `--where TABLE=<predicate>`
	// row filters (ADR-0173 Phase 1), keyed by SOURCE table name. Threaded
	// onto the SOURCE verifier ONLY (never the target), so `verify --depth
	// count/sample` compares matching-source rows against the already-
	// filtered target subset — otherwise a filtered migrate would
	// false-report a count mismatch. Must match the predicates the migrate
	// run used. nil/empty is the unfiltered default; a source engine that
	// can't push it down is refused loudly (via [migcore.ApplyRowFilters]).
	RowFilters map[string]string

	// Format is "text" (default) or "json".
	Format string

	// Out is the destination for the rendered report. Required.
	Out io.Writer

	// Progress is the ADR-0155 presentation sink. nil is the [progress.Nop]
	// default (verify's own report to Out is the byte-identical non-TTY
	// output); the CLI sets a [progress.TTYSink] only for an interactive
	// terminal, where it also points Out at io.Discard so the report and
	// the live view don't both write stdout.
	Progress progress.Sink
}

// sink returns the presentation sink, defaulting a nil Progress to the
// no-op sink so every call-site is nil-free.
func (v *Verifier) sink() progress.Sink {
	if v.Progress == nil {
		return progress.Nop{}
	}
	return v.Progress
}

// VerifyResult is the structured outcome of a verify run. The renderer
// consumes this; CLI callers also inspect HasMismatch() and
// HasUnverified() to set exit codes.
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
	Name string `json:"name"`
	// Status is the table's verdict, one word a JSON consumer can branch
	// on without re-deriving it from the fields below: "ok",
	// "count-mismatch", "column-mismatch", "sample-mismatch" or
	// "unverified". It exists because the verdicts do not all live in one
	// field — a column-mismatch table has count_mismatch false, no
	// sample_mismatch and no reason, and reading only those would take it
	// for clean. See [VerifyTableResult.verdict].
	Status string `json:"status"`

	SourceRowCount int64 `json:"source_row_count"`
	TargetRowCount int64 `json:"target_row_count"`
	CountMismatch  bool  `json:"count_mismatch"`
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
	// ColumnsMissingOnTarget names the source columns the target table
	// does not carry (sample depth only — it is the depth that reads
	// columns). Non-empty is a MISMATCH: the source holds values the
	// target has nowhere to keep, which no row count shows. Rows are not
	// sampled for such a table; the missing names are the finding.
	ColumnsMissingOnTarget []string `json:"columns_missing_on_target,omitempty"`
	// ColumnsExtraOnTarget names target columns the source table does
	// not carry (sample depth only). Informational, like
	// [VerifyResult.ExtraOnTarget]: a target-only column holds nothing
	// the source can be missing (a shard-injection column, an
	// operator-added one), so it is reported, left out of the hash, and
	// does not fail the table.
	ColumnsExtraOnTarget []string `json:"columns_extra_on_target,omitempty"`
	// Reason is non-empty when this table couldn't be verified
	// (e.g. table not on target side, a count/sample error, engine
	// doesn't support sampling). A Reason-carrying table renders as
	// SKIPPED in the report and counts toward
	// [VerifySummary.TablesUnverified] — a failure class, not a pass
	// (Bug 190). The Source/Target counts are 0 when the failure
	// preceded counting.
	Reason string `json:"reason,omitempty"`
}

// VerifySummary is the high-level rollup the operator reads first.
type VerifySummary struct {
	TablesChecked  int `json:"tables_checked"`
	TablesClean    int `json:"tables_clean"`
	TablesMismatch int `json:"tables_mismatch"`

	// TablesUnverified counts tables that could NOT be verified at
	// all: missing on the target, a source/target count error, or a
	// sample-mode error (each table's Reason names which). These are
	// a FAILURE class, not a benign skip (Bug 190): verify is the
	// loss detector, and a detector that could not examine a table
	// must not report overall success — the CLI maps a non-zero
	// TablesUnverified to a non-zero exit. Deliberate exclusions
	// (--include-table / --exclude-table, config filters) are applied
	// before verification starts and never land here, so they stay
	// exit-neutral.
	TablesUnverified int `json:"tables_unverified"`

	// TablesExtraOnTarget is the count of tables present on the
	// target but absent from the source. Reported informationally
	// (the names land in [VerifyResult.ExtraOnTarget]); not flagged
	// as mismatches since extra tables don't imply data loss — they
	// could be operator-managed tables outside sluice's scope. The
	// existing `sluice schema diff` is the right tool when they are
	// drift; verify keeps its lane focused on row-data concerns.
	TablesExtraOnTarget int `json:"tables_extra_on_target,omitempty"`
}

// verdict is the table's [VerifyTableResult.Status]: unverified first
// (nothing else was established), then the mismatch kinds in the order
// the text report names them.
func (t *VerifyTableResult) verdict() string {
	switch {
	case t.Reason != "":
		return "unverified"
	case t.CountMismatch:
		return "count-mismatch"
	case len(t.ColumnsMissingOnTarget) > 0:
		return "column-mismatch"
	case t.SampleMismatch > 0:
		return "sample-mismatch"
	default:
		return "ok"
	}
}

// HasMismatch reports whether any table failed verification. Used by
// the CLI to set exit code 1 (vs 0 for clean, 2 for op error).
func (r *VerifyResult) HasMismatch() bool {
	return r.Summary.TablesMismatch > 0
}

// mismatchKinds is the order [VerifyResult.MismatchKinds] names the
// mismatch statuses in — the order [VerifyTableResult.verdict] tests them.
var mismatchKinds = []string{"count-mismatch", "column-mismatch", "sample-mismatch"}

// MismatchKinds names how the mismatched tables drifted: one "N kind"
// per kind present, e.g. "1 column-mismatch, 1 sample-mismatch", or ""
// when nothing mismatched. The kinds are each table's verdict — the same
// word its [VerifyTableResult.Status] carries — so the CLI's closing line
// names the drift the per-table rows report (Bug 303: it used to call
// every kind a row-count mismatch).
func (r *VerifyResult) MismatchKinds() string {
	counts := make(map[string]int, len(mismatchKinds))
	for i := range r.Tables {
		counts[r.Tables[i].verdict()]++
	}
	var parts []string
	for _, k := range mismatchKinds {
		if counts[k] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[k], k))
		}
	}
	return strings.Join(parts, ", ")
}

// HasUnverified reports whether any table could not be verified at
// all (Bug 190). The CLI maps this to a non-zero exit — an incomplete
// verification must never pass an rc-gated check. Deliberately
// excluded tables never reach the result (filters are applied before
// verification), and extra-on-target tables stay informational;
// neither trips this.
func (r *VerifyResult) HasUnverified() bool {
	return r.Summary.TablesUnverified > 0
}

// Run executes the verify pass. Returns the result + a possibly-nil
// error. On operational failure (couldn't open a connection, engine
// doesn't implement Verifier, etc.) returns (nil, error). On
// successful execution returns a non-nil result whose HasMismatch /
// HasUnverified distinguish clean from drift / incomplete.
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

	sink := v.sink()
	sink.PhaseStarted(verifyPhaseSchema)

	// Connect-phase hint routing (Bug 196 residual, Supabase replica
	// probe 2026-07-17): verify's opens get the same [migcore.WrapWithHint]
	// treatment as migrate's door, so a resolve failure against an
	// AAAA-only host (Supabase direct endpoints) carries the coded
	// IPv6-only remedy instead of the resolver's bare no-data error —
	// and the static connect hints (refused/auth/db-missing) ride too.
	sr, err := v.Source.OpenSchemaReader(ctx, v.SourceDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("verify: open source schema reader: %w", err))
	}
	defer migcore.CloseIf(sr)

	tr, err := v.Target.OpenSchemaReader(ctx, v.TargetDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("verify: open target schema reader: %w", err))
	}
	defer migcore.CloseIf(tr)
	// Same reasoning as the diff path: verify reads both schemas to compare
	// what exists, translating nothing, so a verbatim-only type on either
	// side must not fail the command. Source uses the computed same-engine
	// predicate; target is same-engine by construction.
	migcore.ApplyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(v.Source, v.Target))
	migcore.ApplyVerbatimExtensionPassthrough(tr, true)

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
	if err := migcore.ApplyTableFilter(ctx, srcSchema, v.Filter); err != nil {
		return nil, err
	}

	// ADR-0173 Phase 1 + audit F0-4: validate the --where keys against the
	// (table-scoped) source schema and canonicalize them to the schema's own
	// table casing BEFORE threading them onto the SOURCE verifier. Without the
	// gate a typo'd or mis-cased key silently disables the filter and verify
	// counts the WHOLE source against the (correctly filtered) target — a false
	// MISMATCH, or worse a false PASS confirming an over-copied migrate that
	// rode the identical unvalidated lookup. Applying to the source verifier
	// only is deliberate: the target holds just the filtered rows and must be
	// counted whole. A source engine that can't push it down is refused loudly
	// here, before any counting.
	canonFilters, err := migcore.ValidateRowFilterKeys(srcSchema, v.RowFilters)
	if err != nil {
		return nil, err
	}
	if err := migcore.ApplyRowFilters(sr, canonFilters, v.Source.Name()); err != nil {
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
	sink.PhaseCompleted(verifyPhaseSchema)
	sink.PhaseStarted(verifyPhaseCompare)

	for _, srcTable := range tables {
		tr := VerifyTableResult{Name: srcTable.Name}
		tgtTable, present := tgtTables[srcTable.Name]
		if !present {
			tr.Reason = "table not present on target"
			result.Tables = append(result.Tables, tr)
			result.Summary.TablesUnverified++
			continue
		}
		srcCount, err := srcVerifier.ExactRowCount(ctx, srcTable)
		if err != nil {
			tr.Reason = fmt.Sprintf("source count error: %v", err)
			result.Tables = append(result.Tables, tr)
			result.Summary.TablesUnverified++
			continue
		}
		tgtCount, err := tgtVerifier.ExactRowCount(ctx, tgtTable)
		if err != nil {
			tr.Reason = fmt.Sprintf("target count error: %v", err)
			result.Tables = append(result.Tables, tr)
			result.Summary.TablesUnverified++
			continue
		}
		tr.SourceRowCount = srcCount
		tr.TargetRowCount = tgtCount
		tr.CountMismatch = srcCount != tgtCount

		// Sample mode: also compare row content hashes for the
		// requested sample size. Skip on count-mode runs and on tables
		// where the engine's SampleVerifier surface isn't usable
		// (no PK, etc.) — those land as SKIPPED with a clear reason
		// and count toward TablesUnverified: the operator asked for
		// sample depth and this table didn't get it (Bug 190).
		if depth == VerifyDepthSample {
			if reason := v.sampleTable(ctx, srcVerifier, tgtVerifier, srcTable, tgtTable, sampleRows, sampleSeed, &tr); reason != "" {
				tr.Reason = reason
				result.Tables = append(result.Tables, tr)
				result.Summary.TablesUnverified++
				continue
			}
		}

		result.Tables = append(result.Tables, tr)
		switch {
		case tr.CountMismatch || tr.SampleMismatch > 0 || len(tr.ColumnsMissingOnTarget) > 0:
			result.Summary.TablesMismatch++
		default:
			result.Summary.TablesClean++
		}
	}
	for i := range result.Tables {
		result.Tables[i].Status = result.Tables[i].verdict()
	}
	result.Summary.TablesChecked = len(result.Tables)
	sink.PhaseCompleted(verifyPhaseCompare)

	if err := v.render(result); err != nil {
		return result, fmt.Errorf("verify: render: %w", err)
	}
	// ADR-0155 summary panel (TTY only; Nop ignores it). The report to Out
	// above is the byte-identical non-TTY output; these Fields are the
	// interactive rollup the operator reads first.
	s := result.Summary
	sink.Summary(progress.Result{Fields: []progress.Field{
		{Label: "Checked", Value: progress.HumanCount(int64(s.TablesChecked))},
		{Label: "Clean", Value: progress.HumanCount(int64(s.TablesClean))},
		{Label: "Mismatched", Value: progress.HumanCount(int64(s.TablesMismatch))},
		{Label: "Unverified", Value: progress.HumanCount(int64(s.TablesUnverified))},
	}})
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
	fmt.Fprintf(&sb, "-- result: %d table(s) checked, %d clean, %d mismatched, %d could not be verified\n",
		r.Summary.TablesChecked, r.Summary.TablesClean, r.Summary.TablesMismatch, r.Summary.TablesUnverified)
	sb.WriteString("\n")

	for _, t := range r.Tables {
		switch {
		case t.Reason != "":
			fmt.Fprintf(&sb, "%-40s SKIPPED (%s)\n", t.Name, t.Reason)
		case t.CountMismatch:
			fmt.Fprintf(&sb, "%-40s MISMATCH source=%d target=%d (delta=%+d)\n",
				t.Name, t.SourceRowCount, t.TargetRowCount, t.TargetRowCount-t.SourceRowCount)
		case len(t.ColumnsMissingOnTarget) > 0:
			fmt.Fprintf(&sb, "%-40s COLUMN-MISMATCH counts=%d/%d (rows not sampled: the target lacks source columns)\n",
				t.Name, t.SourceRowCount, t.TargetRowCount)
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
		if len(t.ColumnsMissingOnTarget) > 0 {
			fmt.Fprintf(&sb, "   column(s) missing on target: %s\n", strings.Join(t.ColumnsMissingOnTarget, ", "))
		}
		if len(t.ColumnsExtraOnTarget) > 0 {
			fmt.Fprintf(&sb, "   column(s) on target only (informational; not hashed): %s\n", strings.Join(t.ColumnsExtraOnTarget, ", "))
		}
	}
	if len(r.ExtraOnTarget) > 0 {
		sb.WriteString("\n-- tables present on target but absent on source (informational; not counted as mismatches):\n")
		for _, n := range r.ExtraOnTarget {
			fmt.Fprintf(&sb, "   %s\n", n)
		}
		sb.WriteString("-- run `sluice schema diff --source-driver=... --source=... --target-driver=... --target=...` if you need to reconcile structural drift.\n")
	}
	if r.Summary.TablesUnverified > 0 {
		fmt.Fprintf(&sb, "\n-- %d table(s) could not be verified (SKIPPED above) — an unverified table is not a pass; non-zero exit code follows. Deliberately-unmigrated tables can be excluded with --exclude-table.\n",
			r.Summary.TablesUnverified)
	}
	if r.Summary.TablesMismatch > 0 && r.Depth == VerifyDepthCount {
		sb.WriteString("\n-- non-zero exit code follows; re-run with --depth=sample to compare row content for drift root-cause.\n")
	}
	_, err := io.WriteString(v.Out, sb.String())
	return err
}

// sampleTable runs sample depth for one table into tr. It returns a
// non-empty reason when the table could not be sampled at all (the
// caller marks it unverified); a source column missing on the target is
// recorded on tr as a mismatch and returns "" without sampling — the
// missing names are the finding.
func (v *Verifier) sampleTable(ctx context.Context, srcVerifier, tgtVerifier ir.Verifier, srcTable, tgtTable *ir.Table, n int, seed int64, tr *VerifyTableResult) string {
	srcSV, srcOK := srcVerifier.(ir.SampleVerifier)
	tgtSV, tgtOK := tgtVerifier.(ir.SampleVerifier)
	if !srcOK || !tgtOK {
		return "sample mode not supported on this engine (no ir.SampleVerifier implementation)"
	}
	plan, err := planSampleColumns(srcTable, tgtTable)
	if err != nil {
		return fmt.Sprintf("sample-hash error: %v", err)
	}
	tr.ColumnsMissingOnTarget = plan.missing
	tr.ColumnsExtraOnTarget = plan.extra
	if len(plan.missing) > 0 {
		return ""
	}
	algo := ir.HashMD5
	if v.StrictHash {
		algo = ir.HashSHA256
	}
	if err := compareSampleHashes(ctx, srcSV, tgtSV, srcTable, tgtTable, plan.cols, n, seed, algo, tr); err != nil {
		return fmt.Sprintf("sample-hash error: %v", err)
	}
	return ""
}

// sampleColumnPlan is what [planSampleColumns] decides for one table:
// the column plan both sides hash, and the columns only one side has.
type sampleColumnPlan struct {
	cols    ir.SampleColumns
	missing []string // source columns the target lacks, sorted
	extra   []string // target columns the source lacks, sorted
}

// planSampleColumns fixes, ONCE, which columns sample-mode hashes and in
// what order — the fix for VERIFY-SAMPLE-COLUMN-ORDER. Each side used to
// hash its own columns in its own declared order, so the comparison
// matched columns by POSITION: a target whose `b` and `c` held each
// other's values, declared in swapped order (exactly what a RENAME swap
// leaves), hashed identically to its source and reported clean —
// measured on MySQL and Postgres. Matching by NAME, in one order both
// sides share, makes a value under the wrong name change the hash, and
// lets a genuine reorder (same values, same names, different declared
// order) verify clean instead of falsely mismatching.
//
// The order is the source column names sorted bytewise: independent of
// either side's declaration, so neither side's DDL can steer it. Names
// match exactly; a case-only difference reads as one missing and one
// extra column, which is loud rather than silent.
//
// The key is the SOURCE primary key, read by name on both sides: the
// rows are identified as the source identifies them, whatever key the
// target declares. A source with no primary key cannot be sampled
// deterministically and is refused (the table lands SKIPPED, unverified).
func planSampleColumns(src, tgt *ir.Table) (sampleColumnPlan, error) {
	if src.PrimaryKey == nil || len(src.PrimaryKey.Columns) == 0 {
		return sampleColumnPlan{}, errors.New("table has no primary key (sample-mode requires a PK for deterministic sampling)")
	}
	onTarget := make(map[string]struct{}, len(tgt.Columns))
	for _, c := range tgt.Columns {
		onTarget[c.Name] = struct{}{}
	}
	onSource := make(map[string]struct{}, len(src.Columns))
	var p sampleColumnPlan
	for _, c := range src.Columns {
		onSource[c.Name] = struct{}{}
		if _, ok := onTarget[c.Name]; ok {
			p.cols.Hashed = append(p.cols.Hashed, c.Name)
		} else {
			p.missing = append(p.missing, c.Name)
		}
	}
	for _, c := range tgt.Columns {
		if _, ok := onSource[c.Name]; !ok {
			p.extra = append(p.extra, c.Name)
		}
	}
	sort.Strings(p.cols.Hashed)
	sort.Strings(p.missing)
	sort.Strings(p.extra)
	for _, k := range src.PrimaryKey.Columns {
		p.cols.Key = append(p.cols.Key, k.Column)
	}
	return p, nil
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
func compareSampleHashes(ctx context.Context, src, tgt ir.SampleVerifier, srcTable, tgtTable *ir.Table, cols ir.SampleColumns, n int, seed int64, algo ir.HashAlgorithm, tr *VerifyTableResult) error {
	srcSamples, err := src.SampleRowHashes(ctx, srcTable, cols, n, seed, algo)
	if err != nil {
		return fmt.Errorf("source-side: %w", err)
	}
	tgtSamples, err := tgt.SampleRowHashes(ctx, tgtTable, cols, n, seed, algo)
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
