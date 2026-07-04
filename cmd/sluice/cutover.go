// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// CutoverCmd implements `sluice cutover`. The two-phase sequence
// priming subcommand — severity-A finding F10 of the 2026-05-22
// Reddit-research run, see ADR-0062.
//
// At cutover (the operator-driven moment of flipping traffic from
// the old source DB to the new target DB), sluice re-reads the
// source's sequence / AUTO_INCREMENT state and applies it to the
// target with a safety margin. Without this step, the first
// post-cutover INSERT on the target risks a primary-key collision
// against rows that were inserted on the source during the CDC
// catch-up window (whose IDs replicated via CDC, but whose
// sequence-position did NOT replicate — CDC carries row-level
// changes, not catalog-level sequence advances).
//
// The command is operator-invoked, never auto-triggered. Operators
// drive it as the final step of a sluice-managed cutover:
//
//   - `sluice sync stop --wait`     (drain CDC catch-up)
//   - operator flips application traffic to the target
//   - `sluice cutover`              (this command — close the sequence gap)
//
// The command is idempotent: running it twice does not regress
// sequence values, and the second invocation reports every table as
// "noop". Operators recovering from a partial network failure
// during the first invocation can re-run without thinking.
//
// **Refuse-loudly on target-ahead.** If the target's sequence is
// already ahead of source by more than the safety margin, the
// command refuses with exit code 2 and an operator-actionable hint
// ("manual re-snapshot recommended"). This catches the scenario
// where the operator INSERTed against the target post-cutover
// before running this command, and a forward bump would now risk a
// collision.
type CutoverCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only prime sequences on these tables (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Prime sequences on every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	// ADR-0118 finding 3: the canonical name drops the redundant command-name
	// prefix — under the `cutover` subcommand `cutover-` is pure redundancy.
	// The old --cutover-sequence-margin spelling keeps working as a deprecated
	// alias (every existing command line / script is unchanged); passing it
	// specifically emits the one-time deprecation WARN below
	// (warnDeprecatedSequenceMargin), mirroring the ADR-0091
	// --forward-schema-add-column posture.
	SequenceMargin int64 `name:"sequence-margin" aliases:"cutover-sequence-margin" help:"Safety margin added on top of every source sequence value before applying. Operator headroom against in-flight source-side INSERTs between the read and the apply (or between the read and the operator flipping traffic). The same margin doubles as the idempotency tolerance — a re-run within margin rows of the first run does NOT refuse. Default 1000. (The old spelling --cutover-sequence-margin still works as a deprecated alias.)" default:"1000" placeholder:"N"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). Threaded to the writer's pg_get_serial_sequence lookups so cutover resolves sequences in the same namespace the migration landed in (ADR-0031). MySQL operators use a different --target DSN database instead." placeholder:"NAME"`

	Format string `help:"Output format: 'text' (human-readable, default) or 'json' (machine-readable, suitable for piping through 'jq' or scrape into metrics tooling)." default:"text" enum:"text,json" placeholder:"FORMAT"`
}

// Run implements the cutover subcommand.
//
// Exit codes:
//   - 0: every table primed or noop.
//   - non-zero: at least one table refused (target ahead of source by
//     more than margin), or a connection / catalog error fired.
//
// On any error the partial report is still rendered to stdout so
// operators piping the output to a metrics tool see the per-table
// detail of whatever did succeed before the failure.
func (c *CutoverCmd) Run(_ *Globals) error {
	// ADR-0118 finding 3: one-time deprecation WARN, fired ONLY when the
	// operator passed the OLD --cutover-sequence-margin spelling (kong has no
	// per-alias set-tracking, so we read the literal token from os.Args, not
	// the resolved value — the zero-value-safety property: an unset flag at
	// its default never trips this). Mirrors the ADR-0091
	// --forward-schema-add-column posture.
	warnDeprecatedSequenceMargin()

	source, err := resolveEngine(c.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	target, err := resolveEngine(c.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

	if len(c.IncludeTable) > 0 && len(c.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	if c.SequenceMargin < 0 {
		return fmt.Errorf("--sequence-margin=%d must be non-negative", c.SequenceMargin)
	}
	if strings.TrimSpace(c.Source) == "" {
		return errors.New("--source is required")
	}
	if strings.TrimSpace(c.Target) == "" {
		return errors.New("--target is required")
	}

	filter, err := pipeline.NewTableFilter(c.IncludeTable, c.ExcludeTable)
	if err != nil {
		return err
	}

	cut := &pipeline.Cutover{
		Source:       source,
		Target:       target,
		SourceDSN:    c.Source,
		TargetDSN:    c.Target,
		Margin:       c.SequenceMargin,
		Filter:       filter,
		TargetSchema: c.TargetSchema,
	}
	report, runErr := cut.Run(kongContext())
	// Always render whatever was accumulated — operators piping into
	// metrics tooling benefit from the partial result on failures.
	if report != nil {
		renderCutoverReport(report, c.Format)
	}
	if runErr != nil {
		if errors.Is(runErr, ir.ErrCutoverSequenceTargetAhead) {
			// Loud-failure refusal class: exit non-zero with the
			// per-table detail already rendered above plus a tail
			// operator-actionable hint.
			fmt.Fprintln(os.Stderr, "cutover: one or more tables refused; manual re-snapshot recommended for the refused table(s)")
		}
		return runErr
	}
	return nil
}

// renderCutoverReport prints the per-table priming outcome to
// stdout. Text format is human-readable; JSON is the
// machine-consumable shape for piping through `jq` / Prometheus
// scrapers.
func renderCutoverReport(report *ir.SequencePrimeReport, format string) {
	if format == "json" {
		renderCutoverReportJSON(report)
		return
	}
	if len(report.Actions) == 0 {
		fmt.Println("cutover: no identity / AUTO_INCREMENT columns or standalone sequences in the source schema (nothing to prime)")
		return
	}
	var primed, noop, skipped, refused int
	for _, a := range report.Actions {
		subject := cutoverActionSubject(a)
		switch a.Outcome {
		case "primed":
			primed++
			fmt.Printf("primed:   %s — source=%d, target=%d -> %d\n",
				subject, a.SourceValue, a.TargetBefore, a.TargetAfter)
		case "noop":
			noop++
			fmt.Printf("noop:     %s — target=%d already at or above apply point (source=%d)\n",
				subject, a.TargetBefore, a.SourceValue)
		case "skipped":
			skipped++
			fmt.Printf("skipped:  %s — %s\n", subject, a.Reason)
		case "refused":
			refused++
			fmt.Printf("REFUSED:  %s — %s\n", subject, a.Reason)
		default:
			fmt.Printf("unknown:  %s — outcome=%q\n", subject, a.Outcome)
		}
	}
	fmt.Printf("\ncutover: %d primed, %d noop, %d skipped, %d refused\n",
		primed, noop, skipped, refused)
}

// cutoverActionSubject renders the object an action describes:
// `table.column` for identity / AUTO_INCREMENT entries, `sequence
// <name>` for standalone-sequence entries (item 51).
func cutoverActionSubject(a ir.SequencePrimeAction) string {
	if a.Sequence != "" {
		return "sequence " + a.Sequence
	}
	return a.Table + "." + a.Column
}

// renderCutoverReportJSON emits a stable JSON shape. Field names use
// snake_case to match sluice's other JSON-mode outputs (sync-health,
// schema preview, matview refresh). The `sequence` field is the
// item-51 append-only extension: empty for identity entries,
// populated (with table/column empty) for standalone sequences.
func renderCutoverReportJSON(report *ir.SequencePrimeReport) {
	var sb strings.Builder
	sb.WriteString(`{"actions":[`)
	for i, a := range report.Actions {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb,
			`{"table":%q,"column":%q,"sequence":%q,"source_value":%d,"target_before":%d,"target_after":%d,"outcome":%q,"reason":%q}`,
			a.Table, a.Column, a.Sequence, a.SourceValue, a.TargetBefore, a.TargetAfter, a.Outcome, a.Reason)
	}
	sb.WriteString(`]}`)
	fmt.Println(sb.String())
}
