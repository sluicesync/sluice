// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/progress"
)

// Trigger pretty-view phases + specs (ADR-0155 phase 2). The `trigger`
// commands are CLI-orchestrated (single engine calls, no pipeline Run with
// a Progress field), so the CLI drives these one-phase checklists inline;
// on the non-TTY path the sink is [progress.Nop] and the historical stdout
// output is byte-identical. --dry-run always selects the non-pretty path
// (wantPrettyProgress excludes it) so the DDL preview keeps its exact shape.
var (
	triggerPhaseInstall = progress.Phase{Key: "install", Label: "Install"}
	triggerPhaseRemove  = progress.Phase{Key: "remove", Label: "Remove"}
	triggerPhasePrune   = progress.Phase{Key: "prune", Label: "Prune"}

	triggerSetupProgressSpec = progress.Spec{
		Title:      "sluice trigger setup",
		Phases:     []progress.Phase{triggerPhaseInstall},
		LabelWidth: 14,
	}
	triggerTeardownProgressSpec = progress.Spec{
		Title:      "sluice trigger teardown",
		Phases:     []progress.Phase{triggerPhaseRemove},
		LabelWidth: 14,
	}
	triggerPruneProgressSpec = progress.Spec{
		Title:      "sluice trigger prune",
		Phases:     []progress.Phase{triggerPhasePrune},
		LabelWidth: 12,
	}
)

// pgtriggerSetupMode names the DDL-detection mode a `trigger setup` plan
// landed in, shared by the stdout applied-line and the pretty summary panel.
func pgtriggerSetupMode(plan *pgtrigger.Plan) string {
	if !plan.EventTriggerSupported {
		return "polled-fingerprint-only"
	}
	return "event-trigger + polling"
}

// triggerDrivers is the set of trigger-CDC source engines the `sluice trigger`
// commands install/remove. The default ("postgres-trigger") preserves the
// original PG-only behaviour; "sqlite-trigger" (ADR-0135) installs the
// change-log + per-table capture triggers in a local SQLite file (the --dsn is
// the file path); "d1-trigger" (ADR-0136) installs the SAME state on a live
// Cloudflare D1 database over the HTTP query API (the --dsn is the d1:// form;
// the token is env-only, CLOUDFLARE_API_TOKEN). The PG-only flags (--schema,
// --allow-polled-fingerprint, --capture-payload) do not apply to the SQLite/D1
// engines and are ignored there.
const (
	triggerDriverPostgres = pgtrigger.EngineName       // "postgres-trigger"
	triggerDriverSQLite   = sqlitetrigger.EngineName   // "sqlite-trigger"
	triggerDriverD1       = sqlitetrigger.EngineNameD1 // "d1-trigger"
)

// TriggerCmd groups the operator-facing trigger-engine setup +
// teardown commands. ADR-0066 §10: setup is deliberately explicit —
// the operator runs `sluice trigger setup --dsn=...` once, separately
// from `sluice sync start`, so the source-side DDL is visible at the
// CLI rather than implicitly applied on first sync.
//
// The subcommand namespace is generic (`sluice trigger ...`) rather
// than engine-specific (`sluice pgtrigger ...`) so a hypothetical
// future `mysql-trigger` engine can share the same surface without a
// CLI breaking change.
type TriggerCmd struct {
	Setup    TriggerSetupCmd    `cmd:"" help:"Install the trigger-engine state (change-log table, capture function, per-table triggers) on the source PG database."`
	Teardown TriggerTeardownCmd `cmd:"" help:"Remove every trace of the trigger engine from the source PG database."`
	Prune    TriggerPruneCmd    `cmd:"" help:"Reap durably-applied rows from the source change-log to bound its growth (ADR-0137; safe to run while a sync is live)."`
}

// TriggerSetupCmd installs the source-side state needed by the
// `postgres-trigger` engine. See ADR-0066 §10 for the operator-side
// flow. The command refuses-loudly on any §14 boundary (no-PK,
// UNLOGGED, generated columns, custom domain-over-UDT).
type TriggerSetupCmd struct {
	SourceDriver string   `help:"Trigger-CDC source engine to install: 'postgres-trigger' (default), 'sqlite-trigger' (a local SQLite file; --dsn is the file path), or 'd1-trigger' (a live Cloudflare D1 database over the HTTP query API; --dsn is the d1:// form, token env-only)." enum:"postgres-trigger,sqlite-trigger,d1-trigger" default:"postgres-trigger"`
	DSN          string   `help:"Source DSN. For postgres-trigger: a PG DSN (URI or libpq KV form; the connecting role needs CREATE on the target schema, TRIGGER on each replicated table, and INSERT on sluice_change_log). For sqlite-trigger: the SQLite file path. For d1-trigger: d1://<account_id>/<database_id> (or d1://<database_id> + CLOUDFLARE_ACCOUNT_ID); the API token is read from CLOUDFLARE_API_TOKEN." required:"" placeholder:"DSN"`
	Tables       []string `help:"Tables to install per-table capture triggers on (comma-separated, repeatable). Required for v1; empty-list discovery is a follow-up." required:"" sep:"," placeholder:"TABLE"`
	Schema       string   `help:"PG schema (namespace) the change-log + capture function + per-table triggers live in. Defaults to the DSN's 'schema' query parameter (typically 'public'). Ignored for sqlite-trigger / d1-trigger (flat namespace)." placeholder:"NAME"`
	DryRun       bool     `help:"Print the DDL the command would apply and exit; no source-side state is modified." short:"n"`

	AllowPolledFingerprint bool `help:"Opt in to the polled schema-fingerprint fallback (§7) on tiers that deny event-trigger creation. Default off: the engine refuses-loudly on such tiers so the operator explicitly acknowledges the weaker DDL-detection mode."`

	CapturePayload string `help:"How much of each changed row the capture trigger writes to sluice_change_log (ADR-0068). 'full' (default) writes the full before- and after-image on every UPDATE — byte-identical to prior releases, keeps a full-before-image apply WHERE. 'changed' trims the UPDATE after-image to PK + changed columns while keeping the full before-image (so the apply WHERE still does optimistic divergence detection). 'minimal' also trims the before-image to the PK, so the apply WHERE becomes a PK match (last-write-wins; safe for one-way CDC with no concurrent target writers) — reaches toward ~2x source-write overhead. INSERT is unchanged in all modes." enum:"full,changed,minimal" default:"full"`
}

// Run implements `sluice trigger setup`.
func (c *TriggerSetupCmd) Run(g *Globals) error {
	if c.DSN == "" {
		return errors.New("--dsn is required")
	}
	if len(c.Tables) == 0 {
		return errors.New("--tables is required for v1 (pass --tables=t1,t2,...)")
	}
	switch c.SourceDriver {
	case triggerDriverSQLite:
		return c.runSQLiteLike(g, triggerDriverSQLite, sqlitetrigger.Setup)
	case triggerDriverD1:
		return c.runSQLiteLike(g, triggerDriverD1, sqlitetrigger.SetupD1)
	}

	// ADR-0155: pretty TTY view for an interactive apply. --dry-run excludes
	// pretty (wantPrettyProgress), so the DDL preview keeps its exact shape;
	// refusals are printed AFTER the live view tears down (never mid-render).
	pretty := wantPrettyProgress(g, false, c.DryRun, false)
	runCtx, cancel := context.WithCancel(kongContext())
	defer cancel()
	var (
		plan *pgtrigger.Plan
		sink progress.Sink = progress.Nop{}
	)
	runErr := runWithProgress(pretty, cancel, triggerSetupProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(triggerPhaseInstall)
			var e error
			plan, e = pgtrigger.Setup(runCtx, c.DSN, pgtrigger.SetupOptions{
				Tables:                 c.Tables,
				Schema:                 c.Schema,
				DryRun:                 c.DryRun,
				AllowPolledFingerprint: c.AllowPolledFingerprint,
				CapturePayload:         pgtrigger.CapturePayload(c.CapturePayload),
			})
			if (plan != nil && len(plan.Refusals) > 0) || e != nil {
				return e
			}
			sink.PhaseCompleted(triggerPhaseInstall)
			if pretty {
				sink.Summary(progress.Result{Fields: []progress.Field{
					{Label: "Statements", Value: progress.HumanCount(int64(len(plan.Statements)))},
					{Label: "DDL detection", Value: pgtriggerSetupMode(plan)},
					{Label: "PG version", Value: fmt.Sprintf("%d", plan.PGVersionNum)},
				}})
			}
			return nil
		})
	if plan != nil && len(plan.Refusals) > 0 {
		fmt.Fprintln(os.Stderr, "trigger setup refused — see refusals below:")
		for _, r := range plan.Refusals {
			fmt.Fprintf(os.Stderr, "  - %s.%s: %s\n      → %s\n", r.Schema, r.Table, r.Reason, r.Hint)
		}
		return runErr
	}
	if runErr != nil {
		return runErr
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- pgtrigger setup --dry-run (%d statement(s)) --\n", len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
			fmt.Fprintln(os.Stdout)
		}
		if !plan.EventTriggerSupported {
			fmt.Fprintln(os.Stdout, "-- NOTE: the connecting role lacks event-trigger creation; --allow-polled-fingerprint was used or required (§7).")
		}
		return nil
	}
	if pretty {
		return nil // summary panel replaced the applied-line
	}
	fmt.Fprintf(
		os.Stdout,
		"pgtrigger setup applied (%d statement(s); DDL-detection mode: %s; PG version_num=%d)\n",
		len(plan.Statements), pgtriggerSetupMode(plan), plan.PGVersionNum,
	)
	return nil
}

// runSQLiteLike handles `sluice trigger setup` for the SQLite-family engines —
// 'sqlite-trigger' (ADR-0135, a local file) and 'd1-trigger' (ADR-0136, a live
// D1 over HTTP). Both install the SAME change-log + per-table capture triggers
// and share identical CLI output; only the installer (setupFn) and the label
// differ. The PG-only flags are not applicable here.
func (c *TriggerSetupCmd) runSQLiteLike(
	g *Globals,
	label string,
	setupFn func(context.Context, string, sqlitetrigger.SetupOptions) (*sqlitetrigger.Plan, error),
) error {
	pretty := wantPrettyProgress(g, false, c.DryRun, false)
	runCtx, cancel := context.WithCancel(kongContext())
	defer cancel()
	var (
		plan *sqlitetrigger.Plan
		sink progress.Sink = progress.Nop{}
	)
	runErr := runWithProgress(pretty, cancel, triggerSetupProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(triggerPhaseInstall)
			var e error
			plan, e = setupFn(runCtx, c.DSN, sqlitetrigger.SetupOptions{
				Tables: c.Tables,
				DryRun: c.DryRun,
			})
			if (plan != nil && len(plan.Refusals) > 0) || e != nil {
				return e
			}
			sink.PhaseCompleted(triggerPhaseInstall)
			if pretty {
				sink.Summary(progress.Result{Fields: []progress.Field{
					{Label: "Statements", Value: progress.HumanCount(int64(len(plan.Statements)))},
				}})
			}
			return nil
		})
	if plan != nil && len(plan.Refusals) > 0 {
		fmt.Fprintln(os.Stderr, "trigger setup refused — see refusals below:")
		for _, r := range plan.Refusals {
			fmt.Fprintf(os.Stderr, "  - %s: %s\n      → %s\n", r.Table, r.Reason, r.Hint)
		}
		return runErr
	}
	if runErr != nil {
		return runErr
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- %s setup --dry-run (%d statement(s)) --\n", label, len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
			fmt.Fprintln(os.Stdout)
		}
		return nil
	}
	if pretty {
		return nil // summary panel replaced the applied-line
	}
	fmt.Fprintf(os.Stdout, "%s setup applied (%d statement(s))\n", label, len(plan.Statements))
	return nil
}

// TriggerTeardownCmd removes every trace of the trigger engine from
// the source. Idempotent — re-running on a partially-uninstalled
// source proceeds cleanly via DROP ... IF EXISTS.
//
// Destructive: drops sluice_change_log (unless --keep-data) and every
// per-table sluice-installed trigger. Mirrors `sluice slot drop`: a
// confirmation prompt fires by default; --yes skips it for
// scripted/CI use.
type TriggerTeardownCmd struct {
	SourceDriver string   `help:"Trigger-CDC source engine to tear down: 'postgres-trigger' (default), 'sqlite-trigger', or 'd1-trigger'." enum:"postgres-trigger,sqlite-trigger,d1-trigger" default:"postgres-trigger"`
	DSN          string   `help:"Source DSN (PG DSN for postgres-trigger; SQLite file path for sqlite-trigger; d1:// form for d1-trigger, token via CLOUDFLARE_API_TOKEN)." required:"" placeholder:"DSN"`
	Tables       []string `help:"Tables whose per-table triggers should be dropped. Empty (default) discovers every table with a sluice-installed trigger in the source." sep:"," placeholder:"TABLE"`
	Schema       string   `help:"PG schema. Defaults to the DSN's 'schema' query parameter. Ignored for sqlite-trigger / d1-trigger." placeholder:"NAME"`
	KeepData     bool     `help:"Retain sluice_change_log (and the meta table) for forensics. Default drops them — the engine's promise is to remove every trace from the source."`
	DryRun       bool     `help:"Print the DDL and exit." short:"n"`
	Yes          bool     `help:"Skip the destructive-action confirmation prompt. Mirrors 'slot drop --yes'." short:"y"`
}

// Run implements `sluice trigger teardown`.
func (c *TriggerTeardownCmd) Run(g *Globals) error {
	if c.DSN == "" {
		return errors.New("--dsn is required")
	}
	switch c.SourceDriver {
	case triggerDriverSQLite:
		return c.runSQLiteLike(g, "sqlite-trigger", "SQLite source", sqlitetrigger.Teardown)
	case triggerDriverD1:
		return c.runSQLiteLike(g, "d1-trigger", "Cloudflare D1 source", sqlitetrigger.TeardownD1)
	}
	// The destructive-confirmation prompt reads stdin / writes stdout, so it
	// MUST run before any live view owns the terminal.
	if !c.DryRun && !c.Yes {
		prompt := "Tear down the sluice trigger engine on the source (drop per-table triggers"
		if c.KeepData {
			prompt += "; keep the change-log table)? [y/N] "
		} else {
			prompt += " AND the sluice_change_log table)? [y/N] "
		}
		ok, err := confirmDestructive(os.Stdin, os.Stdout, prompt)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "aborted")
			return nil
		}
	}

	// ADR-0155: pretty TTY view for an interactive teardown. --dry-run
	// excludes pretty so the DDL preview keeps its exact shape.
	pretty := wantPrettyProgress(g, false, c.DryRun, false)
	runCtx, cancel := context.WithCancel(kongContext())
	defer cancel()
	var (
		plan *pgtrigger.Plan
		sink progress.Sink = progress.Nop{}
	)
	runErr := runWithProgress(pretty, cancel, triggerTeardownProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(triggerPhaseRemove)
			var e error
			plan, e = pgtrigger.Teardown(runCtx, c.DSN, pgtrigger.TeardownOptions{
				Tables:   c.Tables,
				Schema:   c.Schema,
				KeepData: c.KeepData,
				DryRun:   c.DryRun,
			})
			if e != nil {
				return e
			}
			sink.PhaseCompleted(triggerPhaseRemove)
			if pretty {
				sink.Summary(progress.Result{Fields: []progress.Field{
					{Label: "Statements", Value: progress.HumanCount(int64(len(plan.Statements)))},
					{Label: "Kept data", Value: boolYesNoCLI(c.KeepData)},
				}})
			}
			return nil
		})
	if runErr != nil {
		return runErr
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- pgtrigger teardown --dry-run (%d statement(s)) --\n", len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
		}
		return nil
	}
	if pretty {
		return nil // summary panel replaced the applied-line
	}
	fmt.Fprintf(os.Stdout, "pgtrigger teardown applied (%d statement(s); keep-data=%v)\n",
		len(plan.Statements), c.KeepData)
	return nil
}

// runSQLiteLike handles `sluice trigger teardown` for the SQLite-family engines —
// 'sqlite-trigger' (ADR-0135, a local file) and 'd1-trigger' (ADR-0136, a live
// D1 over HTTP). Both drop the per-table capture triggers and (unless
// --keep-data) the change-log + meta + columns tables; only the remover
// (teardownFn) and the labels differ. label is the engine name for the output
// lines; sourceLabel names the source in the confirmation prompt. Installing
// triggers MODIFIES the operator's database, so the destructive prompt fires for
// D1 too (ADR-0136 §5).
func (c *TriggerTeardownCmd) runSQLiteLike(
	g *Globals,
	label, sourceLabel string,
	teardownFn func(context.Context, string, sqlitetrigger.TeardownOptions) (*sqlitetrigger.Plan, error),
) error {
	// The destructive-confirmation prompt reads stdin / writes stdout, so it
	// MUST run before any live view owns the terminal.
	if !c.DryRun && !c.Yes {
		prompt := "Tear down the sluice trigger engine on the " + sourceLabel + " (drop per-table triggers"
		if c.KeepData {
			prompt += "; keep the change-log table)? [y/N] "
		} else {
			prompt += " AND the sluice_change_log table)? [y/N] "
		}
		ok, err := confirmDestructive(os.Stdin, os.Stdout, prompt)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "aborted")
			return nil
		}
	}
	pretty := wantPrettyProgress(g, false, c.DryRun, false)
	runCtx, cancel := context.WithCancel(kongContext())
	defer cancel()
	var (
		plan *sqlitetrigger.Plan
		sink progress.Sink = progress.Nop{}
	)
	runErr := runWithProgress(pretty, cancel, triggerTeardownProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(triggerPhaseRemove)
			var e error
			plan, e = teardownFn(runCtx, c.DSN, sqlitetrigger.TeardownOptions{
				Tables:   c.Tables,
				KeepData: c.KeepData,
				DryRun:   c.DryRun,
			})
			if e != nil {
				return e
			}
			sink.PhaseCompleted(triggerPhaseRemove)
			if pretty {
				sink.Summary(progress.Result{Fields: []progress.Field{
					{Label: "Statements", Value: progress.HumanCount(int64(len(plan.Statements)))},
					{Label: "Kept data", Value: boolYesNoCLI(c.KeepData)},
				}})
			}
			return nil
		})
	if runErr != nil {
		return runErr
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- %s teardown --dry-run (%d statement(s)) --\n", label, len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
		}
		return nil
	}
	if pretty {
		return nil // summary panel replaced the applied-line
	}
	fmt.Fprintf(os.Stdout, "%s teardown applied (%d statement(s); keep-data=%v)\n",
		label, len(plan.Statements), c.KeepData)
	return nil
}

// TriggerPruneCmd implements `sluice trigger prune` (ADR-0137, Bug 165). The
// trigger-CDC capture never reaps consumed rows, so the source change-log grows
// unbounded for the life of a continuous sync. This command reaps rows the
// TARGET has already durably applied — the ONLY safe lower bound.
//
// The correctness crux (silent-loss avoidance): a change-log row may be pruned
// only if its id is <= the watermark the applier has PERSISTED to the target's
// cdc-state. The exactly-once contract advances that watermark only on durable
// apply, so the target's persisted last_id IS the durably-applied frontier. The
// CDC reader's read cursor runs AHEAD of it; pruning on the read cursor (or the
// source's MAX(id), or a TTL) would delete not-yet-applied rows → warm-resume
// reads id > durable_watermark and finds them gone → silent permanent loss. So
// the prune bound is read from the TARGET, and the command REFUSES LOUDLY if it
// cannot read that position (it never prunes blind).
type TriggerPruneCmd struct {
	SourceDriver string `help:"Trigger-CDC source engine whose change-log to prune: 'postgres-trigger' (default), 'sqlite-trigger', or 'd1-trigger'." enum:"postgres-trigger,sqlite-trigger,d1-trigger" default:"postgres-trigger" group:"source"`
	Source       string `help:"Source DSN where sluice_change_log lives (PG DSN for postgres-trigger; SQLite file path for sqlite-trigger; d1:// form for d1-trigger, token via CLOUDFLARE_API_TOKEN)." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
	Schema       string `help:"PG source schema holding sluice_change_log. Defaults to the DSN's 'schema' parameter. Ignored for sqlite-trigger / d1-trigger." placeholder:"NAME" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. postgres, mysql) — where the durably-applied CDC position lives. See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN (the same target the sync applies to)." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string `help:"Stream identifier whose durable position bounds the prune (the same --stream-id the sync uses)." required:"" placeholder:"ID"`

	Keep   int64 `help:"Safety margin: keep the most-recent N change-log ids below the durable frontier unpruned. Belt-and-suspenders — the frontier itself is already durably applied, so even 0 is safe. Default 1000." default:"1000" placeholder:"N"`
	Vacuum bool  `help:"After pruning, VACUUM to reclaim file space (sqlite-trigger / d1-trigger only; PG relies on autovacuum). Off by default — VACUUM rewrites the whole database."`
	DryRun bool  `help:"Compute and print the prune bound without deleting anything." short:"n"`
}

// Run implements `sluice trigger prune`.
func (c *TriggerPruneCmd) Run(g *Globals) error {
	if c.Source == "" {
		return errors.New("--source is required")
	}
	if c.Target == "" {
		return errors.New("--target is required")
	}
	if c.StreamID == "" {
		return errors.New("--stream-id is required")
	}
	if c.Keep < 0 {
		return errors.New("--keep must be >= 0")
	}

	ctx := kongContext()

	// Step 1 — read the durably-applied frontier from the TARGET (the only safe
	// lower bound). Refuses loudly when no durable position exists.
	appliedLastID, err := c.readDurableFrontier(ctx)
	if err != nil {
		return err
	}

	// Step 2 — compute the prune bound. cut <= 0 means nothing below the frontier
	// minus the margin: a safe no-op.
	cut, doPrune := computePruneCut(appliedLastID, c.Keep)
	if !doPrune {
		fmt.Fprintf(os.Stdout,
			"trigger prune: durable frontier last_id=%d, --keep=%d ⇒ cut=%d (<= 0); nothing to prune\n",
			appliedLastID, c.Keep, cut)
		return nil
	}

	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- trigger prune --dry-run --\n")
		fmt.Fprintf(os.Stdout,
			"durable frontier last_id=%d, --keep=%d ⇒ would DELETE FROM %s WHERE id <= %d\n",
			appliedLastID, c.Keep, sqlitetrigger.ChangeLogTable, cut)
		return c.runPruneDryRun(ctx)
	}

	// Step 3 — engine-dispatched DELETE on the SOURCE, under the pretty view
	// (ADR-0155). The pre-phase steps above stay non-pretty so their stdout
	// notes / dry-run / no-op output is byte-identical; only the DELETE +
	// summary render through the live view.
	pretty := wantPrettyProgress(g, false, false, false)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		out  triggerPruneOutcome
		sink progress.Sink = progress.Nop{}
	)
	runErr := runWithProgress(pretty, cancel, triggerPruneProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(triggerPhasePrune)
			var e error
			out, e = c.runPrune(runCtx, cut)
			if e != nil {
				return e
			}
			sink.PhaseCompleted(triggerPhasePrune)
			if pretty {
				sink.Summary(progress.Result{Fields: []progress.Field{
					{Label: "Deleted", Value: progress.HumanCount(out.deleted)},
					{Label: "Remaining", Value: progress.HumanCount(out.remaining)},
					{Label: "Vacuumed", Value: boolYesNoCLI(out.vacuumed)},
				}})
			}
			return nil
		})
	if runErr != nil {
		return runErr
	}
	if !pretty {
		printPruneResult(out.label, cut, out.deleted, out.remainingMin, out.remaining, out.vacuumed)
	}
	return nil
}

// readDurableFrontier opens the TARGET's ChangeApplier, reads the persisted CDC
// state for the stream via ListStreams (the SAME read path `sluice sync status` /
// `sync health` use), and decodes the trigger-CDC {"last_id":N} token to the
// durably-applied id. It also CROSS-CHECKS the stream's recorded source
// fingerprint against --source to refuse a --source/--stream-id mis-pairing
// (see [TriggerPruneCmd.crossCheckSource]). Refuses loudly (operationalError,
// exit 2) when the target engine is unknown, the streams can't be read, or —
// critically — no position exists yet (no durable frontier ⇒ no safe lower
// bound ⇒ never prune blind).
func (c *TriggerPruneCmd) readDurableFrontier(ctx context.Context) (int64, error) {
	target, err := resolveEngine(c.TargetDriver)
	if err != nil {
		return 0, operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}
	applier, err := target.OpenChangeApplier(ctx, c.Target)
	if err != nil {
		return 0, operationalError{err: fmt.Errorf("open target applier: %w", err)}
	}
	defer func() {
		if cl, ok := applier.(io.Closer); ok {
			_ = cl.Close()
		}
	}()

	// ListStreams (not ReadPosition) so we get the recorded source fingerprint
	// alongside the position in one read — both come from the same cdc-state row.
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return 0, operationalError{err: fmt.Errorf("read durable streams from the target: %w", err)}
	}
	var found *ir.StreamStatus
	for i := range streams {
		if streams[i].StreamID == c.StreamID {
			found = &streams[i]
			break
		}
	}
	if found == nil {
		return 0, operationalError{err: fmt.Errorf(
			"no durable CDC position for stream %q on the target — refusing to prune; "+
				"without a durably-applied frontier there is no safe lower bound (the applier has not persisted a position yet)",
			c.StreamID,
		)}
	}

	if err := c.crossCheckSource(found.SourceDSNFingerprint); err != nil {
		return 0, err
	}

	appliedLastID, err := decodeAppliedLastID(c.SourceDriver, found.Position.Token)
	if err != nil {
		return 0, operationalError{err: fmt.Errorf("decode durable position for stream %q: %w", c.StreamID, err)}
	}
	return appliedLastID, nil
}

// crossCheckSource refuses the one real over-delete path: a --source/--stream-id
// mis-pairing. If --source points at change-log A but --stream-id resolves to a
// DIFFERENT source B's durable frontier (a different id space), applying B's cut
// to A's change-log can delete A's not-yet-applied rows = silent loss. The stream
// recorded its source fingerprint (ADR-0031); we recompute --source's and refuse
// LOUDLY on mismatch.
//
// The check can only run when BOTH fingerprints are known. ADR-0031 fingerprints
// only host:port:db, so a SQLite file path or a d1:// DSN yields "" — for those
// sources the cross-check can't run; we print a note and rely on the operator
// passing the exact --source/--stream-id pair the sync uses (documented in
// docs/operator/trigger-changelog-retention.md). Extending the fingerprint to
// file/d1 sources is a tracked follow-up.
func (c *TriggerPruneCmd) crossCheckSource(storedFingerprint string) error {
	computed := pipeline.FingerprintSourceDSN(c.Source)
	if storedFingerprint == "" || computed == "" {
		fmt.Fprintf(os.Stderr,
			"note: stream %q source identity not cross-checked (no recorded fingerprint for this source type) — "+
				"ensure --source is the exact source this stream syncs from\n",
			c.StreamID)
		return nil
	}
	if computed != storedFingerprint {
		return operationalError{err: fmt.Errorf(
			"--source (fingerprint %s) does not match the source recorded for stream %q (fingerprint %s) — "+
				"refusing to prune the wrong change-log; pass the exact --source/--stream-id pair the sync uses",
			computed, c.StreamID, storedFingerprint,
		)}
	}
	return nil
}

// decodeAppliedLastID dispatches the trigger-CDC token decode to the source
// engine's position codec. All three trigger engines share the {"last_id":N}
// shape, but the decode stays engine-owned (the codec is the wire-shape's single
// owner). Pure (no I/O) so the prune-bound path is unit-testable.
func decodeAppliedLastID(sourceDriver, token string) (int64, error) {
	switch sourceDriver {
	case triggerDriverSQLite, triggerDriverD1:
		return sqlitetrigger.AppliedLastID(token)
	case triggerDriverPostgres:
		return pgtrigger.AppliedLastID(token)
	default:
		return 0, fmt.Errorf("unknown --source-driver %q", sourceDriver)
	}
}

// computePruneCut derives the inclusive DELETE bound from the durably-applied
// frontier and the operator's safety margin: cut = appliedLastID - keep. A
// non-positive cut means there is nothing safely below the frontier minus the
// margin, so prune=false (the caller reports a no-op). Pure — unit-tested.
func computePruneCut(appliedLastID, keep int64) (cut int64, prune bool) {
	cut = appliedLastID - keep
	if cut <= 0 {
		return cut, false
	}
	return cut, true
}

// triggerPruneOutcome is the engine-neutral result of a SOURCE-side prune,
// so the caller can either print it (non-TTY, byte-identical) or feed it to
// the ADR-0155 summary panel (pretty view).
type triggerPruneOutcome struct {
	label        string
	deleted      int64
	remainingMin int64
	remaining    int64
	vacuumed     bool
}

// runPrune dispatches the SOURCE-side DELETE to the trigger engine and
// returns the normalized outcome (the caller renders it).
func (c *TriggerPruneCmd) runPrune(ctx context.Context, cut int64) (triggerPruneOutcome, error) {
	switch c.SourceDriver {
	case triggerDriverSQLite:
		res, err := sqlitetrigger.Prune(ctx, c.Source, sqlitetrigger.PruneOptions{Cut: cut, Vacuum: c.Vacuum})
		if err != nil {
			return triggerPruneOutcome{}, err
		}
		return triggerPruneOutcome{"sqlite-trigger", res.Deleted, res.RemainingMin, res.Remaining, res.Vacuumed}, nil
	case triggerDriverD1:
		res, err := sqlitetrigger.PruneD1(ctx, c.Source, sqlitetrigger.PruneOptions{Cut: cut, Vacuum: c.Vacuum})
		if err != nil {
			return triggerPruneOutcome{}, err
		}
		return triggerPruneOutcome{"d1-trigger", res.Deleted, res.RemainingMin, res.Remaining, res.Vacuumed}, nil
	case triggerDriverPostgres:
		if c.Vacuum {
			return triggerPruneOutcome{}, errors.New("--vacuum is not supported for postgres-trigger (PG reclaims space via autovacuum); re-run without --vacuum")
		}
		res, err := pgtrigger.Prune(ctx, c.Source, pgtrigger.PruneOptions{Cut: cut, Schema: c.Schema})
		if err != nil {
			return triggerPruneOutcome{}, err
		}
		return triggerPruneOutcome{"pgtrigger", res.Deleted, res.RemainingMin, res.Remaining, false}, nil
	default:
		return triggerPruneOutcome{}, fmt.Errorf("unknown --source-driver %q", c.SourceDriver)
	}
}

// runPruneDryRun reports the current change-log stats (no DELETE) so the operator
// can preview the prune.
func (c *TriggerPruneCmd) runPruneDryRun(ctx context.Context) error {
	var (
		minID, count int64
		err          error
	)
	switch c.SourceDriver {
	case triggerDriverSQLite:
		var res *sqlitetrigger.PruneResult
		res, err = sqlitetrigger.Prune(ctx, c.Source, sqlitetrigger.PruneOptions{DryRun: true})
		if res != nil {
			minID, count = res.RemainingMin, res.Remaining
		}
	case triggerDriverD1:
		var res *sqlitetrigger.PruneResult
		res, err = sqlitetrigger.PruneD1(ctx, c.Source, sqlitetrigger.PruneOptions{DryRun: true})
		if res != nil {
			minID, count = res.RemainingMin, res.Remaining
		}
	case triggerDriverPostgres:
		var res *pgtrigger.PruneResult
		res, err = pgtrigger.Prune(ctx, c.Source, pgtrigger.PruneOptions{Schema: c.Schema, DryRun: true})
		if res != nil {
			minID, count = res.RemainingMin, res.Remaining
		}
	default:
		return fmt.Errorf("unknown --source-driver %q", c.SourceDriver)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "change-log currently holds %d row(s) (min id %d)\n", count, minID)
	return nil
}

// printPruneResult renders the operator-facing prune outcome on stdout.
func printPruneResult(label string, cut, deleted, remainingMin, remaining int64, vacuumed bool) {
	if remaining == 0 {
		fmt.Fprintf(os.Stdout,
			"%s prune: deleted %d change-log row(s) with id <= %d; change-log now empty\n",
			label, deleted, cut)
	} else {
		fmt.Fprintf(os.Stdout,
			"%s prune: deleted %d change-log row(s) with id <= %d; %d row(s) remain (min id %d)\n",
			label, deleted, cut, remaining, remainingMin)
	}
	if vacuumed {
		fmt.Fprintln(os.Stdout, "  VACUUM applied (file space reclaimed)")
	}
}
