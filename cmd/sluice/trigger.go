// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

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
func (c *TriggerSetupCmd) Run(_ *Globals) error {
	if c.DSN == "" {
		return errors.New("--dsn is required")
	}
	if len(c.Tables) == 0 {
		return errors.New("--tables is required for v1 (pass --tables=t1,t2,...)")
	}
	switch c.SourceDriver {
	case triggerDriverSQLite:
		return c.runSQLiteLike(triggerDriverSQLite, sqlitetrigger.Setup)
	case triggerDriverD1:
		return c.runSQLiteLike(triggerDriverD1, sqlitetrigger.SetupD1)
	}
	ctx := kongContext()
	plan, err := pgtrigger.Setup(ctx, c.DSN, pgtrigger.SetupOptions{
		Tables:                 c.Tables,
		Schema:                 c.Schema,
		DryRun:                 c.DryRun,
		AllowPolledFingerprint: c.AllowPolledFingerprint,
		CapturePayload:         pgtrigger.CapturePayload(c.CapturePayload),
	})
	if plan != nil && len(plan.Refusals) > 0 {
		fmt.Fprintln(os.Stderr, "trigger setup refused — see refusals below:")
		for _, r := range plan.Refusals {
			fmt.Fprintf(os.Stderr, "  - %s.%s: %s\n      → %s\n", r.Schema, r.Table, r.Reason, r.Hint)
		}
		return err
	}
	if err != nil {
		return err
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
	mode := "event-trigger + polling"
	if !plan.EventTriggerSupported {
		mode = "polled-fingerprint-only"
	}
	fmt.Fprintf(
		os.Stdout,
		"pgtrigger setup applied (%d statement(s); DDL-detection mode: %s; PG version_num=%d)\n",
		len(plan.Statements), mode, plan.PGVersionNum,
	)
	return nil
}

// runSQLiteLike handles `sluice trigger setup` for the SQLite-family engines —
// 'sqlite-trigger' (ADR-0135, a local file) and 'd1-trigger' (ADR-0136, a live
// D1 over HTTP). Both install the SAME change-log + per-table capture triggers
// and share identical CLI output; only the installer (setupFn) and the label
// differ. The PG-only flags are not applicable here.
func (c *TriggerSetupCmd) runSQLiteLike(
	label string,
	setupFn func(context.Context, string, sqlitetrigger.SetupOptions) (*sqlitetrigger.Plan, error),
) error {
	ctx := kongContext()
	plan, err := setupFn(ctx, c.DSN, sqlitetrigger.SetupOptions{
		Tables: c.Tables,
		DryRun: c.DryRun,
	})
	if plan != nil && len(plan.Refusals) > 0 {
		fmt.Fprintln(os.Stderr, "trigger setup refused — see refusals below:")
		for _, r := range plan.Refusals {
			fmt.Fprintf(os.Stderr, "  - %s: %s\n      → %s\n", r.Table, r.Reason, r.Hint)
		}
		return err
	}
	if err != nil {
		return err
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- %s setup --dry-run (%d statement(s)) --\n", label, len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
			fmt.Fprintln(os.Stdout)
		}
		return nil
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
func (c *TriggerTeardownCmd) Run(_ *Globals) error {
	if c.DSN == "" {
		return errors.New("--dsn is required")
	}
	switch c.SourceDriver {
	case triggerDriverSQLite:
		return c.runSQLiteLike("sqlite-trigger", "SQLite source", sqlitetrigger.Teardown)
	case triggerDriverD1:
		return c.runSQLiteLike("d1-trigger", "Cloudflare D1 source", sqlitetrigger.TeardownD1)
	}
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
	ctx := kongContext()
	plan, err := pgtrigger.Teardown(ctx, c.DSN, pgtrigger.TeardownOptions{
		Tables:   c.Tables,
		Schema:   c.Schema,
		KeepData: c.KeepData,
		DryRun:   c.DryRun,
	})
	if err != nil {
		return err
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- pgtrigger teardown --dry-run (%d statement(s)) --\n", len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
		}
		return nil
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
	label, sourceLabel string,
	teardownFn func(context.Context, string, sqlitetrigger.TeardownOptions) (*sqlitetrigger.Plan, error),
) error {
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
	ctx := kongContext()
	plan, err := teardownFn(ctx, c.DSN, sqlitetrigger.TeardownOptions{
		Tables:   c.Tables,
		KeepData: c.KeepData,
		DryRun:   c.DryRun,
	})
	if err != nil {
		return err
	}
	if c.DryRun {
		fmt.Fprintf(os.Stdout, "-- %s teardown --dry-run (%d statement(s)) --\n", label, len(plan.Statements))
		for _, s := range plan.Statements {
			fmt.Fprintln(os.Stdout, s+";")
		}
		return nil
	}
	fmt.Fprintf(os.Stdout, "%s teardown applied (%d statement(s); keep-data=%v)\n",
		label, len(plan.Statements), c.KeepData)
	return nil
}

// triggerSetupExampleSchema is a tiny helper the help-text generator
// could reach for if we ever wanted to render an example DSN in the
// command's --help output. Kept around but unused for now; keeps the
// strings import non-conditional.
//
//nolint:unused
func triggerSetupExampleSchema(s string) string {
	return strings.TrimSpace(s)
}

// _ ensures the context import stays referenced even if the kong
// wiring ever moves Run signatures around. The Run method uses
// kongContext() which returns context.Context.
var _ = context.Background
