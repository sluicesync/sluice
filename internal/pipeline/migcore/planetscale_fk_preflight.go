// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

// # PlanetScale foreign-key preflight + the migration plan/gotcha report
//
// Two operator-facing pre-copy surfaces, shared by BOTH copy-phase entry points
// (migrate and sync cold-start — they share the schema-apply + copy + index +
// constraints phases), so they live here in migcore rather than on the Migrator.
//
// ## Why (the field report)
//
// A 30 GB PlanetScale region-to-region move took 7–8 hours across 4–5 restarts
// because the foreign-key + timeout gotchas surfaced one at a time, at the END
// of the migration, after the whole copy. The constraints phase is the last
// phase, so anything wrong with it is discovered last. On a PlanetScale target
// several real failure modes live there:
//
//   - FK support DISABLED (`allow_foreign_key_constraints` off): the platform
//     rejects `ADD FOREIGN KEY` outright. Confirmable, highest confidence → a
//     loud coded refusal BEFORE the copy.
//   - FK support enabled but SAFE MIGRATIONS on: the branch blocks direct DDL
//     (errno 1105), and sluice's FK add — including the item-109 metadata-only
//     escape, which is itself a direct `ALTER … ADD FOREIGN KEY` under
//     `foreign_key_checks=0` — is direct DDL. There is NO deploy-request
//     fallback for FK constraints (Vitess Online DDL refuses FK-participating
//     tables), so this too fails at the constraints phase. Lower confidence
//     (the run MIGHT be small enough to not care, or the operator may plan to
//     disable safe migrations) → a WARN before the copy, never a refusal.
//   - The errno-3024 statement-time wall on a large FK-heavy table: handled
//     mid-run by the existing hints; the plan report nudges toward
//     `--planetscale-raise-query-timeout` / `--skip-foreign-keys` up front.
//
// The rule (coordinator, and the loud-failure tenet): refuse only what is
// CONFIRMABLE (FK disabled); everything else is a WARN that still fires before
// the copy. Never refuse a config that might work.
//
// Grounding for the "--skip-foreign-keys keeps each FK's referencing columns
// indexed" claim this file's remedies repeat (2026-08-22 invariant sweep; the
// engine-set-claim rule): the guarantee is SLUICE'S OWN, not a source-engine
// fact. pipeline.applySkipForeignKeys (skip_foreign_keys.go, shared by migrate
// AND sync cold-start) synthesizes a backing index on any FK's referencing
// column tuple no copied index already covers as a left-prefix, precisely
// because a PG source never auto-indexed them and a naive skip on a MySQL
// target would leave them bare. Pinned by internal/pipeline's
// skip_foreign_keys_test.go family matrix. If that synthesis is ever narrowed,
// these three remedy strings (and internal/sluicecode's siblings) overclaim.

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// PlanetScaleFKStatus is the verdict the FK preflight hands back for the plan
// report (Feature 2). A confirmable-DISABLED target does not produce a status —
// it produces the coded refusal error instead.
type PlanetScaleFKStatus int

const (
	// PSFKNotApplicable means the preflight said nothing: not a PlanetScale
	// target, the source has no foreign keys, or `--skip-foreign-keys` opted out.
	PSFKNotApplicable PlanetScaleFKStatus = iota

	// PSFKEnabled means FK support is on and safe migrations off — the FKs can land.
	PSFKEnabled

	// PSFKRiskySafeMigrations means FK support is on but the branch has safe
	// migrations on, which blocks the direct DDL the FK add uses (no
	// FK-constraint deploy-request fallback) — WARNed, not refused.
	PSFKRiskySafeMigrations

	// PSFKUnverified means a PlanetScale target with foreign keys where sluice
	// could not verify the settings (no service token, or the probe failed) —
	// WARNed.
	PSFKUnverified
)

// PlanetScaleFKPreflightInput is what the FK preflight needs. The pipeline
// gathers it; this function owns the WARN/refuse decision and the wording.
type PlanetScaleFKPreflightInput struct {
	// TargetIsPlanetScale is m.Target.Name() == "planetscale".
	TargetIsPlanetScale bool

	// SkipForeignKeys mirrors --skip-foreign-keys: set ⇒ the run creates no
	// FKs, so the whole preflight is a no-op (the operator already opted out).
	SkipForeignKeys bool

	// ForeignKeyCount is how many foreign keys the (filtered, translated)
	// source schema carries — the count the constraints phase would add.
	ForeignKeyCount int

	// Checker probes the control plane. nil ⇒ no service token resolved; the
	// preflight downgrades to a WARN it cannot confirm.
	Checker ir.PlanetScaleForeignKeyChecker
}

// PreflightPlanetScaleForeignKeys runs the pre-copy FK-risk preflight. It
// returns a [PlanetScaleFKStatus] for the plan report and, ONLY on a
// confirmable FK-support-disabled target, a coded refusal error
// ([sluicecode.CodePSForeignKeysNotEnabled]). Every other outcome is a WARN (or
// an INFO) plus a nil error — the run proceeds.
//
// Shared by migrate and sync cold-start; both pass their own entry-point noun
// in for the WARN prose to read naturally, but the remedy is identical.
func PreflightPlanetScaleForeignKeys(ctx context.Context, in PlanetScaleFKPreflightInput) (PlanetScaleFKStatus, error) {
	// Opted out, non-PlanetScale target, or no foreign keys to add: nothing to
	// say. (`--skip-foreign-keys` strips the FKs from the IR, so the constraints
	// phase issues no FK DDL and a disabled/safe-migrations target is moot.)
	if in.SkipForeignKeys || !in.TargetIsPlanetScale || in.ForeignKeyCount == 0 {
		return PSFKNotApplicable, nil
	}

	// No credentials to verify with: WARN that the FKs land at the END and that
	// two PlanetScale settings can make that fail after the whole copy. Still
	// fires before the copy — the operator learns the risk up front.
	if in.Checker == nil {
		slog.WarnContext(ctx,
			"planetscale: this run will create foreign keys AFTER the copy, but no PlanetScale service token was supplied so sluice cannot verify the target's foreign-key-support / safe-migrations settings. "+
				"If foreign-key support is off (allow_foreign_key_constraints), or the branch has safe migrations on (which blocks the direct DDL the FK add uses — there is no deploy-request fallback for FK constraints), the constraints phase fails AFTER the whole copy and --resume re-hits it. "+
				"For a large, foreign-key-heavy schema the reliable path is --skip-foreign-keys up front (each FK's referencing columns stay indexed, so you can add the constraints out-of-band); "+
				"the alternatives are enabling FK support / disabling safe migrations on the target, growing the cluster, or --planetscale-raise-query-timeout for the errno-3024 validation wall. "+
				"Pass --planetscale-org + a service token (PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN) and sluice checks these up front instead of guessing.",
			slog.Int("foreign_keys", in.ForeignKeyCount))
		return PSFKUnverified, nil
	}

	status, err := in.Checker.ForeignKeyStatus(ctx)
	if err != nil {
		// Advisory degrade (same posture as the slot-headroom probe): a probe
		// error must not fail a run that would otherwise succeed. WARN and go.
		slog.WarnContext(ctx,
			"planetscale: could not probe the target's foreign-key-support / safe-migrations settings; proceeding. "+
				"If foreign-key support is off, or safe migrations is on, the foreign-key constraints phase fails AFTER the whole copy — re-run with --skip-foreign-keys to migrate without the foreign keys if you hit that.",
			slog.Int("foreign_keys", in.ForeignKeyCount),
			slog.String("error", err.Error()))
		return PSFKUnverified, nil
	}

	// Confirmable FK-support DISABLED: the highest-confidence failure. Refuse
	// loudly BEFORE the copy — a seconds-long fix beats hours-then-fail.
	if !status.ForeignKeysEnabled {
		return PSFKNotApplicable, &sluicecode.CodedError{
			Code: sluicecode.CodePSForeignKeysNotEnabled,
			Hint: "enable foreign key support on the target PlanetScale database, or re-run with --skip-foreign-keys",
			Err: fmt.Errorf("pipeline: the PlanetScale target has foreign-key support disabled "+
				"(allow_foreign_key_constraints is off, read back as foreign_keys_enabled=false), but this run's source schema declares %d foreign key(s) it would add after the copy. "+
				"PlanetScale rejects ADD FOREIGN KEY outright while support is off, so the run would fail at the constraints phase — after the whole copy — and --resume re-hits the identical wall. "+
				"Enable foreign key support on the target database (PlanetScale dashboard → the database's Settings → \"Allow foreign key constraints\", or PATCH the database's allow_foreign_key_constraints via the PlanetScale API), then re-run — "+
				"or re-run with --skip-foreign-keys to migrate WITHOUT the foreign keys (each FK's referencing columns stay indexed, so you can add the constraints out-of-band afterward)", in.ForeignKeyCount),
		}
	}

	// FK support ON but safe migrations ON: the direct DDL the FK add uses is
	// blocked (errno 1105), and there is no FK-constraint deploy-request
	// fallback. WARN before the copy — do not refuse a config that MIGHT work
	// (a small run, or the operator disabling safe migrations for the window).
	if status.SafeMigrations {
		slog.WarnContext(ctx,
			"planetscale: foreign-key support is enabled, but the target branch has safe migrations ON, which blocks the direct DDL sluice's foreign-key add uses (errno 1105 \"direct DDL is disabled\") — and there is no deploy-request fallback for FK constraints. "+
				"So the foreign keys would fail at the constraints phase AFTER the whole copy, and --resume re-hits it deterministically. "+
				"For a large, foreign-key-heavy schema, re-run with --skip-foreign-keys (each FK's referencing columns stay indexed, so you can add the constraints out-of-band afterward), or disable safe migrations on the branch for the migration window and re-enable it after.",
			slog.Int("foreign_keys", in.ForeignKeyCount))
		return PSFKRiskySafeMigrations, nil
	}

	// FK support on, safe migrations off: the foreign keys can land. INFO so the
	// operator knows the constraints phase is expected to run (a huge FK-heavy
	// table can still hit the errno-3024 wall — the plan report nudges toward
	// --planetscale-raise-query-timeout, and the item-109 metadata-only escape
	// covers the unfiltered path).
	slog.InfoContext(ctx,
		"planetscale: target has foreign-key support enabled and safe migrations off; the run's foreign keys will be added after the copy",
		slog.Int("foreign_keys", in.ForeignKeyCount))
	return PSFKEnabled, nil
}

// fkStatusWord renders a [PlanetScaleFKStatus] for the plan report's attribute.
func fkStatusWord(s PlanetScaleFKStatus) string {
	switch s {
	case PSFKEnabled:
		return "enabled"
	case PSFKRiskySafeMigrations:
		return "enabled-but-safe-migrations-blocks-direct-ddl"
	case PSFKUnverified:
		return "unverified-no-service-token"
	default:
		return "n/a"
	}
}

// MigrationPlanSummary is the pre-copy "plan + gotcha report" (Feature 2): a
// concise, structured, once-emitted summary of what the run is about to do, so
// the operator sees the plan and the PlanetScale gotchas ONCE, before
// committing hours — rather than discovering them one at a time at the end.
//
// It is deliberately quiet on a boring run: [LogMigrationPlanSummary] emits
// nothing unless there is something worth saying (a PlanetScale target, any
// foreign keys, or a large table).
type MigrationPlanSummary struct {
	Command             string // "migrate" / "sync cold-start" — the report's noun
	TargetEngine        string
	TargetIsPlanetScale bool

	TableCount      int
	ForeignKeyCount int
	FKStatus        PlanetScaleFKStatus

	// LargestTable / LargestTableRows describe the biggest table by row
	// estimate; LargestTableKnown is false when no estimate was available (the
	// source engine has no estimator, or every probe failed). The estimate is
	// from STATISTICS and is routinely off by a wide margin — labelled as an
	// estimate for exactly that reason (roadmap item 124).
	LargestTable      string
	LargestTableRows  int64
	LargestTableKnown bool

	// UpfrontIndexes / RaiseQueryTimeout mirror the flags, so the report can
	// nudge only when the mitigation is NOT already engaged.
	UpfrontIndexes    bool
	RaiseQueryTimeout bool
}

// largePlanTableRows is the row-estimate floor above which a table is "large"
// for the plan report's PlanetScale index/timeout nudges. It mirrors the
// ADR-0182 query-timeout size gate's floor (pipeline.queryTimeoutRaiseMinRows =
// 1,000,000) so the report nudges toward exactly the runs that gate would act
// on; kept as a local constant because migcore is imported BY the pipeline, not
// the reverse.
const largePlanTableRows int64 = 1_000_000

// LogMigrationPlanSummary emits the concise plan/gotcha report — or nothing,
// when the run is small and un-noteworthy (no PlanetScale target, no foreign
// keys, no large table). Structured slog so it honours --no-progress /
// --log-format; the verbose PlanetScale nudge is gated on a large table.
func LogMigrationPlanSummary(ctx context.Context, s MigrationPlanSummary) {
	largeTable := s.LargestTableKnown && s.LargestTableRows >= largePlanTableRows
	// Gate: only speak when there is something worth saying.
	if !s.TargetIsPlanetScale && s.ForeignKeyCount == 0 && !largeTable {
		return
	}

	attrs := []any{
		slog.String("command", s.Command),
		slog.String("target", s.TargetEngine),
		slog.Int("tables", s.TableCount),
		slog.Int("foreign_keys", s.ForeignKeyCount),
	}
	if s.LargestTableKnown {
		attrs = append(attrs,
			slog.String("largest_table", s.LargestTable),
			slog.Int64("largest_table_estimated_rows", s.LargestTableRows))
	}
	if s.TargetIsPlanetScale && s.ForeignKeyCount > 0 {
		attrs = append(attrs, slog.String("fk_support", fkStatusWord(s.FKStatus)))
	}
	slog.InfoContext(ctx, "migration plan: review before the copy starts (row counts are STATISTICS estimates)", attrs...)

	// PlanetScale large-table gotcha nudge — only when a large table is present
	// AND the mitigation is not already engaged, so it never nags a run that is
	// already doing the right thing.
	if s.TargetIsPlanetScale && largeTable {
		switch {
		case s.UpfrontIndexes:
			slog.InfoContext(ctx,
				"planetscale: large table present; secondary indexes build UPFRONT during the copy (--upfront-indexes), avoiding the post-copy ALTER that hits the statement-time wall",
				slog.String("largest_table", s.LargestTable),
				slog.Int64("largest_table_estimated_rows", s.LargestTableRows))
		default:
			nudge := "consider --upfront-indexes (build indexes during the copy) "
			if !s.RaiseQueryTimeout {
				nudge += "and/or --planetscale-raise-query-timeout (raise the keyspace statement-time limit for the run) "
			}
			slog.InfoContext(ctx,
				"planetscale: large table present; secondary indexes and foreign keys build AFTER the copy and can hit PlanetScale's statement-time wall (errno 3024) — "+nudge+
					"to reduce the risk of a late failure. A service token also arms the ADR-0148 deploy-request fallback for the index build.",
				slog.String("largest_table", s.LargestTable),
				slog.Int64("largest_table_estimated_rows", s.LargestTableRows))
		}
	}
}

// CountForeignKeys totals the foreign keys across a schema's tables — the count
// the constraints phase would add, and the FK preflight's trigger.
func CountForeignKeys(schema *ir.Schema) int {
	if schema == nil {
		return 0
	}
	n := 0
	for _, t := range schema.Tables {
		n += len(t.ForeignKeys)
	}
	return n
}
