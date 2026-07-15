// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"

	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/progress"
)

// BackfillCmd implements `sluice backfill` (ADR-0159): the expand-
// contract "migrate" step. Single-endpoint — the backfill runs INSIDE
// one database (no source/target pair), walking the table's primary
// key in bounded batches and issuing one UPDATE per batch, with the
// cursor persisted in the same database's sluice_migrate_state tables
// so a killed run resumes where it left off.
//
// The --set expressions and the --where predicate are native SQL for
// the --driver engine, emitted verbatim (same-DB, so there is no
// cross-dialect translation to do — the --expr-override posture).
type BackfillCmd struct {
	Driver string `help:"Engine name for the database (e.g. mysql, postgres, planetscale). See 'sluice engines'." required:"" placeholder:"NAME"`
	DSN    string `help:"Database DSN. Backfill is same-database: it reads and updates this one endpoint." required:"" placeholder:"DSN"`
	Table  string `help:"Table to backfill." required:"" placeholder:"TABLE"`

	Set   []string `help:"Assignment 'col = <expr>' applied to every matched row (repeatable). The expression is native SQL for the engine, emitted verbatim — split at the FIRST '=', so expressions may themselves contain '='." required:"" placeholder:"'COL = EXPR'" sep:"none"`
	Where string   `help:"Native-SQL predicate scoping which rows are backfilled. Make it self-describing (e.g. 'new_col IS NULL') so re-runs and crash-resume skip already-done rows." placeholder:"PREDICATE"`

	BatchSize int  `help:"Rows per bounded UPDATE batch (keyset-chunked walk of the primary key). 0 uses sluice's bulk-copy default." placeholder:"N"`
	DryRun    bool `help:"Print the generated per-chunk UPDATE statement and an affected-row estimate, then exit without writing anything."`
	Restart   bool `help:"Discard the stored resume cursor for this exact spec (--set/--where) and start over from the beginning of the table."`
}

// Run implements `sluice backfill`.
func (b *BackfillCmd) Run(g *Globals) error {
	engine, err := resolveEngine(b.Driver)
	if err != nil {
		return fmt.Errorf("--driver: %w", err)
	}
	if engine, err = applyEngineOptions(engine, g); err != nil {
		return err
	}
	sets, err := pipeline.ParseBackfillSets(b.Set)
	if err != nil {
		return err
	}

	backfiller := &pipeline.Backfiller{
		Engine:    engine,
		DSN:       b.DSN,
		Table:     b.Table,
		Sets:      sets,
		Where:     b.Where,
		BatchSize: b.BatchSize,
		DryRun:    b.DryRun,
		Restart:   b.Restart,
		Out:       os.Stdout,
	}

	// ADR-0155: pretty TTY view only for an interactive, non-dry-run
	// invocation (a dry-run prints a preview, not phase progress).
	pretty := !b.DryRun && wantPrettyProgress(g, false, false, false)
	ctx, cancel := context.WithCancel(kongContext())
	defer cancel()
	return runWithProgress(pretty, cancel, pipeline.BackfillProgressSpec,
		func(s progress.Sink) { backfiller.Progress = s },
		func() error {
			_, err := backfiller.Run(ctx)
			return err
		})
}
