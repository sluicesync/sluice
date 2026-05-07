// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/pipeline"
)

// SchemaAddTableCmd implements `sluice schema add-table` — the
// Phase 1 MVP of the mid-stream add-table feature
// (`docs/dev/design-mid-stream-add-table.md`). It brings a new
// source table into an active CDC stream's scope without forcing a
// destructive `--reset-target-data` recovery cycle.
//
// Workflow the operator follows:
//
//	# 1. drain the running stream cleanly
//	sluice sync stop --wait --target-driver=postgres --target=... --stream-id=live
//
//	# 2. extend the stream's scope to the new table
//	sluice schema add-table users_v2 \
//	  --source-driver=postgres --source=... \
//	  --target-driver=postgres --target=... \
//	  --stream-id=live
//
//	# 3. resume — CDC picks up the new table from this point forward
//	sluice sync start --target-driver=postgres --target=... ...
//
// The command is destructive on the target (creates a table, copies
// rows, on Postgres also extends the publication). It prompts for
// typed confirmation (the table name) unless --yes is supplied,
// mirroring the friction tier of `--reset-target-data`.
//
// Phase 1 is intentionally narrow: one table per invocation, the
// stream must be drained first (live add-table is Phase 2), and the
// source/target must be the same engine pair the active stream uses.
// See the proto-ADR for the design space.
type SchemaAddTableCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	Table    string `arg:"" help:"Unqualified name of the new source table to add to the active stream's scope. The schema/database is inferred from --source." placeholder:"TABLE"`
	StreamID string `help:"Stream identifier; must match the active stream's id (run 'sluice sync status' to list)." required:"" placeholder:"ID"`

	TypeOverride []string `help:"Force a specific target type for a column on the new table (repeatable). Format: 'TABLE.COLUMN=TYPE'. CLI form of the YAML 'mappings:' config; for target-type options, use the YAML form." placeholder:"TABLE.COLUMN=TYPE"`
	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. Emitted verbatim; ADR-0016 translator skips overridden columns." placeholder:"TABLE.COLUMN=EXPRESSION"`

	SlotName string `help:"Override the temporary replication-slot name used for the snapshot capture on engines with a slot concept (Postgres). Defaults to 'sluice_addtable_<table>'. Engines without slots ignore this flag." placeholder:"NAME"`

	DryRun bool `help:"Print the plan (which table, source publication update, target DDL summary) without modifying the source publication, target schema, or capturing a snapshot." short:"n"`
	Yes    bool `help:"Skip the typed-confirmation prompt." short:"y"`
}

// Run implements `sluice schema add-table`.
func (s *SchemaAddTableCmd) Run(g *Globals) error {
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

	if s.Table == "" {
		return errors.New("table name is required (positional argument)")
	}
	if s.StreamID == "" {
		return errors.New("--stream-id is required")
	}

	mappings, err := resolveMappings(s.TypeOverride, cfg)
	if err != nil {
		return err
	}
	exprMappings, err := resolveExpressionMappings(s.ExprOverride, cfg)
	if err != nil {
		return err
	}

	// Typed confirmation (the table name) unless --yes / --dry-run.
	// Same friction tier as --reset-target-data: this is a target-
	// schema mutation and a source-side publication update; the
	// operator should be deliberate. Dry-run skips the prompt — it
	// doesn't modify anything.
	if !s.Yes && !s.DryRun {
		ok, err := confirmTypedDestructive(os.Stdin, os.Stdout,
			fmt.Sprintf("This will create table %q on the target, bulk-copy its rows, and extend the source publication. Type %q to confirm: ", s.Table, s.Table),
			s.Table)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "aborted")
			return nil
		}
	}

	add := &pipeline.AddTable{
		Source:             source,
		Target:             target,
		SourceDSN:          s.Source,
		TargetDSN:          s.Target,
		StreamID:           s.StreamID,
		TableName:          s.Table,
		Mappings:           mappings,
		ExpressionMappings: exprMappings,
		SlotName:           s.SlotName,
		DryRun:             s.DryRun,
	}
	return add.Run(kongContext())
}
