// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/pipeline"
)

// SchemaAddTableCmd implements `sluice schema add-table` — the
// Phase 1 MVP of the mid-stream add-table feature
// (`docs/dev/design/mid-stream-add-table.md`). It brings a new
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
// Phase 1 (default) is intentionally narrow: one table per invocation,
// the stream must be drained first, and the source/target must be the
// same engine pair the active stream uses. Phase 2 (`--no-drain`,
// PG-only, ADR-0030) lifts the drain precondition for high-availability
// workloads. See `docs/dev/design/mid-stream-add-table.md` and ADR-0030
// for the design space and correctness story.
type SchemaAddTableCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	Table    string `arg:"" help:"Unqualified name of the new source table to add to the active stream's scope. The schema/database is inferred from --source." placeholder:"TABLE"`
	StreamID string `help:"Stream identifier; must match the active stream's id (run 'sluice sync status' to list)." required:"" placeholder:"ID"`

	TypeOverride []string `help:"Force a specific target type for a column on the new table (repeatable). Format: 'TABLE.COLUMN=TYPE'. CLI form of the YAML 'mappings:' config; for target-type options, use the YAML form." placeholder:"TABLE.COLUMN=TYPE" sep:"none"`
	ExprOverride []string `help:"Replace a generated column's body with operator-supplied target-dialect text (repeatable). Format: 'TABLE.COLUMN=EXPRESSION'. Emitted verbatim; ADR-0016 translator skips overridden columns." placeholder:"TABLE.COLUMN=EXPRESSION" sep:"none"`

	SlotName string `help:"Override the temporary replication-slot name used for the snapshot capture on engines with a slot concept (Postgres). Defaults to 'sluice_addtable_<table>'. Engines without slots ignore this flag." placeholder:"NAME"`

	NoDrain bool `help:"Phase 2 live add: run add-table against an actively-streaming sync without first running 'sync stop --wait'. PG-only in this release; MySQL sources still require the drained workflow. See ADR-0030 for the correctness story." name:"no-drain"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). Must match the active stream's --target-schema, or be omitted to inherit the recorded value (Bug 46 / ADR-0031). When the active stream was started with --target-schema=NAME, the new table lands in NAME (rather than 'public') so CDC events the active stream's applier routes to NAME.<table> arrive at a real table. Mismatch (operator-supplied flag differs from recorded) refuses loudly. MySQL operators use a different --target DSN database instead." placeholder:"NAME"`

	// ADR-0048 Shape A defensive refusal. Per DP-3, cross-shard
	// schema migration (including add-table mid-stream) is Phase 2;
	// v1 is the drained model. This flag exists so an operator on
	// a Shape A stream who tries `schema add-table` gets a loud
	// operator-actionable refusal instead of running the Phase 1
	// add-table path against a discriminator-aware target — which
	// would either silently drop the discriminator on new rows or
	// crash via Bug-80-shape regression on the read path. Defensive,
	// not exhaustive: forgetful operators who DON'T pass the flag
	// will still hit the underlying breakage. Persisting Shape A
	// state in sluice_cdc_state for automatic detection is the
	// follow-up. Task #8 / catalog backlog.
	InjectShardColumn string `help:"Re-pass the Shape A discriminator column NAME=VALUE if this stream was started with --inject-shard-column on 'sync start'. Currently refuses loudly: Shape A add-table mid-stream is Phase 2 per ADR-0048 DP-3 — use the drained model: 'sync stop --wait' -> schema migrate (including add-table) -> 'sync start --resume'." placeholder:"NAME=VALUE"`

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
	// Value-fidelity flags (task 2.5): add-table reads the source table and emits
	// target DDL, so the value-decode + sql_mode-emit policies apply.
	if source, err = applyEngineOptions(source, g); err != nil {
		return err
	}
	if target, err = applyEngineOptions(target, g); err != nil {
		return err
	}

	if s.Table == "" {
		return errors.New("table name is required (positional argument)")
	}
	if s.StreamID == "" {
		return errors.New("--stream-id is required")
	}

	// ADR-0048 Shape A defensive refusal (task #8). add-table
	// mid-stream on a Shape A stream is Phase 2 per DP-3; the v1
	// path is the drained model. Refuse loudly with operator-
	// actionable recovery hint when the operator re-passes the
	// shard-column flag.
	if strings.TrimSpace(s.InjectShardColumn) != "" {
		return fmt.Errorf(
			"add-table mid-stream on a Shape A stream is not supported in v1 — " +
				"ADR-0048 DP-3 resolved cross-shard schema migration to the drained " +
				"model (Phase 2 / live add-table is deferred). Recovery: stop the " +
				"sharded streams via 'sluice sync stop --wait --stream-id <id>' on " +
				"each shard; on the source side, evolve the table set as needed; " +
				"resume each shard via 'sluice sync start --resume --inject-shard-column " +
				"NAME=VALUE --stream-id <id>'. See ADR-0048 §4 'Cross-shard schema-" +
				"migration coordination' for the design rationale and the Phase 2 " +
				"surface that would lift this restriction",
		)
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
		LiveMode:           s.NoDrain,
		TargetSchema:       s.TargetSchema,
	}
	return add.Run(kongContext())
}
