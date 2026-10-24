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
// the stream must be drained first (and the add checks that it is: it
// refuses DRAINED-ADD-TABLE-STREAM-NOT-STOPPED unless the stream's control
// row records a stop), and the source/target must be the same engine pair
// the active stream uses. Phase 2 (`--no-drain`, ADR-0030) lifted the drain
// precondition and is WITHDRAWN on every source: each live path lost the
// changes made to the table during the add, the Postgres one last (Bug
// 332). The flag stays and refuses (NO-DRAIN-POSTGRES-SOURCE,
// NO-DRAIN-BINLOG-SOURCE, NO-DRAIN-VSTREAM-SOURCE). See
// `docs/dev/design/mid-stream-add-table.md` and ADR-0030.
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

	NoDrain bool `help:"WITHDRAWN on every source (Bug 332). It was the Phase 2 live add: add-table against a stream that is still running. It stays so a script that passes it gets a loud refusal, before anything is read or written, naming the drained workflow. Every live path it took lost the changes made to the new table during the add: a Postgres source (NO-DRAIN-POSTGRES-SOURCE; the running stream dropped them until it admitted the table, or applied them while the copy ran so the copy undid deletes and updates; the strict zero-loss claimed since v0.32.0 held for inserts alone), a MySQL or MariaDB binlog source (NO-DRAIN-BINLOG-SOURCE) and a PlanetScale or Vitess (VStream) source (NO-DRAIN-VSTREAM-SOURCE); the trigger-CDC sources never had one. The drained workflow: sync stop --wait, then add-table, then restart the stream (sync start with the same --stream-id, or the fleet for a sync run leg, which restarts from its config). Keep the stream stopped, with no fleet reload and no supervisor or systemd restart, until add-table returns: add-table does not detect a running stream." name:"no-drain"`

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
	InjectShardColumn string `help:"Re-pass the Shape A discriminator column NAME=VALUE if this stream was started with --inject-shard-column on 'sync start'. Currently refuses loudly: Shape A add-table mid-stream is Phase 2 per ADR-0048 DP-3 — use the drained model: 'sync stop --wait' -> schema migrate (including add-table) -> 'sync start' re-run with the same --stream-id (a restart warm-resumes from the persisted position; there is no resume flag)." placeholder:"NAME=VALUE"`

	AcceptTargetOnlyUniqueKey []string `name:"accept-target-only-unique-key" placeholder:"TABLE.KEY=FINGERPRINT" help:"Accept a unique key a PRE-CREATED target table carries beyond the source's, as the SOURCE-UNIQUE-KEY-ABSENT refusal prints it (repeatable): the fingerprint is a hash of the key's definition as the target engine reports it, with the catalog row of every column the key depends on, so any later change to either refuses again until re-accepted (see docs/operator/cdc-streaming.md for exactly what is and is not hashed). Without it, a target unique key no source key implies refuses with SOURCE-UNIQUE-KEY-ABSENT before the table is copied: every insert reaches the target as an upsert that resolves a conflict through any unique key the target has, so the source's next duplicate of such a key would overwrite a different row. Pass the same acceptance to every later 'sync start' of the stream."`

	Abandon bool `help:"Retire an add-table instead of adding the table. For a pending one (a failed or killed add-table, which 'sync start' refuses on: INCLUDE-TABLE-SCOPE-DRIFT), it marks the stream's pending record for TABLE ABANDONED (it does not remove it), drops the dead add's inactive sluice_addtable_<table> snapshot slot, and on a Postgres source takes TABLE out of the publication unless the stream established it or another sluice slot exists on the same database. For a completed one, it takes TABLE out of the stream's live-added set. Refuses while the add-table's heartbeat is fresh (it is still running). Then RESTART THE STREAM: 'sync health' exits 1 (ADD-TABLE ABANDONED) until a start finds TABLE out of scope or not applied and clears the record; any other start refuses. The target table is left as it is and may hold a partial copy."`

	DryRun bool `help:"Print the plan (which table, source publication update, target DDL summary) without modifying the source publication, target schema, or capturing a snapshot." short:"n"`
	Yes    bool `help:"Confirm the add. Required when stdin is not a terminal (scripts, CI, agents): without it the command refuses with SLUICE-E-CONFIRMATION-REQUIRED instead of prompting. On a terminal it skips the typed table-name prompt." short:"y"`

	ControlKeyspace string `name:"control-keyspace" help:"MySQL/PlanetScale/Vitess target only: the unsharded sidecar keyspace the stream's control tables live in (see 'sync start --control-keyspace'). Omit to auto-detect on a sharded target; must match what the stream was started with. Empty + unsharded/non-Vitess target = the default keyspace." placeholder:"KEYSPACE"`
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
	if source, err = applySourceEngineOptions(source, g); err != nil {
		return err
	}
	if target, err = applyEngineOptions(target, g); err != nil {
		return err
	}
	// Sidecar control keyspace (explicit flag or auto-detect) so the
	// live-add's control-table reads/writes hit the same qualified tables the
	// running stream uses. Inert on non-MySQL / unsharded targets (the
	// ADR-0118 inert-flag WARN names the non-MySQL case).
	warnInertFlags(kongContext(), "schema add-table", s.SourceDriver, s.TargetDriver)
	if target, err = applyControlKeyspace(kongContext(), target, s.ControlKeyspace, s.Target); err != nil {
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
				"resume each shard by re-running 'sluice sync start --inject-shard-column " +
				"NAME=VALUE --stream-id <id>' (a restart warm-resumes from that stream's " +
				"persisted position; there is no resume flag). See ADR-0048 §4 'Cross-shard schema-" +
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
	// doesn't modify anything. Same door as every other prompt site:
	// no terminal on stdin, no prompt — a coded refusal instead.
	if !s.Yes && !s.DryRun {
		if err := refuseUnlessTerminal(fmt.Sprintf("adding table %q to the stream", s.Table)); err != nil {
			return err
		}
		prompt := fmt.Sprintf("This will create table %q on the target, bulk-copy its rows, and extend the source publication. Type %q to confirm: ", s.Table, s.Table)
		if s.Abandon {
			prompt = fmt.Sprintf("This will mark the pending add-table record for %q ABANDONED (or, for a completed add, take it out of the stream's live-added set), drop its inactive snapshot slot, and may take it out of the source publication. Its target table may hold a partial copy. Restart the stream afterwards. Type %q to confirm: ", s.Table, s.Table)
		}
		ok, err := confirmTypedDestructive(kongContext(), os.Stdin, os.Stdout, prompt, s.Table)
		if err != nil {
			return err
		}
		if !ok {
			// Same declined sentinel as the --reset-target-data sites
			// (wave 4b abort contract): a decline is a non-zero exit,
			// never a silent success. The command has no --format flag,
			// so the sentinel's text rendering is the whole surface.
			return errConfirmDeclined
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
		Abandon:            s.Abandon,

		AcceptTargetOnlyUniqueKeys: s.AcceptTargetOnlyUniqueKey,
	}
	return add.Run(kongContext())
}
