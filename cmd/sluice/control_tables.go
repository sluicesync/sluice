// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"strings"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ControlTablesCmd groups operations on sluice's own control tables
// (ADR-0165). Today that is the bootstrap DDL printer; future
// control-table tooling (roadmap item 65) slots in here.
type ControlTablesCmd struct {
	DDL ControlTablesDDLCmd `cmd:"" help:"Print the exact statements that create sluice's control tables (migrate-state + cdc-state), for bootstrapping a target that refuses direct DDL or a Postgres target whose sync role holds only DML."`
}

// ControlTablesDDLCmd implements `sluice control-tables ddl`: print
// the control-table CREATE statements, single-sourced from the
// engine's own definitions, so an operator can ship them through a
// governed channel — the PlanetScale safe-migrations bootstrap
// (`sluice deploy-ddl --ddl '<statement>'` per statement). A separate
// read-only printer rather than a deploy-ddl flag: it needs no
// credentials, no org/database, and its output composes with any
// channel (deploy-ddl, the pscale UI, a reviewed migration file).
type ControlTablesDDLCmd struct {
	Engine string `help:"Engine whose control-table dialect to print. The default bootstrap consumer is PlanetScale (safe migrations blocks direct DDL); mysql/mariadb/vitess print the same dialect (one CREATE per table). postgres (and postgres-trigger, whose target-side tables are Postgres's) prints the Postgres set — each table's CREATE plus the ADD COLUMN, CREATE INDEX and UTC-DEFAULT statements later releases added — for an owner to run so a DML-only sync role can start. Other engines are refused by name." default:"planetscale" placeholder:"NAME"`
}

// controlTableDDLEngines lists the registered engines that publish their
// control-table DDL, derived from the registry so the refusal cannot name a
// stale set.
func controlTableDDLEngines() []string {
	var out []string
	for _, name := range engines.Names() {
		if e, ok := engines.Get(name); ok {
			if _, ok := e.(ir.ControlTableDDLProvider); ok {
				out = append(out, name)
			}
		}
	}
	return out
}

// Run implements `sluice control-tables ddl`. Output is pure SQL plus
// `--` comment lines, so it can be pasted or piped as-is.
func (c *ControlTablesDDLCmd) Run() error {
	engine, err := resolveEngine(c.Engine)
	if err != nil {
		return err
	}
	provider, ok := engine.(ir.ControlTableDDLProvider)
	if !ok {
		return fmt.Errorf("control-tables ddl: engine %q does not publish its control-table DDL (supported: %s)",
			engine.Name(), strings.Join(controlTableDDLEngines(), ", "))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "-- sluice control tables (%s dialect) — the migrate-state + cdc-state set\n", engine.Name())
	for _, line := range provider.ControlTableDDLGuidance() {
		fmt.Fprintf(&b, "-- %s\n", line)
	}
	for _, stmt := range provider.ControlTableDDL() {
		fmt.Fprintf(&b, "\n-- %s\n%s;\n", stmt.Table, stmt.DDL)
	}
	_, err = fmt.Fprint(os.Stdout, b.String())
	return err
}
