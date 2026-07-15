// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// ControlTablesCmd groups operations on sluice's own control tables
// (ADR-0165). Today that is the bootstrap DDL printer; future
// control-table tooling (roadmap item 65) slots in here.
type ControlTablesCmd struct {
	DDL ControlTablesDDLCmd `cmd:"" help:"Print the exact CREATE statements for sluice's control tables (migrate-state + cdc-state), for bootstrapping a target that refuses direct DDL."`
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
	Engine string `help:"Engine whose control-table dialect to print. The bootstrap consumer is PlanetScale (safe migrations blocks direct DDL); mysql/vitess print the same dialect." default:"planetscale" placeholder:"NAME"`
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
		return fmt.Errorf("control-tables ddl: engine %q does not publish its control-table DDL (supported: the mysql family — mysql, planetscale, vitess)", engine.Name())
	}

	var b strings.Builder
	fmt.Fprintf(&b, "-- sluice control tables (%s dialect) — the migrate-state + cdc-state set\n", engine.Name())
	fmt.Fprintf(&b, "-- On a PlanetScale branch with safe migrations enabled, direct DDL is refused\n")
	fmt.Fprintf(&b, "-- (Error 1105), so ship each statement via a deploy request:\n")
	fmt.Fprintf(&b, "--   sluice deploy-ddl --org <org> --database <db> --ddl '<statement>'\n")
	for _, stmt := range provider.ControlTableDDL() {
		fmt.Fprintf(&b, "\n-- %s\n%s;\n", stmt.Table, stmt.DDL)
	}
	_, err = fmt.Fprint(os.Stdout, b.String())
	return err
}
