// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// # Control-table bootstrap on safe-migrations targets (ADR-0165)
//
// A PlanetScale branch with safe migrations enabled refuses EVERY
// direct DDL statement (Error 1105 "direct DDL is disabled"), table-
// exists or not — including the CREATEs and column-migration ALTERs
// for sluice's own control tables. The ensure paths are detect-first
// so they issue no DDL when the tables are already current; when DDL
// is genuinely needed, the refusal here classifies it into the coded
// SLUICE-E-PS-DIRECT-DDL-BLOCKED refusal naming the governed
// bootstrap channel: `sluice control-tables ddl` prints the exact
// statements (single-sourced from this engine via
// [Engine.ControlTableDDL]), `sluice deploy-ddl` ships each one via a
// PlanetScale deploy request.

// isDirectDDLDisabledErr reports whether err is the PlanetScale/Vitess
// safe-migrations refusal: Error 1105 with "direct DDL is disabled".
// 1105 is Vitess's generic wrap code, so the message text is part of
// the match (same posture as [wrapDDLError]).
func isDirectDDLDisabledErr(err error) bool {
	var mysqlErr *gomysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1105 &&
		strings.Contains(mysqlErr.Message, "direct DDL is disabled")
}

// wrapControlTableBootstrapError classifies a control-table DDL
// failure: the safe-migrations refusal becomes the coded
// SLUICE-E-PS-DIRECT-DDL-BLOCKED refusal carrying the exact statement
// to ship (deploy-ddl-pasteable, whitespace-collapsed) plus the
// bootstrap recipe; every other error passes through unchanged so the
// call site's own wrap stays outermost. [ErrSafeMigrationsBlocked]
// stays in the chain for errors.Is callers (the [wrapDDLError]
// contract).
func wrapControlTableBootstrapError(err error, statement string) error {
	if !isDirectDDLDisabledErr(err) {
		return err
	}
	oneLine := strings.Join(strings.Fields(statement), " ")
	return sluicecode.Wrap(
		sluicecode.CodePSDirectDDLBlocked,
		"ship sluice's control-table DDL through a PlanetScale deploy request: `sluice control-tables ddl` prints the statements, `sluice deploy-ddl --ddl '<statement>'` deploys each one; then re-run",
		fmt.Errorf("%w: %w | "+
			"sluice needs to run control-table DDL that this branch's safe-migrations setting refuses "+
			"(every direct DDL statement is blocked). Ship it through a deploy request instead: "+
			"`sluice control-tables ddl` prints the full control-table set and "+
			"`sluice deploy-ddl --ddl '<statement>'` deploys one statement; then re-run. "+
			"The statement that failed here is: %s",
			ErrSafeMigrationsBlocked, err, oneLine),
	)
}

// ControlTableDDL implements [ir.ControlTableDDLProvider]: the CREATE
// statements for sluice's migrate-state and cdc-state control tables,
// single-sourced from the same builders the Ensure* paths execute so
// the printed bootstrap DDL can never drift from what sluice would
// create. Unqualified names (no --control-keyspace sidecar): the
// bootstrap consumer is a PlanetScale database's default keyspace,
// where a deploy request lands the tables alongside the data.
func (e Engine) ControlTableDDL() []ir.ControlTableStatement {
	return []ir.ControlTableStatement{
		{Table: migrateStateTableName, DDL: migrateStateHeaderDDL()},
		{Table: migrateProgressTableName, DDL: migrateProgressDDL()},
		{Table: controlTableName, DDL: controlTableDDL("")},
		{Table: schemaHistoryTableName, DDL: schemaHistoryTableDDL("")},
		{Table: shardConsolidationLeaseTableName, DDL: shardConsolidationLeaseTableDDL("")},
	}
}
