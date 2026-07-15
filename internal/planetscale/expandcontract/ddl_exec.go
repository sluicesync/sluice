// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package expandcontract

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
)

// execBranchDDL is the real ExecDDL: apply one verbatim DDL statement
// to a PlanetScale dev branch over a direct MySQL connection, using
// the branch password the orchestrator just minted. PlanetScale
// permits direct DDL on a dev branch (safe migrations gate only the
// production branch), which is exactly why the expand/contract DDL is
// applied HERE and shipped to production via a deploy request.
//
// This is a deliberate, contained go-sql-driver use outside the mysql
// engine package: the connection is to a control-plane-minted
// credential/host pair (api.BranchPassword), not an operator DSN, so
// none of the engine's DSN normalization/flavor machinery applies.
// The engine-neutral pipeline never sees this package.
func execBranchDDL(ctx context.Context, pw *api.BranchPassword, database, ddl string) error {
	if pw == nil || pw.AccessHostURL == "" {
		return errors.New("expand-contract: branch password carries no access host")
	}
	cfg := gomysql.NewConfig()
	cfg.User = pw.Username
	cfg.Passwd = pw.PlainText
	cfg.Net = "tcp"
	cfg.Addr = pw.AccessHostURL // port 3306 default; PlanetScale hosts require TLS
	cfg.DBName = database
	cfg.TLSConfig = "true"

	connector, err := gomysql.NewConnector(cfg)
	if err != nil {
		return fmt.Errorf("expand-contract: build branch connection: %w", err)
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		// The DDL text is the operator's own --expand-ddl/--contract-ddl;
		// echoing it back is safe and is the fastest path to a fix.
		return fmt.Errorf("expand-contract: branch DDL failed (%s): %w", ddl, err)
	}
	return nil
}

// ensureStateOnBranch creates sluice's migrate-state control tables on
// the DEV branch (where PlanetScale permits direct DDL) so they ship
// to production inside the expand deploy request. On a safe-migrations
// production branch — the expand-contract PREREQUISITE — direct DDL is
// refused (Error 1105 "direct DDL is disabled"), which blocks the
// ADR-0159 backfill's own control-table bootstrap; staging the tables
// on the branch is the PlanetScale-shaped channel for them
// (live-caught 2026-07-15: the migrate leg could never start on a
// real safe-migrations branch without this).
//
// The engine's own state store does the creating, so the schema stays
// single-sourced (incl. the ADR-0082 state_format shape); the store
// opens against a DSN built from the just-minted branch password.
func ensureStateOnBranch(ctx context.Context, engine ir.Engine, pw *api.BranchPassword, database string) error {
	if pw == nil || pw.AccessHostURL == "" {
		return errors.New("expand-contract: branch password carries no access host")
	}
	opener, ok := engine.(ir.MigrationStateStoreOpener)
	if !ok {
		return fmt.Errorf("expand-contract: engine %q does not implement ir.MigrationStateStoreOpener", engine.Name())
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?tls=true",
		pw.Username, pw.PlainText, pw.AccessHostURL, database)
	store, err := opener.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		return fmt.Errorf("expand-contract: open migrate-state store on dev branch: %w", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		return fmt.Errorf("expand-contract: stage migrate-state tables on dev branch: %w", err)
	}
	return nil
}
