// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package expandcontract

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gomysql "github.com/go-sql-driver/mysql"

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
