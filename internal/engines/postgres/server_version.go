// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// pgVersionFailoverSupport is the lowest server_version_num that
// accepts the FAILOVER option on the CREATE_REPLICATION_SLOT
// replication-protocol command. PG 17.0 = 170000. Anything below
// that number rejects FAILOVER as an unknown option, so sluice has
// to take the FAILOVER-less path on those servers and warn the
// operator that slot HA depends on Patroni / sync_replication_slots
// configuration outside the slot itself.
//
// server_version_num packs major/minor as MMmm00 (e.g. 17.2 →
// 170002), so a simple ">= 170000" check is the right gate even
// for future point releases.
const pgVersionFailoverSupport = 170000

// serverVersionNum returns the Postgres server's numeric version
// (server_version_num), e.g. 170002 for PG 17.2 or 160006 for PG
// 16.6. Used by the slot-creation path to decide whether to opt in
// to the FAILOVER flag (PG 17+ only).
//
// The query goes through the regular *sql.DB pool, not the
// replication connection — replication-mode connections only
// accept replication-protocol commands, not normal SQL.
func serverVersionNum(ctx context.Context, db *sql.DB) (int, error) {
	var s string
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&s); err != nil {
		return 0, fmt.Errorf("postgres: read server_version_num: %w", err)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("postgres: parse server_version_num %q: %w", s, err)
	}
	return n, nil
}
