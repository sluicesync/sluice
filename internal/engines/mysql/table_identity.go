// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.TableIdentityReader = Engine{}

// TableIdentities implements [ir.TableIdentityReader]: each named table's
// InnoDB TABLE_ID in the DSN's database (information_schema.INNODB_TABLES on
// MySQL 8, INNODB_SYS_TABLES on 5.7 and MariaDB). RENAME TABLE keeps the id
// (measured by TestTableIdentities_SurviveRename); a drop and re-create, and
// an ALTER that rebuilds the table, give a new one — the latter only costs a
// rename made in the same window an acknowledgement
// (--accept-table-rename). InnoDB stores a name in its filename encoding
// (`@002d` for '-', …), so a table whose name needs encoding is not matched
// and has no id: the rename refuses and names the flag rather than guess.
//
// The PlanetScale and Vitess flavors return no ids: vtgate does not expose
// the InnoDB dictionary per keyspace, so a rename there is acknowledged with
// the flag.
func (e Engine) TableIdentities(ctx context.Context, dsn string, tables []string) (map[string]string, error) {
	out := map[string]string{}
	if len(tables) == 0 || e.Flavor == FlavorPlanetScale || e.Flavor == FlavorVitess {
		return out, nil
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var database string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
		return nil, fmt.Errorf("mysql: read table identities: %w", err)
	}
	ids, err := innodbTableIDs(ctx, db, "INNODB_TABLES", database)
	if err != nil {
		if ids, err = innodbTableIDs(ctx, db, "INNODB_SYS_TABLES", database); err != nil {
			return nil, fmt.Errorf("mysql: read table identities: %w", err)
		}
	}
	for _, t := range tables {
		if id, ok := ids[t]; ok {
			out[t] = id
		}
	}
	return out, nil
}

func innodbTableIDs(ctx context.Context, db *sql.DB, view, database string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT NAME, CAST(TABLE_ID AS CHAR) FROM information_schema."+view+" WHERE NAME LIKE ?",
		strings.ReplaceAll(strings.ReplaceAll(database, `\`, `\\`), "_", `\_`)+"/%")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	prefix := database + "/"
	for rows.Next() {
		var name, id string
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		if strings.HasPrefix(name, prefix) {
			out[strings.TrimPrefix(name, prefix)] = id
		}
	}
	return out, rows.Err()
}
