//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
)

// TestSchemaReader_ExcludesControlTableRoster pins roadmap item 65b on
// the real engine: a database carrying EVERY sluice control table (the
// promoted ex-target / cutover shape) plus one user table enumerates
// only the user table. Iterates the shared roster rather than a
// hand-copied list so a table added to [appliershared.ControlTableNames]
// is automatically covered here.
func TestSchemaReader_ExcludesControlTableRoster(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	var ddl strings.Builder
	for _, name := range appliershared.ControlTableNames() {
		fmt.Fprintf(&ddl, "CREATE TABLE `%s` (id INT PRIMARY KEY);\n", name)
	}
	ddl.WriteString("CREATE TABLE roster_user_table (id INT PRIMARY KEY);")
	applyDDL(t, dsn, ddl.String())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := r.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Tables) != 1 || schema.Tables[0].Name != "roster_user_table" {
		var got []string
		for _, tbl := range schema.Tables {
			got = append(got, tbl.Name)
		}
		t.Fatalf("tables = %v; want exactly [roster_user_table] — a sluice control table leaked into user-table enumeration", got)
	}
}
