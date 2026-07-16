// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/appliershared"
)

// TestReadSchema_ExcludesControlTableRoster pins the roster at the
// mydumper door (audit-2026-07-15 MED-D0-6): a dump of a promoted
// ex-target carries EVERY sluice control table alongside the user tables,
// and enumerating them would let a later sync resume from a stale
// imported position. The exclusion mirrors the live mysql reader's and is
// LOGGED, never silent. Iterates the shared roster so a future control
// table is automatically covered.
func TestReadSchema_ExcludesControlTableRoster(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)
	writeDumpFile(t, dir, "shop.users-schema.sql", "CREATE TABLE `users` (`id` bigint NOT NULL);")
	writeDumpFile(t, dir, "shop.users.00000.sql", "INSERT INTO `users` VALUES (1);")
	for _, name := range appliershared.ControlTableNames() {
		writeDumpFile(t, dir, "shop."+name+"-schema.sql",
			"CREATE TABLE `"+name+"` (`id` bigint NOT NULL);")
		writeDumpFile(t, dir, "shop."+name+".00000.sql",
			"INSERT INTO `"+name+"` VALUES (1);")
	}

	sr, err := Engine{}.OpenSchemaReader(context.Background(), dir)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	logs := captureSlog(t)
	schema, err := sr.ReadSchema(context.Background())
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Tables) != 1 || schema.Tables[0].Name != "users" {
		got := make([]string, 0, len(schema.Tables))
		for _, tbl := range schema.Tables {
			got = append(got, tbl.Name)
		}
		t.Fatalf("tables = %v; want exactly [users] — a control table leaked into user-table enumeration", got)
	}
	if out := logs.String(); !strings.Contains(out, "excluded sluice control tables") ||
		!strings.Contains(out, "engine=mydumper") {
		t.Fatalf("the exclusion must be logged, never silent:\n%s", out)
	}
}
