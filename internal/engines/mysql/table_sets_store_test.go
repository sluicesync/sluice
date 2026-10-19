// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// releasedControlTableShape is sluice_cdc_state's table-set columns as
// v0.27.0 through v0.162.0 left them: live_added_tables TEXT in the table's
// default character set, and none of the record columns.
var releasedControlTableShape = []tableSetColumnShape{{name: "live_added_tables", typ: "text", charset: "latin1"}}

// TestTableSetColumnsStatement_BuiltFromTheShape pins R9e: the refusal's
// statement ADDs only missing columns and MODIFYs only refused ones. The
// fixed form ADDed live_added_tables too, which fails with 1060 on every
// control table a released binary created.
func TestTableSetColumnsStatement_BuiltFromTheShape(t *testing.T) {
	const ref = "`sluice_cdc_state`"
	longtext := func(name string) tableSetColumnShape { return tableSetColumnShape{name: name, typ: "longtext"} }
	for _, tc := range []struct {
		name   string
		shapes []tableSetColumnShape
		want   string
	}{
		{
			"released shape: ADD the record columns, MODIFY live_added_tables", releasedControlTableShape,
			"ALTER TABLE `sluice_cdc_state` ADD COLUMN `pending_added_tables` LONGTEXT NULL, ADD COLUMN `established_tables` LONGTEXT NULL, " +
				"ADD COLUMN `dropped_tables` LONGTEXT NULL, MODIFY COLUMN `live_added_tables` LONGTEXT CHARACTER SET utf8mb4 NULL",
		},
		{
			"no live_added_tables: ADD all four", nil,
			"ALTER TABLE `sluice_cdc_state` ADD COLUMN `pending_added_tables` LONGTEXT NULL, ADD COLUMN `established_tables` LONGTEXT NULL, " +
				"ADD COLUMN `dropped_tables` LONGTEXT NULL, ADD COLUMN `live_added_tables` LONGTEXT CHARACTER SET utf8mb4 NULL",
		},
		{
			"live_added_tables LONGTEXT but utf8mb3: MODIFY it",
			[]tableSetColumnShape{
				longtext("pending_added_tables"), longtext("established_tables"), longtext("dropped_tables"),
				{name: "live_added_tables", typ: "longtext", charset: "utf8mb3"},
			},
			"ALTER TABLE `sluice_cdc_state` MODIFY COLUMN `live_added_tables` LONGTEXT CHARACTER SET utf8mb4 NULL",
		},
		{
			"a TEXT record column: MODIFY it",
			[]tableSetColumnShape{
				longtext("pending_added_tables"),
				{name: "established_tables", typ: "text"},
				longtext("dropped_tables"),
				{name: "live_added_tables", typ: "longtext", charset: "utf8mb4"},
			},
			"ALTER TABLE `sluice_cdc_state` MODIFY COLUMN `established_tables` LONGTEXT NULL",
		},
		{
			"up to date: nothing",
			[]tableSetColumnShape{
				longtext("pending_added_tables"), longtext("established_tables"), longtext("dropped_tables"),
				{name: "live_added_tables", typ: "LONGTEXT", charset: "UTF8MB4"},
			},
			"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tableSetColumnsStatement(ref, tc.shapes); got != tc.want {
				t.Fatalf("statement:\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// TestTableSetColumnsStatement_DocsGiveTheReleasedShapeStatement holds the
// upgrade docs to the statement the code builds for the shape every released
// control table has (R9e: they gave the ADD-all form, which fails there).
func TestTableSetColumnsStatement_DocsGiveTheReleasedShapeStatement(t *testing.T) {
	want := tableSetColumnsStatement("`sluice_cdc_state`", releasedControlTableShape)
	root := filepath.Join("..", "..", "..")
	for _, doc := range []string{
		"docs/managed-services.md", "docs/schema-change-runbook.md", "skills/planetscale-migration/SKILL.md",
	} {
		body, err := os.ReadFile(filepath.Join(root, doc))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), want) {
			t.Errorf("%s does not give the upgrade statement for a released control table:\n%s", doc, want)
		}
	}
}
