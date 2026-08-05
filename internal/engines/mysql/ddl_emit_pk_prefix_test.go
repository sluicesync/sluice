// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL half of roadmap item 120's sibling sweep.
//
// Item 120 is a SILENT constraint weakening on a SQLite target: its table-level
// PRIMARY KEY clause dropped ir.IndexColumn.Length, so `PRIMARY KEY (email(20),
// id)` became `PRIMARY KEY (email, id)` and the target admitted rows the source
// rejects. Closing it means enumerating the other engines that render a PK
// column list and saying, for each, fixed-or-exempt-with-a-reason:
//
//	sqlite     FIXED — refused at emitTableDef + PreflightIndexes
//	postgres   EXEMPT — emitIndexColumnList(..., enforcesUniqueness=true) already
//	           refuses it (pinned in index_prefix_length_test.go)
//	pgtrigger  EXEMPT — its schema writer IS postgres's (Engine.OpenSchemaWriter
//	           and PreflightIndexes both delegate)
//	mysql      EXEMPT — this file. The reason cited is a fact about the TARGET
//	           ("MySQL supports prefix lengths natively"), so it gets a test
//	           rather than a sentence.
//
// The exemption is only as good as the rendering: if emitIndexColumnList ever
// stopped emitting the prefix, MySQL→MySQL would acquire the same silent
// widening with nothing to catch it — the refusal lives in the OTHER engines.
package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestEmitCreateTablePrimaryKeyKeepsThePrefixLength(t *testing.T) {
	tbl := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email", Length: 20}, {Column: "id"}},
		},
	}
	ddl, err := emitTableDef(tbl)
	if err != nil {
		t.Fatalf("emitTableDef refused a prefixed PRIMARY KEY, which MySQL accepts: %v", err)
	}
	if !strings.Contains(ddl, "PRIMARY KEY (`email`(20), `id`)") {
		t.Fatalf("the PK prefix did not survive to the DDL — a MySQL target would then admit two rows "+
			"sharing the first 20 characters of email, which the source forbids (roadmap item 120's "+
			"defect, on this engine).\ngot: %s", ddl)
	}
}
