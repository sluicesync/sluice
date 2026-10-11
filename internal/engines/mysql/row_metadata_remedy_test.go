// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestRowMetadataFullRemedy_SpelledForTheFlavor pins every refusal that tells
// an operator to set binlog_row_metadata=FULL to a statement the source can
// run: MariaDB has no SET PERSIST (it is a syntax error there), so a MariaDB
// reader names SET GLOBAL plus the option-file line, and a MySQL reader names
// SET PERSIST. Both binlog flavors × both hints that carry the setting.
func TestRowMetadataFullRemedy_SpelledForTheFlavor(t *testing.T) {
	hints := map[string]func(Flavor) string{
		"catalog-ahead refusal": func(f Flavor) string {
			ce, ok := sluicecode.FromError(errCDCCatalogAheadOfPosition(&tableSchema{Schema: "app", Name: "t"}, aheadDDL{stmt: "ALTER TABLE t ADD c INT"}, f))
			if !ok {
				t.Fatal("errCDCCatalogAheadOfPosition is not a coded error")
			}
			return ce.Hint
		},
		"ENUM-LABEL-NOT-RECOVERABLE": enumLabelRemedyBinlog,
	}
	for name, hint := range hints {
		mysql, maria := hint(FlavorVanilla), hint(FlavorMariaDB)
		if !strings.Contains(mysql, "SET PERSIST binlog_row_metadata = 'FULL'") {
			t.Errorf("%s, MySQL: %q; want SET PERSIST", name, mysql)
		}
		for _, want := range []string{"SET GLOBAL binlog_row_metadata = 'FULL'", "[mariadbd]", "option file", "has no SET PERSIST"} {
			if !strings.Contains(maria, want) {
				t.Errorf("%s, MariaDB: %q; want it to name %q", name, maria, want)
			}
		}
		if strings.Contains(maria, "SET PERSIST binlog_row_metadata") {
			t.Errorf("%s, MariaDB: %q tells the operator to run SET PERSIST, which MariaDB lacks", name, maria)
		}
	}
}
