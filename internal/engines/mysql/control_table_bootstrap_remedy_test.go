// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestControlTableBootstrapRemedy_AlterNamesTheAlter pins the remedy the
// safe-migrations refusal carries: `sluice control-tables ddl` prints CREATE
// TABLE IF NOT EXISTS only, which cannot bring an existing table up to date,
// so a refused ALTER must name the ALTER itself (shipped with `sluice
// deploy-ddl`), and only a refused CREATE may point at `control-tables ddl`.
func TestControlTableBootstrapRemedy_AlterNamesTheAlter(t *testing.T) {
	refused := &gomysql.MySQLError{Number: 1105, Message: "direct DDL is disabled"}

	alter := "ALTER TABLE sluice_migrate_state\n  ADD COLUMN source_identity TEXT NULL"
	err := wrapControlTableBootstrapError(refused, alter)
	if !errors.Is(err, ir.ErrControlTableDDLRequired) {
		t.Fatalf("the refusal does not carry ir.ErrControlTableDDLRequired: %v", err)
	}
	remedy := ir.ControlTableDDLRemedy(err)
	if !strings.Contains(remedy, "sluice deploy-ddl --ddl 'ALTER TABLE sluice_migrate_state ADD COLUMN source_identity TEXT NULL'") ||
		!strings.Contains(remedy, "CREATE statements only") {
		t.Errorf("ALTER remedy = %q; want the one-line ALTER shipped with deploy-ddl and the control-tables caveat", remedy)
	}
	if strings.Contains(err.Error(), "prints the full control-table set") {
		t.Errorf("an ALTER refusal still sends the operator to the CREATE set: %v", err)
	}

	create := "CREATE TABLE IF NOT EXISTS `sluice_migrate_state` (migration_id VARCHAR(255))"
	err = wrapControlTableBootstrapError(refused, create)
	if remedy := ir.ControlTableDDLRemedy(err); !strings.Contains(remedy, "`sluice control-tables ddl`") ||
		strings.Contains(remedy, "CREATE statements only") {
		t.Errorf("CREATE remedy = %q; want the control-tables set", remedy)
	}

	if err := wrapControlTableBootstrapError(errors.New("other"), alter); errors.Is(err, ir.ErrControlTableDDLRequired) {
		t.Error("a non-safe-migrations error was marked")
	}
}
