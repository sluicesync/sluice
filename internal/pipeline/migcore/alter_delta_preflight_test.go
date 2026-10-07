// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestApplyAlterDelta_RefusedAspectEmitsNoDDLForTheOthers: a delta that adds
// a column (applied) AND changes the primary key (refused) must refuse
// before emitting the ADD COLUMN — otherwise the refusal leaves the target
// half-altered. The same check is PreflightAlterDelta, which chain restore
// and verify now run over every delta before writing anything (Bug 297
// review).
func TestApplyAlterDelta_RefusedAspectEmitsNoDDLForTheOthers(t *testing.T) {
	before := baseTable()
	after := cloneTable(before)
	after.Columns = append(after.Columns, &ir.Column{Name: "note", Type: ir.Varchar{Length: 32}, Nullable: true})
	after.PrimaryKey = &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}, {Column: "amount"}}}
	d := &irbackup.SchemaDeltaEntry{Kind: irbackup.SchemaDeltaAlterTable, Table: before.Name, Before: before, After: after}
	ac := AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: "chain restore", BackupID: "i1"}

	w := &recordingSchemaWriter{}
	err := ApplyAlterDelta(context.Background(), w, d, ac)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSchemaDeltaUnsupported {
		t.Fatalf("ApplyAlterDelta: err = %v; want %s", err, sluicecode.CodeBackupSchemaDeltaUnsupported)
	}
	if n := w.calls(); n != 0 {
		t.Errorf("ApplyAlterDelta emitted %d DDL call(s) (added columns %+v) before refusing the primary-key aspect", n, w.addedColumns)
	}
	if err := PreflightAlterDelta(d, ac); err == nil || !strings.Contains(err.Error(), "primary-key") {
		t.Errorf("PreflightAlterDelta = %v; want the primary-key refusal", err)
	}
}
