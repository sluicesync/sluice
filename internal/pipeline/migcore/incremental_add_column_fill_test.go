// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestApplyAlterDelta_NamesAnUnreproducibleAddColumnFill pins the replay
// side of GC-36 (2) on every DEFAULT family a recorded ADD COLUMN can
// carry: a constant (none, literal, quoted/numeric/boolean expression)
// replays the source's fill and stays quiet; a time, session, random,
// sequence or unknown-function DEFAULT is named under
// [AddColumnFillNotReproducibleMarker], on both replay origins. The real-
// server half is TestIncrementalBackup_ChainRestore_AddColumnDefaults.
func TestApplyAlterDelta_NamesAnUnreproducibleAddColumnFill(t *testing.T) {
	for _, tc := range []struct {
		name string
		def  ir.DefaultValue
		warn bool
	}{
		{"nil", nil, false},
		{"none", ir.DefaultNone{}, false},
		{"literal", ir.DefaultLiteral{Value: "abc"}, false},
		{"quoted-expression", ir.DefaultExpression{Expr: "'abc'::text"}, false},
		{"numeric-expression", ir.DefaultExpression{Expr: "1.10"}, false},
		{"boolean-expression", ir.DefaultExpression{Expr: "true"}, false},
		{"pg-now", ir.DefaultExpression{Expr: "now()"}, true},
		{"current-timestamp-keyword", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP"}, true},
		{"mysql-current-timestamp-6", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"}, true},
		{"clock-timestamp", ir.DefaultExpression{Expr: "clock_timestamp()"}, true},
		{"random-uuid", ir.DefaultExpression{Expr: "gen_random_uuid()"}, true},
		{"session", ir.DefaultExpression{Expr: "CURRENT_USER"}, true},
		{"sequence", ir.DefaultExpression{Expr: "nextval('s'::regclass)"}, true},
		{"unknown-function", ir.DefaultExpression{Expr: "my_func()"}, true},
	} {
		for _, origin := range []string{"chain restore", "broker"} {
			t.Run(tc.name+"/"+origin, func(t *testing.T) {
				before := baseTable()
				after := cloneTable(before)
				after.Columns = append(after.Columns, &ir.Column{Name: "filled", Type: ir.Varchar{Length: 32}, Nullable: true, Default: tc.def})

				buf := &logcapture.Buffer{}
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
				defer slog.SetDefault(prev)

				w := &recordingSchemaWriter{}
				if err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
					Kind: irbackup.SchemaDeltaAlterTable, Table: before.Name, Before: before, After: after,
				}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: origin}); err != nil {
					t.Fatalf("ApplyAlterDelta: %v", err)
				}
				if len(w.addedColumns) != 1 {
					t.Fatalf("addedColumns = %+v; want [filled] — the WARN never refuses the column", w.addedColumns)
				}
				logs := buf.String()
				warned := strings.Contains(logs, AddColumnFillNotReproducibleMarker) &&
					strings.Contains(logs, `"column":"filled"`) &&
					strings.Contains(logs, fmt.Sprintf(`"msg":"%s: `, origin))
				if warned != tc.warn {
					t.Errorf("warned = %v; want %v (default %#v)\nlogs: %s", warned, tc.warn, tc.def, logs)
				}
			})
		}
	}
}
