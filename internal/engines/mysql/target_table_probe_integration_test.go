//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_TargetTableExists is the MySQL half of the unmapped-table
// preflight's independent binding (audit backlog C-11).
//
// MySQL never had Postgres's silent skip — an unresolvable table failed the
// write and halted the stream — so the preflight buys timing here rather than
// correctness: the complete list at start instead of the first offending table
// hours in. The probe still has to be right, and the failure direction that
// matters is the opposite of Postgres's: a probe that wrongly answers "absent"
// would refuse a stream MySQL would have applied perfectly.
//
// The absent case also pins that a missing table is distinguished
// STRUCTURALLY. information_schema answers zero rows rather than raising errno
// 1146, so loadTableSchema synthesises the condition — and it now wraps a typed
// sentinel rather than leaving callers to match its message (audit backlog
// C-1).
func TestChangeApplier_TargetTableExists(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE present (id BIGINT NOT NULL PRIMARY KEY, v TEXT)
			ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	probe, ok := applier.(ir.TargetTableProbe)
	if !ok {
		t.Fatal("mysql ChangeApplier no longer implements ir.TargetTableProbe — " +
			"the unmapped-table preflight silently stops running for this engine")
	}

	// The applier resolves its own target database; passing "" as the schema
	// is the shape the dispatch path uses for a single-database stream.
	for _, tc := range []struct {
		name  string
		table string
		want  bool
	}{
		{"a table that exists", "present", true},
		{"a table that does NOT exist", "absent", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := probe.TargetTableExists(ctx, "", tc.table)
			if err != nil {
				t.Fatalf("TargetTableExists(%q): %v", tc.table, err)
			}
			if got != tc.want {
				t.Errorf("TargetTableExists(%q) = %v; want %v", tc.table, got, tc.want)
			}
		})
	}
}
