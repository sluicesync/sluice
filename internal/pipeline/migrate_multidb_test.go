// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestNewDatabaseFilterMutualExclusion mirrors the table-filter gate:
// both include and exclude is rejected at construction.
func TestNewDatabaseFilterMutualExclusion(t *testing.T) {
	_, err := NewDatabaseFilter([]string{"app_a"}, []string{"app_b"})
	if err == nil {
		t.Fatal("expected error for both include and exclude; got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("err = %v; want a mutual-exclusion message", err)
	}
}

func TestNewDatabaseFilterRejectsBadPattern(t *testing.T) {
	if _, err := NewDatabaseFilter([]string{"[unclosed"}, nil); err == nil {
		t.Errorf("expected error for malformed include pattern; got nil")
	}
	if _, err := NewDatabaseFilter(nil, []string{"[unclosed"}); err == nil {
		t.Errorf("expected error for malformed exclude pattern; got nil")
	}
}

func TestDatabaseFilterAllows(t *testing.T) {
	cases := []struct {
		name   string
		filter DatabaseFilter
		db     string
		want   bool
	}{
		{"empty passes all", DatabaseFilter{}, "anything", true},
		{"include literal hit", DatabaseFilter{Include: []string{"app_a"}}, "app_a", true},
		{"include literal miss", DatabaseFilter{Include: []string{"app_a"}}, "app_b", false},
		{"include glob hit", DatabaseFilter{Include: []string{"app_*"}}, "app_b", true},
		{"include glob miss", DatabaseFilter{Include: []string{"app_*"}}, "billing", false},
		{"exclude literal drop", DatabaseFilter{Exclude: []string{"scratch"}}, "scratch", false},
		{"exclude literal keep", DatabaseFilter{Exclude: []string{"scratch"}}, "app_a", true},
		{"exclude glob drop", DatabaseFilter{Exclude: []string{"tmp_*"}}, "tmp_99", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.filter.Allows(c.db); got != c.want {
				t.Errorf("Allows(%q) = %v; want %v", c.db, got, c.want)
			}
		})
	}
}

func TestDatabaseFilterIsEmpty(t *testing.T) {
	if !(DatabaseFilter{}).IsEmpty() {
		t.Error("zero DatabaseFilter should be empty")
	}
	if (DatabaseFilter{Include: []string{"x"}}).IsEmpty() {
		t.Error("include-populated filter should not be empty")
	}
	if (DatabaseFilter{Exclude: []string{"x"}}).IsEmpty() {
		t.Error("exclude-populated filter should not be empty")
	}
}

// TestMultiDatabaseModeDispatch verifies the flag combinations that
// engage the fan-out path. Back-compat (no flag) must stay single-DB.
func TestMultiDatabaseModeDispatch(t *testing.T) {
	cases := []struct {
		name string
		m    Migrator
		want bool
	}{
		{"no flags = single-db", Migrator{}, false},
		{"all-databases", Migrator{AllDatabases: true}, true},
		{"include-database", Migrator{DatabaseFilter: DatabaseFilter{Include: []string{"app_*"}}}, true},
		{"exclude-database", Migrator{DatabaseFilter: DatabaseFilter{Exclude: []string{"scratch"}}}, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.m.multiDatabaseMode(); got != c.want {
				t.Errorf("multiDatabaseMode() = %v; want %v", got, c.want)
			}
		})
	}
}

// TestValidateMultiDatabase pins the loud-failure preconditions.
func TestValidateMultiDatabase(t *testing.T) {
	cases := []struct {
		name    string
		m       Migrator
		wantErr string
	}{
		{
			name:    "all + include conflict",
			m:       Migrator{AllDatabases: true, DatabaseFilter: DatabaseFilter{Include: []string{"x"}}},
			wantErr: "mutually exclusive",
		},
		{
			name:    "target-schema conflict",
			m:       Migrator{AllDatabases: true, TargetSchema: "ns"},
			wantErr: "--target-schema is incompatible",
		},
		{
			name: "inject-shard conflict",
			m: Migrator{
				AllDatabases:      true,
				InjectShardColumn: ShardColumnSpec{Name: "shard", Value: "1"},
			},
			wantErr: "--inject-shard-column",
		},
		{
			name: "clean all-databases",
			m:    Migrator{AllDatabases: true},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.m.validateMultiDatabase()
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v; want substring %q", err, c.wantErr)
			}
		})
	}
}

// TestMultiDBMigrationID checks the per-database id derivation: empty
// base stays empty (auto-derive per DSN), explicit base gets suffixed.
func TestMultiDBMigrationID(t *testing.T) {
	if got := multiDBMigrationID("", "app_a"); got != "" {
		t.Errorf("empty base should stay empty; got %q", got)
	}
	if got := multiDBMigrationID("mig", "app_a"); got != "mig/app_a" {
		t.Errorf("got %q; want mig/app_a", got)
	}
}

// stubNoListerEngine implements ir.Engine but NOT ir.DatabaseLister, so
// the orchestrator must refuse a multi-database run loudly.
type stubNoListerEngine struct{ stubEngineBase }

func (stubNoListerEngine) Name() string { return "noLister" }

// TestRunMultiDatabaseRefusesNonLister verifies the loud refusal when a
// database-scope flag is set against a source that can't enumerate
// databases.
func TestRunMultiDatabaseRefusesNonLister(t *testing.T) {
	m := &Migrator{
		Source:       stubNoListerEngine{},
		Target:       stubNoListerEngine{},
		SourceDSN:    "src",
		TargetDSN:    "tgt",
		AllDatabases: true,
	}
	err := m.runMultiDatabase(context.Background())
	if err == nil || !strings.Contains(err.Error(), "enumerate databases") {
		t.Fatalf("err = %v; want a 'cannot enumerate databases' refusal", err)
	}
	if !strings.Contains(err.Error(), "noLister") {
		t.Errorf("err %q should name the offending engine", err)
	}
}

// stubEngineBase provides no-op implementations of the ir.Engine
// methods the multi-database refusal path never reaches, so the test
// stubs only have to override Name. The Open* methods panic to catch a
// path that should have refused earlier.
type stubEngineBase struct{}

func (stubEngineBase) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (stubEngineBase) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	panic("unexpected OpenSchemaReader")
}

func (stubEngineBase) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	panic("unexpected OpenSchemaWriter")
}

func (stubEngineBase) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	panic("unexpected OpenRowReader")
}

func (stubEngineBase) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	panic("unexpected OpenRowWriter")
}

func (stubEngineBase) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	panic("unexpected OpenCDCReader")
}

func (stubEngineBase) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	panic("unexpected OpenChangeApplier")
}

func (stubEngineBase) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	panic("unexpected OpenSnapshotStream")
}
