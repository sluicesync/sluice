// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The create-tables phase must not run on a resume whose recorded state
// already marks every in-scope table complete — measured at 122 GB
// against real PlanetScale, where re-issuing `CREATE TABLE` for a table
// already holding 20M rows killed `migrate --resume` in its FIRST phase
// (errno 1105 "direct DDL is disabled", rc=3 after one second, zero
// bulk-copy operations) and so made the errno-3024 / safe-migrations
// hints' own recovery advice — "re-run with --resume, the data is
// already copied, the ADR-0148 deploy-request fallback builds the index"
// — unreachable: the run never got as far as the index phase where that
// fallback lives.
//
// These pins assert on the PHASE REACHED, not on an exit code, and the
// refusal is genuinely armed: [ddlRefusingSchemaWriter] fails EVERY
// create-tables call with the field's 1105 error, so a test that greens
// is a test where the statement was never issued. The must-NOT-break
// direction (present-but-incomplete / absent table still gets its DDL)
// is pinned by the same writer surfacing that refusal.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// errDirectDDLDisabled mimics the PlanetScale safe-migrations refusal
// verbatim (the field error text, minus the driver type — the
// orchestrator only ever sees an opaque error from the writer, which is
// exactly the seam where 1105 surfaces).
var errDirectDDLDisabled = errors.New("Error 1105 (HY000): direct DDL is disabled")

// ddlRefusingSchemaWriter is a SchemaWriter that records the phases the
// orchestrator drives and REFUSES create-tables outright, the way a
// safe-migrations PlanetScale branch does. Every other phase succeeds so
// a run that gets past create-tables reaches the index phase.
type ddlRefusingSchemaWriter struct {
	phases       []string
	createCalls  int
	createTables []string
}

func (w *ddlRefusingSchemaWriter) CreateTablesWithoutConstraints(_ context.Context, s *ir.Schema) error {
	w.phases = append(w.phases, "CreateTablesWithoutConstraints")
	w.createCalls++
	if s != nil {
		for _, t := range s.Tables {
			w.createTables = append(w.createTables, t.Name)
		}
	}
	return errDirectDDLDisabled
}

func (w *ddlRefusingSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error {
	w.phases = append(w.phases, "CreateIndexes")
	return nil
}

func (w *ddlRefusingSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error {
	w.phases = append(w.phases, "CreateConstraints")
	return nil
}

func (w *ddlRefusingSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	w.phases = append(w.phases, "SyncIdentitySequences")
	return nil
}

func (w *ddlRefusingSchemaWriter) CreateViews(context.Context, *ir.Schema) error {
	w.phases = append(w.phases, "CreateViews")
	return nil
}

func (w *ddlRefusingSchemaWriter) reached(phase string) bool {
	for _, p := range w.phases {
		if p == phase {
			return true
		}
	}
	return false
}

// twoTableSchema is the field shape reduced to its essentials: two
// already-copied tables, each with a PK so the resume classification is
// the interesting one (complete → skip) rather than the no-PK fallback.
func twoTableSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Name:       "customers",
				Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			{
				Name:       "orders",
				Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
		},
	}
}

func runPhasesWithRefusingWriter(t *testing.T, state *ir.MigrationState, resuming bool) (*ddlRefusingSchemaWriter, error) {
	t.Helper()
	sw := &ddlRefusingSchemaWriter{}
	err := runBulkCopyPhases(
		context.Background(),
		resumeContext{}, // disabled: markPhase / writeState no-op
		state,
		twoTableSchema(),
		nil, // createSchema: no ADR-0166 gate on resume → the full schema
		noopRowReader{},
		sw,
		noopRowWriter{},
		resuming,
		1000, // bulkBatchSize
		nil,  // parallel deps: serial, so no chunk/reader machinery
		1,    // tableParallelism
		nil,  // redactor
		ShardColumnSpec{},
		false, // upfrontIndexes
		false, // analyzeAfter
	)
	return sw, err
}

// TestResume_AllTablesComplete_SkipsCreateTablesAndReachesIndexPhase is
// THE gate: with direct DDL refused and every in-scope table recorded
// complete, the resume must get PAST create-tables and INTO the index
// phase (where ADR-0148's deploy-request fallback lives) rather than
// dying in phase 1.
func TestResume_AllTablesComplete_SkipsCreateTablesAndReachesIndexPhase(t *testing.T) {
	state := &ir.MigrationState{
		TableProgress: map[string]ir.TableProgress{
			"customers": {State: ir.TableProgressComplete, RowsCopied: 2_000_000},
			"orders":    {State: ir.TableProgressComplete, RowsCopied: 20_000_000},
		},
	}
	sw, err := runPhasesWithRefusingWriter(t, state, true)
	if err != nil {
		t.Fatalf("resume with every table recorded complete failed: %v\nphases reached: %v", err, sw.phases)
	}
	if sw.createCalls != 0 {
		t.Errorf("create-tables was issued %d time(s) for a fully-complete resume; want 0 (tables it would CREATE are already recorded complete)", sw.createCalls)
	}
	if sw.reached("CreateTablesWithoutConstraints") {
		t.Error("phase log contains CreateTablesWithoutConstraints; the phase must be skipped outright, not merely tolerated")
	}
	if !sw.reached("CreateIndexes") {
		t.Fatalf("the run did not reach the index phase (phases: %v) — the ADR-0148 deploy-request fallback is only reachable from there, so the errno-3024 / safe-migrations hints' advice would still dead-end", sw.phases)
	}
	// The whole point is that the phases AFTER create-tables run, so pin
	// the ordering too. (No CreateViews: the views phase is skipped for a
	// schema that declares none — migcore.RunViewsPhase's own contract.)
	want := []string{"SyncIdentitySequences", "CreateIndexes", "CreateConstraints"}
	if got := strings.Join(sw.phases, ","); got != strings.Join(want, ",") {
		t.Errorf("phase order = %q; want %q", got, strings.Join(want, ","))
	}
}

// TestResume_IncompleteOrAbsentTable_StillGetsItsDDL pins the
// must-NOT-break direction: the skip is keyed to "every table recorded
// complete", so any table that is present-but-incomplete or absent from
// the recorded state keeps its CREATE — here proven by the refusal
// surfacing, which can only happen if the statement was issued.
func TestResume_IncompleteOrAbsentTable_StillGetsItsDDL(t *testing.T) {
	cases := []struct {
		name     string
		progress map[string]ir.TableProgress
	}{
		{
			// A mid-copy crash: orders carries a cursor, so it is
			// resumable — and still needs its DDL re-issued.
			name: "in-progress with cursor",
			progress: map[string]ir.TableProgress{
				"customers": {State: ir.TableProgressComplete},
				"orders":    {State: ir.TableProgressInProgress, LastPK: []any{int64(5000)}},
			},
		},
		{
			name: "in-progress without cursor (truncate-and-redo)",
			progress: map[string]ir.TableProgress{
				"customers": {State: ir.TableProgressComplete},
				"orders":    {State: ir.TableProgressInProgress},
			},
		},
		{
			name: "sticky no-PK truncate-and-redo",
			progress: map[string]ir.TableProgress{
				"customers": {State: ir.TableProgressComplete},
				"orders":    {State: ir.TableProgressNoPKTruncateAndRedo},
			},
		},
		{
			// A table added to the source (or to --include-table) since
			// the last attempt: absent from the recorded state, so it
			// genuinely does not exist on the target.
			name: "absent from the recorded state",
			progress: map[string]ir.TableProgress{
				"customers": {State: ir.TableProgressComplete},
			},
		},
		{
			name:     "empty recorded state",
			progress: map[string]ir.TableProgress{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &ir.MigrationState{TableProgress: tc.progress}
			sw, err := runPhasesWithRefusingWriter(t, state, true)
			if err == nil {
				t.Fatalf("resume with a non-complete table succeeded; want the create-tables refusal to surface (phases: %v)", sw.phases)
			}
			if !errors.Is(err, errDirectDDLDisabled) {
				t.Errorf("err = %v; want the create-tables refusal", err)
			}
			if sw.createCalls != 1 {
				t.Errorf("create-tables issued %d time(s); want exactly 1 — a table that is not recorded complete must still get its DDL", sw.createCalls)
			}
			if sw.reached("CreateIndexes") {
				t.Error("the run reached the index phase despite a failed create-tables phase")
			}
		})
	}
}

// TestNoResume_CompleteStateStillCreates pins that the skip is scoped to
// --resume. A fresh run never consults TableProgress for this decision
// (its equivalent is the ADR-0166 pre-create gate, which VALIDATES the
// target's column shape instead of trusting a recorded row), so a stale
// all-complete state row must not suppress a fresh run's DDL.
func TestNoResume_CompleteStateStillCreates(t *testing.T) {
	state := &ir.MigrationState{
		TableProgress: map[string]ir.TableProgress{
			"customers": {State: ir.TableProgressComplete},
			"orders":    {State: ir.TableProgressComplete},
		},
	}
	sw, err := runPhasesWithRefusingWriter(t, state, false)
	if err == nil {
		t.Fatalf("fresh (non-resume) run skipped create-tables; want the refusal to surface (phases: %v)", sw.phases)
	}
	if sw.createCalls != 1 {
		t.Errorf("create-tables issued %d time(s) on a fresh run; want exactly 1", sw.createCalls)
	}
}

// TestCreateTablesRedundantOnResume pins the predicate's boundary
// directly — including the shapes the phase-level tests above cannot
// construct (nil schema, table-less schema carrying schema-level
// objects, a nil table entry).
func TestCreateTablesRedundantOnResume(t *testing.T) {
	complete := func(names ...string) ir.MigrationState {
		p := make(map[string]ir.TableProgress, len(names))
		for _, n := range names {
			p[n] = ir.TableProgress{State: ir.TableProgressComplete}
		}
		return ir.MigrationState{TableProgress: p}
	}

	cases := []struct {
		name     string
		schema   *ir.Schema
		state    ir.MigrationState
		resuming bool
		want     bool
	}{
		{"all complete on resume", twoTableSchema(), complete("customers", "orders"), true, true},
		{"all complete but not resuming", twoTableSchema(), complete("customers", "orders"), false, false},
		{"one absent", twoTableSchema(), complete("customers"), true, false},
		{
			"one in-progress",
			twoTableSchema(),
			ir.MigrationState{TableProgress: map[string]ir.TableProgress{
				"customers": {State: ir.TableProgressComplete},
				"orders":    {State: ir.TableProgressInProgress},
			}},
			true,
			false,
		},
		{
			"one no-PK truncate-and-redo",
			twoTableSchema(),
			ir.MigrationState{TableProgress: map[string]ir.TableProgress{
				"customers": {State: ir.TableProgressComplete},
				"orders":    {State: ir.TableProgressNoPKTruncateAndRedo},
			}},
			true,
			false,
		},
		{"nil create schema", nil, complete("customers", "orders"), true, false},
		// A table-less schema can still carry schema-level objects the
		// create-tables phase emits (PG standalone sequences, the target
		// schema itself), and an empty create set is no evidence the
		// prior attempt created anything — so it is never redundant.
		{
			"table-less schema carrying a sequence",
			&ir.Schema{Sequences: []*ir.Sequence{{Name: "s"}}},
			complete("customers"),
			true,
			false,
		},
		{"nil table entry", &ir.Schema{Tables: []*ir.Table{nil}}, complete("customers"), true, false},
		{"empty state on resume", twoTableSchema(), ir.MigrationState{}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := createTablesRedundantOnResume(tc.schema, tc.state, tc.resuming); got != tc.want {
				t.Errorf("createTablesRedundantOnResume = %v; want %v", got, tc.want)
			}
		})
	}
}
