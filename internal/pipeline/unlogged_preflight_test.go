// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the spanning-sync UNLOGGED-table census wiring
// (capture-completeness G2). The engine-side census itself is
// integration-pinned against real PG in
// internal/engines/postgres/unlogged_preflight_integration_test.go;
// these tests pin the PIPELINE half: the predicate carries the sync's
// effective table filter (Bug 246), non-implementing sources skip, and
// BOTH spanning stream-open chokepoints — cold start AND warm resume —
// invoke the preflight BEFORE opening anything (the warm-resume half
// exists because ALTER TABLE … SET UNLOGGED succeeds mid-sync under FOR
// ALL TABLES, so a cold-start-only census goes blind after the first
// open).
package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// unloggedPreflightStub embeds the panicking stubEngine and adds exactly
// the surfaces the two spanning open paths touch before the census:
// database enumeration, the census itself, and the respective opener
// (which records an invocation instead of panicking, so the tests can
// assert the refusal fired FIRST).
type unloggedPreflightStub struct {
	stubEngine

	preflightErr     error
	gotSchemas       []string
	gotAllowed       func(schema, table string) bool
	preflightCalls   int
	coldOpenerCalls  int
	serverCDCCalls   int
	listedDatabases  []string
	listDatabasesErr error
}

func (s *unloggedPreflightStub) ListDatabases(context.Context, string) ([]string, error) {
	return s.listedDatabases, s.listDatabasesErr
}

func (s *unloggedPreflightStub) PreflightSpanningUnloggedTables(_ context.Context, _ string, schemas []string, allowed func(schema, table string) bool) error {
	s.preflightCalls++
	s.gotSchemas = schemas
	s.gotAllowed = allowed
	return s.preflightErr
}

func (s *unloggedPreflightStub) OpenMultiDatabaseSnapshotStream(context.Context, string, []string) (*ir.SnapshotStream, error) {
	s.coldOpenerCalls++
	return nil, errors.New("stub: cold opener reached (the census should have refused first)")
}

func (s *unloggedPreflightStub) OpenServerCDCReader(context.Context, string) (ir.CDCReader, error) {
	s.serverCDCCalls++
	return nil, errors.New("stub: server CDC opener reached (the census should have refused first)")
}

// Compile-time pins: the stub satisfies the optional surfaces the two
// open paths type-assert on, so a signature drift fails the build here
// rather than silently skipping the paths under test.
var (
	_ ir.UnloggedCapturePreflighter  = (*unloggedPreflightStub)(nil)
	_ ir.MultiDatabaseSnapshotOpener = (*unloggedPreflightStub)(nil)
	_ ir.ServerCDCReaderOpener       = (*unloggedPreflightStub)(nil)
	_ ir.DatabaseLister              = (*unloggedPreflightStub)(nil)
)

// TestPreflightSpanningUnloggedTables_PredicateCarriesTableFilter pins
// the Bug 246 half: the predicate handed to the engine is the sync's
// effective table filter, so an --exclude-table'd unlogged table must
// evaluate NOT allowed while an unfiltered one evaluates allowed.
func TestPreflightSpanningUnloggedTables_PredicateCarriesTableFilter(t *testing.T) {
	stub := &unloggedPreflightStub{}
	filter, err := migcore.NewTableFilter(nil, []string{"scratch"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	s := &Streamer{Source: stub, SourceDSN: "dsn", Filter: filter}
	if err := s.preflightSpanningUnloggedTables(context.Background(), []string{"s1", "s2"}); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if stub.preflightCalls != 1 {
		t.Fatalf("preflight calls = %d; want 1", stub.preflightCalls)
	}
	if len(stub.gotSchemas) != 2 || stub.gotSchemas[0] != "s1" || stub.gotSchemas[1] != "s2" {
		t.Errorf("schemas = %v; want [s1 s2]", stub.gotSchemas)
	}
	if stub.gotAllowed == nil {
		t.Fatal("predicate not passed")
	}
	if stub.gotAllowed("s1", "scratch") {
		t.Error("predicate allows the --exclude-table'd table; the census would refuse a table the operator already excluded (Bug 246)")
	}
	if !stub.gotAllowed("s1", "orders") {
		t.Error("predicate rejects an unfiltered table; the census would silently skip an in-scope unlogged table")
	}
}

// TestPreflightSpanningUnloggedTables_NonImplementingSourceSkips pins the
// opportunistic-skip posture: a source without the surface (MySQL — no
// unlogged concept) is a silent no-op, not an error.
func TestPreflightSpanningUnloggedTables_NonImplementingSourceSkips(t *testing.T) {
	s := &Streamer{Source: stubEngine{}, SourceDSN: "dsn"}
	if err := s.preflightSpanningUnloggedTables(context.Background(), []string{"s1"}); err != nil {
		t.Fatalf("non-implementing source must skip; got %v", err)
	}
}

// TestColdStartMultiDatabase_UnloggedCensusRefusesBeforeOpening pins the
// cold-start chokepoint: the census refusal surfaces BEFORE the spanning
// snapshot opener runs (which would create the slot + FOR ALL TABLES
// publication on the source).
func TestColdStartMultiDatabase_UnloggedCensusRefusesBeforeOpening(t *testing.T) {
	refusal := errors.New("unlogged census refusal")
	stub := &unloggedPreflightStub{
		listedDatabases: []string{"s1"},
		preflightErr:    refusal,
	}
	s := &Streamer{
		Source:         stub,
		Target:         stubEngine{},
		SourceDSN:      "src",
		TargetDSN:      "tgt",
		DatabaseFilter: DatabaseFilter{Include: []string{"s1"}},
	}
	_, stop, err := s.coldStartMultiDatabase(context.Background(), nil, nil, "sid", freshCopyNone)
	if stop != nil {
		stop()
	}
	if !errors.Is(err, refusal) {
		t.Fatalf("coldStartMultiDatabase err = %v; want the census refusal", err)
	}
	if stub.preflightCalls != 1 {
		t.Errorf("preflight calls = %d; want 1", stub.preflightCalls)
	}
	if stub.coldOpenerCalls != 0 {
		t.Errorf("spanning snapshot opener ran %d time(s) after the census refused; the refusal must land before anything is created", stub.coldOpenerCalls)
	}
}

// TestWarmResumeMultiDatabase_UnloggedCensusRefusesBeforeOpening pins the
// warm-resume chokepoint — the SET-UNLOGGED-flip half of the door: a
// resume re-runs the census and refuses before opening the server-wide
// CDC reader.
func TestWarmResumeMultiDatabase_UnloggedCensusRefusesBeforeOpening(t *testing.T) {
	refusal := errors.New("unlogged census refusal")
	stub := &unloggedPreflightStub{
		listedDatabases: []string{"s1"},
		preflightErr:    refusal,
	}
	s := &Streamer{
		Source:         stub,
		Target:         stubEngine{},
		SourceDSN:      "src",
		TargetDSN:      "tgt",
		DatabaseFilter: DatabaseFilter{Include: []string{"s1"}},
	}
	_, stop, err := s.warmResumeMultiDatabase(
		context.Background(), ir.Position{Engine: "stub", Token: "tok"}, nil, nil, "sid",
	)
	if stop != nil {
		stop()
	}
	if !errors.Is(err, refusal) {
		t.Fatalf("warmResumeMultiDatabase err = %v; want the census refusal", err)
	}
	if stub.preflightCalls != 1 {
		t.Errorf("preflight calls = %d; want 1", stub.preflightCalls)
	}
	if stub.serverCDCCalls != 0 {
		t.Errorf("server-wide CDC reader opened %d time(s) after the census refused; the refusal must land before the stream opens", stub.serverCDCCalls)
	}
}
