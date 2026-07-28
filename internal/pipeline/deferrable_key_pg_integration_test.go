//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 211 — the PATH pin for the DEFERRABLE-key refusal.
//
// The engine-level matrix (internal/engines/postgres/change_applier_
// deferrable_key_integration_test.go) proves the arbiter selection and
// the preflight function. This file proves the thing that actually broke
// operators: a real PG→PG `sluice sync` against a source whose primary
// key is DEFERRABLE.
//
// Pre-fix behaviour, reproduced by the v0.103.2 regression cycle: the
// cold start succeeded, the stream entered CDC, and then the FIRST
// change to that table killed it —
//
//	ERROR: ON CONFLICT does not support deferrable unique constraints/
//	       exclusion constraints as arbiters (SQLSTATE 55000)
//
// with zero retries, every warm resume dying on the same change, and a
// plain-PK sibling table in the same stream stalling behind it. Post-fix
// the run refuses at start with SLUICE-E-TARGET-DEFERRABLE-KEY, naming
// the table, before a single change is applied.
//
// The test is deliberately non-vacuous in both directions: the refusal
// case FAILS on the pre-fix code (Run would not return this error — it
// would stream and then die on the change, or block until the context
// deadline), and the sibling case FAILS if the refusal over-reaches and
// stops an ordinary immediate-PK sync from working.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// deferrableKeySeedDDL is the regression cycle's own fixture: one table
// whose PK is DEFERRABLE, one ordinary table riding the same stream so
// the blast radius is visible.
//
// The `REPLICA IDENTITY FULL` on dpk is load-bearing for THIS test, not
// part of the original repro. Roadmap item 93 added a SOURCE-side gate
// for the same catalog bit — a deferrable PK is not a usable replica
// identity either — and it runs at the very start of cold start, so
// without this ALTER the run would refuse there and never reach the
// target-side gate under test. FULL gives the source a publishable
// identity while leaving the TARGET's carried key deferrable (
// relreplident is not part of the IR and is not copied), which is
// exactly the Bug 211 shape in isolation.
const deferrableKeySeedDDL = `
CREATE TABLE dpk   (id int, v text, CONSTRAINT dpk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
ALTER TABLE dpk REPLICA IDENTITY FULL;
CREATE TABLE plain (id int PRIMARY KEY, v text);
INSERT INTO dpk   (id, v) VALUES (1, 'a'), (2, 'b');
INSERT INTO plain (id, v) VALUES (1, 'a'), (2, 'b');
`

// TestDeferrableKey_SyncRefusesAtStart is the gate. A PG→PG sync whose
// scope includes a deferrable-PK table must exit with the coded refusal
// rather than entering CDC and dying on the first change to it.
func TestDeferrableKey_SyncRefusesAtStart(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, deferrableKeySeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "deferrable-key",
		SlotName:  "deferrable_key",
	}

	// A generous bound that is still far shorter than "forever": pre-fix
	// this run enters CDC and stays up until the deadline (or dies on the
	// first change), so a timeout here is itself a failure signal.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if err == nil {
		t.Fatal("sync start succeeded against a target whose primary key is DEFERRABLE — " +
			"the Bug 211 gate did not fire; the stream would die on the first change to that table")
	}
	if ctx.Err() != nil {
		t.Fatalf("sync ran to the context deadline instead of refusing at start: %v", err)
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetDeferrableKey {
		t.Fatalf("sync failed, but not with %s.\n got: %v", sluicecode.CodeTargetDeferrableKey, err)
	}
	if !strings.Contains(err.Error(), "dpk") {
		t.Errorf("refusal does not name the offending table: %v", err)
	}
	if !strings.Contains(err.Error(), "DEFERRABLE") {
		t.Errorf("refusal does not name the cause: %v", err)
	}
	if coded.Hint == "" {
		t.Error("refusal carries no remedy hint")
	}

	// The refusal must not name the table it CAN serve — an operator
	// reading it should know exactly which table to fix.
	if strings.Contains(err.Error(), "plain") {
		t.Errorf("refusal names the ordinary sibling table: %v", err)
	}
}

// TestDeferrableKey_ImmediatePKSiblingStillStreams is the over-refusal
// guard on the same fixture: scoped to the ordinary table, the very same
// source must cold-start and take live CDC exactly as before. A gate
// that stopped ordinary syncs would be worse than the bug it closes.
func TestDeferrableKey_ImmediatePKSiblingStillStreams(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, deferrableKeySeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "deferrable-key-sibling",
		SlotName:  "deferrable_key_sibling",
		Filter:    migcore.TableFilter{Include: []string{"plain"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	if !waitForRowCount(t, targetDSN, "plain", 2, 90*time.Second) {
		t.Fatal("cold start never delivered the seed rows for the immediate-PK table")
	}
	applyDDL(t, sourceDSN, "INSERT INTO plain (id, v) VALUES (3, 'cdc');")
	if !waitForRowCount(t, targetDSN, "plain", 3, 90*time.Second) {
		t.Fatal("CDC never delivered the live insert for the immediate-PK table")
	}

	select {
	case err := <-runErr:
		t.Fatalf("stream exited early: %v", err)
	default:
	}
}
