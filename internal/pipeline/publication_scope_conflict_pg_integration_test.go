//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0175 — Postgres publication scope isolation.
//
// The Tier-1 gate for the shared-publication clobber. Two concurrent
// PG-source streams with DISJOINT --include-table scopes used to
// silently starve each other: the publication name is a hardcoded
// `sluice_pub`, and each cold start ran
// `ALTER PUBLICATION sluice_pub SET TABLE <its own tables>`, which
// replaces the member set atomically. The first stream's slot stayed
// healthy and kept advancing while pgoutput emitted nothing for its
// tables — no error, `sync status`/`health` both green.
//
// These tests are deliberately non-vacuous: TestPublicationScope_
// TwoStreams_IsolatedByPublicationName FAILS on the pre-fix code
// (stream A never receives its post-B-coldstart insert), and
// TestPublicationScope_ConflictRefusedWithoutIsolation asserts the
// guard fires rather than silently rescoping.
//
// The class this guards is not Postgres-specific in spirit: any future
// engine that grows a SHARED source-side filter object belongs here.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// createPGDatabase creates an additional database on the same server
// as dsn and returns a DSN pointing at it. The wave tests need a
// SECOND target so each stream lands somewhere independent.
func createPGDatabase(t *testing.T, dsn, name string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open for CREATE DATABASE %s: %v", name, err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	out, err := buildPGDSN(dsn, name)
	if err != nil {
		t.Fatalf("build DSN for %s: %v", name, err)
	}
	return out
}

// waitForActiveSluiceSlot polls until the named replication slot
// exists AND is active. The guard's conflict signal is slot EXISTENCE
// (the ADR-0175 residual closure), which arrives strictly before
// activity — but the conflict test waits for ACTIVE anyway so it also
// proves the running wave is genuinely delivering when the refusal
// protects it, not merely registered.
func waitForActiveSluiceSlot(t *testing.T, dsn, slotName string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		active := pgQueryOne[bool](t, dsn,
			`SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $1), false)`,
			slotName)
		if active {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// pubScopeSeedDDL creates the four tables the wave-split tests use.
// Two land in stream A's scope, two in stream B's.
const pubScopeSeedDDL = `
CREATE TABLE orders      (id int PRIMARY KEY, note text);
CREATE TABLE order_items (id int PRIMARY KEY, note text);
CREATE TABLE users       (id int PRIMARY KEY, note text);
CREATE TABLE sessions    (id int PRIMARY KEY, note text);
INSERT INTO orders      (id, note) VALUES (1, 'seed');
INSERT INTO order_items (id, note) VALUES (1, 'seed');
INSERT INTO users       (id, note) VALUES (1, 'seed');
INSERT INTO sessions    (id, note) VALUES (1, 'seed');
`

// TestPublicationScope_TwoStreams_IsolatedByPublicationName is the
// regression gate. Wave A streams {orders, order_items} to target A;
// wave B then cold-starts for {users, sessions} against the SAME
// source. With per-stream publication names, B's cold start must not
// disturb A's scope — A keeps delivering.
//
// Pre-fix this fails at the final assertion: B's
// `ALTER PUBLICATION sluice_pub SET TABLE users, sessions` drops
// orders/order_items out of scope and A goes silently dry.
func TestPublicationScope_TwoStreams_IsolatedByPublicationName(t *testing.T) {
	sourceDSN, targetA, cleanup := startPostgresLogical(t)
	defer cleanup()

	targetB := createPGDatabase(t, sourceDSN, "target_b")

	applyDDL(t, sourceDSN, pubScopeSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// ---- Wave A: {orders, order_items} → target A ----
	streamA := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetA,
		StreamID:  "wave-a",
		SlotName:  "wave_a",
		// ADR-0175: per-stream publication is what makes concurrent
		// disjoint-scope streams safe. Without this the two streams
		// share `sluice_pub` and clobber each other.
		PublicationName: "wave_a",
		Filter:          migcore.TableFilter{Include: []string{"orders", "order_items"}},
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	errA := make(chan error, 1)
	go func() { errA <- streamA.Run(ctxA) }()

	if !waitForRowCount(t, targetA, "orders", 1, 60*time.Second) {
		t.Fatal("wave A: cold-start snapshot never delivered the orders seed row")
	}

	// Prove A's CDC leg is live BEFORE B exists, so a later failure is
	// unambiguously B's doing rather than A never having worked.
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (2, 'pre-b');")
	if !waitForRowCount(t, targetA, "orders", 2, 60*time.Second) {
		t.Fatal("wave A: CDC never delivered the pre-B insert (A was never healthy; the test proves nothing)")
	}

	// ---- Wave B: {users, sessions} → target B, cold-starting while A runs ----
	streamB := &Streamer{
		Source:          pgEng,
		Target:          pgEng,
		SourceDSN:       sourceDSN,
		TargetDSN:       targetB,
		StreamID:        "wave-b",
		SlotName:        "wave_b",
		PublicationName: "wave_b",
		Filter:          migcore.TableFilter{Include: []string{"users", "sessions"}},
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	errB := make(chan error, 1)
	go func() { errB <- streamB.Run(ctxB) }()

	if !waitForRowCount(t, targetB, "users", 1, 60*time.Second) {
		t.Fatal("wave B: cold-start snapshot never delivered the users seed row")
	}

	// ---- The load-bearing assertion: A still works after B cold-started ----
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (3, 'post-b');")
	if !waitForRowCount(t, targetA, "orders", 3, 60*time.Second) {
		t.Fatal("ADR-0175 REGRESSION: wave A stopped receiving changes after wave B cold-started — " +
			"B's publication rescope de-scoped A's tables (the silent-loss shape this gate exists for)")
	}

	// And B is genuinely live too, so the fix didn't just starve B instead.
	applyDDL(t, sourceDSN, "INSERT INTO users (id, note) VALUES (2, 'post-b');")
	if !waitForRowCount(t, targetB, "users", 2, 60*time.Second) {
		t.Fatal("wave B: CDC never delivered its own post-cold-start insert")
	}

	// Neither stream should have died.
	select {
	case err := <-errA:
		t.Fatalf("wave A exited early: %v", err)
	case err := <-errB:
		t.Fatalf("wave B exited early: %v", err)
	default:
	}
}

// TestPublicationScope_ConflictRefusedWithoutIsolation pins the guard:
// when a second stream would NARROW a publication another ACTIVE
// sluice slot is reading through, the cold start refuses loudly with
// SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT instead of silently
// rescoping. This is the safety net for operators who don't know to
// pass --publication-name.
func TestPublicationScope_ConflictRefusedWithoutIsolation(t *testing.T) {
	sourceDSN, targetA, cleanup := startPostgresLogical(t)
	defer cleanup()

	targetB := createPGDatabase(t, sourceDSN, "target_b")

	applyDDL(t, sourceDSN, pubScopeSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Wave A on the DEFAULT publication (no PublicationName set).
	streamA := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetA,
		StreamID:  "wave-a",
		SlotName:  "wave_a",
		Filter:    migcore.TableFilter{Include: []string{"orders", "order_items"}},
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	errA := make(chan error, 1)
	go func() { errA <- streamA.Run(ctxA) }()

	if !waitForRowCount(t, targetA, "orders", 1, 60*time.Second) {
		t.Fatal("wave A: cold-start snapshot never delivered the orders seed row")
	}
	// A's slot must be ACTIVE for the guard's signal to be present.
	if !waitForActiveSluiceSlot(t, sourceDSN, "sluice_wave_a", 60*time.Second) {
		t.Fatal("wave A: slot never became active; the guard's detection signal is absent")
	}

	// Wave B, also on the default publication, with a DISJOINT scope —
	// its cold start would `SET TABLE users, sessions`, removing A's
	// tables. Expect a loud refusal.
	streamB := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetB,
		StreamID:  "wave-b",
		SlotName:  "wave_b",
		Filter:    migcore.TableFilter{Include: []string{"users", "sessions"}},
	}
	ctxB, cancelB := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelB()

	runErr := streamB.Run(ctxB)
	if runErr == nil {
		t.Fatal("wave B cold-started successfully against a publication another active stream is reading — " +
			"the ADR-0175 guard did not fire")
	}
	coded, ok := sluicecode.FromError(runErr)
	if !ok || coded.Code != sluicecode.CodeCDCPublicationScopeConflict {
		t.Fatalf("wave B failed, but not with the expected coded refusal.\n got: %v", runErr)
	}
	// The message must be operator-actionable: name the at-risk tables
	// and both remedies.
	msg := runErr.Error()
	for _, want := range []string{"orders", "--publication-name"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q; got:\n%s", want, msg)
		}
	}

	// A must be untouched by the refused attempt — still delivering.
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (2, 'after-refusal');")
	if !waitForRowCount(t, targetA, "orders", 2, 60*time.Second) {
		t.Fatal("wave A stopped delivering after wave B's refused cold start — the guard must refuse BEFORE mutating")
	}

	select {
	case err := <-errA:
		t.Fatalf("wave A exited early: %v", err)
	default:
	}
}

// TestPublicationScope_InactiveSlotStillConflicts pins the ADR-0175
// residual closure (2026-07-23): the guard's conflict signal is slot
// EXISTENCE, not activity. A stream that is stopped mid-migration
// holds an INACTIVE slot and a resumable position that expects its
// scope — the original activity predicate was blind to exactly this
// window, so a narrowing rescope timed inside it silently starved the
// stopped stream on resume. With existence semantics the rescope must
// refuse, and the refusal must tell the operator the conflicting slot
// is inactive (so a genuinely abandoned slot can be dropped).
func TestPublicationScope_InactiveSlotStillConflicts(t *testing.T) {
	sourceDSN, target, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, pubScopeSeedDDL)

	// The state a stopped-mid-migration wave leaves behind: the shared
	// publication scoped to ITS tables, and its slot present but
	// inactive (no consumer attached).
	applyDDL(t, sourceDSN, "CREATE PUBLICATION sluice_pub FOR TABLE orders, order_items;")
	applyDDL(t, sourceDSN, "SELECT pg_create_logical_replication_slot('sluice_ghost_wave', 'pgoutput');")
	if active := pgQueryOne[bool](t, sourceDSN,
		`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, "sluice_ghost_wave"); active {
		t.Fatal("setup: the ghost slot is unexpectedly active; the pin needs an INACTIVE conflicting slot")
	}

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	stream := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: target,
		StreamID:  "wave-b",
		SlotName:  "wave_b",
		Filter:    migcore.TableFilter{Include: []string{"users", "sessions"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	runErr := stream.Run(ctx)
	if runErr == nil {
		t.Fatal("cold start rescoped a publication another (inactive) sluice slot holds a claim on — " +
			"the existence-semantics guard did not fire")
	}
	coded, ok := sluicecode.FromError(runErr)
	if !ok || coded.Code != sluicecode.CodeCDCPublicationScopeConflict {
		t.Fatalf("failed, but not with the expected coded refusal.\n got: %v", runErr)
	}
	msg := runErr.Error()
	for _, want := range []string{"orders", "inactive", "sluice_ghost_wave", "--publication-name"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q; got:\n%s", want, msg)
		}
	}

	// Refuse-before-mutate: the stopped wave's scope must be intact.
	members := pgQueryOne[int](t, sourceDSN,
		`SELECT COUNT(*) FROM pg_publication_rel pr
		   JOIN pg_class c ON c.oid = pr.prrelid
		   JOIN pg_publication p ON p.oid = pr.prpubid
		  WHERE p.pubname = $1 AND c.relname IN ('orders','order_items')`, "sluice_pub")
	if members != 2 {
		t.Fatalf("publication membership changed under a refused rescope: orders/order_items members = %d, want 2", members)
	}
}

// TestPublicationScope_WideningIsNotAConflict pins the no-fire side:
// a rescope that only ADDS tables removes nothing, so it must proceed
// even with another active sluice slot present. This is the ADR-0122
// fleet shape and the `schema add-table` path — the widest-blast-radius
// risk of the guard is over-firing on them.
func TestPublicationScope_WideningIsNotAConflict(t *testing.T) {
	sourceDSN, targetA, cleanup := startPostgresLogical(t)
	defer cleanup()

	targetB := createPGDatabase(t, sourceDSN, "target_b")

	applyDDL(t, sourceDSN, pubScopeSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Wave A: a NARROW scope on the default publication.
	streamA := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetA,
		StreamID:  "wave-a",
		SlotName:  "wave_a",
		Filter:    migcore.TableFilter{Include: []string{"orders"}},
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	errA := make(chan error, 1)
	go func() { errA <- streamA.Run(ctxA) }()

	if !waitForRowCount(t, targetA, "orders", 1, 60*time.Second) {
		t.Fatal("wave A: cold-start snapshot never delivered the orders seed row")
	}
	if !waitForActiveSluiceSlot(t, sourceDSN, "sluice_wave_a", 60*time.Second) {
		t.Fatal("wave A: slot never became active")
	}

	// Wave B on the same default publication with a SUPERSET scope:
	// {orders, users} adds `users` and removes nothing.
	streamB := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetB,
		StreamID:  "wave-b",
		SlotName:  "wave_b",
		Filter:    migcore.TableFilter{Include: []string{"orders", "users"}},
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	errB := make(chan error, 1)
	go func() { errB <- streamB.Run(ctxB) }()

	if !waitForRowCount(t, targetB, "users", 1, 60*time.Second) {
		t.Fatal("wave B: a WIDENING rescope was refused or stalled — the guard over-fired on the fleet/add-table shape")
	}

	select {
	case err := <-errB:
		t.Fatalf("wave B exited early (widening must be allowed): %v", err)
	default:
	}
}
