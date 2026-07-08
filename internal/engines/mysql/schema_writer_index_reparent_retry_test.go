// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the overlapped index-build reparent-retry (ADR-0114 parity,
// audit N-15b), mirroring the PG pin set
// (postgres/schema_writer_index_overlap_reparent_test.go): the pure policy
// (rides-transient / terminal-unchanged / reacquire-rides-budget /
// loud-exhaustion / ctx-cancel) plus WIRING pins that drive the retry
// through BuildTableIndexesFromChannel on BOTH overlap paths — the vanilla
// concurrent workers and the VStream serial build-as-copied branch — via the
// fake driver (the Bug-180 "pin the fix through the layer that reaches it"
// lesson). The no-double-create pin models an ALTER that committed
// server-side but died unacknowledged (probeBuilt): the retry's
// detect-then-skip probe must skip it, never re-emit.

package mysql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// shrinkIndexReparentEnvelope makes the shared ADR-0108 reparent-retry
// envelope fast for tests and restores it on cleanup. Production never
// mutates these (see the vars' comment in row_writer_reparent_retry.go).
func shrinkIndexReparentEnvelope(t *testing.T) {
	t.Helper()
	ob, oc, ow, oa := coldCopyReparentBackoffBaseVar, coldCopyReparentBackoffCapVar, coldCopyReparentMaxWallVar, coldCopyReparentRetryAttemptsVar
	coldCopyReparentBackoffBaseVar = time.Millisecond
	coldCopyReparentBackoffCapVar = 2 * time.Millisecond
	coldCopyReparentMaxWallVar = 5 * time.Second
	coldCopyReparentRetryAttemptsVar = 100000
	t.Cleanup(func() {
		coldCopyReparentBackoffBaseVar, coldCopyReparentBackoffCapVar, coldCopyReparentMaxWallVar, coldCopyReparentRetryAttemptsVar = ob, oc, ow, oa
	})
}

// --- the pure retry policy ---

// TestRetryIndexBuildWithReparent_RidesTransientThenSucceeds: a build that
// hits N reparent transients then succeeds converges, re-acquiring a fresh
// conn before each retry.
func TestRetryIndexBuildWithReparent_RidesTransientThenSucceeds(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	attempts, reacquires := 0, 0
	attempt := func(context.Context) error {
		attempts++
		if attempts <= 3 {
			return errSimulatedReparent
		}
		return nil
	}
	reacquire := func(context.Context) error { reacquires++; return nil }

	if err := retryIndexBuildWithReparent(context.Background(), `indexes on "t"`, attempt, reacquire); err != nil {
		t.Fatalf("expected success after riding transients, got %v", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d; want 4 (3 transient + 1 success)", attempts)
	}
	if reacquires != 3 {
		t.Fatalf("reacquires = %d; want 3 (one fresh conn before each retry)", reacquires)
	}
}

// TestRetryIndexBuildWithReparent_TerminalNoRetry: a real DDL fault (1062
// duplicate key — explicitly NOT retriable in classifyApplierError) returns
// unchanged after one attempt, with NO reacquire. The build phase must still
// fail loudly on a genuine error.
func TestRetryIndexBuildWithReparent_TerminalNoRetry(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	fault := &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '1' for key 'orders.u_uidx'"}
	attempts, reacquires := 0, 0
	attempt := func(context.Context) error { attempts++; return fault }
	reacquire := func(context.Context) error { reacquires++; return nil }

	err := retryIndexBuildWithReparent(context.Background(), `indexes on "t"`, attempt, reacquire)
	if !errors.Is(err, fault) {
		t.Fatalf("expected the terminal fault unchanged, got %v", err)
	}
	if attempts != 1 || reacquires != 0 {
		t.Fatalf("attempts=%d reacquires=%d; want 1/0 (no retry on a real DDL fault)", attempts, reacquires)
	}
}

// TestRetryIndexBuildWithReparent_ReacquireErrorRidesBudget: a reacquire that
// itself fails with a transient (the target still mid-reparent) is classified
// on the next iteration and ridden, then succeeds.
func TestRetryIndexBuildWithReparent_ReacquireErrorRidesBudget(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	attempts, reacquires := 0, 0
	attempt := func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errSimulatedReparent
		}
		return nil
	}
	reacquire := func(context.Context) error {
		reacquires++
		if reacquires == 1 {
			return errSimulatedReparent // target still mid-reparent — must ride this too
		}
		return nil
	}
	if err := retryIndexBuildWithReparent(context.Background(), `indexes on "t"`, attempt, reacquire); err != nil {
		t.Fatalf("expected success after riding a reacquire transient, got %v", err)
	}
	if reacquires != 2 {
		t.Fatalf("reacquires = %d; want 2 (first failed-transient, second ok)", reacquires)
	}
}

// TestRetryIndexBuildWithReparent_ExhaustionIsLoud: an always-transient build
// surfaces a LOUD terminal error wrapping the last transient after the
// wall-clock bound (never silent, never infinite).
func TestRetryIndexBuildWithReparent_ExhaustionIsLoud(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	coldCopyReparentMaxWallVar = 40 * time.Millisecond
	attempt := func(context.Context) error { return errSimulatedReparent }
	reacquire := func(context.Context) error { return nil }

	err := retryIndexBuildWithReparent(context.Background(), `indexes on "t"`, attempt, reacquire)
	if err == nil {
		t.Fatal("expected a loud terminal error on exhaustion, got nil")
	}
	if !errors.Is(err, errSimulatedReparent) {
		t.Fatalf("exhaustion error must wrap the last transient, got %v", err)
	}
	if !strings.Contains(err.Error(), "still failing after riding") {
		t.Fatalf("exhaustion error must name the ridden window, got %v", err)
	}
}

// TestRetryIndexBuildWithReparent_CtxCancel: ctx cancel during the backoff
// unwinds promptly with ctx.Err().
func TestRetryIndexBuildWithReparent_CtxCancel(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	ctx, cancel := context.WithCancel(context.Background())
	attempt := func(context.Context) error { cancel(); return errSimulatedReparent }
	reacquire := func(context.Context) error { return nil }

	err := retryIndexBuildWithReparent(ctx, `indexes on "t"`, attempt, reacquire)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on cancel during backoff, got %v", err)
	}
}

// --- wiring: the vanilla concurrent worker path ---

// TestBuildTableIndexesFromChannel_VanillaRidesReparentTransient drives the
// retry THROUGH the vanilla worker path: t0's first ALTER dies with the
// reparent transient (not landed — the probe still answers "absent"), and
// the phase must converge by replaying the ALTER on a fresh connection —
// two ALTERs recorded, callback fired, nil error.
func TestBuildTableIndexesFromChannel_VanillaRidesReparentTransient(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	rec := &indexRecorder{
		exists:             false,
		transientFailsLeft: map[string]int{"t0": 1},
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	var mu sync.Mutex
	var fired []string
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	tables := []*ir.Table{indexedTable("t0")}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	ch <- tables[0]
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
		t.Fatalf("BuildTableIndexesFromChannel must ride the reparent transient; got %v", err)
	}
	if n := rec.alterCountFor("t0"); n != 2 {
		t.Errorf("ALTER count for t0 = %d; want 2 (killed attempt + replay)", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 1 || fired[0] != "t0" {
		t.Errorf("IndexesBuilt fired for %v; want [t0] exactly once, after the successful replay", fired)
	}
}

// TestBuildTableIndexesFromChannel_VanillaPermanentStaysTerminal pins that a
// NON-transient ALTER fault on the vanilla worker path is NOT retried: one
// attempt, the error surfaces loudly, and no table is marked indexed.
func TestBuildTableIndexesFromChannel_VanillaPermanentStaysTerminal(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	fault := &gomysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}
	rec := &indexRecorder{
		exists:   false,
		failTbls: map[string]error{"t0": fault},
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	var mu sync.Mutex
	var fired []string
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	tables := []*ir.Table{indexedTable("t0")}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	ch <- tables[0]
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := w.BuildTableIndexesFromChannel(ctx, schema, ch)
	if !errors.Is(err, fault) {
		t.Fatalf("a permanent DDL fault must surface unchanged; got %v", err)
	}
	if n := rec.alterCountFor("t0"); n != 1 {
		t.Errorf("ALTER count for t0 = %d; want 1 (no retry on a real DDL fault)", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 0 {
		t.Errorf("IndexesBuilt fired for %v after a permanent failure", fired)
	}
}

// TestBuildTableIndexesFromChannel_VanillaExhaustionIsLoud pins the bounded
// budget on the worker path: an ALWAYS-transient target exhausts the
// wall-clock deadline and fails LOUDLY (naming the ridden window) — never
// silent, never infinite.
func TestBuildTableIndexesFromChannel_VanillaExhaustionIsLoud(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	coldCopyReparentMaxWallVar = 40 * time.Millisecond
	rec := &indexRecorder{
		exists:             false,
		transientFailsLeft: map[string]int{"t0": 1 << 30}, // effectively forever
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	tables := []*ir.Table{indexedTable("t0")}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	ch <- tables[0]
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := w.BuildTableIndexesFromChannel(ctx, schema, ch)
	if err == nil {
		t.Fatal("an always-transient target must exhaust the budget loudly; got nil")
	}
	if !strings.Contains(err.Error(), "still failing after riding") {
		t.Fatalf("exhaustion error must name the ridden window, got %v", err)
	}
}

// TestBuildTableIndexesWithReparentRetry_ReopensConnection pins the
// load-bearing reopen contract at the wrapper level: after a reparent
// transient, the retry must hand the worker a FRESH *sql.Conn (the pinned
// one is dead post-reparent), never the original.
func TestBuildTableIndexesWithReparentRetry_ReopensConnection(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	rec := &indexRecorder{
		exists:             false,
		transientFailsLeft: map[string]int{"t0": 1},
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	orig, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire initial conn: %v", err)
	}
	jobs := w.indexBuildJobsForTables([]*ir.Table{indexedTable("t0")})
	if len(jobs) != 1 {
		t.Fatalf("want 1 job, got %d", len(jobs))
	}
	got, err := w.buildTableIndexesWithReparentRetry(ctx, orig, jobs[0])
	if err != nil {
		t.Fatalf("buildTableIndexesWithReparentRetry: %v", err)
	}
	if got == nil {
		t.Fatal("returned conn is nil after a successful retry")
	}
	defer func() { _ = got.Close() }()
	if got == orig {
		t.Fatal("retry reused the dead pinned connection; a reparent kills it — a FRESH conn must be re-acquired")
	}
}

// --- wiring: the VStream serial build-as-copied path ---

// TestBuildTableIndexesFromChannel_VStreamRidesReparentTransient drives the
// retry THROUGH the serial branch every production PlanetScale/Vitess target
// takes: t0's first ALTER dies with the reparent transient (not landed), the
// build replays and converges, and build-then-mark holds (the callback fires
// exactly once, after the successful replay). Both usesVStream flavors are
// exercised (pin the class).
func TestBuildTableIndexesFromChannel_VStreamRidesReparentTransient(t *testing.T) {
	for _, flavor := range []Flavor{FlavorPlanetScale, FlavorVitess} {
		t.Run(flavor.String(), func(t *testing.T) {
			shrinkIndexReparentEnvelope(t)
			rec := &indexRecorder{
				exists:             false,
				transientFailsLeft: map[string]int{"t0": 1},
			}
			db := newIndexFakeDB(t, rec)
			w := &SchemaWriter{db: db, schema: "testdb", flavor: flavor}

			var mu sync.Mutex
			var fired []string
			var markedBeforeSuccess bool
			w.SetTableIndexedCallback(func(table *ir.Table) {
				mu.Lock()
				fired = append(fired, table.Name)
				// build-then-mark: by t0's callback time its replay ALTER
				// must be on record (2 attempts: killed + replayed).
				if table.Name == "t0" && rec.alterCountFor("t0") < 2 {
					markedBeforeSuccess = true
				}
				mu.Unlock()
			})

			tables := []*ir.Table{indexedTable("t0"), indexedTable("t1")}
			schema := &ir.Schema{Tables: tables}
			ch := make(chan *ir.Table, len(tables))
			for _, tbl := range tables {
				ch <- tbl
			}
			close(ch)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
				t.Fatalf("vstream serial build must ride the reparent transient; got %v", err)
			}
			if n := rec.alterCountFor("t0"); n != 2 {
				t.Errorf("ALTER count for t0 = %d; want 2 (killed attempt + replay)", n)
			}
			if n := rec.alterCountFor("t1"); n != 1 {
				t.Errorf("ALTER count for t1 = %d; want 1 (untouched by t0's retry)", n)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(fired) != 2 {
				t.Errorf("IndexesBuilt fired for %v; want both tables", fired)
			}
			if markedBeforeSuccess {
				t.Error("build-then-mark violated: t0 marked indexed before its replay ALTER landed")
			}
		})
	}
}

// TestBuildTableIndexesFromChannel_VStreamNoDoubleCreateAfterMidAlterRetry is
// the idempotency pin the retry leans on: the combined ALTER COMMITS
// server-side but the connection dies before the ack (probeBuilt — the
// recorded ALTER "landed"), classified transient. The retry's
// detect-then-skip probe must find every index present and emit NO second
// ALTER — exactly ONE ALTER on record, nil error, table marked. A
// double-create here would raise MySQL 1061 in production.
func TestBuildTableIndexesFromChannel_VStreamNoDoubleCreateAfterMidAlterRetry(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	rec := &indexRecorder{
		probeBuilt:         true, // probes answer from recorded ALTERs
		transientFailsLeft: map[string]int{"t0": 1},
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}

	var mu sync.Mutex
	var fired []string
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	tables := []*ir.Table{indexedTable("t0")}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	ch <- tables[0]
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
		t.Fatalf("retry after a committed-but-unacked ALTER must converge; got %v", err)
	}
	if n := rec.alterCountFor("t0"); n != 1 {
		t.Errorf("ALTER count for t0 = %d; want exactly 1 — the retry re-probed and must NOT double-create (would be a 1061 in production)", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 1 || fired[0] != "t0" {
		t.Errorf("IndexesBuilt fired for %v; want [t0]", fired)
	}
}

// TestBuildTableIndexesFromChannel_VStreamExhaustionLeavesUnmarked pins the
// serial branch's bounded budget + build-then-mark under exhaustion: an
// always-transient target fails LOUDLY after the wall-clock deadline and the
// table stays IndexesBuilt=false so a --resume rebuilds it.
func TestBuildTableIndexesFromChannel_VStreamExhaustionLeavesUnmarked(t *testing.T) {
	shrinkIndexReparentEnvelope(t)
	coldCopyReparentMaxWallVar = 40 * time.Millisecond
	rec := &indexRecorder{
		exists:             false,
		transientFailsLeft: map[string]int{"t0": 1 << 30},
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVitess}

	var mu sync.Mutex
	var fired []string
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	tables := []*ir.Table{indexedTable("t0")}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	ch <- tables[0]
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := w.BuildTableIndexesFromChannel(ctx, schema, ch)
	if err == nil {
		t.Fatal("an always-transient target must exhaust the budget loudly; got nil")
	}
	if !strings.Contains(err.Error(), "still failing after riding") {
		t.Fatalf("exhaustion error must name the ridden window, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 0 {
		t.Errorf("IndexesBuilt fired for %v after exhaustion; build-then-mark requires the table stay unmarked for --resume", fired)
	}
}
