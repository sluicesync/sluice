//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for multi-schema Postgres `sync start` (ADR-0075
// Phase 2b): cold-start N source SCHEMAS under ONE spanning exported
// snapshot → N same-named target namespaces, then steady-state CDC through
// the ONE database-wide logical slot, routed per-change to the right
// namespace, plus stop → restart warm-resume from the one persisted LSN.
//
// A PG logical replication slot is DATABASE-WIDE — one slot, one LSN,
// per-event routing — so this is the symmetric analog of ADR-0074's
// server-wide MySQL binlog, NOT an N-slot fan-out.
//
//	(a) PG → PG: two source schemas → two same-named target schemas. Cold-
//	    start copies both from the one spanning snapshot; then INSERT/
//	    UPDATE/DELETE in BOTH schemas concurrently routes to the right
//	    target schema (no cross-schema bleed); then stop → write more →
//	    restart WARM-RESUMES from the one persisted LSN (no re-copy, zero
//	    loss/dup).
//	(b) PG → MySQL: each source schema → a same-named MySQL database; same
//	    cold-start + steady-state + warm-resume zero-loss assertions.
//	(c) Single-schema back-compat: no --*-schema flag → byte-identical to
//	    today (the unset-scope path drops non-bound-schema events).
//	(d) Scope pin: an out-of-scope schema's changes are NEVER applied.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// startPostgresLogicalMultiSchema boots a wal_level=logical PG container
// and returns the source database DSN plus a same-server PG target
// database DSN (target_db). Reuses startPostgresLogical, which already
// configures logical replication and creates target_db.
func startPostgresLogicalMultiSchema(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	return startPostgresLogical(t)
}

// TestStreamer_MultiSchema_PostgresToPostgres is scenario (a): the full
// cold-start → steady-state → warm-resume arc, PG → PG, across two source
// schemas under one database-wide slot.
func TestStreamer_MultiSchema_PostgresToPostgres(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	// Two user schemas, each with a same-named `widgets` table (namespace
	// isolation). `public` is left untouched and NOT selected.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.widgets   (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE billing.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO sales.widgets   (id, name) VALUES (1, 'a-one'), (2, 'a-two');
		INSERT INTO billing.widgets (id, name) VALUES (1, 'b-one'), (2, 'b-two'), (3, 'b-three');
	`)

	pgEng, _ := engines.Get("postgres")
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:         pgEng,
			Target:         pgEng,
			SourceDSN:      pgSource,
			TargetDSN:      pgTarget,
			StreamID:       "multischema-pg2pg",
			DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		}
	}

	// ---- Cold-start + steady-state ----
	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- newStreamer().Run(streamCtx) }()

	if !waitForPGSchemaCount(t, pgTarget, "sales", "widgets", 2, 60*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("cold-start never delivered sales.widgets")
	}
	if !waitForPGSchemaCount(t, pgTarget, "billing", "widgets", 3, 60*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("cold-start never delivered billing.widgets")
	}

	// Steady-state DML in BOTH schemas: insert/update/delete, routed per
	// source schema through the one slot.
	applyPGDDL(t, pgSource, `
		INSERT INTO sales.widgets   (id, name) VALUES (3, 'a-three');
		UPDATE sales.widgets   SET name='a-one-upd' WHERE id=1;
		INSERT INTO billing.widgets (id, name) VALUES (4, 'b-four');
		DELETE FROM billing.widgets WHERE id=2;
	`)

	// sales: 2 + 1 insert = 3. billing: 3 + 1 insert - 1 delete = 3.
	if !waitForPGSchemaCount(t, pgTarget, "sales", "widgets", 3, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC never delivered sales insert")
	}
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM billing.widgets`, 3, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC never settled billing (want 3)")
	}
	// Let the update/delete land deterministically.
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM sales.widgets WHERE name='a-one-upd'`, 1, 15*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC never routed the sales UPDATE")
	}

	// Cross-schema bleed guards: the UPDATE landed in sales only, the
	// DELETE in billing only.
	if got := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM billing.widgets WHERE name='a-one-upd'`); got != 0 {
		t.Errorf("cross-schema bleed: billing has sales' a-one-upd (%d); want 0", got)
	}
	if got := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM billing.widgets WHERE id=2`); got != 0 {
		t.Errorf("billing DELETE not routed: id=2 still present (%d); want 0", got)
	}

	// Stop the first stream cleanly (drain), capturing the persisted LSN.
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("first stream did not return after ctx cancel")
	}

	// ---- Write MORE while stopped, then restart → WARM-RESUME ----
	applyPGDDL(t, pgSource, `
		INSERT INTO sales.widgets   (id, name) VALUES (5, 'a-five');
		INSERT INTO billing.widgets (id, name) VALUES (6, 'b-six');
	`)

	// Record the target counts before restart so we can prove no re-copy
	// (a re-cold-start would TRUNCATE+re-copy; warm-resume must not).
	salesBefore := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM sales.widgets`)
	if salesBefore != 3 {
		t.Fatalf("pre-restart sales count = %d; want 3", salesBefore)
	}

	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- newStreamer().Run(resumeCtx) }()
	defer func() {
		resumeCancel()
		select {
		case <-resumeErr:
		case <-time.After(20 * time.Second):
			t.Error("resumed stream did not return after ctx cancel")
		}
	}()

	// The two while-stopped inserts must arrive via warm-resume (sales→4,
	// billing→4). If a re-cold-start had happened it would still converge
	// to these counts, so the zero-DUP assertion below (exact counts) is
	// the real warm-resume gate.
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM sales.widgets`, 4, 30*time.Second) {
		t.Fatalf("warm-resume never delivered the while-stopped sales insert")
	}
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM billing.widgets`, 4, 30*time.Second) {
		t.Fatalf("warm-resume never delivered the while-stopped billing insert")
	}

	// Zero loss / zero dup: EXACT source==target parity on both schemas.
	assertPGSchemaParity(t, pgSource, pgTarget, "sales", "widgets")
	assertPGSchemaParity(t, pgSource, pgTarget, "billing", "widgets")
}

// TestStreamer_MultiSchema_PostgresToMySQL is scenario (b): each source
// schema → a same-named MySQL database; cold-start + steady-state +
// warm-resume zero-loss.
func TestStreamer_MultiSchema_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgresLogicalMultiSchema(t)
	defer pgCleanup()
	_, mysqlHome, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.accounts   (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		CREATE TABLE billing.accounts (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		INSERT INTO sales.accounts   (id, tag) VALUES (1, 'src-1'), (2, 'src-2');
		INSERT INTO billing.accounts (id, tag) VALUES (1, 'bil-1'), (2, 'bil-2'), (3, 'bil-3');
	`)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	// mysqlHome names a real MySQL database (the home for sluice_cdc_state);
	// user data routes to per-source-schema MySQL databases (sales/billing)
	// auto-created via EnsureDatabase.
	mysqlServer := serverDSN(t, mysqlHome)
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:         pgEng,
			Target:         mysqlEng,
			SourceDSN:      pgSource,
			TargetDSN:      mysqlHome,
			StreamID:       "multischema-pg2my",
			DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		}
	}

	// ---- Cold-start + steady-state ----
	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- newStreamer().Run(streamCtx) }()

	if !waitForRowCountMySQLDB(t, mysqlServer, "sales", "accounts", 2, 60*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("cold-start never delivered sales.accounts to MySQL")
	}
	if !waitForRowCountMySQLDB(t, mysqlServer, "billing", "accounts", 3, 60*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("cold-start never delivered billing.accounts to MySQL")
	}

	applyPGDDL(t, pgSource, `
		INSERT INTO sales.accounts   (id, tag) VALUES (3, 'src-3');
		INSERT INTO billing.accounts (id, tag) VALUES (4, 'bil-4');
		UPDATE sales.accounts   SET tag='src-1-upd' WHERE id=1;
		DELETE FROM billing.accounts WHERE id=2;
	`)

	if !waitForRowCountMySQLDB(t, mysqlServer, "sales", "accounts", 3, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC never delivered sales insert to MySQL")
	}
	// billing: 3 + 1 - 1 = 3.
	if !waitForRowCountMySQLDB(t, mysqlServer, "billing", "accounts", 3, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC never settled billing on MySQL")
	}
	// The UPDATE routed to sales only. Poll for it — the UPDATE is a
	// distinct CDC event that may land just after the INSERT count settles.
	if !waitForMySQLDBScalar(t, mysqlServer, "sales", "SELECT COUNT(*) FROM accounts WHERE tag='src-1-upd'", 1, 15*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("sales UPDATE not routed: src-1-upd never reached 1")
	}
	// Cross-schema bleed: billing must not contain sales' update.
	if got := queryStringMySQL(t, mysqlServer, "billing", "SELECT COUNT(*) FROM accounts WHERE tag='src-1-upd'"); got != "0" {
		t.Errorf("cross-schema bleed: billing has sales' src-1-upd (%s); want 0", got)
	}

	// Stop, write more, restart → warm-resume.
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("first PG→MySQL stream did not return after ctx cancel")
	}

	applyPGDDL(t, pgSource, `
		INSERT INTO sales.accounts   (id, tag) VALUES (5, 'src-5');
		INSERT INTO billing.accounts (id, tag) VALUES (6, 'bil-6');
	`)

	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- newStreamer().Run(resumeCtx) }()
	defer func() {
		resumeCancel()
		select {
		case <-resumeErr:
		case <-time.After(20 * time.Second):
			t.Error("resumed PG→MySQL stream did not return after ctx cancel")
		}
	}()

	// sales: 3 → 4, billing: 3 → 4 via warm-resume.
	if !waitForRowCountMySQLDB(t, mysqlServer, "sales", "accounts", 4, 30*time.Second) {
		t.Fatalf("warm-resume never delivered while-stopped sales insert to MySQL")
	}
	if !waitForRowCountMySQLDB(t, mysqlServer, "billing", "accounts", 4, 30*time.Second) {
		t.Fatalf("warm-resume never delivered while-stopped billing insert to MySQL")
	}

	// Zero loss / zero dup: exact source==target parity.
	srcSales := pgScalarCount(pgSource, `SELECT COUNT(*) FROM sales.accounts`)
	srcBilling := pgScalarCount(pgSource, `SELECT COUNT(*) FROM billing.accounts`)
	tgtSales := mysqlDBRowCount(t, mysqlServer, "sales", "accounts")
	tgtBilling := mysqlDBRowCount(t, mysqlServer, "billing", "accounts")
	if srcSales != tgtSales {
		t.Errorf("sales zero-loss/dup FAIL: src=%d tgt=%d", srcSales, tgtSales)
	}
	if srcBilling != tgtBilling {
		t.Errorf("billing zero-loss/dup FAIL: src=%d tgt=%d", srcBilling, tgtBilling)
	}
}

// TestStreamer_MultiSchema_SingleSchemaBackCompat is scenario (c): a normal
// single-schema `sync start` (NO --*-schema flag) takes the single-schema
// path byte-identically — multiDatabaseMode() false, no scope/routing set.
func TestStreamer_MultiSchema_SingleSchemaBackCompat(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE gadgets (id BIGINT PRIMARY KEY, label TEXT NOT NULL);
		INSERT INTO gadgets (id, label) VALUES (1, 'x');
	`)

	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: pgSource,
		TargetDSN: pgTarget,
		StreamID:  "single-schema-backcompat",
		// No DatabaseFilter / AllDatabases — single-schema path.
	}
	if streamer.multiDatabaseMode() {
		t.Fatal("single-schema streamer reported multiDatabaseMode()=true")
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("single-schema streamer did not return after ctx cancel")
		}
	}()

	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM public.gadgets`, 1, 60*time.Second) {
		t.Fatalf("single-schema cold-start never delivered seed row")
	}
	applyPGDDL(t, pgSource, "INSERT INTO gadgets (id, label) VALUES (2, 'y');")
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM public.gadgets`, 2, 30*time.Second) {
		t.Fatalf("single-schema CDC never delivered second row")
	}
}

// TestStreamer_MultiSchema_OutOfScopeNeverApplied is scenario (d) — the
// scope pin. A schema NOT in --include-schema must never reach the target:
// neither cold-start (it's never copied) nor steady-state CDC (the reader's
// inScope predicate drops its events even though the DB-wide slot decodes
// them).
func TestStreamer_MultiSchema_OutOfScopeNeverApplied(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	// `sales` is selected; `secret` is NOT. Both carry a `vault` table.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA secret;
		CREATE TABLE sales.vault  (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
		CREATE TABLE secret.vault (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO sales.vault  (id, v) VALUES (1, 'ok-1');
		INSERT INTO secret.vault (id, v) VALUES (1, 'leak-1');
	`)

	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      pgTarget,
		StreamID:       "multischema-scope-pin",
		DatabaseFilter: DatabaseFilter{Include: []string{"sales"}},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("scope-pin streamer did not return after ctx cancel")
		}
	}()

	// Cold-start: sales lands; secret was never copied (its target schema
	// must not even exist).
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM sales.vault`, 1, 60*time.Second) {
		t.Fatalf("cold-start never delivered sales.vault")
	}

	// Steady-state: write to BOTH schemas. The out-of-scope `secret` write
	// must be dropped by the reader's inScope filter.
	applyPGDDL(t, pgSource, `
		INSERT INTO sales.vault  (id, v) VALUES (2, 'ok-2');
		INSERT INTO secret.vault (id, v) VALUES (2, 'leak-2');
	`)
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM sales.vault`, 2, 30*time.Second) {
		t.Fatalf("CDC never delivered the in-scope sales insert")
	}
	// Let any (wrongly-applied) out-of-scope event have a chance to land.
	time.Sleep(3 * time.Second)

	// The `secret` schema must NOT exist on the target (never copied, never
	// routed). information_schema is the authority.
	if n := pgScalarCount(pgTarget, `
		SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'secret'`); n != 0 {
		t.Errorf("out-of-scope schema 'secret' exists on target (%d); it must never be created or written", n)
	}
}

// --- helpers ---

// waitForPGScalar polls a scalar count query on the PG target until it
// equals want (exactly) or the timeout elapses.
func waitForPGScalar(t *testing.T, dsn, query string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pgScalarCount(dsn, query) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// waitForMySQLDBScalar polls a scalar count query against database on the
// MySQL server until it equals want (exactly) or the timeout elapses.
// Errors during a poll (e.g. database not yet created) read as a non-match.
func waitForMySQLDBScalar(t *testing.T, serverDSNStr, database, query string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlDBScalarSoft(serverDSNStr, database, query) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// mysqlDBScalarSoft returns the scalar int for query against database on
// the server, tolerating errors (returns -1) so a poll before the database
// exists doesn't fatal.
func mysqlDBScalarSoft(serverDSNStr, database, query string) int {
	dsn, err := buildMySQLDSN(serverDSNStr, database)
	if err != nil {
		return -1
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return -1
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return -1
	}
	return n
}

// assertPGSchemaParity asserts that schema.table holds EXACTLY the same row
// count on source and target — the zero-loss / zero-dup gate.
func assertPGSchemaParity(t *testing.T, srcDSN, tgtDSN, schema, table string) {
	t.Helper()
	q := "SELECT COUNT(*) FROM " + schema + "." + table
	src := pgScalarCount(srcDSN, q)
	tgt := pgScalarCount(tgtDSN, q)
	if src != tgt {
		t.Errorf("%s.%s zero-loss/dup FAIL: src=%d tgt=%d", schema, table, src, tgt)
	}
}

// TestStreamer_MultiSchema_BoundSchemaAndTruncateRouting closes the two
// routing-family gaps the ADR-0075 Phase 2b review flagged (Bug-74
// corollary): (1) the BOUND schema (public == the applier's a.schema, the
// routedSchema collapse branch) is in the selected set and must route
// correctly, and (2) a CDC TRUNCATE — a distinct change-type from
// insert/update/delete — must route to exactly one same-named table across
// schemas without bleeding. public.widgets and sales.widgets share a name;
// a TRUNCATE of one must leave the other intact.
func TestStreamer_MultiSchema_BoundSchemaAndTruncateRouting(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	// public is the BOUND schema (the connection default) AND selected.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE sales.widgets  (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO public.widgets (id, name) VALUES (1, 'p-one'), (2, 'p-two');
		INSERT INTO sales.widgets  (id, name) VALUES (1, 's-one'), (2, 's-two'), (3, 's-three');
	`)

	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source: pgEng, Target: pgEng,
		SourceDSN: pgSource, TargetDSN: pgTarget,
		StreamID:       "multischema-bound-truncate",
		DatabaseFilter: DatabaseFilter{Include: []string{"public", "sales"}},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() { streamCancel(); <-runErr }()

	// Cold-start delivers both same-named tables to their own namespaces.
	if !waitForPGSchemaCount(t, pgTarget, "public", "widgets", 2, 60*time.Second) {
		t.Fatalf("cold-start never delivered public.widgets (bound schema)")
	}
	if !waitForPGSchemaCount(t, pgTarget, "sales", "widgets", 3, 60*time.Second) {
		t.Fatalf("cold-start never delivered sales.widgets")
	}

	// Steady-state: an INSERT into the BOUND schema (exercises the
	// routedSchema collapse branch for emit+apply), plus a TRUNCATE of the
	// non-bound same-named table.
	applyPGDDL(t, pgSource, `
		INSERT INTO public.widgets (id, name) VALUES (3, 'p-three');
		TRUNCATE sales.widgets;
	`)

	// public got the insert (bound-schema routing): 2 + 1 = 3.
	if !waitForPGSchemaCount(t, pgTarget, "public", "widgets", 3, 30*time.Second) {
		t.Fatalf("CDC never routed the bound-schema (public) INSERT")
	}
	// sales was truncated → 0; the TRUNCATE must have routed to sales only.
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM sales.widgets`, 0, 30*time.Second) {
		t.Fatalf("CDC TRUNCATE never routed to sales.widgets (want 0 rows)")
	}
	// Bleed guard: public.widgets must be untouched by sales' TRUNCATE.
	if got := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM public.widgets`); got != 3 {
		t.Errorf("TRUNCATE bled across schemas: public.widgets = %d; want 3 (intact)", got)
	}
}

// TestStreamer_MultiSchema_SlotLossRefusesLoudly closes ADR-0075 Phase 2b
// review nit #2 — the multi-schema ErrPositionInvalid fall-through is now
// pinned. A PG failover/switchover (or pg_drop_replication_slot) loses the
// logical slot; on restart the multi-schema sync must (a) detect the
// invalid position and fall through to cold-start, and (b) hit the Bug-9
// cold-start preflight, which REFUSES LOUDLY because the target already
// holds data — exit non-zero, no silent partial re-copy, target untouched.
// (Rig-confirmed contract on v0.99.28: loud refusal, not silent loss and
// not auto-re-converge. The data-preserving recovery is --reset-target-data
// or a deliberate --force-cold-start.)
func TestStreamer_MultiSchema_SlotLossRefusesLoudly(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.widgets   (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE billing.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO sales.widgets   (id, name) VALUES (1, 'a-one');
		INSERT INTO billing.widgets (id, name) VALUES (1, 'b-one');
	`)

	pgEng, _ := engines.Get("postgres")
	newStreamer := func() *Streamer {
		return &Streamer{
			Source: pgEng, Target: pgEng,
			SourceDSN: pgSource, TargetDSN: pgTarget,
			StreamID:       "multischema-slotloss-refuse",
			DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		}
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- newStreamer().Run(streamCtx) }()
	if !waitForPGSchemaCount(t, pgTarget, "sales", "widgets", 1, 60*time.Second) ||
		!waitForPGSchemaCount(t, pgTarget, "billing", "widgets", 1, 60*time.Second) {
		streamCancel()
		<-runErr
		t.Fatal("initial cold-start never delivered both schemas")
	}
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("first stream did not return after ctx cancel")
	}

	if dropSluiceSlots(t, pgSource) == 0 {
		t.Skip("no sluice replication slot found to drop; cannot exercise the slot-loss path")
	}

	// Restart: warm-resume sees the missing slot → ErrPositionInvalid →
	// coldStartMultiDatabase → Bug-9 preflight refuses (target has data).
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- newStreamer().Run(resumeCtx) }()

	select {
	case err := <-resumeErr:
		if err == nil {
			t.Fatal("slot-loss restart returned nil; want a loud cold-start refusal (target already has data)")
		}
		if !strings.Contains(err.Error(), "already contains data") && !strings.Contains(err.Error(), "cold-start refused") {
			t.Errorf("slot-loss restart error = %v; want a Bug-9 cold-start refusal naming the populated target", err)
		}
	case <-time.After(60 * time.Second):
		resumeCancel()
		t.Fatal("slot-loss restart neither refused nor returned within 60s")
	}
}

// dropSluiceSlots drops every replication slot named like 'sluice%' on the
// source and returns how many it dropped. The slot must be inactive (the
// stream stopped) for the drop to succeed.
func dropSluiceSlots(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open source for slot drop: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'sluice%'")
	if err != nil {
		t.Fatalf("list replication slots: %v", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			_ = rows.Close()
			t.Fatalf("scan slot name: %v", err)
		}
		names = append(names, n)
	}
	_ = rows.Close()
	for _, n := range names {
		if _, err := db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", n); err != nil {
			t.Fatalf("drop slot %q: %v", n, err)
		}
	}
	return len(names)
}
