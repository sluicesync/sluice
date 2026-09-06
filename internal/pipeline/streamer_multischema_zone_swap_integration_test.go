//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SLM-1d on real containers: the MULTI-SCHEMA Postgres lane's twin of
// TestStreamer_PGSource_StoppedStreamZoneSwap.
//
// The single-stream pin next door proves the seeded first-boundary door
// works. It could not prove anything about a `--schemas` stream, because
// that stream opened its reader through
// [Streamer.coldStartMultiDatabase] / [Streamer.warmResumeMultiDatabase],
// and neither wired a seed at all — the reader ran with schemaSeed == nil,
// the door returned nil for want of a prior, and the first RelationMessage
// primed. Audit 2026-09-06 finding 1 measured the consequence on
// postgres:16 and this pin reproduces exactly that flow:
//
//	cold start over {sales, billing} → one applied CDC transaction →
//	a clean stop past its post-commit position → on the source, under
//	`SET TIME ZONE 'Asia/Tokyo'`, `ALTER TABLE sales.events ALTER COLUMN
//	c TYPE timestamp` plus one INSERT → warm resume.
//
// Before the fix that resume returned nil, applied row 4 into the target's
// unchanged `timestamp with time zone` column, and left source row 1
// reading 21:00 UTC against a target reading 12:00 — a nine-hour silent
// divergence at exit 0. It must now REFUSE, naming the qualified relation.
//
// # Why the namespace in the refusal text is load-bearing here
//
// Both schemas hold a same-named `events`. The seed is keyed by
// (namespace, table): a fan-out seed that dropped the namespace would
// collide the two, and the assertion that the refusal names
// `sales.events` — while `billing.events`, untouched, is still streaming
// its own rows — is what distinguishes a correctly keyed seed from one
// that happened to refuse on the other schema's prior.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestStreamer_MultiSchema_StoppedStreamZoneSwapRefuses(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	// Two schemas, each with a SAME-NAMED events table carrying a
	// zone-aware column and three rows at the instant 12:00 UTC.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.events   (id BIGINT PRIMARY KEY, c TIMESTAMPTZ NOT NULL);
		CREATE TABLE billing.events (id BIGINT PRIMARY KEY, c TIMESTAMPTZ NOT NULL);
		ALTER TABLE sales.events   REPLICA IDENTITY FULL;
		ALTER TABLE billing.events REPLICA IDENTITY FULL;
		INSERT INTO sales.events   (id, c) VALUES (1, '2020-01-01 12:00:00+00'), (2, '2020-01-01 12:00:00+00');
		INSERT INTO billing.events (id, c) VALUES (1, '2020-01-01 12:00:00+00');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	const streamID = "slm1d-multischema"
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:         pgEng,
			Target:         pgEng,
			SourceDSN:      pgSource,
			TargetDSN:      pgTarget,
			StreamID:       streamID,
			DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		}
	}

	// ---- Run 1: cold start, one applied CDC transaction, clean stop ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	errc1 := make(chan error, 1)
	go func() { errc1 <- newStreamer().Run(ctx1) }()

	if !waitForPGSchemaCount(t, pgTarget, "sales", "events", 2, 90*time.Second) ||
		!waitForPGSchemaCount(t, pgTarget, "billing", "events", 1, 90*time.Second) {
		cancel1()
		<-errc1
		t.Fatal("cold start never delivered the seed rows")
	}
	anchor := waitMultiSchemaPosition(t, pgEng, pgTarget, streamID)
	applyPGDDL(t, pgSource, `INSERT INTO sales.events (id, c) VALUES (100, '2020-01-01 12:00:00+00');`)
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM sales.events WHERE id=100`, 1, 90*time.Second) {
		cancel1()
		<-errc1
		t.Fatal("the CDC row never landed")
	}
	// A clean `sync stop --wait` resumes at the NEXT transaction, so the
	// swap below arrives as the table's first RelationMessage — the shape
	// the door has to catch.
	waitMultiSchemaPositionPast(t, pgEng, pgTarget, streamID, anchor)
	cancel1()
	select {
	case <-errc1:
	case <-time.After(60 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	// ---- The source-only swap while the stream is stopped ----
	// The mechanism is asserted on the source before any resume: PG casts
	// every stored value against the EXECUTING SESSION's TimeZone, so a
	// timestamptz→timestamp ALTER under Asia/Tokyo moves the 12:00 UTC
	// instant to the naive wall clock 21:00. Nothing on the pgoutput wire
	// carries that session zone, so the target cannot reproduce it.
	applyPGDDL(t, pgSource, `
		SET TIME ZONE 'Asia/Tokyo';
		ALTER TABLE sales.events ALTER COLUMN c TYPE timestamp;
		INSERT INTO sales.events (id, c) VALUES (4, '2020-01-01 12:00:00');
	`)
	if got := pgTextAtUTC(t, pgSource, "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM sales.events WHERE id = 1"); got != "2020-01-01 21:00:00" {
		t.Fatalf("mechanism premise: source sales.events row 1 reads %q under UTC after the ALTER ran under Asia/Tokyo; want \"2020-01-01 21:00:00\" — the session-TimeZone cast this refusal exists for did not happen", got)
	}

	// ---- Run 2: the warm resume must REFUSE ----
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	errc2 := make(chan error, 1)
	go func() { errc2 <- newStreamer().Run(ctx2) }()

	var err error
	select {
	case err = <-errc2:
	case <-time.After(120 * time.Second):
		landed := "absent"
		if pgScalarCount(pgTarget, `SELECT COUNT(*) FROM sales.events WHERE id=4`) > 0 {
			landed = "LANDED in the target's unchanged column (" +
				pgTextAtUTC(t, pgTarget, "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM sales.events WHERE id = 4") + " under UTC)"
		}
		cancel2()
		<-errc2
		t.Fatalf("the multi-schema warm resume did not refuse the stopped-stream zone swap within 120s — it primed. "+
			"target sales.events.c is still %q, the post-swap row 4 is %s, and pre-existing row 1 reads %q on the target vs %q on the source",
			pgQualifiedColumnType(t, pgTarget, "sales", "events", "c"), landed,
			pgTextAtUTC(t, pgTarget, "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM sales.events WHERE id = 1"),
			pgTextAtUTC(t, pgSource, "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM sales.events WHERE id = 1"))
	}
	if err == nil {
		t.Fatal("the multi-schema warm resume returned nil; want the session-TimeZone cast refusal on sales.events")
	}
	for _, want := range []string{"cannot be forwarded", "sales.events", `column "c"`, "TimeZone", "while the stream was stopped", "drained model"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q; got: %v", want, err)
		}
	}
	// Nothing moved: the target column is untouched and the post-swap row
	// never landed.
	if got := pgQualifiedColumnType(t, pgTarget, "sales", "events", "c"); got != "timestamp with time zone" {
		t.Errorf("target sales.events.c is %q after the refusal; want it untouched at \"timestamp with time zone\"", got)
	}
	if n := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM sales.events WHERE id=4`); n != 0 {
		t.Errorf("the post-swap row landed in the zone-mismatched column (%d rows); want 0", n)
	}
	// The sibling namespace's same-named table was never the subject: a seed
	// keyed on the bare name would have compared sales' prior against
	// billing's relation (or the reverse) and named the wrong one.
	if strings.Contains(err.Error(), "billing.events") {
		t.Errorf("the refusal names billing.events; the seed collided the two same-named tables: %v", err)
	}
}

// waitMultiSchemaPosition blocks until the stream has a persisted position.
func waitMultiSchemaPosition(t *testing.T, eng ir.Engine, targetDSN, streamID string) ir.Position {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if pos, ok := readMultiSchemaPosition(t, eng, targetDSN, streamID); ok {
			return pos
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no persisted position within 60s")
	return ir.Position{}
}

// waitMultiSchemaPositionPast blocks until the persisted position has moved
// off `from` — the applied transaction's post-commit write has landed, so a
// clean stop resumes AFTER it rather than re-delivering it.
func waitMultiSchemaPositionPast(t *testing.T, eng ir.Engine, targetDSN, streamID string, from ir.Position) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if pos, ok := readMultiSchemaPosition(t, eng, targetDSN, streamID); ok && pos.Token != from.Token {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("persisted position did not move past %s within 60s", from.Token)
}

func readMultiSchemaPosition(t *testing.T, eng ir.Engine, targetDSN, streamID string) (ir.Position, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	app, err := eng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open applier: %v", err)
	}
	defer closeIfErrIgnoredForTest(app)
	pos, ok, err := app.ReadPosition(ctx, streamID)
	if err != nil {
		t.Fatalf("read position: %v", err)
	}
	return pos, ok
}

// pgTextAtUTC renders a value through a UTC session — the independent
// expected value for every comparison in this pin.
func pgTextAtUTC(t *testing.T, dsn, query string) string {
	t.Helper()
	db := openPGForTest(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET TIME ZONE 'UTC'"); err != nil {
		t.Fatalf("pin UTC session: %v", err)
	}
	var s string
	if err := conn.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return s
}

func pgQualifiedColumnType(t *testing.T, dsn, schema, table, column string) string {
	t.Helper()
	db := openPGForTest(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var dt string
	const q = `SELECT data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name=$3`
	if err := db.QueryRowContext(ctx, q, schema, table, column).Scan(&dt); err != nil {
		t.Fatalf("column type %s.%s.%s: %v", schema, table, column, err)
	}
	return strings.ToLower(dt)
}

func openPGForTest(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %q: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
