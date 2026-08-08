//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The PG incremental replay floor (2026-08-07 invariant sweep). Three
// places in the tree rest a safety argument on one fact about the
// PostgreSQL walsender:
//
//   - `pipeline.windowAdvancedPast` — "logical decoding begins at
//     max(requested_lsn, confirmed_flush_lsn) … so the REQUESTED LSN, not
//     the flush point, is the floor and delivered positions never regress
//     below start."
//   - `Engine.PreflightChainResume` — "a confirmed_flush_lsn at or behind
//     the parent position is healthy … the WAL is still retained,
//     PostgreSQL replays from the requested LSN." This is the ELSE of a
//     silent-loss refusal: everything at-or-behind is waved through.
//   - `pipeline.rotationBoundaryResumeStart` — "no skipThrough is needed: a
//     fresh StreamChanges(P_N) delivers nothing BELOW P_N", which declines
//     to build a filter BECAUSE of the floor.
//
// Its only evidence was one hand-run transcript pasted into a comment, and
// the branch that acts on it (`flushLSN <= fromLSN` → proceed) had no
// server-touching test at all: the existing slot tests cover the AHEAD
// refusal, the missing slot, and the happy path where the previous run's
// ack landed.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// floorSlot is this test's own slot name so it cannot collide with the
// package's shared `sluice_slot` users on the shared container.
const floorSlot = "replay_floor_slot"

// queryOneString runs a single-value query against dsn.
func queryOneString(t *testing.T, dsn, query string, args ...any) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var out string
	if err := db.QueryRow(query, args...).Scan(&out); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return out
}

// TestChainResume_WalsenderFloorsAtTheRequestedLSN_AndThePreflightPassesThatShape
// is the named test the replay-floor premise now has.
//
// IT BINDS TWO HALVES ON PURPOSE. Pinning "PostgreSQL floors at the
// requested LSN" and pinning "sluice's preflight waves a trailing
// confirmed_flush through" as separate tests would leave the ARGUMENT
// unpinned — if the server's behaviour changed, the classification test
// would keep passing. So one test provokes the exact slot state the
// preflight's else-branch describes, asserts that the preflight really
// classifies it as healthy, and THEN asks the server what it delivers.
//
// THE ANTI-VACUITY STEP IS THE POINT. `confirmed_flush_lsn` must be
// STRICTLY BEHIND the requested LSN before anything is asserted. Without
// that check this degrades into the steady-state test that is already green
// — the shape where the flush point and the request coincide, in which the
// floor is untestable because both answers agree.
func TestChainResume_WalsenderFloorsAtTheRequestedLSN_AndThePreflightPassesThatShape(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE floor_t (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		ALTER TABLE floor_t REPLICA IDENTITY FULL;
	`)
	// The publication must predate the transactions below, so that a
	// walsender which DID start at the flush point would genuinely decode
	// and deliver them. Creating it afterwards would make the "absent"
	// assertion pass for the wrong reason.
	applyPGSQL(t, dsn, `CREATE PUBLICATION sluice_pub FOR ALL TABLES;`)

	slotLSN := queryOneString(t, dsn,
		`SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')`, floorSlot)
	t.Cleanup(func() {
		// Best effort: the slot must not outlive the test on a shared
		// container.
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return
		}
		defer func() { _ = db.Close() }()
		_, _ = db.Exec(`SELECT pg_drop_replication_slot($1)`, floorSlot)
	})

	// Two committed transactions the slot has NOT acked. Nothing ever
	// connected to this slot, so confirmed_flush_lsn is still where the slot
	// was created — strictly behind everything below.
	applyPGSQL(t, dsn, `INSERT INTO floor_t (id, tag) VALUES (1, 'before-A');`)
	applyPGSQL(t, dsn, `INSERT INTO floor_t (id, tag) VALUES (2, 'before-B');`)

	requested := queryOneString(t, dsn, `SELECT pg_current_wal_lsn()::text`)
	confirmedFlush := queryOneString(t, dsn,
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, floorSlot)

	reqLSN, err := pglogrepl.ParseLSN(requested)
	if err != nil {
		t.Fatalf("parse requested LSN %q: %v", requested, err)
	}
	flushLSN, err := pglogrepl.ParseLSN(confirmedFlush)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", confirmedFlush, err)
	}
	// ANTI-VACUITY: this is the branch under test, and it only exists when
	// the flush point strictly trails the request.
	if !(flushLSN < reqLSN) {
		t.Fatalf("precondition not met: confirmed_flush_lsn %s is not STRICTLY BEHIND the requested LSN %s "+
			"(slot created at %s) — this run would test the steady-state shape, where the floor is untestable "+
			"because both candidate answers coincide", confirmedFlush, requested, slotLSN)
	}
	t.Logf("provoked the branch: slot created at %s, confirmed_flush_lsn %s, requesting %s (two committed, unacked transactions in between)",
		slotLSN, confirmedFlush, requested)

	pos := ir.Position{
		Engine: engineNamePostgres,
		Token:  `{"slot":"` + floorSlot + `","lsn":"` + requested + `"}`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// HALF ONE — sluice's classification. The preflight must call this shape
	// healthy and proceed; it is the else of the AHEAD refusal, and its
	// stated reason is the floor.
	eng := Engine{}
	if err := eng.PreflightChainResume(ctx, dsn, pos); err != nil {
		t.Fatalf("PreflightChainResume refused a confirmed_flush_lsn BEHIND the parent position: %v.\n"+
			"That shape is documented as healthy (the previous run's final ack did not land before its connection "+
			"closed) and refusing it would break every such chain resume", err)
	}

	// HALF TWO — the server's behaviour, which is what makes half one safe.
	rdr, err := eng.OpenCDCReaderWithSlot(ctx, dsn, floorSlot)
	if err != nil {
		t.Fatalf("OpenCDCReaderWithSlot: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, pos)
	if err != nil {
		t.Fatalf("StreamChanges at the requested LSN: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	applyPGSQL(t, dsn, `INSERT INTO floor_t (id, tag) VALUES (3, 'after-C');`)

	// Ask for THREE so a walsender that started at the flush point has room
	// to deliver the two unacked transactions before the timeout; exactly one
	// must arrive.
	got := drainChanges(t, ctx, changes, 3, 20*time.Second)
	if len(got) != 1 {
		for _, c := range got {
			t.Logf("delivered: %T %+v", c, c)
		}
		t.Fatalf("the stream delivered %d row changes from the requested LSN; want exactly 1 (only 'after-C'). "+
			"More than one means PostgreSQL started at confirmed_flush_lsn rather than the requested LSN — the "+
			"replay floor is NOT the request, and `windowAdvancedPast` / `rotationBoundaryResumeStart` / "+
			"`PreflightChainResume`'s at-or-behind pass-through all rest on it", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("delivered %T; want ir.Insert", got[0])
	}
	if ins.Row["id"] != int64(3) {
		t.Fatalf("delivered id=%v (tag=%v); want id=3 ('after-C'). A lower id means WAL below the requested LSN "+
			"was replayed, which would double-count the parent segment's own content into this window",
			ins.Row["id"], ins.Row["tag"])
	}
	// And the position it carries must not sit below the request either —
	// the half of the claim `windowAdvancedPast` reads as "delivered
	// positions never regress below start".
	decoded, okPos, err := decodePGPos(ins.Pos())
	if err != nil || !okPos {
		t.Fatalf("decode delivered position: ok=%v err=%v", okPos, err)
	}
	gotLSN, err := pglogrepl.ParseLSN(decoded.LSN)
	if err != nil {
		t.Fatalf("parse delivered LSN %q: %v", decoded.LSN, err)
	}
	if gotLSN < reqLSN {
		t.Fatalf("delivered position %s is BELOW the requested LSN %s — a window can record an EndPosition "+
			"behind its own StartPosition and the lineage contiguity check trips on a chain that is fine",
			decoded.LSN, requested)
	}
	t.Logf("PROVEN: requested %s with confirmed_flush_lsn %s; PostgreSQL delivered only id=3 at %s (>= the request), "+
		"and PreflightChainResume classified the trailing-flush slot as healthy", requested, confirmedFlush, decoded.LSN)
}
