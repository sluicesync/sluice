//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Steady-state gap-freedom for the trigger CDC poller (cdc_gapfree.go),
// against real overlapping Postgres sessions.
//
// The class this file pins: the change-log `id` is a BIGSERIAL assigned
// at trigger time while `txid` is assigned at the transaction's FIRST
// write, so id order and xid order are INDEPENDENT. A hold-back that
// filters per row on `txid` therefore agrees with a watermark that
// advances to max(fetched id) only in the orientations where the two
// orders happen to coincide — which is why the predecessor test, built
// on the single orientation "the open transaction holds both the lower
// xid and the lower id", passed for the entire life of the defect.
//
// Every cell below is a real two-session overlap; the assertion is
// COMPLETENESS of the emitted set, not ordering.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// gapFreePollInterval keeps the matrix quick while still giving the
// poller several polls inside each overlap window (the hole guard needs
// holeProofPolls consecutive observations before it arms).
const gapFreePollInterval = 200 * time.Millisecond

// overlapWindow is how long each case leaves a transaction in flight —
// long enough for many polls at gapFreePollInterval, so a reader that
// is going to advance its watermark past an invisible id has every
// opportunity to do so.
const overlapWindow = 2 * time.Second

// openGapFreeReader boots a reader over dsn with the fast poll cadence
// and returns it plus its event channel, anchored at "from now".
func openGapFreeReader(t *testing.T, ctx context.Context, dsn string) <-chan ir.Change {
	t.Helper()
	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	reader.(*CDCReader).SetPollInterval(gapFreePollInterval)
	t.Cleanup(func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	return out
}

// setupCaptureTable creates a fresh per-case table and installs the
// capture triggers on it.
func setupCaptureTable(t *testing.T, ctx context.Context, dsn, table string) {
	t.Helper()
	applyPGSQL(t, dsn, fmt.Sprintf(`CREATE TABLE %s (id BIGINT PRIMARY KEY, label TEXT);`, table))
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{table}, Schema: "public"}); err != nil {
		t.Fatalf("Setup(%s): %v", table, err)
	}
}

// beginSession opens a dedicated pool + transaction. Each session has
// its own connection so the overlap is real from PG's perspective.
func beginSession(t *testing.T, ctx context.Context, dsn string) *sql.Tx {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// assignXID forces the transaction's xid to be assigned NOW, without
// writing to a captured table. This is what makes xid order
// controllable independently of change-log id order — and it is not a
// test contrivance: any transaction that writes to a non-replicated
// table (or takes a row lock) before touching a captured one gets the
// same shape.
func assignXID(t *testing.T, ctx context.Context, tx *sql.Tx, who string) int64 {
	t.Helper()
	var xid int64
	if err := tx.QueryRowContext(ctx, `SELECT pg_current_xact_id()::text::bigint`).Scan(&xid); err != nil {
		t.Fatalf("assign xid (%s): %v", who, err)
	}
	return xid
}

// insertLabel writes one captured row, allocating one change-log id.
func insertLabel(t *testing.T, ctx context.Context, tx *sql.Tx, table string, pk int, label string) {
	t.Helper()
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (id, label) VALUES ($1, $2)`, table), pk, label); err != nil {
		t.Fatalf("insert %s: %v", label, err)
	}
}

// collectLabels drains up to want events and returns the labels seen.
func collectLabels(t *testing.T, out <-chan ir.Change, want int) []string {
	t.Helper()
	got := drainEvents(t, out, want, 15*time.Second)
	labels := make([]string, 0, len(got))
	for _, ev := range got {
		ins, ok := ev.(ir.Insert)
		if !ok {
			t.Fatalf("got %T; want ir.Insert", ev)
		}
		labels = append(labels, fmt.Sprint(ins.Row["label"]))
	}
	return labels
}

// expectNoEvents asserts nothing is emitted for d — used while BOTH
// transactions are in flight, where no change-log row is committed and
// therefore none may be consumed.
func expectNoEvents(t *testing.T, out <-chan ir.Change, d time.Duration) {
	t.Helper()
	if early := drainEvents(t, out, 1, d); len(early) != 0 {
		t.Errorf("%d event(s) emitted while both transactions were in flight: %+v", len(early), early)
	}
}

// TestCDCReader_GapFreedom_OverlapMatrix drives every orientation of
// (xid assignment order × change-log id order × commit order) through
// real overlapping sessions and asserts that BOTH changes arrive.
//
// Against the per-row `txid < pg_snapshot_xmin(...)` hold-back this
// replaces, the four cells where the LATE-committing transaction holds
// the LOWER change-log id while the early-committing one holds the
// lower xid are silent losses: the early transaction's row passes the
// filter, the watermark advances past the invisible lower id, and that
// change never arrives on any poll or restart.
func TestCDCReader_GapFreedom_OverlapMatrix(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Each cell names which session (A or B) goes first on each axis.
	cases := []struct{ xidFirst, idFirst, commitFirst string }{
		{"A", "A", "A"},
		{"A", "A", "B"},
		{"A", "B", "A"},
		{"A", "B", "B"},
		{"B", "A", "A"},
		{"B", "A", "B"},
		{"B", "B", "A"},
		{"B", "B", "B"},
	}
	for i, tc := range cases {
		name := fmt.Sprintf("xid%s_id%s_commit%s", tc.xidFirst, tc.idFirst, tc.commitFirst)
		t.Run(name, func(t *testing.T) {
			table := fmt.Sprintf("overlap_%d", i)
			setupCaptureTable(t, ctx, dsn, table)
			out := openGapFreeReader(t, ctx, dsn)

			txA := beginSession(t, ctx, dsn)
			txB := beginSession(t, ctx, dsn)
			pick := func(who string) *sql.Tx {
				if who == "A" {
					return txA
				}
				return txB
			}
			other := func(who string) string {
				if who == "A" {
					return "B"
				}
				return "A"
			}

			// 1. Fix the xid assignment order.
			assignXID(t, ctx, pick(tc.xidFirst), tc.xidFirst)
			assignXID(t, ctx, pick(other(tc.xidFirst)), other(tc.xidFirst))

			// 2. Fix the change-log id order.
			insertLabel(t, ctx, pick(tc.idFirst), table, 1, tc.idFirst+"-first")
			insertLabel(t, ctx, pick(other(tc.idFirst)), table, 2, other(tc.idFirst)+"-second")

			// Nothing is committed yet, so nothing may be emitted.
			expectNoEvents(t, out, overlapWindow)

			// 3. Commit one, leave the other in flight across several
			// polls — the window where the watermark must NOT advance
			// past the still-invisible id.
			if err := pick(tc.commitFirst).Commit(); err != nil {
				t.Fatalf("commit %s: %v", tc.commitFirst, err)
			}
			time.Sleep(overlapWindow)
			if err := pick(other(tc.commitFirst)).Commit(); err != nil {
				t.Fatalf("commit %s: %v", other(tc.commitFirst), err)
			}

			want := []string{tc.idFirst + "-first", other(tc.idFirst) + "-second"}
			got := collectLabels(t, out, len(want))
			assertSameSet(t, got, want)
		})
	}
}

// TestCDCReader_GapFreedom_InterleavedMultiStatement is the ADR-0066
// repro, verbatim: a multi-statement transaction whose change-log ids
// STRADDLE a concurrent transaction's id.
//
//	T_A: BEGIN; INSERT a;   -- xid 100, change-log id 1
//	T_B: BEGIN; INSERT c;   -- xid 101, change-log id 2  (stays open)
//	T_A: INSERT b; COMMIT;  -- xid 100, change-log id 3
//
// A poll while T_B is open sees ids 1 and 3 — both committed, both with
// txid < the snapshot's xmin — and cannot see id 2. Consuming 1 and 3
// strands id 2 behind the durable watermark forever. No pre-assigned
// xid, no unreplicated table: two ordinary concurrent transactions.
func TestCDCReader_GapFreedom_InterleavedMultiStatement(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	setupCaptureTable(t, ctx, dsn, "interleaved")
	out := openGapFreeReader(t, ctx, dsn)

	txA := beginSession(t, ctx, dsn)
	txB := beginSession(t, ctx, dsn)

	insertLabel(t, ctx, txA, "interleaved", 1, "a")
	insertLabel(t, ctx, txB, "interleaved", 2, "c")
	insertLabel(t, ctx, txA, "interleaved", 3, "b")
	if err := txA.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	// T_B is still open and holds the middle id. 'a' is the contiguous
	// prefix and may legitimately be consumed; 'b' sits ABOVE the
	// invisible id, so consuming it would strand 'c' forever.
	early := drainEvents(t, out, 2, overlapWindow)
	if len(early) > 1 {
		t.Fatalf("consumed past the in-flight change-log id while T_B was open (%d events) — 'c' is now stranded behind the watermark: %+v", len(early), early)
	}
	remaining := []string{"a", "c", "b"}
	if len(early) == 1 {
		if lbl := fmt.Sprint(early[0].(ir.Insert).Row["label"]); lbl != "a" {
			t.Fatalf("first event = %q; want the contiguous prefix 'a'", lbl)
		}
		remaining = []string{"c", "b"}
	}
	if err := txB.Commit(); err != nil {
		t.Fatalf("commit B: %v", err)
	}
	assertSameSet(t, collectLabels(t, out, len(remaining)), remaining)
}

// TestCDCReader_GapFreedom_WaitsOnAnInFlightHole is the negative half
// of the hole protocol, and the one that keeps the fix honest: a hole
// whose allocating transaction is merely IN FLIGHT must never be
// skipped, however long the poller sits on it. A guard that skipped on
// a timeout, a poll count, or an unproven settle probe would emit the
// later row here, advance the watermark, and lose the earlier one.
//
// The row ABOVE the hole must be committed by a transaction whose xid
// was assigned BEFORE the held one's — otherwise the conservative
// settled-ceiling arm hides that row from the poll entirely and the
// hole protocol is never reached. (The first cut of this test did
// exactly that and stayed green against a guard mutated to skip
// without a proof; the arrangement below is what makes it bite.)
func TestCDCReader_GapFreedom_WaitsOnAnInFlightHole(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	setupCaptureTable(t, ctx, dsn, "inflight_hole")
	out := openGapFreeReader(t, ctx, dsn)

	early := beginSession(t, ctx, dsn)
	assignXID(t, ctx, early, "early")

	held := beginSession(t, ctx, dsn)
	insertLabel(t, ctx, held, "inflight_hole", 1, "held")

	// The committed row ABOVE the hole. It is exactly the evidence the
	// guard uses to prove the hole's row was already allocated, so this
	// is also the shape where a too-eager guard would skip.
	insertLabel(t, ctx, early, "inflight_hole", 2, "above")
	if err := early.Commit(); err != nil {
		t.Fatalf("commit early: %v", err)
	}

	// Well past holeProofPolls polls plus the bound assignment and the
	// settle probe, which must keep reporting the held transaction.
	expectNoEvents(t, out, 5*time.Second)

	if err := held.Commit(); err != nil {
		t.Fatalf("commit held: %v", err)
	}
	assertSameSet(t, collectLabels(t, out, 2), []string{"held", "above"})
}

// TestCDCReader_GapFreedom_SkipsAnAbortedHole is the positive half: a
// hole left by a ROLLED-BACK write is permanent, and the stream must
// prove that and move on. Without the proof the contiguity rule would
// wedge the stream on the first rolled-back write to a captured table —
// trading a silent gap for a silent stall.
func TestCDCReader_GapFreedom_SkipsAnAbortedHole(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	setupCaptureTable(t, ctx, dsn, "aborted_hole")
	out := openGapFreeReader(t, ctx, dsn)

	doomed := beginSession(t, ctx, dsn)
	insertLabel(t, ctx, doomed, "aborted_hole", 1, "rolled-back")
	insertLabel(t, ctx, doomed, "aborted_hole", 2, "rolled-back-2")
	if err := doomed.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	applyPGSQL(t, dsn, `INSERT INTO aborted_hole (id, label) VALUES (3, 'after')`)

	assertSameSet(t, collectLabels(t, out, 1), []string{"after"})
}

// The CACHE 1 preflight pin used to live here as
// TestCDCReader_RefusesNonUnitSequenceCache. It was WIDENED and moved to
// cdc_seqconfig_integration_test.go on 2026-08-07: CACHE is one of FOUR
// sequence settings that can put an id at or below the stream's
// watermark, and a gate covering one of four reads as broader than it
// is. See TestCDCReader_RefusesSequenceConfigThatCanReissueIDs (which
// carries the CACHE cell unchanged, plus the other three) and the defect
// proof TestChangeLogAnchor_MinValueZeroStrandsTheChange.

// assertSameSet compares emitted labels against the expected set,
// naming what went missing — a dropped change is the failure this file
// exists to catch, so the message says so explicitly.
func assertSameSet(t *testing.T, got, want []string) {
	t.Helper()
	seen := make(map[string]int, len(got))
	for _, g := range got {
		seen[g]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			t.Errorf("change %q never arrived (got %v, want %v) — a committed change was consumed past and lost", w, got, want)
		}
		delete(seen, w)
	}
	for extra, n := range seen {
		t.Errorf("unexpected change %q emitted %d time(s) (got %v, want %v)", extra, n, got, want)
	}
}
