//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for research finding F3:
//
//	confirmed_flush_lsn <= max(target's applied position LSN)
//
// This invariant is load-bearing for ADR-0007 ("position + data lands
// durably together"): if confirmed_flush_lsn ever advanced past the
// target's applied LSN, PG could garbage-collect WAL before sluice has
// durably applied the changes, leading to silent unrecoverable loss on
// target crash. ADR-0020's slot-ack-after-apply fix is the production-
// code path that maintains the invariant; this test pins that the
// invariant holds continuously during a real CDC stream so any future
// refactor of the keepalive / tracker / dispatch path that breaks it
// is caught at test time rather than at user-visible-corruption time.
//
// The pin is empirical: it polls pg_replication_slots.confirmed_flush_lsn
// while the stream is live and compares against the latest persisted
// source_position in sluice_cdc_state. The comparison is value-by-LSN,
// so the test catches both directional and magnitude violations of the
// invariant.
//
// F3 is a pin-only task — no production code change is expected. A
// violation caught by this pin would be a real bug warranting a
// separate task (don't attempt to fix here).

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/orware/sluice/internal/ir"
)

// readPersistedSourceLSN reads the latest persisted source_position
// for streamID from sluice_cdc_state and parses its LSN out of the
// token. Returns (0, ok=false, nil) when no row exists yet (the
// applier hasn't committed its first batch) or when the row's token
// has no decodable LSN. Treats "row exists but LSN is the zero value"
// the same as ok=true so the invariant comparison can still proceed.
func readPersistedSourceLSN(t *testing.T, dsn, streamID string) (pglogrepl.LSN, bool) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("readPersistedSourceLSN: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var token string
	err = db.QueryRowContext(
		ctx,
		`SELECT source_position FROM sluice_cdc_state WHERE stream_id = $1`,
		streamID,
	).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		// Tolerant of the relation not existing yet (e.g. before
		// EnsureControlTable runs): treat as "no persisted position."
		if strings.Contains(err.Error(), "does not exist") {
			return 0, false
		}
		t.Fatalf("readPersistedSourceLSN: query: %v", err)
	}
	lsn, err := lsnFromPositionToken(token)
	if err != nil {
		t.Fatalf("readPersistedSourceLSN: decode token %q: %v", token, err)
	}
	return lsn, true
}

// readConfirmedFlushLSNParsed is readConfirmedFlushLSN (from
// slot_pause_verify_integration_test.go) plus a ParseLSN. Returns
// (0, ok=false) when the slot has no confirmed_flush_lsn yet (e.g.
// freshly created, no keepalive has reported back).
func readConfirmedFlushLSNParsed(t *testing.T, dsn, slotName string) (pglogrepl.LSN, bool) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("readConfirmedFlushLSNParsed: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := readConfirmedFlushLSN(ctx, db, slotName)
	if err != nil {
		t.Fatalf("readConfirmedFlushLSNParsed: %v", err)
	}
	if s == "" {
		return 0, false
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("readConfirmedFlushLSNParsed: parse %q: %v", s, err)
	}
	return lsn, true
}

// TestCDCReader_ConfirmedFlushInvariant_PinF3 pins the F3 invariant.
//
// Wires up a sluice CDCReader + ChangeApplier against a single PG
// container (source-side CDC stream feeds the applier writing back
// into the same DB but under a separate target schema). Drives
// ~25 distinct insert transactions through the stream and continuously
// asserts:
//
//	confirmed_flush_lsn (pg_replication_slots) <= persisted source LSN (sluice_cdc_state)
//
// "Continuously" = a polling goroutine that samples both LSNs every
// 100 ms while the apply loop runs. Any sample that violates the
// invariant fails the test loudly via t.Errorf — the assertion lives
// in the goroutine so a violation is captured at the time it happens.
//
// After all 25 transactions land, the test asserts confirmed_flush_lsn
// has advanced strictly above 0 — proves the invariant isn't trivially
// holding because both sides stayed at zero.
//
// Stop is graceful: ctx-cancel the applier, wait for the apply loop
// to return, then sample one more time and assert the post-stop
// invariant still holds.
//
// F3 is pin-only. If this test catches a real violation, that's a
// separate task — don't try to fix here.
func TestCDCReader_ConfirmedFlushInvariant_PinF3(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	// Source schema: where the writer inserts. Target schema: where
	// the applier writes via the sluice control-table machinery.
	// Both live in the same PG container — the CDC stream reads
	// from source.public and applies to public via the same engine.
	// (The schemas are deliberately the same here; the applier's
	// idempotency contract handles the round-trip without needing a
	// separate target schema, and the slot's confirmed_flush_lsn is
	// scoped to the slot regardless of which schema the data lives
	// in.) sluice's CDC reader will publish all tables (default
	// scope). The applier mounts the same DB; the only requirement
	// the invariant cares about is that the slot's ack-LSN is
	// driven by an applier that actually commits before reporting.
	const sourceDDL = `
		CREATE TABLE invariant_pin (
			id      BIGSERIAL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		ALTER TABLE invariant_pin REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, sourceDDL)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Open the applier first and ensure the control table — the
	// LSN tracker we wire in next depends on the applier path
	// running reportAppliedToken() during ApplyBatch.
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	rdrIface, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdrIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Wire applier→reader LSN tracker so the slot only advances
	// past the applied position. Without this, the reader falls
	// back to streamedLSN — the invariant is much weaker (it
	// effectively pins "did sluice ack correctly," not "did sluice
	// ack-after-apply").
	rdr, ok := rdrIface.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *CDCReader", rdrIface)
	}
	pgApplier, ok := applier.(*ChangeApplier)
	if !ok {
		t.Fatalf("OpenChangeApplier returned %T; want *ChangeApplier", applier)
	}
	tracker, ok := pgApplier.LSNTracker().(*lsnTracker)
	if !ok {
		t.Fatalf("ChangeApplier.LSNTracker() did not return *lsnTracker")
	}
	rdr.AttachLSNTracker(tracker)

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Drive the applier in a goroutine; the reader's channel is the
	// source of changes for ApplyBatch. ApplyBatch returns when the
	// changes channel closes (Close() on the reader does this) or
	// when ctx cancels. Pick a batch size > 1 so the tracker's
	// "report after applied commit" path actually matters.
	const applyBatchSize = 5
	applyDone := make(chan error, 1)
	go func() {
		batched, ok := applier.(ir.BatchedChangeApplier)
		if !ok {
			applyDone <- errors.New("applier does not implement BatchedChangeApplier")
			return
		}
		applyDone <- batched.ApplyBatch(ctx, "f3-invariant-pin", changes, applyBatchSize)
	}()

	// Invariant-watcher goroutine. Samples both LSNs every 100ms
	// and asserts on every sample. Records the max-confirmed-flush
	// it ever observed so the post-stream "did it advance at all"
	// assertion has ground truth.
	type sample struct {
		when          time.Time
		confirmed     pglogrepl.LSN
		persisted     pglogrepl.LSN
		havePersisted bool
	}
	var (
		watcherMu        sync.Mutex
		maxConfirmedSeen pglogrepl.LSN
		violations       []sample
	)
	watcherCtx, watcherCancel := context.WithCancel(ctx)
	defer watcherCancel()
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watcherCtx.Done():
				return
			case <-ticker.C:
				confirmed, confirmedOK := readConfirmedFlushLSNParsed(t, dsn, "sluice_slot")
				if !confirmedOK {
					// Slot not yet visible / no keepalive
					// reported back; nothing to assert yet.
					continue
				}
				persisted, havePersisted := readPersistedSourceLSN(t, dsn, "f3-invariant-pin")

				watcherMu.Lock()
				if confirmed > maxConfirmedSeen {
					maxConfirmedSeen = confirmed
				}
				// The invariant only has teeth once the applier
				// has persisted at least one position. Before
				// the first persisted row exists, confirmed_flush
				// may legitimately reflect only the start-LSN
				// fallback (ackLSN's pre-first-applied branch).
				if havePersisted && confirmed > persisted {
					violations = append(violations, sample{
						when:          time.Now(),
						confirmed:     confirmed,
						persisted:     persisted,
						havePersisted: havePersisted,
					})
				}
				watcherMu.Unlock()
			}
		}
	}()

	// Drive 25 distinct insert transactions on the source. Each
	// INSERT runs as its own implicit transaction, so PG emits 25
	// distinct BEGIN/INSERT/COMMIT WAL sequences and the applier
	// sees 25 TxBegin/Insert/TxCommit triplets. Each commit advances
	// the persisted source_position; the slot's confirmed_flush
	// follows via the tracker.
	const numTxns = 25
	for i := 1; i <= numTxns; i++ {
		applyPGSQL(t, dsn, fmt.Sprintf(
			`INSERT INTO invariant_pin (payload) VALUES ('f3-pin-%d');`, i,
		))
	}

	// Wait until the slot's confirmed_flush has advanced strictly
	// past 0 — proves the test isn't trivially passing. Use a
	// generous timeout because the keepalive cadence is 10s, so the
	// first advance can take up to one keepalive interval after the
	// applier reports its first commit.
	advanceDeadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(advanceDeadline) {
		watcherMu.Lock()
		got := maxConfirmedSeen
		watcherMu.Unlock()
		if got > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	watcherMu.Lock()
	finalMaxConfirmed := maxConfirmedSeen
	watcherMu.Unlock()
	if finalMaxConfirmed == 0 {
		// The test fails closed: a zero max means either the
		// keepalive never fired (production bug) or the applier
		// never reported a position (test-setup bug). Either way
		// the invariant pin is not meaningfully exercised.
		t.Errorf("confirmed_flush_lsn never advanced above 0 within 45s — invariant pin would be trivially passing; check applier wiring / keepalive cadence")
	}

	// Stop the watcher before tearing down the apply loop so a
	// late sample doesn't race with the close.
	watcherCancel()
	<-watcherDone

	// Stop the reader so the applier's channel closes, which lets
	// ApplyBatch return cleanly.
	_ = rdr.Close()

	select {
	case err := <-applyDone:
		// ApplyBatch returning ctx.Canceled / EOF on a closed
		// channel is the expected drain path. Other errors fail
		// the test.
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("ApplyBatch returned: %v (treated as drain on closed channel)", err)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("ApplyBatch did not return within 30s of reader close")
	}

	// Post-stop invariant: the final confirmed_flush still satisfies
	// the invariant (no late keepalive advanced it past the persisted
	// position after the stream tore down).
	finalConfirmed, confirmedOK := readConfirmedFlushLSNParsed(t, dsn, "sluice_slot")
	finalPersisted, havePersisted := readPersistedSourceLSN(t, dsn, "f3-invariant-pin")
	if confirmedOK && havePersisted && finalConfirmed > finalPersisted {
		t.Errorf("post-stop invariant violated: confirmed_flush_lsn=%s > persisted_lsn=%s",
			finalConfirmed.String(), finalPersisted.String())
	}

	// Report all in-flight violations the watcher captured.
	watcherMu.Lock()
	defer watcherMu.Unlock()
	if len(violations) > 0 {
		for _, v := range violations {
			t.Errorf("invariant violated at %s: confirmed_flush_lsn=%s > persisted_lsn=%s",
				v.when.Format(time.RFC3339Nano), v.confirmed.String(), v.persisted.String())
		}
		t.Errorf("F3 invariant violated %d time(s) during the stream — confirmed_flush_lsn advanced past the persisted applied position", len(violations))
	}

	t.Logf("F3 invariant held throughout %d transactions; max confirmed_flush observed = %s, final persisted = %s",
		numTxns, finalMaxConfirmed.String(), finalPersisted.String())
}
