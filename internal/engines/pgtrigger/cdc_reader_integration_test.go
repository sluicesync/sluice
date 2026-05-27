//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Same-engine integration test for the postgres-trigger CDC reader.
// Boots a PG container (or attaches to one), runs Setup against the
// shared schema, exercises INSERT / UPDATE / DELETE + JSONB-numeric
// round-trip + xmin safety-lag correctness, asserts the reader emits
// the expected ir.Change events.

package pgtrigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPGForTrigger boots a per-test PG container. The trigger
// engine does NOT need wal_level=logical (the whole point), so the
// helper uses the upstream postgres:16 image without CDC GUCs — the
// engine works against the most restricted tier shape we can model
// locally.
//
// Returns the source DSN + a cleanup callback. Uses the upstream
// postgres:16 image (NOT the pre-baked one) so the test exercises
// the engine on the shape operators on Heroku Essential / Render
// Basic would see — those tiers don't run a pre-baked image either.
// The per-test boot cost is acceptable for Phase 1; Phase 2 can
// optimise via a shared container if the integration suite grows.
func startPGForTrigger(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		"postgres:16",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	conn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

// applyPGSQL runs a possibly-multi-statement script against dsn.
func applyPGSQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

// drainEvents reads up to n events from out with a per-event timeout,
// or returns whatever it collected when the channel closes.
func drainEvents(t *testing.T, out <-chan ir.Change, n int, perEventTimeout time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	timer := time.NewTimer(perEventTimeout)
	defer timer.Stop()
	for len(got) < n {
		timer.Reset(perEventTimeout)
		select {
		case ev, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, ev)
		case <-timer.C:
			return got
		}
	}
	return got
}

// TestCDCReader_BasicChangeStream exercises the full polling pipeline:
// INSERT, UPDATE, DELETE issued after StreamChanges starts arrive on
// the channel with decoded values and the trigger-engine position
// envelope. The safety-lag predicate doesn't fire in this test because
// every txn commits before the next poll runs, so commit order ==
// allocation order.
func TestCDCReader_BasicChangeStream(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE orders (
			id     BIGINT PRIMARY KEY,
			amount NUMERIC(20,10) NOT NULL,
			memo   TEXT
		);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	plan, err := Setup(ctx, dsn, SetupOptions{
		Tables: []string{"orders"},
		Schema: "public",
	})
	if err != nil {
		t.Fatalf("Setup: %v (plan=%+v)", err, plan)
	}

	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Apply three operations after the stream has anchored to MAX(id).
	applyPGSQL(t, dsn, `
		INSERT INTO orders (id, amount, memo) VALUES (1, 12345.6789, 'hello');
		UPDATE orders SET memo = 'updated' WHERE id = 1;
		DELETE FROM orders WHERE id = 1;
	`)

	got := drainEvents(t, out, 3, 10*time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d events; want 3 — %+v", len(got), got)
	}

	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("got[0] = %T; want ir.Insert", got[0])
	}
	if ins.Table != "orders" {
		t.Errorf("Insert.Table = %q; want orders", ins.Table)
	}
	if ins.Row["memo"] != "hello" {
		t.Errorf("Insert.Row[memo] = %v; want hello", ins.Row["memo"])
	}
	// numeric(20,10) must preserve precision (Bug-74-class pin).
	// The decoder leaves non-integer numbers as json.Number; the
	// applier's prepareValue path would consult the column type.
	amountText := fmt.Sprintf("%v", ins.Row["amount"])
	if amountText != "12345.6789000000" && amountText != "12345.6789" {
		t.Errorf("Insert.Row[amount] = %v; want 12345.6789(000000) — UseNumber must preserve precision", ins.Row["amount"])
	}

	upd, ok := got[1].(ir.Update)
	if !ok {
		t.Fatalf("got[1] = %T; want ir.Update", got[1])
	}
	if upd.After["memo"] != "updated" {
		t.Errorf("Update.After[memo] = %v; want updated", upd.After["memo"])
	}

	del, ok := got[2].(ir.Delete)
	if !ok {
		t.Fatalf("got[2] = %T; want ir.Delete", got[2])
	}
	if del.Before["id"] != int64(1) {
		t.Errorf("Delete.Before[id] = %v (%T); want int64(1)", del.Before["id"], del.Before["id"])
	}

	// Position envelopes must be the trigger-engine shape so a
	// future resume reads the right Engine tag.
	for i, ev := range got {
		pos := ev.Pos()
		if pos.Engine != EngineName {
			t.Errorf("got[%d].Pos().Engine = %q; want %q", i, pos.Engine, EngineName)
		}
		// LastID must round-trip cleanly through encode/decode.
		var pt pgTriggerPos
		if err := json.Unmarshal([]byte(pos.Token), &pt); err != nil {
			t.Errorf("got[%d] decode token: %v", i, err)
		}
		if pt.LastID <= 0 {
			t.Errorf("got[%d].Pos last_id = %d; want > 0", i, pt.LastID)
		}
	}
}

// TestCDCReader_SafetyLag_OverlappingTxns is the §2 commit-order
// correctness pin. Two transactions overlap: tx-A allocates id=N,
// tx-B allocates id=N+1, tx-B commits FIRST, tx-A commits SECOND.
// A naive reader observing id=N+1 before id=N is durable would skip
// id=N forever — the xmin safety-lag query holds back id=N+1 until
// tx-A is also visible.
//
// The test uses two concurrent connections so the overlap is real
// from PG's perspective (each connection has its own snapshot).
func TestCDCReader_SafetyLag_OverlappingTxns(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"items"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Two overlapping txns. txA opens first (allocating id=1 in the
	// change-log via the trigger), txB opens second (allocating
	// id=2), txB commits FIRST, txA commits SECOND. A reader that
	// doesn't apply the xmin safety-lag would see id=2 land before
	// id=1 in pg_class but skip id=1 forever.
	dbA, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer func() { _ = dbA.Close() }()
	dbB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer func() { _ = dbB.Close() }()

	txA, err := dbA.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	if _, err := txA.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (1, 'A')`); err != nil {
		_ = txA.Rollback()
		t.Fatalf("insert A: %v", err)
	}

	txB, err := dbB.BeginTx(ctx, nil)
	if err != nil {
		_ = txA.Rollback()
		t.Fatalf("begin B: %v", err)
	}
	if _, err := txB.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (2, 'B')`); err != nil {
		_ = txA.Rollback()
		_ = txB.Rollback()
		t.Fatalf("insert B: %v", err)
	}

	// Commit B first; A second.
	if err := txB.Commit(); err != nil {
		_ = txA.Rollback()
		t.Fatalf("commit B: %v", err)
	}
	// Hold A open briefly so the reader has a chance to poll
	// without A's row visible. The safety-lag predicate should
	// keep B's row OUT of the first poll because B's xmin is
	// still >= the snapshot's xmin (A is in-flight).
	time.Sleep(2 * time.Second)
	if err := txA.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	got := drainEvents(t, out, 2, 10*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d events; want 2 (commit-order safety)", len(got))
	}

	// Both events must land. The order they emit in is allocation
	// order (id ASC), but the load-bearing assertion is "no event
	// is skipped". A naive reader would emit the 'B' event in the
	// first poll, advance the watermark past id=1, and then skip
	// 'A' forever.
	labels := map[string]bool{}
	for _, ev := range got {
		if ins, ok := ev.(ir.Insert); ok {
			labels[fmt.Sprint(ins.Row["label"])] = true
		}
	}
	if !labels["A"] {
		t.Errorf("missing event for tx-A (the late-committed txn) — safety-lag predicate failed")
	}
	if !labels["B"] {
		t.Errorf("missing event for tx-B")
	}
}

// TestSetup_RefusesNoPK exercises one of the §14 refuse-loudly
// boundaries via the live preflight query. The hint string is part
// of the operator-actionable contract.
func TestSetup_RefusesNoPK(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE no_pk (a TEXT, b TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"no_pk"}, Schema: "public"})
	if err == nil {
		t.Fatalf("Setup: expected refusal; got nil err (plan=%+v)", plan)
	}
	if plan == nil || len(plan.Refusals) == 0 {
		t.Fatalf("plan.Refusals = empty; want one (plan=%+v err=%v)", plan, err)
	}
	r := plan.Refusals[0]
	if r.Reason != "no-primary-key" {
		t.Errorf("Refusal.Reason = %q; want no-primary-key", r.Reason)
	}
	if !contains(r.Hint, "PRIMARY KEY") {
		t.Errorf("Refusal.Hint = %q; want contains 'PRIMARY KEY'", r.Hint)
	}
}

// TestSetup_RefusesUnlogged covers the second §14 boundary.
func TestSetup_RefusesUnlogged(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE UNLOGGED TABLE u (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"u"}, Schema: "public"})
	if err == nil {
		t.Fatalf("Setup: expected refusal; got nil err (plan=%+v)", plan)
	}
	if plan == nil {
		t.Fatalf("plan is nil")
	}
	var sawUnlogged bool
	for _, r := range plan.Refusals {
		if r.Reason == "unlogged-table" {
			sawUnlogged = true
			if !contains(r.Hint, "UNLOGGED") {
				t.Errorf("Refusal.Hint = %q; want contains 'UNLOGGED'", r.Hint)
			}
		}
	}
	if !sawUnlogged {
		t.Errorf("Refusals = %+v; want an unlogged-table refusal", plan.Refusals)
	}
}

// TestSetup_RefusesGeneratedStored covers the §14 generated-column
// boundary. The trigger engine refuses to replicate GENERATED ALWAYS
// AS ... STORED columns because the target's expression would
// silently overwrite the captured value.
func TestSetup_RefusesGeneratedStored(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE g (
			id BIGINT PRIMARY KEY,
			price NUMERIC NOT NULL,
			tax NUMERIC GENERATED ALWAYS AS (price * 0.1) STORED
		);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"g"}, Schema: "public"})
	if err == nil {
		t.Fatalf("Setup: expected refusal; got nil err (plan=%+v)", plan)
	}
	var sawGen bool
	for _, r := range plan.Refusals {
		if r.Reason == "generated-stored-column" {
			sawGen = true
			if !contains(r.Hint, "GENERATED") {
				t.Errorf("Refusal.Hint = %q; want contains 'GENERATED'", r.Hint)
			}
		}
	}
	if !sawGen {
		t.Errorf("Refusals = %+v; want a generated-stored-column refusal", plan.Refusals)
	}
}

// TestSetup_DryRun_NoSideEffects asserts the dry-run path emits the
// DDL without applying it. The change-log table must NOT exist after
// the call.
func TestSetup_DryRun_NoSideEffects(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE t (id BIGINT PRIMARY KEY);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"t"}, Schema: "public", DryRun: true})
	if err != nil {
		t.Fatalf("Setup --dry-run: %v", err)
	}
	if len(plan.Statements) == 0 {
		t.Fatalf("plan.Statements empty; want non-zero")
	}

	// Confirm the change-log table is absent.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	exists, err := changeLogTableExists(ctx, db, "public")
	if err != nil {
		t.Fatalf("probe change-log: %v", err)
	}
	if exists {
		t.Errorf("change-log table exists after --dry-run; want absent")
	}
}

// TestTeardown_RemovesEverything confirms `Teardown` is a clean
// inverse of `Setup`: after teardown the change-log table is gone,
// the capture function is gone, and the per-table trigger is gone.
// The user's own data table is untouched.
func TestTeardown_RemovesEverything(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE u (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"u"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if _, err := Teardown(ctx, dsn, TeardownOptions{Tables: []string{"u"}, Schema: "public"}); err != nil {
		t.Fatalf("Teardown: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	exists, err := changeLogTableExists(ctx, db, "public")
	if err != nil {
		t.Fatalf("probe change-log: %v", err)
	}
	if exists {
		t.Errorf("change-log table exists after Teardown; want absent")
	}

	// The user table must still exist.
	var userExists bool
	if err := db.QueryRowContext(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname='u' AND n.nspname='public')`,
	).Scan(&userExists); err != nil {
		t.Fatalf("probe user table: %v", err)
	}
	if !userExists {
		t.Errorf("user table dropped by Teardown; want preserved")
	}

	// Re-running Teardown should be idempotent.
	if _, err := Teardown(ctx, dsn, TeardownOptions{Tables: []string{"u"}, Schema: "public"}); err != nil {
		t.Errorf("idempotent re-Teardown: %v", err)
	}
}

// TestOpenCDCReader_RefusesMissingChangeLog asserts the engine
// refuses with the operator-actionable message when the source has
// not been set up. Forgetting to run `sluice trigger setup` must
// surface before any data moves, not silently degrade.
func TestOpenCDCReader_RefusesMissingChangeLog(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e := Engine{}
	_, err := e.OpenCDCReader(ctx, dsn)
	if err == nil {
		t.Fatalf("OpenCDCReader: expected refusal on un-setup source; got nil")
	}
	if !contains(err.Error(), "sluice trigger setup") {
		t.Errorf("err = %v; want hint to run `sluice trigger setup`", err)
	}
}

// TestCDCReader_DDLRefusal_AlterTable confirms the §7 hybrid path:
// when the event trigger is installed (the default on a self-hosted
// PG container with superuser), an observed ALTER TABLE produces a
// refuse-loudly error with the drained-model recovery hint.
func TestCDCReader_DDLRefusal_AlterTable(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE t (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"t"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	applyPGSQL(t, dsn, `ALTER TABLE t ADD COLUMN extra TEXT;`)

	// The pump should record the refusal and close the channel.
	// We give it a generous window — the polling cadence is 1s.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case _, ok := <-out:
			if !ok {
				// Channel closed — the pump exited. Check Err.
				cdc, _ := reader.(*CDCReader)
				if cdc == nil {
					t.Fatalf("reader is not *CDCReader; cannot inspect Err")
				}
				err := cdc.Err()
				if err == nil {
					t.Fatalf("channel closed but Err = nil; want a DDL refusal")
				}
				if !contains(err.Error(), "DDL") {
					t.Errorf("Err = %v; want contains 'DDL'", err)
				}
				if !contains(err.Error(), "drain") && !contains(err.Error(), "sluice migrate") {
					t.Errorf("Err = %v; want the drained-model recovery hint", err)
				}
				return
			}
			// A row event arrived first; that's fine — the DDL
			// marker may come right after.
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for DDL-refusal channel close")
}

// contains is the literal substring check. strings.Contains alias —
// kept local so the test file's imports stay small.
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
