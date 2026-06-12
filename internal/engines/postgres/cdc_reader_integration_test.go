//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the Postgres CDC reader. Boots a Postgres
// container with wal_level=logical (the only non-default GUC the
// reader needs), seeds a table, opens the reader at "from now",
// performs INSERT / UPDATE / DELETE, and asserts the expected
// sequence of ir.Change events arrives.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgresForCDC returns a DSN pointed at a freshly-reset
// database on the shared PG container booted by TestMain. The shared
// container is started with wal_level=logical (and matching
// max_wal_senders / max_replication_slots) so the CDC reader's
// precondition check passes. cleanup is a no-op; teardown belongs to
// TestMain.
//
// Why this helper now uses the shared container while
// startPostgres17ForCDC and startPostgresForCDCImage do NOT: PG 16 is
// the shared container's image; PG 17 is a different image and the
// failover-flag tests need version-specific behaviour, so those
// callers keep booting their own container.
func startPostgresForCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	return newSharedPGDB(t, "source_db")
}

// startPostgres17ForCDC is the PG 17+ counterpart used by the
// failover-flag tests. The image is parameterised on
// startPostgresForCDCImage so the existing PG 16 path keeps using
// the same PG 16 image as the shared TestMain — important because
// not all behaviour we test is version-stable, and silently rolling
// the default would put load-bearing test invariants on a moving
// target.
//
// PG 17 is kept on the upstream `postgres:17` image (not pre-baked,
// task #68): the failover-flag tests are the only callers, the
// per-boot cost on a niche path is acceptable, and adding a third
// pre-baked artifact for one test isn't worth the maintenance.
//
// The "postgres:17" pin here is DELIBERATE: these tests assert PG
// 17–specific behaviour (the FAILOVER slot flag, the 17.x
// server_version_num band), so the SLUICE_TEST_PG_IMAGE override
// the multi-version matrix sets does NOT apply to this helper —
// only sharedPGImage-based boots follow the matrix env.
//
// Stays per-test (does NOT share the container) because the image is
// different from the shared container's PG 16 — sharing would defeat
// the purpose of the PG 17–specific assertion. See the comment block
// at the foot of shared_container_integration_test.go.
func startPostgres17ForCDC(t *testing.T) (dsn string, cleanup func()) {
	return startPostgresForCDCImage(t, "postgres:17")
}

// startPostgresForCDCImage boots a per-test PG container with
// wal_level=logical. Kept per-test (not shared) because it accepts
// the image as a parameter and gets called with "postgres:17" by the
// failover-flag tests. The retry shape mirrors
// pipeline.runMySQLWithRetry / engines/postgres.runPGWithRetry's
// 3-attempt cap — per-test boots multiply by the test count so
// budgets are tighter than the shared TestMain's 5 attempts.
func startPostgresForCDCImage(t *testing.T, image string) (dsn string, cleanup func()) {
	t.Helper()

	container := runPGWithRetry(
		t, image,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				// The image's docker-entrypoint.sh already invokes
				// `postgres "$@"`, so Cmd holds only the GUC overrides.
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=4",
					"-c", "max_replication_slots=4",
				},
			},
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	// First-connect settle gate (the PG17 connect-EOF flake, 3rd
	// firing 2026-06-11 — runs 27382148408 and two prior
	// TestServerVersionNum_PG17 firings): the postgres module's
	// BasicWaitStrategies (ready-log ×2 + port) can pass while the
	// just-restarted server still resets the first real connection
	// ("failed to receive message: unexpected EOF") on a loaded CI
	// runner. The shared TestMain container absorbs this with its
	// 5-attempt boot retry; per-test boots returned immediately and
	// handed the flake to whichever test connected first. Ping with a
	// bounded retry before handing the DSN out, so callers only ever
	// see a server that has actually accepted a connection.
	if err := waitPGFirstConnect(ctx, srcConn); err != nil {
		terminate()
		t.Fatalf("postgres container never accepted a connection after boot: %v", err)
	}
	return srcConn, terminate
}

// waitPGFirstConnect pings dsn until the server accepts a real
// connection or ctx expires. Every error class retries — during the
// settle window the failure shapes vary (EOF, reset, startup refusal)
// and distinguishing them buys nothing; the bounded ctx is the loud
// failure path.
func waitPGFirstConnect(ctx context.Context, dsn string) error {
	var lastErr error
	for {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			lastErr = err
		} else {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = db.PingContext(pingCtx)
			cancel()
			_ = db.Close()
			if err == nil {
				return nil
			}
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last attempt: %w)", ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// applyPGSQL runs a possibly-multi-statement script against the DSN.
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

// waitForSlotInactive polls `pg_replication_slots.active` until the
// named slot is no longer marked active. Required between two CDC
// reader sessions against the same slot.
//
// [CDCReader.Close] is deliberately asynchronous (see its doc comment):
// it cancels the streamer ctx and returns immediately, leaving the
// pump goroutine to close `replConn` "on the way out." PG keeps the
// slot marked active until its walsender backend observes the
// disconnect — which can take many seconds, longer than is reasonable
// for a test poll. We poll briefly for natural release, then force-
// terminate the active backend via `pg_terminate_backend(active_pid)`
// and poll again. This is the standard test idiom for "I need the
// slot back NOW"; production paths don't need this because they
// don't immediately re-attach to the same slot in the same process.
func waitForSlotInactive(t *testing.T, dsn, slotName string, timeout time.Duration) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("waitForSlotInactive: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	checkActive := func() (active bool, exists bool) {
		var a bool
		err := db.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&a)
		if errors.Is(err, sql.ErrNoRows) {
			return false, false
		}
		if err != nil {
			t.Fatalf("waitForSlotInactive: query: %v", err)
		}
		return a, true
	}

	// Phase 1: short polling window for natural release.
	gracePeriod := 3 * time.Second
	if timeout < gracePeriod {
		gracePeriod = timeout
	}
	graceDeadline := time.Now().Add(gracePeriod)
	for time.Now().Before(graceDeadline) {
		active, exists := checkActive()
		if !exists || !active {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Phase 2: force-terminate the active backend.
	if _, err := db.Exec(
		`SELECT pg_terminate_backend(active_pid)
		 FROM pg_replication_slots
		 WHERE slot_name = $1 AND active`,
		slotName,
	); err != nil {
		t.Fatalf("waitForSlotInactive: pg_terminate_backend: %v", err)
	}

	// Phase 3: poll until PG marks the slot inactive after the
	// terminate signal lands.
	deadline := time.Now().Add(timeout - gracePeriod)
	for time.Now().Before(deadline) {
		active, exists := checkActive()
		if !exists || !active {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("waitForSlotInactive: slot %q still active after %s (including pg_terminate_backend)", slotName, timeout)
}

// TestCDCReader_BasicChangeStream exercises the full pgoutput pipeline:
// INSERT / UPDATE / DELETE issued after StreamChanges starts arrive on
// the channel as ir.Insert / ir.Update / ir.Delete with decoded values
// and decodable positions.
func TestCDCReader_BasicChangeStream(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email  VARCHAR(255) NOT NULL,
			active BOOLEAN      NOT NULL DEFAULT true
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Give the replication start a moment to register before
	// generating events. Mirrors the MySQL CDC test.
	time.Sleep(200 * time.Millisecond)

	const dml = `
		INSERT INTO users (email, active) VALUES
			('alice@example.com', true),
			('bob@example.com',   false);
		UPDATE users SET active = false WHERE email = 'alice@example.com';
		DELETE FROM users WHERE email = 'bob@example.com';
	`
	applyPGSQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 4, 30*time.Second)

	if len(got) != 4 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 4 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 4", len(got))
	}

	insAlice, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if insAlice.Schema != "public" || insAlice.Table != "users" {
		t.Errorf("change[0] = %s.%s; want public.users", insAlice.Schema, insAlice.Table)
	}
	if email, _ := insAlice.Row["email"].(string); email != "alice@example.com" {
		t.Errorf("change[0].Row[email] = %#v; want alice@example.com", insAlice.Row["email"])
	}
	if active, _ := insAlice.Row["active"].(bool); !active {
		t.Errorf("change[0].Row[active] = %#v; want true", insAlice.Row["active"])
	}

	insBob, ok := got[1].(ir.Insert)
	if !ok {
		t.Fatalf("change[1] = %T; want ir.Insert", got[1])
	}
	if active, _ := insBob.Row["active"].(bool); active {
		t.Errorf("change[1].Row[active] = %#v; want false", insBob.Row["active"])
	}

	upd, ok := got[2].(ir.Update)
	if !ok {
		t.Fatalf("change[2] = %T; want ir.Update", got[2])
	}
	// REPLICA IDENTITY FULL means the OldTuple carries every column,
	// but emitUpdate narrows Before to the identity-key columns before
	// emit (Bug 92 — the same key-only narrowing the DELETE path has).
	// alice is id=1 (GENERATED BY DEFAULT AS IDENTITY), the only key
	// column; the non-key `active`/`email` columns must NOT appear in
	// Before, or the applier's WHERE would include them and risk the
	// rich-type zero-rows-matched silent UPDATE loss.
	if upd.Before == nil {
		t.Fatal("update.Before is nil despite REPLICA IDENTITY FULL")
	}
	if id, _ := upd.Before["id"].(int64); id != 1 {
		t.Errorf("update.Before[id] = %#v; want int64(1)", upd.Before["id"])
	}
	if _, present := upd.Before["active"]; present {
		t.Errorf("update.Before should be key-only; active leaked in: %#v", upd.Before)
	}
	if _, present := upd.Before["email"]; present {
		t.Errorf("update.Before should be key-only; email leaked in: %#v", upd.Before)
	}
	if after, _ := upd.After["active"].(bool); after {
		t.Errorf("update.After[active] = %#v; want false", upd.After["active"])
	}

	del, ok := got[3].(ir.Delete)
	if !ok {
		t.Fatalf("change[3] = %T; want ir.Delete", got[3])
	}
	if del.Before == nil {
		t.Fatal("delete.Before is nil despite REPLICA IDENTITY FULL")
	}
	// Bug 92: under REPLICA IDENTITY FULL pgoutput flags every column as a
	// key column, so the DELETE Before is narrowed to the table's TRUE
	// PRIMARY KEY (id) — NOT the full row. bob is id=2 (the only PK
	// column). The non-key email/active columns must NOT appear, or a
	// rich-typed DELETE under FULL would carry round-trip-fragile values
	// into the WHERE and silently match zero rows (the same silent-loss
	// class the UPDATE assertion above guards).
	if len(del.Before) != 1 {
		t.Errorf("delete.Before must be key-only under FULL; got %#v", del.Before)
	}
	if id, _ := del.Before["id"].(int64); id != 2 {
		t.Errorf("delete.Before[id] = %#v; want int64(2)", del.Before["id"])
	}
	if _, present := del.Before["email"]; present {
		t.Errorf("delete.Before should be key-only; email leaked in: %#v", del.Before)
	}

	// Positions: each change must encode a slot+lsn pair the engine
	// can decode, and the slot name should match the configured
	// default.
	for i, c := range got {
		pos := c.Pos()
		if pos.Engine != "postgres" {
			t.Errorf("change[%d].Pos.Engine = %q; want postgres", i, pos.Engine)
		}
		decoded, ok, err := decodePGPos(pos)
		if err != nil || !ok {
			t.Errorf("change[%d].Pos failed to decode: ok=%v err=%v", i, ok, err)
			continue
		}
		if decoded.Slot != "sluice_slot" {
			t.Errorf("change[%d].Pos.Slot = %q; want sluice_slot", i, decoded.Slot)
		}
	}
}

// TestCDCReader_UUIDColumnRoundTrip is the Bug 41 regression. Before
// the fix, a UUID column in a CDC-streamed table would crash the pump
// on the first INSERT/UPDATE because pgoutput delivers UUID values in
// text format (36-byte canonical hyphenated string) and the value
// decoder only accepted 16-byte binary or string. Stream exited with:
//
//	postgres: UUID byte slice has length 36; want 16
//
// The fix routes 36-byte []byte through canonicalizeUUIDText so the
// IR's UUID-as-string contract holds end-to-end. This test boots a
// real Postgres, INSERTs and UPDATEs rows carrying a known UUID, and
// asserts the value lands intact on the CDC channel.
func TestCDCReader_UUIDColumnRoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE customers (
			id       INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email    TEXT NOT NULL,
			uuid_col UUID NOT NULL
		);
		ALTER TABLE customers REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Two distinct UUIDs so we can tell INSERT from UPDATE downstream.
	const insertedUUID = "11111111-2222-3333-4444-555555555555"
	const updatedUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	dml := `
		INSERT INTO customers (email, uuid_col) VALUES ('first@test', '` + insertedUUID + `');
		UPDATE customers SET uuid_col = '` + updatedUUID + `' WHERE email = 'first@test';
	`
	applyPGSQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 2, 30*time.Second)
	if len(got) != 2 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 2 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 2", len(got))
	}

	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if v, _ := ins.Row["uuid_col"].(string); v != insertedUUID {
		t.Errorf("insert.Row[uuid_col] = %#v; want %q", ins.Row["uuid_col"], insertedUUID)
	}

	upd, ok := got[1].(ir.Update)
	if !ok {
		t.Fatalf("change[1] = %T; want ir.Update", got[1])
	}
	if v, _ := upd.After["uuid_col"].(string); v != updatedUUID {
		t.Errorf("update.After[uuid_col] = %#v; want %q", upd.After["uuid_col"], updatedUUID)
	}
	// Under REPLICA IDENTITY FULL the OldTuple carries the prior UUID,
	// but emitUpdate narrows Before to the identity-key columns (Bug 92).
	// uuid_col is non-key, so it must NOT survive into Before — leaving
	// it in would put a rich UUID predicate in the applier's WHERE, the
	// exact silent-UPDATE-loss surface. The PK `id` is the only key here.
	if _, present := upd.Before["uuid_col"]; present {
		t.Errorf("update.Before should be key-only; uuid_col leaked in: %#v", upd.Before)
	}
	if id, _ := upd.Before["id"].(int64); id != 1 {
		t.Errorf("update.Before[id] = %#v; want int64(1)", upd.Before["id"])
	}
}

// TestCDCReader_UpdateUnderReplicaIdentityDefault exercises the
// fix for Bug 3: pgoutput omits OldTuple on UPDATEs that don't
// modify the identity-key columns under REPLICA IDENTITY DEFAULT
// (the server-wide default), and the reader synthesizes a
// key-only Before from the after-tuple's PK so the applier can
// build a usable WHERE clause. Without the synthesis, an apply
// downstream would emit "UPDATE t SET ... WHERE " (empty
// predicate) and Postgres rejects with "syntax error at end of
// input".
func TestCDCReader_UpdateUnderReplicaIdentityDefault(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email  VARCHAR(255) NOT NULL,
			active BOOLEAN      NOT NULL DEFAULT true
		);
		-- Intentionally no ALTER TABLE ... REPLICA IDENTITY: the
		-- server-default 'd' (DEFAULT) is the case that triggers
		-- the missing-OldTuple shape on non-PK UPDATEs.
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	const dml = `
		INSERT INTO users (email, active) VALUES ('alice@example.com', true);
		UPDATE users SET active = false WHERE email = 'alice@example.com';
	`
	applyPGSQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 2, 30*time.Second)
	if len(got) != 2 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 2 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 2", len(got))
	}

	upd, ok := got[1].(ir.Update)
	if !ok {
		t.Fatalf("change[1] = %T; want ir.Update", got[1])
	}
	if upd.Before == nil {
		t.Fatal("update.Before is nil; expected synthesized key-only Before from After's PK")
	}
	// The synthesized Before should contain only the PK column.
	if _, hasID := upd.Before["id"]; !hasID {
		t.Errorf("update.Before missing id (expected synthesized PK); got %#v", upd.Before)
	}
	if _, hasNonKey := upd.Before["email"]; hasNonKey {
		t.Errorf("update.Before unexpectedly contains non-key column 'email': %#v", upd.Before)
	}
	if _, hasNonKey := upd.Before["active"]; hasNonKey {
		t.Errorf("update.Before unexpectedly contains non-key column 'active': %#v", upd.Before)
	}
	// And After must reflect the new value.
	if after, _ := upd.After["active"].(bool); after {
		t.Errorf("update.After[active] = %#v; want false", upd.After["active"])
	}
}

// TestCDCReader_DeleteCompositePKUnderReplicaIdentityDefault is the
// regression for Bug 8: a composite-PK DELETE under REPLICA IDENTITY
// DEFAULT used to emit a Before with non-key columns set to nil
// (because pgoutput's 'K' OldTuple has 'n' markers for non-key
// columns). The applier's WHERE then included "non_key IS NULL"
// predicates that never matched the destination row, ADR-0010 ate
// the zero-rows-affected, and the DELETE silently disappeared.
//
// The fix narrows the emitted Before to the relation's key columns.
// This test asserts the shape directly off the CDC channel — no
// applier in the loop, so the test pins the reader's contract
// independently of the resume-idempotency layer.
func TestCDCReader_DeleteCompositePKUnderReplicaIdentityDefault(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE order_items (
			order_id    BIGINT       NOT NULL,
			line_no     SMALLINT     NOT NULL,
			qty         INTEGER      NOT NULL,
			unit_price  NUMERIC(12,4) NOT NULL,
			PRIMARY KEY (order_id, line_no)
		);
		-- Intentionally no ALTER TABLE ... REPLICA IDENTITY: the
		-- server-default 'd' (DEFAULT) is what reproduces Bug 8.
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	const dml = `
		INSERT INTO order_items (order_id, line_no, qty, unit_price) VALUES
			(100, 1, 5, 9.99),
			(100, 2, 3, 1.50);
		DELETE FROM order_items WHERE order_id = 100 AND line_no = 1;
	`
	applyPGSQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 3, 30*time.Second)
	if len(got) != 3 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 3 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 3", len(got))
	}

	del, ok := got[2].(ir.Delete)
	if !ok {
		t.Fatalf("change[2] = %T; want ir.Delete", got[2])
	}
	if del.Schema != "public" || del.Table != "order_items" {
		t.Errorf("delete = %s.%s; want public.order_items", del.Schema, del.Table)
	}
	// Both PK columns must be present in Before — the load-bearing
	// invariant. Single-PK + DEFAULT had this by accident pre-fix
	// (the lone PK column wasn't 'n'); the composite case is what
	// the user's 30-row drift was made of.
	if _, ok := del.Before["order_id"]; !ok {
		t.Errorf("delete.Before missing order_id; got %#v", del.Before)
	}
	if _, ok := del.Before["line_no"]; !ok {
		t.Errorf("delete.Before missing line_no; got %#v", del.Before)
	}
	// Non-key columns must NOT be present — having them as nil entries
	// is exactly the bug shape (forces "qty IS NULL" in WHERE).
	if _, present := del.Before["qty"]; present {
		t.Errorf("delete.Before unexpectedly contains non-key column qty (value %#v); applier WHERE will filter to no rows", del.Before["qty"])
	}
	if _, present := del.Before["unit_price"]; present {
		t.Errorf("delete.Before unexpectedly contains non-key column unit_price (value %#v); applier WHERE will filter to no rows", del.Before["unit_price"])
	}
	// PK values must be the actual deleted row's keys.
	if del.Before["order_id"] != int64(100) {
		t.Errorf("delete.Before[order_id] = %#v; want int64(100)", del.Before["order_id"])
	}
	if del.Before["line_no"] != int64(1) {
		t.Errorf("delete.Before[line_no] = %#v; want int64(1)", del.Before["line_no"])
	}
}

// TestCDCReader_RejectsWrongWALLevel verifies the precondition check.
// Boots a fresh container with wal_level=replica (the default for
// stock postgres images is also replica unless we override) and
// confirms OpenCDCReader → StreamChanges fails with a clear error
// before any replication command is issued.
//
// Stays per-test (does NOT share the container) because the shared
// container is wal_level=logical — it can't test the refusal path
// for a wal_level=replica server. The retry shape applied here is
// the per-test 3-attempt cap (see runPGWithRetry).
func TestCDCReader_RejectsWrongWALLevel(t *testing.T) {
	container := runPGWithRetry(
		// sharedPGImage is the task-#68 pre-baked PG image (or the
		// SLUICE_TEST_PG_IMAGE matrix override — the refusal path is
		// version-agnostic). Safe to reuse here even though the test
		// asserts wal_level=replica refusal: wal_level is a runtime
		// GUC set by container Cmd args, not baked into the image.
		// With no wal_level override in Cmd the server starts at its
		// default "replica" the same way it would against the
		// upstream postgres:16 image.
		t, sharedPGImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		// No wal_level override → server defaults to "replica".
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	_, err = rdr.StreamChanges(ctx, ir.Position{})
	if err == nil {
		t.Fatal("expected wal_level precondition error; got nil")
	}
	// The error should name wal_level so a user can fix it.
	if !contains(err.Error(), "wal_level") {
		t.Errorf("error should mention wal_level; got %q", err.Error())
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestCDCReader_FailoverFlag_PG17 verifies sluice opts into the
// FAILOVER flag on PG 17+ sources. Boots a postgres:17 container,
// cold-starts a CDC reader (which lazily creates the slot), and
// queries pg_replication_slots.failover to confirm the flag is
// true. This is the load-bearing assertion for roadmap item #7
// against vanilla PG — the psverify-tagged sibling covers
// PlanetScale.
func TestCDCReader_FailoverFlag_PG17(t *testing.T) {
	dsn, cleanup := startPostgres17ForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email  VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// StreamChanges triggers slot creation under the hood.
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	_ = changes // we don't need to drain; the slot already exists.

	// Inspect the slot's failover column on a fresh connection
	// (the replication conn can't run normal SQL).
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var failover bool
	err = db.QueryRowContext(
		ctx,
		"SELECT failover FROM pg_replication_slots WHERE slot_name = $1",
		"sluice_slot",
	).Scan(&failover)
	if err != nil {
		t.Fatalf("query failover: %v", err)
	}
	if !failover {
		t.Errorf("pg_replication_slots.failover = false; want true on PG 17+")
	}
}

// TestCDCReader_FailoverFlag_PG16 confirms slot creation succeeds
// on PG 16 (where FAILOVER is not supported) and that the slot
// exists. The pg_replication_slots view in PG 16 doesn't have a
// failover column, so we don't assert on it — we just confirm the
// helper takes the FAILOVER-less path without erroring.
//
// Under the SLUICE_TEST_PG_IMAGE matrix override the shared
// container may be PG 17+; the assertions here (slot creation
// succeeds, slot exists) stay valid on every version — the
// FAILOVER-less-path coverage is only exercised when the shared
// image really is PG 16, i.e. on the default per-PR CI path.
func TestCDCReader_FailoverFlag_PG16(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email  VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if _, err := rdr.StreamChanges(ctx, ir.Position{}); err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var slotName string
	err = db.QueryRowContext(
		ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
		"sluice_slot",
	).Scan(&slotName)
	if err != nil {
		t.Fatalf("query slot: %v", err)
	}
	if slotName != "sluice_slot" {
		t.Errorf("pg_replication_slots.slot_name = %q; want sluice_slot", slotName)
	}
}

// TestServerVersionNum_PG17 sanity-checks the version helper
// against a live server. The exact patch level may drift with the
// official postgres:17 image, so we assert a band rather than an
// exact value: server_version_num for any PG 17.x point release is
// in [170000, 180000).
func TestServerVersionNum_PG17(t *testing.T) {
	dsn, cleanup := startPostgres17ForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	v, err := serverVersionNum(ctx, db)
	if err != nil {
		t.Fatalf("serverVersionNum: %v", err)
	}
	if v < 170000 || v >= 180000 {
		t.Errorf("serverVersionNum = %d; want PG 17.x (170000 ≤ v < 180000)", v)
	}
	if v < pgVersionFailoverSupport {
		t.Errorf("serverVersionNum = %d; want >= pgVersionFailoverSupport (%d)", v, pgVersionFailoverSupport)
	}
}

// drainChanges reads up to want row-level events from changes, with
// an overall timeout. Returns whatever it has if the timeout fires
// or the channel closes. Source-tx boundary events (TxBegin /
// TxCommit, ADR-0027) are silently consumed without counting toward
// want — the assertions in this test file target row-level events,
// and the boundary events are coverage of the BatchedChangeApplier
// flush path (exercised by the applier integration tests).
func drainChanges(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	want int,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	got := make([]ir.Change, 0, want)
	for len(got) < want {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			// TxBegin/TxCommit are applier-internal tx-boundary
			// signals (ADR-0027); ir.SchemaSnapshot is the ADR-0049
			// schema-history boundary event (a reader emits one at
			// first-touch + on each true DDL delta). Both are
			// orthogonal infra on the change stream, not DML — the
			// data-flow tests that use this helper count row/tx
			// changes, so skip them here. Chunk B's own schema-history
			// pins use dedicated collectors (collectPGSnapshots), not
			// this shared helper, so this does not weaken them.
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit, ir.SchemaSnapshot:
				continue
			}
			got = append(got, c)
		case <-deadline.C:
			t.Logf("timed out after %v with %d/%d changes", timeout, len(got), want)
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}

// TestCDCReader_SlotActiveRetry_LiveOwnerStillRefuses pins both
// branches of startReplicationWithSlotActiveRetry: against a slot held
// by a LIVE owner, the second reader (a) visibly retries (the
// transient prior-owner-not-reaped branch fires — the bug15 /
// ADR-0046-recovery race class) and (b) ultimately surfaces the
// original loud SQLSTATE 55006 refusal — the two-concurrent-writers
// guard must survive the retry budget. The self-heal branch (owner
// releases mid-retry) is exercised end-to-end by the bug15
// stop/restart test, which now races the resume directly against the
// release with no grace sleep.
func TestCDCReader_SlotActiveRetry_LiveOwnerStillRefuses(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE retrypin (id BIGSERIAL PRIMARY KEY, v TEXT);`)

	eng := Engine{}

	// Owner A: open and START a stream — it holds the slot live.
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	rdrA, err := eng.OpenCDCReader(ctxA, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader A: %v", err)
	}
	defer func() {
		if c, ok := rdrA.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if _, err := rdrA.StreamChanges(ctxA, ir.Position{}); err != nil {
		t.Fatalf("StreamChanges A: %v", err)
	}

	// Contender B against the same slot, with a ctx window long enough
	// for >=2 retry attempts (0.5s+1s backoffs) but far short of the
	// full budget — B must be RETRYING (not failing fast) and then
	// stop with either ctx.Err or the loud 55006, never a success.
	logs := captureSlog(t)
	ctxB, cancelB := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelB()
	rdrB, err := eng.OpenCDCReader(ctxB, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader B: %v", err)
	}
	defer func() {
		if c, ok := rdrB.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if _, err = rdrB.StreamChanges(ctxB, ir.Position{}); err == nil {
		t.Fatal("second stream on a live-held slot succeeded; the two-writers guard is gone")
	}
	if !strings.Contains(logs.String(), "slot is active") {
		t.Errorf("expected the slot-active retry branch to fire (log line missing); err=%v\nlogs:\n%s", err, logs.String())
	}
	if !strings.Contains(err.Error(), "55006") && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("want the original 55006 refusal or ctx deadline from the bounded wait; got: %v", err)
	}
}
