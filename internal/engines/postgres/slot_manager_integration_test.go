//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the SlotManager surface: list and drop, plus
// the auto-drop path on failed cold-start in CDCReader.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSlotManager_ListEmpty confirms the manager returns an empty
// (non-nil) slice on a freshly-booted source.
func TestSlotManager_ListEmpty(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	slots, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(slots) != 0 {
		t.Errorf("expected no slots on fresh source; got %d: %#v", len(slots), slots)
	}
}

// TestSlotManager_DropExisting walks the happy path: create a slot
// (via the protocol-level command, mirroring CDCReader's path), list
// it, then drop it.
func TestSlotManager_DropExisting(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open a replication connection just to create the slot. We
	// don't keep a CDC reader running — the slot is what we want
	// to manage from outside.
	replConn, err := openReplicationConn(ctx, dsn, "-")
	if err != nil {
		t.Fatalf("openReplicationConn: %v", err)
	}
	const testSlot = "test_slot_drop_existing"
	if _, err := pglogrepl.CreateReplicationSlot(ctx, replConn, testSlot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		t.Fatalf("CreateReplicationSlot: %v", err)
	}
	_ = replConn.Close(ctx)

	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()

	slots, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, s := range slots {
		if s.Name == testSlot {
			found = true
			if s.Plugin != "pgoutput" {
				t.Errorf("slot.Plugin = %q; want pgoutput", s.Plugin)
			}
			if s.Active {
				t.Errorf("slot.Active = true; want false (no consumer connected)")
			}
			break
		}
	}
	if !found {
		t.Fatalf("slot %q not present in List output", testSlot)
	}

	if err := mgr.Drop(ctx, testSlot, false); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	// Confirm it's gone from List.
	slots, err = mgr.List(ctx)
	if err != nil {
		t.Fatalf("List after drop: %v", err)
	}
	for _, s := range slots {
		if s.Name == testSlot {
			t.Errorf("slot %q still listed after Drop", testSlot)
		}
	}
}

// TestSlotManager_DropMissing surfaces errSlotNotFound through the
// returned error so the CLI's --if-exists mode can branch on it.
func TestSlotManager_DropMissing(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := mgr.Drop(ctx, "no_such_slot", false)
	if err == nil {
		t.Fatal("expected error for missing slot")
	}
	if !errors.Is(err, errSlotNotFound) {
		t.Errorf("error should wrap errSlotNotFound; got %v", err)
	}
	// The engine-neutral half, and the reason this assertion is here rather
	// than only in internal/pipeline: `sluice sync decommission` classifies a
	// slot-already-gone drop with errors.Is(err, ir.ErrSlotNotFound) and
	// SWALLOWS it (reports the slot absent). Its own unit stubs necessarily
	// fabricate the error they classify, so they can only prove the classifier
	// agrees with the stub. THIS is the binding that a real Postgres, through
	// the real SlotManager, produces the marker those stubs imitate — without
	// it the pipeline pins are satisfied by construction (audit backlog C-1).
	if !errors.Is(err, ir.ErrSlotNotFound) {
		t.Errorf("error should also wrap ir.ErrSlotNotFound for engine-neutral callers; got %v", err)
	}
}

// TestSlotManager_DropActiveWrapsTheNeutralMarker is the [ir.ErrSlotActive] half
// of the binding above, against a real slot with a live consumer attached.
//
// The decommission path retries for the whole wall-clock budget on this shape
// and then emits a coded refusal, so a marker the engine never sets would turn
// a live-consumer refusal into a generic error — and, before the markers
// existed, the classifier matched the substring "is active", which any
// unrelated message could carry.
func TestSlotManager_DropActiveWrapsTheNeutralMarker(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const slot = "sluice_active_marker"
	stop := createActiveSlotForTest(t, dsn, slot)
	defer stop()

	err := mgr.Drop(ctx, slot, false)
	if err == nil {
		t.Fatal("expected a refusal dropping a slot with a live consumer")
	}
	if !errors.Is(err, ir.ErrSlotActive) {
		t.Errorf("error should wrap ir.ErrSlotActive; got %v", err)
	}
	if errors.Is(err, ir.ErrSlotNotFound) {
		t.Errorf("an active slot must not classify as absent; got %v", err)
	}
}

// TestSlotManager_DropEmptyName rejects the empty string before
// touching the database.
func TestSlotManager_DropEmptyName(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mgr.Drop(ctx, "", false); err == nil {
		t.Error("expected error for empty slot name")
	}
}

// TestSlotManager_PlatformInternalSlot pins the platform-internal
// wire-up end-to-end against a real server, using `pghoard_local`
// (the Aiven-lineage/Vultr backup daemon's always-present slot) as
// the representative: List labels it, a sibling stranger slot stays
// un-annotated, Drop refuses it without --force, and --force still
// removes it (operator override). The roster itself — both entries ×
// stranger names — is unit-pinned in platform_slots_test.go.
func TestSlotManager_PlatformInternalSlot(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	// A physical slot, like the real pghoard_local; plus a stranger
	// physical slot that must keep surfacing un-annotated.
	applyPGSQL(t, dsn, `
		SELECT pg_create_physical_replication_slot('pghoard_local');
		SELECT pg_create_physical_replication_slot('stranger_physical');
	`)

	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slots, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	byName := map[string]ir.SlotInfo{}
	for _, s := range slots {
		byName[s.Name] = s
	}
	if got := byName["pghoard_local"].PlatformNote; got == "" {
		t.Error("pghoard_local: PlatformNote empty; want the Aiven-lineage annotation")
	}
	if got := byName["stranger_physical"].PlatformNote; got != "" {
		t.Errorf("stranger_physical: PlatformNote = %q; want empty (a stranger slot must keep surfacing)", got)
	}

	err = mgr.Drop(ctx, "pghoard_local", false)
	if err == nil || !strings.Contains(err.Error(), "platform-internal") {
		t.Fatalf("Drop without --force = %v; want the platform-internal refusal", err)
	}
	if err := mgr.Drop(ctx, "pghoard_local", true); err != nil {
		t.Fatalf("Drop with --force: %v", err)
	}
	if err := mgr.Drop(ctx, "stranger_physical", false); err != nil {
		t.Fatalf("Drop stranger: %v", err)
	}
}

// TestCDCReader_AutoDropOnFailedColdStart proves the cleanup path:
// cold-start a CDC reader against a publication name that doesn't
// exist (we override the publication after construction to skip the
// CREATE PUBLICATION step), so START_REPLICATION will fail with
// "publication does not exist". The freshly-created slot must be
// auto-dropped before StreamChanges returns.
func TestCDCReader_AutoDropOnFailedColdStart(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE t (id BIGINT PRIMARY KEY);
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdcRdr := rdr.(*CDCReader)
	const slotName = "auto_drop_test_slot"
	cdcRdr.slotName = slotName
	// Point at a publication name that doesn't exist on the server.
	// ensurePublication creates the publication if it's missing, so
	// we instead change the name to one that will be created here
	// (passes ensurePublication) but isn't recognised by
	// START_REPLICATION… actually ensurePublication will create
	// whatever we name. Bypass: we override publication AFTER
	// ensurePublication runs by registering a fake mid-flight isn't
	// available, so we instead test the simpler path: empty
	// publication name causes pgoutput to reject. But ensurePublication
	// would CREATE PUBLICATION "" which fails first. Use a name with
	// a syntax error reserved character that ensurePublication will
	// happily try to quote-create then START_REPLICATION rejects when
	// looking up. The cleanest way: hand-create the publication, then
	// drop it between ensurePublication and START_REPLICATION. That's
	// racy. So instead, use the fact that CreateReplicationSlot can
	// fail when MAX slots reached.
	//
	// Pragmatic test: rely on the shape that — we set a bogus
	// protoVersion that pgoutput rejects on START_REPLICATION. The
	// slot is created by then.
	cdcRdr.protoVersion = 99 // pgoutput only knows v1 and v2

	defer func() { _ = cdcRdr.Close() }()

	_, err = cdcRdr.StreamChanges(ctx, ir.Position{})
	if err == nil {
		t.Fatal("expected START_REPLICATION error for invalid proto_version")
	}
	if !strings.Contains(err.Error(), "START_REPLICATION") {
		t.Errorf("error should name START_REPLICATION; got %q", err.Error())
	}

	// The slot must have been auto-dropped. Confirm by listing.
	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()
	slots, listErr := mgr.List(ctx)
	if listErr != nil {
		t.Fatalf("List after failed cold-start: %v", listErr)
	}
	for _, s := range slots {
		if s.Name == slotName {
			t.Errorf("slot %q was not auto-dropped after failed cold-start; List shows: %#v", slotName, s)
		}
	}
}

// TestCDCReader_PreExistingSlotPreservedOnFailure confirms the
// reverse case: a slot that already existed when StreamChanges is
// called must NOT be dropped if setup fails. The pre-existing slot
// may carry someone else's progress.
func TestCDCReader_PreExistingSlotPreservedOnFailure(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Pre-create the slot via a separate replication connection.
	replConn, err := openReplicationConn(ctx, dsn, "-")
	if err != nil {
		t.Fatalf("openReplicationConn: %v", err)
	}
	const slotName = "preexisting_slot"
	if _, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		t.Fatalf("pre-create slot: %v", err)
	}
	_ = replConn.Close(ctx)

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdcRdr := rdr.(*CDCReader)
	cdcRdr.slotName = slotName
	cdcRdr.protoVersion = 99 // force START_REPLICATION failure
	defer func() { _ = cdcRdr.Close() }()

	_, err = cdcRdr.StreamChanges(ctx, ir.Position{})
	if err == nil {
		t.Fatal("expected error from invalid protocol")
	}

	// Slot must still exist — the failure was on a pre-existing slot
	// we didn't create, so the auto-drop must not fire.
	mgr := openSlotManagerForTest(t, dsn)
	defer func() { _ = mgr.Close() }()
	slots, listErr := mgr.List(ctx)
	if listErr != nil {
		t.Fatalf("List: %v", listErr)
	}
	found := false
	for _, s := range slots {
		if s.Name == slotName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("pre-existing slot %q was incorrectly dropped after failed cold-start", slotName)
	}
}

// openSlotManagerForTest is a small helper that wires up the engine
// in the same way the CLI does. We fail loudly if the engine doesn't
// implement the optional opener — that would be a build-time
// regression in this package, not a runtime branch.
func openSlotManagerForTest(t *testing.T, dsn string) ir.SlotManager {
	t.Helper()
	eng := Engine{}
	opener, ok := any(eng).(ir.SlotManagerOpener)
	if !ok {
		t.Fatal("postgres Engine no longer implements ir.SlotManagerOpener")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr, err := opener.OpenSlotManager(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSlotManager: %v", err)
	}
	return mgr
}

// errSlotNotFound is package-private; tests rely on errors.Is via the
// exported wrapping behaviour. fmt.Errorf %w-wraps it so the
// integration test above just checks the wrapping works end-to-end.
var _ = fmt.Errorf

// createActiveSlotForTest creates a logical slot and leaves a real walsender
// attached to it, so pg_replication_slots.active is genuinely true — the state
// the drop refusal is about. Returns a stop func that releases the consumer.
//
// The slot only reports active while a START_REPLICATION is in flight; merely
// creating it on a replication connection is not enough, since the server
// releases the slot as soon as CREATE_REPLICATION_SLOT returns.
func createActiveSlotForTest(t *testing.T, dsn, slotName string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const pub = "sluice_active_marker_pub"
	adminConn, err := openReplicationConn(ctx, dsn, "-")
	if err != nil {
		t.Fatalf("createActiveSlotForTest: open conn: %v", err)
	}
	if _, err := pglogrepl.CreateReplicationSlot(ctx, adminConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		_ = adminConn.Close(context.Background())
		t.Fatalf("createActiveSlotForTest: CreateReplicationSlot: %v", err)
	}
	_ = adminConn.Close(context.Background())

	applyPGSQL(t, dsn, "CREATE PUBLICATION "+pub+" FOR ALL TABLES")

	streamConn, err := openReplicationConn(context.Background(), dsn, "-")
	if err != nil {
		t.Fatalf("createActiveSlotForTest: open streaming conn: %v", err)
	}
	if err := pglogrepl.StartReplication(context.Background(), streamConn, slotName, 0,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", pub),
		}}); err != nil {
		_ = streamConn.Close(context.Background())
		t.Fatalf("createActiveSlotForTest: START_REPLICATION: %v", err)
	}

	// The walsender marks the slot active asynchronously; wait for the server
	// to agree rather than assuming it, so the assertion under test is about
	// the refusal and not about a race.
	waitForSlotActive(t, dsn, slotName)

	return func() { _ = streamConn.Close(context.Background()) }
}

// waitForSlotActive is the inverse of waitForSlotInactive: poll until the
// server itself reports the slot active, so a test asserting the active-slot
// refusal is not racing the walsender's own bookkeeping.
func waitForSlotActive(t *testing.T, dsn, slotName string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("waitForSlotActive: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var active bool
		err := db.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&active)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("waitForSlotActive: query: %v", err)
		}
		if err == nil && active {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("waitForSlotActive: slot %q never became active", slotName)
}
