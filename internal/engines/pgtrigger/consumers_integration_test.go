//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the item-115 SOURCE-SIDE CONSUMER REGISTRY on pgtrigger.
// The cut DECISION is shared with every trigger engine and unit-pinned in
// internal/engines/internal/triggercdc; what needs a live server is PG's own
// half — the ON CONFLICT upsert, the now()-relative age projection, the
// pg_class registry probe, and the schema-version gate — plus the end-to-end
// proof that a slower peer's unread rows survive a faster peer's prune.

package pgtrigger

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
)

// openTriggerReader opens a CDCReader against a set-up source.
func openTriggerReader(t *testing.T, ctx context.Context, dsn string) *CDCReader {
	t.Helper()
	r, err := openCDCReader(ctx, dsn, "")
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	cr := r.(*CDCReader)
	t.Cleanup(func() { _ = cr.Close() })
	return cr
}

// TestItem115_SlowPeersUnreadRowsSurviveAFastPeersPrune_PG is the pgtrigger half
// of the item-115 gate, and the test that fails at HEAD.
//
// Two syncs read one source change log. The fast one has durably applied through
// id 100 and runs the auto-prune; the slow one has only applied through 20.
// Before the fix the cut was the fast stream's frontier alone, so ids 21..100 —
// rows the slow sync had not read — were deleted before it ever saw them.
//
// The independent expected value is the SLOW peer's registered frontier (20),
// which comes from that stream's own target and is not derived from anything the
// fast stream's prune path computes.
func TestItem115_SlowPeersUnreadRowsSurviveAFastPeersPrune_PG(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()
	seedSyntheticChangeLog(t, dsn, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cr := openTriggerReader(t, ctx, dsn)

	if err := cr.RegisterChangeLogConsumer(ctx, "slow-sync", `{"last_id":20}`); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := cr.RegisterChangeLogConsumer(ctx, "fast-sync", `{"last_id":100}`); err != nil {
		t.Fatalf("register fast: %v", err)
	}

	deleted, err := cr.PruneConsumedChangeLogToRegisteredMin(ctx, "fast-sync", `{"last_id":100}`, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 20 {
		t.Errorf("deleted = %d; want 20 (cut at the SLOW peer's frontier, not the fast peer's)", deleted)
	}
	ids := pgChangeLogIDs(t, dsn)
	if len(ids) != 80 || ids[0] != 21 || ids[len(ids)-1] != 100 {
		t.Fatalf("remaining ids = %v (len %d); want 21..100 — every row the slow sync has not read must survive",
			ids, len(ids))
	}

	// And the brake releases: once the slow peer catches up, the next prune
	// reaps to the new MIN.
	if err := cr.RegisterChangeLogConsumer(ctx, "slow-sync", `{"last_id":60}`); err != nil {
		t.Fatalf("re-register slow: %v", err)
	}
	deleted, err = cr.PruneConsumedChangeLogToRegisteredMin(ctx, "fast-sync", `{"last_id":100}`, 0)
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if deleted != 40 {
		t.Errorf("second prune deleted = %d; want 40 (ids 21..60)", deleted)
	}
}

// TestItem115_RegistryRefusals_PG pins the three fail-closed refusals against a
// real PG, each with NOTHING deleted: an empty registry, a caller that is not
// registered, and a change log whose schema_version predates the registry.
//
// The version fixture is what an OLDER sluice binary writes — a plain UPDATE to
// 1, exactly the value its idempotent meta upsert records — deliberately not
// something this version's installer can produce, so the gate is not
// self-referential (the item-104 trap).
func TestItem115_RegistryRefusals_PG(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()
	seedSyntheticChangeLog(t, dsn, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cr := openTriggerReader(t, ctx, dsn)

	// (1) Registry present and EMPTY: indistinguishable from "nobody has
	// registered yet", so it must refuse rather than prune everything.
	if _, err := cr.PruneConsumedChangeLogToRegisteredMin(ctx, "self", `{"last_id":10}`, 0); err == nil {
		t.Error("prune against an EMPTY registry returned nil; want a loud refusal")
	}

	// (2) A peer is registered but we are not: a cut from peers alone is not a
	// safe bound for this stream.
	if err := cr.RegisterChangeLogConsumer(ctx, "someone-else", `{"last_id":9}`); err != nil {
		t.Fatalf("register peer: %v", err)
	}
	if _, err := cr.PruneConsumedChangeLogToRegisteredMin(ctx, "me", `{"last_id":10}`, 0); err == nil {
		t.Error("prune by an unregistered caller returned nil; want a loud refusal")
	}

	// (3) An older binary re-ran its own `trigger setup`: the registry table
	// survives, schema_version goes back to 1, and that binary streams WITHOUT
	// registering — so its sync is invisible and we must refuse.
	if err := cr.RegisterChangeLogConsumer(ctx, "me", `{"last_id":10}`); err != nil {
		t.Fatalf("register self: %v", err)
	}
	applyPGSQL(t, dsn, "UPDATE public."+ChangeLogMetaTable+" SET schema_version = 1")
	_, err := cr.PruneConsumedChangeLogToRegisteredMin(ctx, "me", `{"last_id":10}`, 0)
	if err == nil {
		t.Error("prune with schema_version below the registry floor returned nil; want a loud refusal")
	} else if !errors.Is(err, triggercdc.ErrConsumerRegistryUnavailable) {
		t.Errorf("error %v does not wrap ErrConsumerRegistryUnavailable", err)
	}

	// (4) And the registry table itself absent — the un-migrated v1 shape.
	applyPGSQL(t, dsn, "DROP TABLE public."+ChangeLogConsumersTable)
	_, err = cr.PruneConsumedChangeLogToRegisteredMin(ctx, "me", `{"last_id":10}`, 0)
	if err == nil {
		t.Error("prune against an un-migrated change log returned nil; want a loud refusal")
	} else if !errors.Is(err, triggercdc.ErrConsumerRegistryUnavailable) {
		t.Errorf("error %v does not wrap ErrConsumerRegistryUnavailable", err)
	}

	if ids := pgChangeLogIDs(t, dsn); len(ids) != 10 {
		t.Errorf("remaining ids = %v; every refusal must delete NOTHING", ids)
	}
}

// TestItem115_SetupMigratesAV1Install_PG pins the migration path on PG: a change
// log installed before the registry existed (the table dropped, schema_version
// rewritten to 1 — the v1 shape) is migrated by re-running the idempotent
// `sluice trigger setup`, with no manual DDL and no data movement.
func TestItem115_SetupMigratesAV1Install_PG(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()
	seedSyntheticChangeLog(t, dsn, 5)

	// Regress the source to the pre-item-115 shape.
	applyPGSQL(t, dsn, "DROP TABLE public."+ChangeLogConsumersTable)
	applyPGSQL(t, dsn, "UPDATE public."+ChangeLogMetaTable+" SET schema_version = 1")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup (the migration): %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var ver int
	if err := db.QueryRowContext(ctx,
		"SELECT schema_version FROM public."+ChangeLogMetaTable+" WHERE singleton_pk").Scan(&ver); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if ver < triggercdc.ConsumerRegistrySchemaVer {
		t.Errorf("schema_version = %d after migration; want >= %d", ver, triggercdc.ConsumerRegistrySchemaVer)
	}

	cr := openTriggerReader(t, ctx, dsn)
	if err := cr.RegisterChangeLogConsumer(ctx, "self", `{"last_id":3}`); err != nil {
		t.Fatalf("register after migration: %v", err)
	}
	deleted, err := cr.PruneConsumedChangeLogToRegisteredMin(ctx, "self", `{"last_id":3}`, 0)
	if err != nil {
		t.Fatalf("prune after migration: %v", err)
	}
	if deleted != 3 {
		t.Errorf("deleted = %d; want 3 (the migrated source prunes normally)", deleted)
	}
	// The pre-migration change-log rows are untouched: the prune removed 1..3
	// and 4..5 survive.
	//
	// Observed, and the reason this is not an exact-count assertion: on PG the
	// migration's own `CREATE TABLE sluice_change_log_consumers` is picked up by
	// the engine's DDL event trigger and appended to the change log as an op='X'
	// marker, which a LIVE reader turns into a loud schema-change refusal. That
	// is why the migration is documented as a maintenance-window action —
	// re-running `sluice trigger setup` already drops and recreates the capture
	// triggers, so it was never safe against a live stream anyway.
	ids := pgChangeLogIDs(t, dsn)
	if len(ids) < 2 || ids[0] != 4 || ids[1] != 5 {
		t.Errorf("remaining ids = %v; want them to start [4 5]", ids)
	}
}

// TestItem115_OperatorPruneIsClampedToTheRegistry_PG pins the sibling path: the
// operator-run `sluice trigger prune` (engine entry point [Prune]) is clamped to
// the registry too, so an explicit prune cannot out-delete a registered peer.
func TestItem115_OperatorPruneIsClampedToTheRegistry_PG(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()
	seedSyntheticChangeLog(t, dsn, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cr := openTriggerReader(t, ctx, dsn)
	if err := cr.RegisterChangeLogConsumer(ctx, "slow-sync", `{"last_id":20}`); err != nil {
		t.Fatalf("register slow: %v", err)
	}

	res, err := Prune(ctx, dsn, PruneOptions{Cut: 100})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.ClampedTo != 20 || res.Deleted != 20 {
		t.Errorf("ClampedTo = %d, Deleted = %d; want 20 and 20 (the operator's cut lowered to the slowest consumer)",
			res.ClampedTo, res.Deleted)
	}
	if ids := pgChangeLogIDs(t, dsn); len(ids) != 80 {
		t.Errorf("remaining = %d rows; want 80 (ids 21..100 survive)", len(ids))
	}
}

// TestItem115_StaleConsumerHoldsTheBrake_PG pins the deliberate non-behaviour on
// a real server: a registration that has gone quiet (its updated_at backdated
// past the staleness threshold, using the SOURCE's clock) still holds the prune
// back. Auto-eviction would delete the rows a stream that is down for
// maintenance has not read — the silent loss the registry exists to prevent.
func TestItem115_StaleConsumerHoldsTheBrake_PG(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()
	seedSyntheticChangeLog(t, dsn, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cr := openTriggerReader(t, ctx, dsn)
	if err := cr.RegisterChangeLogConsumer(ctx, "abandoned", `{"last_id":5}`); err != nil {
		t.Fatalf("register abandoned: %v", err)
	}
	if err := cr.RegisterChangeLogConsumer(ctx, "live", `{"last_id":50}`); err != nil {
		t.Fatalf("register live: %v", err)
	}
	applyPGSQL(t, dsn,
		"UPDATE public."+ChangeLogConsumersTable+" SET updated_at = now() - INTERVAL '7 days' WHERE consumer_id = 'abandoned'")

	deleted, err := cr.PruneConsumedChangeLogToRegisteredMin(ctx, "live", `{"last_id":50}`, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 5 {
		t.Errorf("deleted = %d; want 5 — a stale consumer is WARNED about, never evicted", deleted)
	}
	if ids := pgChangeLogIDs(t, dsn); len(ids) != 45 {
		t.Errorf("remaining = %d rows; want 45", len(ids))
	}
}
