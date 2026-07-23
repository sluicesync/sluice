//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-PG gate for `sluice sync decommission` (audit 2026-07-23
// DEVEX-3 / Q3): the full lifecycle of a stopped filtered wave —
// active-stream refusal while it runs, dry-run touching nothing, then
// the real decommission removing exactly the stream's own objects
// (slot + per-stream publication) while the shared `sluice_pub`
// bystander survives, the control row goes, and a re-run refuses
// cleanly with the sync-status pointer.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// waitForSluiceSlotInactive polls until the named slot exists but has
// no attached consumer — the state a just-stopped wave leaves behind
// once PG reaps its walsender.
func waitForSluiceSlotInactive(t *testing.T, dsn, slotName string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exists := pgQueryOne[bool](t, dsn,
			`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slotName)
		if exists {
			active := pgQueryOne[bool](t, dsn,
				`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName)
			if !active {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// decommissionSourceState is the (slot exists, per-stream publication
// exists, shared publication exists) triple the test re-reads between
// phases.
func decommissionSourceState(t *testing.T, dsn string) (slot, pub, sharedPub bool) {
	t.Helper()
	slot = pgQueryOne[bool](t, dsn,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sluice_wave_a')`)
	pub = pgQueryOne[bool](t, dsn,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'sluice_wave_a')`)
	sharedPub = pgQueryOne[bool](t, dsn,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'sluice_pub')`)
	return slot, pub, sharedPub
}

func TestDecommission_PG_FullLifecycle(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE orders (id int PRIMARY KEY, note text);
		CREATE TABLE users  (id int PRIMARY KEY, note text);
		INSERT INTO orders (id, note) VALUES (1, 'seed');
		INSERT INTO users  (id, note) VALUES (1, 'seed');
	`)
	// A shared-default bystander publication decommission must NEVER
	// drop (the dropOwnPublicationIfPerStream guard, exercised on the
	// real catalog).
	applyDDL(t, sourceDSN, "CREATE PUBLICATION sluice_pub FOR TABLE users;")

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// A filtered wave with per-stream names — the staged-wave
	// Mechanism-B shape this command exists for.
	stream := &Streamer{
		Source:          pgEng,
		Target:          pgEng,
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		StreamID:        "wave-a",
		SlotName:        "wave_a",
		PublicationName: "wave_a",
		Filter:          migcore.TableFilter{Include: []string{"orders"}},
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- stream.Run(runCtx) }()

	if !waitForRowCount(t, targetDSN, "orders", 1, 60*time.Second) {
		t.Fatal("cold start never delivered the seed row")
	}
	if !waitForActiveSluiceSlot(t, sourceDSN, "sluice_wave_a", 60*time.Second) {
		t.Fatal("the wave's slot never became active")
	}
	// Prove the stream is fully in CDC mode before the refusal check:
	// a delivered CDC change means the anchor position (the control
	// row) is durably written — probing decommission in the
	// slot-active-but-anchor-not-yet-written window would refuse on
	// "no stream recorded" instead of the active-slot code.
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (2, 'cdc-live');")
	if !waitForRowCount(t, targetDSN, "orders", 2, 60*time.Second) {
		t.Fatal("CDC never delivered the post-cold-start insert")
	}

	ctx := context.Background()
	applier, err := pgEng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open target applier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	slots, err := pgEng.(ir.SlotManagerOpener).OpenSlotManager(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("open slot manager: %v", err)
	}
	defer func() { _ = slots.Close() }()

	// ---- Phase 1: the stream is LIVE — decommission must refuse ----
	rep, err := DecommissionStream(ctx, applier, slots, "wave-a", false)
	if err == nil {
		t.Fatal("decommissioning a RUNNING stream succeeded — the active-slot refusal did not fire")
	}
	if rep != nil {
		t.Errorf("active refusal returned a report (%+v); nothing must have been done", rep)
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeDecommissionStreamActive {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeDecommissionStreamActive)
	}
	if slot, pub, shared := decommissionSourceState(t, sourceDSN); !slot || !pub || !shared {
		t.Fatalf("the refused attempt mutated the source: slot=%v pub=%v shared=%v; want all true", slot, pub, shared)
	}
	// And the stream is genuinely unharmed — still delivering.
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (3, 'after-refusal');")
	if !waitForRowCount(t, targetDSN, "orders", 3, 60*time.Second) {
		t.Fatal("the stream stopped delivering after the refused decommission — refusal must not disturb it")
	}

	// ---- Phase 2: stop the wave (the operator's `sync stop`) ----
	cancelRun()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("streamer did not return after ctx cancel")
	}
	if !waitForSluiceSlotInactive(t, sourceDSN, "sluice_wave_a", 60*time.Second) {
		t.Fatal("the wave's slot never went inactive after the stop")
	}

	// ---- Phase 3: dry run — full report, zero mutation ----
	rep, err = DecommissionStream(ctx, applier, slots, "wave-a", true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !rep.DryRun || !rep.SlotDropped || !rep.PublicationDropped || !rep.ControlRowCleared {
		t.Errorf("dry-run report = %+v; want every would-removal flagged", rep)
	}
	if slot, pub, shared := decommissionSourceState(t, sourceDSN); !slot || !pub || !shared {
		t.Fatalf("dry run mutated the source: slot=%v pub=%v shared=%v; want all true", slot, pub, shared)
	}
	if _, rowExists, err := readRecordedPublicationState(ctx, applier, "wave-a"); err != nil || !rowExists {
		t.Fatalf("dry run touched the control row (exists=%v, err=%v)", rowExists, err)
	}

	// ---- Phase 4: the real decommission ----
	rep, err = DecommissionStream(ctx, applier, slots, "wave-a", false)
	if err != nil {
		t.Fatalf("decommission: %v", err)
	}
	if !rep.SlotDropped || !rep.PublicationDropped || !rep.ControlRowCleared {
		t.Errorf("report = %+v; want all three removals", rep)
	}
	if rep.SlotName != "sluice_wave_a" || rep.PublicationName != "sluice_wave_a" {
		t.Errorf("report names = slot %q / pub %q; want the RECORDED resolved names sluice_wave_a", rep.SlotName, rep.PublicationName)
	}
	slot, pub, shared := decommissionSourceState(t, sourceDSN)
	if slot {
		t.Error("replication slot sluice_wave_a still exists after decommission")
	}
	if pub {
		t.Error("per-stream publication sluice_wave_a still exists after decommission")
	}
	if !shared {
		t.Error("the shared sluice_pub was dropped — the per-stream guard failed on the real catalog")
	}
	if _, rowExists, err := readRecordedPublicationState(ctx, applier, "wave-a"); err != nil || rowExists {
		t.Fatalf("control row still present after decommission (exists=%v, err=%v)", rowExists, err)
	}

	// ---- Phase 5: re-run — clean refusal pointing at sync status ----
	rep, err = DecommissionStream(ctx, applier, slots, "wave-a", false)
	if err == nil {
		t.Fatal("re-running decommission on a fully-decommissioned stream must refuse (the control row is gone)")
	}
	if rep != nil {
		t.Errorf("re-run returned a report (%+v); want nil", rep)
	}
	if !strings.Contains(err.Error(), "sync status") {
		t.Errorf("re-run err = %v; must name `sluice sync status`", err)
	}
	if _, _, shared := decommissionSourceState(t, sourceDSN); !shared {
		t.Error("the re-run dropped the shared publication")
	}
}
