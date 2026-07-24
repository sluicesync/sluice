// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for `sluice sync decommission`'s orchestration
// (decommission.go): precondition refusals fire BEFORE any mutation,
// removals run in order (slot → publication → control row), the
// control row is kept on any source-side failure so a re-run can
// finish, and the engine-without-slots / legacy-empty-name postures
// skip with a stated reason instead of guessing. The real-database
// halves live in decommission_pg_integration_test.go.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fastDecommissionRetry compresses the slot-drop retry budget and
// backoff so the retry-path pins run in milliseconds instead of sleeping
// out the real 5-minute wall-clock budget.
func fastDecommissionRetry(t *testing.T) {
	t.Helper()
	budget, base, maxB := decommissionSlotReleaseBudget, decommissionDropBaseBackoff, decommissionDropMaxBackoff
	decommissionSlotReleaseBudget = 30 * time.Millisecond
	decommissionDropBaseBackoff = time.Millisecond
	decommissionDropMaxBackoff = 4 * time.Millisecond
	t.Cleanup(func() {
		decommissionSlotReleaseBudget = budget
		decommissionDropBaseBackoff = base
		decommissionDropMaxBackoff = maxB
	})
}

// decomApplier is a ChangeApplier + StreamCleaner whose ListStreams
// serves canned control rows and whose ClearStream records into a
// shared order log.
type decomApplier struct {
	stubChangeApplier
	streams  []ir.StreamStatus
	listErr  error
	clearErr error
	order    *[]string
}

func (a *decomApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return a.streams, a.listErr
}

func (a *decomApplier) ClearStream(_ context.Context, streamID string) error {
	if a.clearErr != nil {
		return a.clearErr
	}
	if a.order != nil {
		*a.order = append(*a.order, "clear:"+streamID)
	}
	return nil
}

// decomSlotMgr is an ir.SlotManager + ir.StreamPublicationDropper
// with scripted results, recording into the shared order log.
type decomSlotMgr struct {
	slots      []ir.SlotInfo
	listErr    error
	dropErrs   []error // consumed per Drop call; nil past the end
	stickyDrop error   // when set, EVERY Drop returns it (models a slot held active forever, exhausting the wall-clock budget)
	dropN      int
	pubOut     ir.PublicationDropOutcome
	pubErr     error
	order      *[]string
}

func (m *decomSlotMgr) List(context.Context) ([]ir.SlotInfo, error) {
	return m.slots, m.listErr
}

func (m *decomSlotMgr) Drop(_ context.Context, name string, force bool) error {
	if force {
		return errors.New("decommission must never force-drop")
	}
	if m.stickyDrop != nil {
		m.dropN++
		return m.stickyDrop
	}
	var err error
	if m.dropN < len(m.dropErrs) {
		err = m.dropErrs[m.dropN]
	}
	m.dropN++
	if err == nil && m.order != nil {
		*m.order = append(*m.order, "drop-slot:"+name)
	}
	return err
}

func (m *decomSlotMgr) Close() error { return nil }

func (m *decomSlotMgr) DropStreamPublication(_ context.Context, name string, dryRun bool) (ir.PublicationDropOutcome, error) {
	if m.pubErr != nil {
		return ir.PublicationDropSkippedShared, m.pubErr
	}
	if m.order != nil {
		tag := "drop-pub:"
		if dryRun {
			tag = "dry-pub:"
		}
		*m.order = append(*m.order, tag+name)
	}
	return m.pubOut, nil
}

// slotOnlyMgr is an ir.SlotManager WITHOUT the publication surface,
// for the "engine does not expose publication management" posture.
type slotOnlyMgr struct{ decomSlotMgr }

// The embedded method set would leak DropStreamPublication; shadow it
// away by wrapping only the SlotManager methods.
type slotOnlyView struct{ inner *slotOnlyMgr }

func (v slotOnlyView) List(ctx context.Context) ([]ir.SlotInfo, error) { return v.inner.List(ctx) }
func (v slotOnlyView) Drop(ctx context.Context, name string, force bool) error {
	return v.inner.Drop(ctx, name, force)
}
func (v slotOnlyView) Close() error { return nil }

func decomRow(streamID, slot, pub string) ir.StreamStatus {
	return ir.StreamStatus{StreamID: streamID, SlotName: slot, PublicationName: pub}
}

// TestDecommission_FullPathOrder pins the happy path and its order:
// slot dropped, then publication, then the control row — the row must
// be LAST so a mid-flight failure keeps the record a re-run needs.
func TestDecommission_FullPathOrder(t *testing.T) {
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
		order:   &order,
	}
	slots := &decomSlotMgr{
		slots:  []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		pubOut: ir.PublicationDropDropped,
		order:  &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	want := []string{"drop-slot:sluice_wave_a", "drop-pub:sluice_wave_a", "clear:wave-a"}
	if len(order) != len(want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("order[%d] = %q; want %q", i, order[i], want[i])
		}
	}
	if !rep.SlotDropped || !rep.PublicationDropped || !rep.ControlRowCleared {
		t.Errorf("report = %+v; want all three removals flagged", rep)
	}
}

// TestDecommission_UnknownStreamRefuses pins precondition (a): no
// control row → a clear refusal naming `sync status`, nothing called.
func TestDecommission_UnknownStreamRefuses(t *testing.T) {
	var order []string
	applier := &decomApplier{order: &order}
	slots := &decomSlotMgr{order: &order}

	rep, err := DecommissionStream(context.Background(), applier, slots, "nope", false)
	if err == nil {
		t.Fatal("expected refusal for an unrecorded stream")
	}
	if rep != nil {
		t.Errorf("report = %+v; want nil (nothing was done)", rep)
	}
	if !strings.Contains(err.Error(), "sync status") {
		t.Errorf("err = %v; must name `sluice sync status` as the way to list streams", err)
	}
	if len(order) != 0 {
		t.Errorf("mutations happened despite the refusal: %v", order)
	}
}

// TestDecommission_ActiveSlotRefusesCoded pins precondition (b): an
// active slot is a live stream — refuse with the registered code and
// the sync-stop remedy, mutating nothing (including under --dry-run).
func TestDecommission_ActiveSlotRefusesCoded(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		var order []string
		applier := &decomApplier{
			streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
			order:   &order,
		}
		slots := &decomSlotMgr{
			slots: []ir.SlotInfo{{Name: "sluice_wave_a", Active: true}},
			order: &order,
		}

		rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", dryRun)
		if err == nil {
			t.Fatalf("dryRun=%v: expected the active-slot refusal", dryRun)
		}
		if rep != nil {
			t.Errorf("dryRun=%v: report = %+v; want nil", dryRun, rep)
		}
		coded, ok := sluicecode.FromError(err)
		if !ok || coded.Code != sluicecode.CodeDecommissionStreamActive {
			t.Fatalf("dryRun=%v: err = %v; want %s", dryRun, err, sluicecode.CodeDecommissionStreamActive)
		}
		if !strings.Contains(err.Error(), "sync stop") || !strings.Contains(coded.Hint, "--wait") {
			t.Errorf("dryRun=%v: refusal must name the `sync stop --wait` remedy; got err=%v hint=%q", dryRun, err, coded.Hint)
		}
		if len(order) != 0 {
			t.Errorf("dryRun=%v: mutations happened despite the refusal: %v", dryRun, order)
		}
	}
}

// TestDecommission_SlotDropRetriesActiveShape pins the 55006-reap
// idiom: a drop that fails with the active shape while the pre-check
// saw inactive (the lingering-walsender window) is retried and
// succeeds within the budget.
func TestDecommission_SlotDropRetriesActiveShape(t *testing.T) {
	fastDecommissionRetry(t)
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "")},
		order:   &order,
	}
	slots := &decomSlotMgr{
		slots:    []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		dropErrs: []error{errors.New(`postgres: drop slot "sluice_wave_a": replication slot "sluice_wave_a" is active for PID 42 (SQLSTATE 55006)`)},
		pubOut:   ir.PublicationDropSkippedShared,
		order:    &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if !rep.SlotDropped {
		t.Errorf("report = %+v; want SlotDropped after the retry", rep)
	}
	if slots.dropN != 2 {
		t.Errorf("Drop calls = %d; want 2 (one active-shape failure, one success)", slots.dropN)
	}
}

// TestDecommission_SlotHeldPastBudgetRefusesCoded pins the exhausted
// side of the retry: a slot held active through the whole budget is a
// genuinely live consumer — the coded refusal, publication and row
// untouched.
func TestDecommission_SlotHeldPastBudgetRefusesCoded(t *testing.T) {
	fastDecommissionRetry(t)
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
		order:   &order,
	}
	active := errors.New(`postgres: drop slot "sluice_wave_a": slot is active (a CDC consumer is currently connected); pass --force to drop anyway`)
	slots := &decomSlotMgr{
		slots:      []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		stickyDrop: active, // held active through the whole wall-clock budget
		order:      &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err == nil {
		t.Fatal("expected the active-slot refusal after retry exhaustion")
	}
	if rep != nil {
		t.Errorf("report = %+v; want nil", rep)
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeDecommissionStreamActive {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeDecommissionStreamActive)
	}
	if len(order) != 0 {
		t.Errorf("publication/row were touched despite the refusal: %v", order)
	}
}

// TestDecommission_SlotDropFailureKeepsControlRow pins the
// partial-failure posture: a non-active slot-drop failure still
// attempts the publication (maximal progress per run) but the control
// row is KEPT and the error says so.
func TestDecommission_SlotDropFailureKeepsControlRow(t *testing.T) {
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
		order:   &order,
	}
	slots := &decomSlotMgr{
		slots:    []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		dropErrs: []error{errors.New("permission denied to drop replication slot")},
		pubOut:   ir.PublicationDropDropped,
		order:    &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err == nil {
		t.Fatal("expected the incomplete-decommission error")
	}
	if rep == nil {
		t.Fatal("partial failure must still report what happened")
	}
	if rep.SlotDropped {
		t.Error("SlotDropped = true despite the drop failing")
	}
	if !rep.PublicationDropped {
		t.Error("publication drop should still have been attempted (maximal progress)")
	}
	if rep.ControlRowCleared {
		t.Error("control row must be KEPT on a source-side failure")
	}
	for _, step := range order {
		if strings.HasPrefix(step, "clear:") {
			t.Errorf("ClearStream was called despite the failure: %v", order)
		}
	}
	for _, want := range []string{"INCOMPLETE", "control row was kept", "permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v; want it to contain %q", err, want)
		}
	}
}

// TestDecommission_PublicationDropFailureKeepsControlRow is the (d)
// sibling: slot removed, publication drop fails → row kept, error
// reports the slot as done and the re-run posture.
func TestDecommission_PublicationDropFailureKeepsControlRow(t *testing.T) {
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
		order:   &order,
	}
	slots := &decomSlotMgr{
		slots:  []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		pubErr: errors.New("must be owner of publication sluice_wave_a"),
		order:  &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err == nil {
		t.Fatal("expected the incomplete-decommission error")
	}
	if !rep.SlotDropped {
		t.Error("the slot drop preceding the failure must be reported")
	}
	if rep.PublicationDropped || rep.ControlRowCleared {
		t.Errorf("report = %+v; publication must not read dropped, row must be kept", rep)
	}
	if !strings.Contains(err.Error(), "sluice_wave_a") {
		t.Errorf("err = %v; must name the publication", err)
	}
}

// TestDecommission_RerunAfterPartialCompletes pins idempotency: a
// re-run that finds the slot already gone and the publication already
// absent treats both as success and clears the row.
func TestDecommission_RerunAfterPartialCompletes(t *testing.T) {
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
		order:   &order,
	}
	slots := &decomSlotMgr{
		slots:  nil, // slot no longer listed
		pubOut: ir.PublicationDropAlreadyAbsent,
		order:  &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err != nil {
		t.Fatalf("re-run must complete cleanly: %v", err)
	}
	if !rep.SlotAlreadyAbsent || !rep.PublicationAlreadyAbsent || !rep.ControlRowCleared {
		t.Errorf("report = %+v; want already-absent + cleared", rep)
	}
	if slots.dropN != 0 {
		t.Errorf("Drop called %d times for an absent slot; want 0", slots.dropN)
	}
}

// TestDecommission_DropRaceSlotGoneIsSuccess pins the List-says-there
// /Drop-says-gone race: the slot vanished between the probe and the
// drop — the goal state, reported as already absent.
func TestDecommission_DropRaceSlotGoneIsSuccess(t *testing.T) {
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "")},
	}
	slots := &decomSlotMgr{
		slots:    []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		dropErrs: []error{errors.New(`postgres: slot not found: "sluice_wave_a"`)},
		pubOut:   ir.PublicationDropSkippedShared,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if !rep.SlotAlreadyAbsent || rep.SlotDropped {
		t.Errorf("report = %+v; want SlotAlreadyAbsent on the drop race", rep)
	}
	if !rep.ControlRowCleared {
		t.Error("row must still clear")
	}
}

// TestDecommission_NoSlotManagerClearsRowOnly pins the MySQL-family /
// trigger-CDC posture: no slot manager → both source-side steps skip
// with a stated reason, and the control row still clears — the
// still-useful half.
func TestDecommission_NoSlotManagerClearsRowOnly(t *testing.T) {
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "", "")},
		order:   &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, nil, "wave-a", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if rep.SlotSkipped == "" || rep.PublicationSkipped == "" {
		t.Errorf("report = %+v; both skips must carry a reason", rep)
	}
	if !rep.ControlRowCleared {
		t.Error("control row must clear on a slotless source")
	}
	if len(order) != 1 || order[0] != "clear:wave-a" {
		t.Errorf("order = %v; want only the clear", order)
	}
}

// TestDecommission_LegacyEmptyNamesSkipAndSay pins the legacy-row
// posture on a slot-capable source: empty recorded slot/publication
// names drop NOTHING (guessing the default could hit another stream)
// and say so, while the row still clears.
func TestDecommission_LegacyEmptyNamesSkipAndSay(t *testing.T) {
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("legacy", "", "")},
	}
	slots := &decomSlotMgr{
		slots:  []ir.SlotInfo{{Name: "sluice_slot", Active: false}},
		pubOut: ir.PublicationDropSkippedShared,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "legacy", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if slots.dropN != 0 {
		t.Errorf("Drop was called %d times on an empty recorded slot name; want 0 (never guess)", slots.dropN)
	}
	if !strings.Contains(rep.SlotSkipped, "slot list") {
		t.Errorf("SlotSkipped = %q; must point at `sluice slot list`/`slot drop`", rep.SlotSkipped)
	}
	if !strings.Contains(rep.PublicationSkipped, "never dropped") {
		t.Errorf("PublicationSkipped = %q; must state the shared default is never dropped", rep.PublicationSkipped)
	}
	if !rep.ControlRowCleared {
		t.Error("control row must still clear")
	}
}

// TestDecommission_SharedPublicationOutcomeSkips pins the engine-guard
// echo: a recorded name the engine reports as the shared default is
// surfaced as a skip, not a drop, and doesn't block the row clear.
func TestDecommission_SharedPublicationOutcomeSkips(t *testing.T) {
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_pub")},
	}
	slots := &decomSlotMgr{
		slots:  []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		pubOut: ir.PublicationDropSkippedShared,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if rep.PublicationDropped {
		t.Error("shared default must never read as dropped")
	}
	if !strings.Contains(rep.PublicationSkipped, "shared engine default") {
		t.Errorf("PublicationSkipped = %q; must explain the shared-default guard", rep.PublicationSkipped)
	}
	if !rep.SlotDropped || !rep.ControlRowCleared {
		t.Errorf("report = %+v; slot + row must still complete", rep)
	}
}

// TestDecommission_SlotManagerWithoutPublicationSurface pins the
// future-engine posture: a SlotManager without
// ir.StreamPublicationDropper skips with the manual-drop reason.
func TestDecommission_SlotManagerWithoutPublicationSurface(t *testing.T) {
	inner := &slotOnlyMgr{decomSlotMgr{
		slots: []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
	}}
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
	}

	rep, err := DecommissionStream(context.Background(), applier, slotOnlyView{inner: inner}, "wave-a", false)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if !strings.Contains(rep.PublicationSkipped, "does not expose publication management") {
		t.Errorf("PublicationSkipped = %q; want the no-surface reason", rep.PublicationSkipped)
	}
	if !rep.SlotDropped || !rep.ControlRowCleared {
		t.Errorf("report = %+v; slot + row must still complete", rep)
	}
}

// TestDecommission_DryRunTouchesNothing pins --dry-run: every probe
// runs, no mutation happens, and the report reads as "would".
func TestDecommission_DryRunTouchesNothing(t *testing.T) {
	var order []string
	applier := &decomApplier{
		streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "sluice_wave_a")},
		order:   &order,
	}
	slots := &decomSlotMgr{
		slots:  []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}},
		pubOut: ir.PublicationDropDropped,
		order:  &order,
	}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", true)
	if err != nil {
		t.Fatalf("DecommissionStream: %v", err)
	}
	if !rep.DryRun || !rep.SlotDropped || !rep.PublicationDropped || !rep.ControlRowCleared {
		t.Errorf("report = %+v; dry-run must flag every would-removal", rep)
	}
	if slots.dropN != 0 {
		t.Errorf("Drop called %d times under --dry-run; want 0", slots.dropN)
	}
	// The publication surface is probed with dryRun=true; ClearStream
	// must not run at all.
	for _, step := range order {
		if strings.HasPrefix(step, "clear:") || strings.HasPrefix(step, "drop-slot:") || strings.HasPrefix(step, "drop-pub:") {
			t.Errorf("dry-run mutated: %v", order)
		}
	}
}

// noCleanerApplier serves control rows but deliberately lacks
// ClearStream, for the cannot-finish-must-not-start precondition pin.
type noCleanerApplier struct {
	stubChangeApplier
	streams []ir.StreamStatus
}

func (a *noCleanerApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return a.streams, nil
}

// TestDecommission_NoStreamCleanerRefusesUpFront pins the
// cannot-finish-must-not-start precondition: an applier without
// StreamCleaner refuses BEFORE any source-side drop.
func TestDecommission_NoStreamCleanerRefusesUpFront(t *testing.T) {
	applier := &noCleanerApplier{streams: []ir.StreamStatus{decomRow("wave-a", "sluice_wave_a", "")}}
	slots := &decomSlotMgr{slots: []ir.SlotInfo{{Name: "sluice_wave_a", Active: false}}}

	rep, err := DecommissionStream(context.Background(), applier, slots, "wave-a", false)
	if err == nil {
		t.Fatal("expected the no-cleaner refusal")
	}
	if rep != nil {
		t.Errorf("report = %+v; want nil", rep)
	}
	if !strings.Contains(err.Error(), "does not support clearing the cdc-state row") {
		t.Errorf("err = %v; want the no-cleaner wording", err)
	}
	if slots.dropN != 0 {
		t.Errorf("the slot was dropped despite the refusal (%d Drop calls) — a run that can't finish must not start", slots.dropN)
	}
}
