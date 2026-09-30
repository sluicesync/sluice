// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeSlotAckReleaser records every release the ceiling sidecar hands it.
type fakeSlotAckReleaser struct {
	mu       sync.Mutex
	released []ir.Position
	err      error
}

func (r *fakeSlotAckReleaser) ReleaseSlotAckTo(pos ir.Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = append(r.released, pos)
	return r.err
}

func (r *fakeSlotAckReleaser) releases() []ir.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ir.Position(nil), r.released...)
}

// TestReleaseDurableSlotAck_ReleasesTheTargetsDurablePosition pins the
// ceiling tick's one job (GC-41): the position it releases is the one the
// TARGET reports as persisted, re-stamped with the SOURCE engine's name —
// the applier stamps positions with its own engine name, and the Postgres
// reader's decoder refuses a foreign tag, so an un-stamped release from a
// MySQL target would fail every tick and freeze the slot.
func TestReleaseDurableSlotAck_ReleasesTheTargetsDurablePosition(t *testing.T) {
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Engine: "mysql", Token: `{"slot":"s","lsn":"0/10"}`}}
	releaser := &fakeSlotAckReleaser{}

	failing := releaseDurableSlotAck(context.Background(), applier, releaser, "s1", "postgres", false)

	if failing {
		t.Error("a successful release left the failure latch set")
	}
	got := releaser.releases()
	if len(got) != 1 {
		t.Fatalf("releases = %d; want exactly 1", len(got))
	}
	if got[0].Engine != "postgres" || got[0].Token != applier.pos.Token {
		t.Errorf("released %+v; want the target's token re-stamped as the source engine (postgres)", got[0])
	}
}

// TestReleaseDurableSlotAck_NothingPersistedReleasesNothing: a stream with no
// persisted row has nothing durable to release, and releasing anything would
// be releasing something the target does not hold.
func TestReleaseDurableSlotAck_NothingPersistedReleasesNothing(t *testing.T) {
	applier := &autoPruneFakeApplier{found: false}
	releaser := &fakeSlotAckReleaser{}

	if failing := releaseDurableSlotAck(context.Background(), applier, releaser, "s1", "postgres", false); failing {
		t.Error("no persisted row is not a failure")
	}
	if n := len(releaser.releases()); n != 0 {
		t.Errorf("releases = %d; want 0 when the target has persisted nothing", n)
	}
}

// TestReleaseDurableSlotAck_FailuresHoldTheAck: a target read failure or a
// release the reader refuses must hold the ack (retention, never loss), set
// the latch so the WARN fires once per streak, and clear it on recovery.
func TestReleaseDurableSlotAck_FailuresHoldTheAck(t *testing.T) {
	ctx := context.Background()

	down := &autoPruneFakeApplier{readErr: errors.New("target down")}
	releaser := &fakeSlotAckReleaser{}
	if !releaseDurableSlotAck(ctx, down, releaser, "s1", "postgres", false) {
		t.Error("a read failure did not set the failure latch")
	}
	if n := len(releaser.releases()); n != 0 {
		t.Errorf("releases = %d after a read failure; want 0 (hold the ack)", n)
	}

	refusing := &fakeSlotAckReleaser{err: errors.New("decode")}
	up := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: "x"}}
	if !releaseDurableSlotAck(ctx, up, refusing, "s1", "postgres", true) {
		t.Error("a refused release cleared the failure latch")
	}

	if releaseDurableSlotAck(ctx, up, releaser, "s1", "postgres", true) {
		t.Error("a successful release after a failing streak left the latch set")
	}
}

// TestStartSlotAckCeiling_NoOpWithoutASlot: a source with no slot-keeping
// reader (MySQL, VStream, the trigger engines, every bare test Streamer)
// never spawns the sidecar and never reads the target.
func TestStartSlotAckCeiling_NoOpWithoutASlot(t *testing.T) {
	s := &Streamer{Source: stubEngine{}}
	applier := &autoPruneFakeApplier{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startSlotAckCeiling(ctx, "s1", applier)

	if applier.reads() != 0 {
		t.Errorf("applier reads = %d; want 0 (no slot ⇒ no sidecar)", applier.reads())
	}
}

// TestCaptureSlotAckReleaser records a slot-keeping reader and leaves the
// field nil for any other.
func TestCaptureSlotAckReleaser(t *testing.T) {
	s := &Streamer{}
	s.captureSlotAckReleaser(struct{}{})
	if s.slotAck != nil {
		t.Fatal("a reader without ReleaseSlotAckTo was captured")
	}
	r := &fakeSlotAckReleaser{}
	s.captureSlotAckReleaser(r)
	if s.slotAck != r {
		t.Fatal("a slot-keeping reader was not captured")
	}
}
