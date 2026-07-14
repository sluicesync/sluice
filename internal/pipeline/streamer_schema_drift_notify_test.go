// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/notify"
)

// capturingNotifier records every delivered notification (and can be made to
// error) so the edge-once fire path is assertable without a live sink.
type capturingNotifier struct {
	mu       sync.Mutex
	got      []notify.Notification
	failWith error
}

func (c *capturingNotifier) Notify(_ context.Context, n notify.Notification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, n)
	return c.failWith
}

func (c *capturingNotifier) Name() string { return "capture" }

func (c *capturingNotifier) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

// TestMakeSchemaDriftNotification pins the refusal-error → Notification
// mapping: critical + schema-drift category, a title that names the stalled
// stream, and a body that IS the refusal message (so it carries the drift
// shape + offending table + the recovery hint verbatim).
func TestMakeSchemaDriftNotification(t *testing.T) {
	refusal := errors.New(`RENAME COLUMN "a"→"b" on "orders" cannot be auto-forwarded ... ` +
		`recovery: drained model — run 'sluice sync stop --wait', then run schema migrate`)
	at := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	n := makeSchemaDriftNotification("app-prod", refusal, at)

	if n.Level != notify.LevelCritical {
		t.Errorf("Level = %q; want critical", n.Level)
	}
	if n.Category != notify.CategorySchemaDrift {
		t.Errorf("Category = %q; want schema-drift", n.Category)
	}
	if n.StreamID != "app-prod" {
		t.Errorf("StreamID = %q; want app-prod", n.StreamID)
	}
	if !strings.Contains(n.Title, "app-prod") || !strings.Contains(n.Title, "manual recovery") {
		t.Errorf("Title %q should name the stream and the manual-recovery need", n.Title)
	}
	if n.Body != refusal.Error() {
		t.Errorf("Body must be the refusal message verbatim:\n got %q\nwant %q", n.Body, refusal.Error())
	}
	if !strings.Contains(n.Body, "recovery: drained model") {
		t.Errorf("Body %q lost the recovery hint", n.Body)
	}
	if !n.At.Equal(at) {
		t.Errorf("At = %v; want %v", n.At, at)
	}
}

// TestSchemaDriftLatchDecision pins the edge-once semantics as a pure
// function: a new refusal fires, the same one re-observed holds, a cleared
// pending re-arms, and the same refusal after a clear fires again.
func TestSchemaDriftLatchDecision(t *testing.T) {
	cases := []struct {
		name         string
		pending      string
		lastFired    string
		wantFire     bool
		wantNewLatch string
	}{
		{"new refusal → fire", "errX", "", true, "errX"},
		{"same refusal re-observed → hold", "errX", "errX", false, "errX"},
		{"different refusal → fire", "errY", "errX", true, "errY"},
		{"cleared (stall resolved) → re-arm", "", "errX", false, ""},
		{"same refusal after clear → fire again", "errX", "", true, "errX"},
		{"no pending, no prior → no-op", "", "", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fire, latch := schemaDriftLatchDecision(tc.pending, tc.lastFired)
			if fire != tc.wantFire || latch != tc.wantNewLatch {
				t.Errorf("schemaDriftLatchDecision(%q,%q) = (%v,%q); want (%v,%q)",
					tc.pending, tc.lastFired, fire, latch, tc.wantFire, tc.wantNewLatch)
			}
		})
	}
}

// storeRefusal / clearRefusal drive the streamer's schemaSnapshotErr the way
// the intercept and a resume respectively do.
func storeRefusal(s *Streamer, err error) { s.schemaSnapshotErr.Store(&err) }

func clearRefusal(s *Streamer) {
	var none error
	s.schemaSnapshotErr.Store(&none)
}

// TestObserveSchemaDriftForNotify_EdgeOnce pins the whole streamer fire path:
// one refusal fires exactly once even when re-observed (a retry loop), and it
// re-arms after the stall clears so a fresh refusal fires again.
func TestObserveSchemaDriftForNotify_EdgeOnce(t *testing.T) {
	captured := &capturingNotifier{}
	s := &Streamer{schemaDriftNotifierForTest: captured}
	refusal := errors.New("pipeline: forward schema add-column: RENAME COLUMN refused ... recovery: drained model")
	storeRefusal(s, refusal)

	// First observation fires.
	s.observeSchemaDriftForNotify(context.Background(), "s1")
	if captured.count() != 1 {
		t.Fatalf("first observe: fires = %d; want 1", captured.count())
	}
	// Re-observing the SAME pending refusal (retry loop) does NOT re-fire.
	s.observeSchemaDriftForNotify(context.Background(), "s1")
	s.observeSchemaDriftForNotify(context.Background(), "s1")
	if captured.count() != 1 {
		t.Fatalf("edge-once violated: fires = %d; want 1", captured.count())
	}

	// Stall clears (a resume) → re-arm.
	clearRefusal(s)
	s.observeSchemaDriftForNotify(context.Background(), "s1")
	if captured.count() != 1 {
		t.Fatalf("clear must not fire: fires = %d; want 1", captured.count())
	}
	// A subsequent refusal fires again.
	storeRefusal(s, errors.New("pipeline: forward schema add-column: a DIFFERENT refusal"))
	s.observeSchemaDriftForNotify(context.Background(), "s1")
	if captured.count() != 2 {
		t.Fatalf("re-armed refusal should fire: fires = %d; want 2", captured.count())
	}

	// The delivered notification carries the schema-drift shape.
	if got := captured.got[0]; got.Category != notify.CategorySchemaDrift || got.Level != notify.LevelCritical {
		t.Errorf("delivered notification = %+v; want critical schema-drift", got)
	}
}

// TestObserveSchemaDriftForNotify_Gating pins the two inert paths: suppressed
// ⇒ never fires; no sink configured ⇒ never fires (and never panics).
func TestObserveSchemaDriftForNotify_Gating(t *testing.T) {
	t.Run("suppressed → no fire", func(t *testing.T) {
		captured := &capturingNotifier{}
		s := &Streamer{schemaDriftNotifierForTest: captured, SuppressSchemaDriftNotify: true}
		storeRefusal(s, errors.New("refused"))
		s.observeSchemaDriftForNotify(context.Background(), "s1")
		if captured.count() != 0 {
			t.Fatalf("suppressed must not fire: fires = %d", captured.count())
		}
	})

	t.Run("no sink configured → no fire, no panic", func(_ *testing.T) {
		s := &Streamer{} // no test seam, no notify URLs
		storeRefusal(s, errors.New("refused"))
		s.observeSchemaDriftForNotify(context.Background(), "s1") // must not panic
	})
}

// TestObserveSchemaDriftForNotify_FailureIsolated pins that a dead sink is
// swallowed (never propagated — observe has no error return) and the latch
// still advances so the failed fire is not retried into a spam loop.
func TestObserveSchemaDriftForNotify_FailureIsolated(t *testing.T) {
	captured := &capturingNotifier{failWith: errors.New("sink down")}
	s := &Streamer{schemaDriftNotifierForTest: captured}
	storeRefusal(s, errors.New("refused ... recovery: drained model"))

	s.observeSchemaDriftForNotify(context.Background(), "s1")
	s.observeSchemaDriftForNotify(context.Background(), "s1")
	if captured.count() != 1 {
		t.Fatalf("a dead sink must be attempted once and swallowed, not retried: attempts = %d", captured.count())
	}
}
