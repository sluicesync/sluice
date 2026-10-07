// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// clearingReplayApplier is a replayApplier that declares
// [ir.ApplyMarksClearer] and records, in order, each clear and each
// position write.
type clearingReplayApplier struct {
	replayApplier
	mu     sync.Mutex
	events []string
}

func (a *clearingReplayApplier) ClearApplyMarks(_ context.Context, streamID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, "clear:"+streamID)
	return nil
}

func (a *clearingReplayApplier) WritePosition(ctx context.Context, s string, p ir.Position) error {
	a.mu.Lock()
	a.events = append(a.events, "write")
	a.mu.Unlock()
	return a.replayApplier.WritePosition(ctx, s, p)
}

// droppingWriter lets a --reset-target-data cold start drop and restore.
type droppingWriter struct {
	replayKeyWriter
	owner *clearingReplayApplier
}

func (w *droppingWriter) DropTable(context.Context, *ir.Table) error {
	w.owner.mu.Lock()
	defer w.owner.mu.Unlock()
	w.owner.events = append(w.owner.events, "drop")
	return nil
}

func (w *droppingWriter) IsTableEmpty(context.Context, *ir.Table) (bool, error) { return true, nil }

// TestBroker_ColdStartsClearTheStreamsMarks is ADR-0191 §3.4 (2), the unit
// half of P10: both cold starts clear the broker stream's ADR-0190 apply
// marks before they write a position or drop anything. Since the broker's
// changes carry the reader's identities, a stale mark naming a transaction
// the next incremental delivers first would be trusted and could skip a
// change the target never received. The real-server half, which plants such
// a mark and grades the target against the source, is
// TestBroker_ColdStartClearsAStaleMarkThatWouldSkip.
func TestBroker_ColdStartsClearTheStreamsMarks(t *testing.T) {
	for _, entry := range []string{"at-chain-id", "reset-target-data"} {
		t.Run(entry, func(t *testing.T) {
			store, fullID, _ := brokerReplayFixture(t, true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &clearingReplayApplier{}
			app.onWrite = func(ir.Position) { runCancel() } // stop once the cold start recorded its position
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: &droppingWriter{replayKeyWriter: replayKeyWriter{keyed: true}, owner: app}}
			if entry == "at-chain-id" {
				b.AtChainID = fullID
			} else {
				b.ResetTargetData = true
			}
			runErr := b.Run(runCtx)
			t.Logf("Run = %v", runErr)
			app.mu.Lock()
			events := slices.Clone(app.events)
			app.mu.Unlock()
			t.Logf("%s: %v", entry, events)
			cleared := slices.Index(events, "clear:"+b.StreamID)
			if cleared < 0 {
				t.Fatalf("the %s cold start never cleared stream %q's apply marks", entry, b.StreamID)
			}
			for i, e := range events[:cleared] {
				if e == "write" || e == "drop" {
					t.Errorf("the %s cold start %s (event %d) before it cleared the marks", entry, e, i)
				}
			}
			// Anti-vacuity: the step the clear must precede was reached — the
			// position write for --at-chain-id, the drop for a reset (whose
			// restore then stops on this stub's missing schema writer).
			reached := "write"
			if entry != "at-chain-id" {
				reached = "drop"
			}
			if !slices.Contains(events, reached) {
				t.Fatalf("the %s cold start never reached its %s: the cell grades nothing", entry, reached)
			}
		})
	}
}
