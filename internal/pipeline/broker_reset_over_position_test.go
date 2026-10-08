// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// cleaningApplier is a coveringApplier that is also an ir.StreamCleaner, and
// records how many tables had been dropped when the stream was cleared.
type cleaningApplier struct {
	coveringApplier
	dropped          *int
	clearedAtDropped int // -1 = never cleared
}

func (a *cleaningApplier) ClearStream(context.Context, string) error {
	a.clearedAtDropped = *a.dropped
	return nil
}

// TestBroker_ResetOverAnExistingPosition is the ADR-0191 review's MEDIUM:
// --reset-target-data over a target that already holds a broker position. The
// warm-resume branch used to come first and swallow the flag, so the remedy
// SLUICE-E-BROKER-INCREMENTAL-REWRITTEN, a corrupt position and
// BROKER-INCREMENTAL-PARTIAL prescribe resumed instead — and, for a
// key-changing source without identities, did the unmarked re-run those
// messages warn against. Graded on each position shape (a clean token, one
// standing inside an incremental, an undecodable one): the reset reaches the
// drop, and clears the stream's position row BEFORE it (a crash between the
// drop and the new position must not leave the old one over a half-restored
// target). Without a cleaner the reset refuses before dropping anything. The
// real-server half is TestBroker_ResetOverAPosition_Postgres.
func TestBroker_ResetOverAnExistingPosition(t *testing.T) {
	store, fullID := doorFixture(t, doorRows(false, true, false, false), true)
	tokens := map[string]ir.Position{
		"clean token":       encodeBrokerPosition("test://fe1", fullID),
		"in-progress token": encodeBrokerFrontier("test://fe1", fullID, &brokerInProgress{BackupID: "x", Chunks: strings.Repeat("0", 64), Through: 3}),
		"corrupt token":     {Engine: BackupBrokerPositionEngineV2, Token: `{"_engine":"` + BackupBrokerPositionEngineV2 + `","last_applied_backup_id":""}`},
	}
	for name, tok := range tokens {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dropped := 0
			app := &cleaningApplier{dropped: &dropped, clearedAtDropped: -1}
			p := tok
			app.resume = &p
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: resetDoorWriter{replayKeyWriter: replayKeyWriter{keyed: true}, dropped: &dropped}}
			b.ResetTargetData = true
			err := b.Run(ctx)
			if dropped == 0 {
				t.Fatalf("--reset-target-data over an existing position never reached the drop (Run = %v)", err)
			}
			if app.clearedAtDropped != 0 {
				t.Errorf("the stream's position row was cleared after %d drop(s) (-1 = never); want it cleared before the first", app.clearedAtDropped)
			}
		})
	}

	t.Run("no stream cleaner: refused before the drop", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		dropped := 0
		app := &coveringApplier{}
		p := encodeBrokerPosition("test://fe1", fullID)
		app.resume = &p
		b := newReplayBroker(store, &app.replayApplier, true)
		b.Target = replayTargetEngine{applier: app, rw: resetDoorWriter{replayKeyWriter: replayKeyWriter{keyed: true}, dropped: &dropped}}
		b.ResetTargetData = true
		err := b.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "cannot clear") {
			t.Fatalf("Run = %v; want the refusal naming the row it cannot clear", err)
		}
		if dropped != 0 {
			t.Errorf("dropped %d table(s) before refusing", dropped)
		}
	})
}

// TestBroker_AtChainIDOverAnExistingPosition pins the second flag the
// warm-resume branch used to swallow: --at-chain-id over a target that
// already holds a broker position. An assertion that agrees with the
// position (the same last fully applied incremental, nothing in progress)
// resumes; one that disagrees refuses BROKER-AT-CHAIN-ID-CONFLICT before
// anything is applied — before the fix, a target re-restored to an earlier
// link resumed from the stale row and skipped the incrementals between.
func TestBroker_AtChainIDOverAnExistingPosition(t *testing.T) {
	store, fullID := doorFixture(t, doorRows(false, true, false, false), true)
	for _, tc := range []struct {
		name      string
		tok       ir.Position
		at        string
		wantError bool
	}{
		{name: "agrees", tok: encodeBrokerPosition("test://fe1", fullID), at: fullID},
		{name: "names another link", tok: encodeBrokerPosition("test://fe1", fullID), at: "0123456789abcdef", wantError: true},
		{name: "position inside an incremental", tok: encodeBrokerFrontier("test://fe1", fullID, &brokerInProgress{BackupID: "x", Chunks: strings.Repeat("0", 64), Through: 3}), at: fullID, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &coveringApplier{}
			p := tc.tok
			app.resume = &p
			app.onWrite = func(ir.Position) { runCancel() }
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: true}}
			b.AtChainID = tc.at
			err := b.Run(runCtx)
			if !tc.wantError {
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("Run = %v; want an agreeing --at-chain-id to resume", err)
				}
				if len(app.received) == 0 {
					t.Fatal("the agreeing run applied nothing: it did not resume")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), BrokerAtChainIDConflictMarker) {
				t.Fatalf("Run = %v; want %s", err, BrokerAtChainIDConflictMarker)
			}
			if len(app.received) != 0 || len(app.written) != 0 {
				t.Errorf("the refused run reached the target: %d changes, %d position writes", len(app.received), len(app.written))
			}
		})
	}
}
