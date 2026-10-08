// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestBroker_ClassicTokenResumeWithholdsTheKeylessLift is the v0.157.0
// review's LOW (item 2): a broker resumed over a CLASSIC `backup-broker`
// token — written by v0.156.12 or older, which kept no frontier and no apply
// marks — may have been interrupted inside the next incremental, whose
// committed prefix then has nothing to skip it. So the keyless lift is
// withheld for that one incremental (BROKER-CLASSIC-RESUME), even though it
// records identities and the target's marks cover the table. The same
// position as a v2 token replays (the reverse), and a classic resume whose
// next incremental touches no keyless table is not refused (the narrowing).
func TestBroker_ClassicTokenResumeWithholdsTheKeylessLift(t *testing.T) {
	classic := func(fullID string) ir.Position {
		tok := `{"_engine":"` + BackupBrokerPositionEngine + `","chain_url":"test://fe1","last_applied_backup_id":"` + fullID + `"}`
		return ir.Position{Engine: BackupBrokerPositionEngine, Token: tok}
	}
	for _, tc := range []struct {
		name        string
		touchKL     bool
		classicTok  bool
		wantRefused bool
	}{
		{name: "classic token, keyless table touched: refused", touchKL: true, classicTok: true, wantRefused: true},
		{name: "v2 token at the same position: replayed", touchKL: true},
		{name: "classic token, no keyless table touched: replayed", classicTok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, fullID := doorFixture(t, doorRows(tc.touchKL, !tc.touchKL, true, false), true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &coveringApplier{}
			p := encodeBrokerPosition("test://fe1", fullID)
			if tc.classicTok {
				p = classic(fullID)
			}
			app.resume = &p
			app.onWrite = func(ir.Position) { runCancel() }
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: true}}
			err := b.Run(runCtx)
			if !tc.wantRefused {
				if err != nil {
					t.Fatalf("Run = %v; want the incremental replayed", err)
				}
				if len(app.received) == 0 {
					t.Fatal("nothing reached the applier")
				}
				return
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBrokerKeylessTable || !strings.Contains(err.Error(), BrokerClassicResumeMarker) {
				t.Fatalf("Run = %v; want %s with %s", err, sluicecode.CodeBrokerKeylessTable, BrokerClassicResumeMarker)
			}
			if len(app.received) != 0 || len(app.written) != 0 {
				t.Errorf("the refused incremental reached the target: %d changes, %d position writes", len(app.received), len(app.written))
			}
		})
	}
}
