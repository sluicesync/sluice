// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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
//
// The refused token is PARKED: at the full's boundary, nothing in progress
// (the only shape a classic token takes). It is refused on purpose (Bug 298):
// an interrupted old run left its token exactly where a clean stop would, so
// the park proves nothing about the next incremental.
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
			assertClassicResumeHint(t, ce.Hint, fullID, false)
			// A plain re-run refuses again: nothing advanced the token.
			if err := newClassicRun(store, app).Run(ctx); err == nil || !strings.Contains(err.Error(), BrokerClassicResumeMarker) {
				t.Errorf("a plain re-run = %v; want %s again", err, BrokerClassicResumeMarker)
			}
		})
	}
}

// newClassicRun is a broker over app's target, as the refused cell built it.
func newClassicRun(store irbackup.Store, app *coveringApplier) *SyncFromBackup {
	b := newReplayBroker(store, &app.replayApplier, true)
	b.Target = replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: true}}
	return b
}

// assertClassicResumeHint pins Bug 298: the BROKER-CLASSIC-RESUME refusal's
// remedy is its own — --reset-target-data, or the operator's --at-chain-id
// assertion over the token's link after deleting the row — and not the general
// keyless remedy (re-key the source, take a new full, replay a chain this
// sluice wrote), which a chain this sluice wrote already meets and still
// refuses. A mixed refusal keeps the general remedy for its other tables.
func assertClassicResumeHint(t *testing.T, hint, lastApplied string, mixed bool) {
	t.Helper()
	for _, want := range []string{BrokerClassicResumeMarker, "--reset-target-data", "--at-chain-id=" + lastApplied, "sluice_cdc_state", "refuses again"} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint lacks %q: %s", want, hint)
		}
	}
	if got := strings.Contains(hint, brokerKeylessHint); got != mixed {
		t.Errorf("hint carries the general keyless remedy = %v; want %v: %s", got, mixed, hint)
	}
}

// TestClassicResumeHint_ChoosesByReason pins the hint's selection: a refusal
// naming no BROKER-CLASSIC-RESUME table keeps the general remedy, one naming
// only such tables gets the classic remedy alone, and a mixed one gets both.
func TestClassicResumeHint_ChoosesByReason(t *testing.T) {
	b := &SyncFromBackup{StreamID: "s", classicSuspect: "INC2", classicAfter: "INC1"}
	classic := migcore.ReplayKeylessTable{Name: "a", Reason: migcore.ReplayKeylessReason("no key; and " + BrokerClassicResumeMarker + ": …")}
	other := migcore.ReplayKeylessTable{Name: "b", Reason: migcore.ReplayKeylessTargetUnjudged}
	if _, ok := b.classicResumeHint([]migcore.ReplayKeylessTable{other}); ok {
		t.Error("a refusal naming no classic-resume table took the classic remedy")
	}
	hint, ok := b.classicResumeHint([]migcore.ReplayKeylessTable{classic})
	if !ok {
		t.Fatal("a classic-resume refusal kept the general remedy")
	}
	assertClassicResumeHint(t, hint, "INC1", false)
	if !strings.Contains(hint, "INC2") || !strings.Contains(hint, `"s"`) {
		t.Errorf("hint does not name the suspect incremental and the stream: %s", hint)
	}
	hint, _ = b.classicResumeHint([]migcore.ReplayKeylessTable{classic, other})
	assertClassicResumeHint(t, hint, "INC1", true)
}
