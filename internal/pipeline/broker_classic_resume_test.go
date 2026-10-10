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
		name         string
		touchKL      bool
		classicTok   bool
		wantRefused  bool
		noIdentities bool // the incremental records none: --at-chain-id would refuse again
	}{
		{name: "classic token, keyless table touched: refused", touchKL: true, classicTok: true, wantRefused: true},
		{name: "classic token, incremental without identities: refused, reset only", touchKL: true, classicTok: true, wantRefused: true, noIdentities: true},
		{name: "v2 token at the same position: replayed", touchKL: true},
		{name: "classic token, no keyless table touched: replayed", classicTok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, fullID := doorFixture(t, doorRows(tc.touchKL, !tc.touchKL, true, false), !tc.noIdentities)
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
			assertClassicResumeHint(t, ce.Hint, fullID, !tc.noIdentities, tc.noIdentities)
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

// assertClassicResumeHint pins Bug 298 and its review: the
// BROKER-CLASSIC-RESUME refusal's remedy is its own — always
// --reset-target-data — and not the general keyless remedy (re-key the
// source, take a new full, replay a chain this sluice wrote), which a chain
// this sluice wrote already meets and still refuses.
//   - atChain: the operator's --at-chain-id assertion is offered, which it
//     must be exactly when it would lift the table (F2). Its condition is
//     about every run since the position was written, and a later run's
//     refusal is named as NO evidence, never cited as proof (F1).
//   - general: the general remedy rides along for a reason other than the
//     classic token.
func assertClassicResumeHint(t *testing.T, hint, lastApplied string, atChain, general bool) {
	t.Helper()
	for _, want := range []string{BrokerClassicResumeMarker, "--reset-target-data", "refuses again"} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint lacks %q: %s", want, hint)
		}
	}
	if got := strings.Contains(hint, "--at-chain-id"); got != atChain {
		t.Errorf("hint offers --at-chain-id = %v; want %v: %s", got, atChain, hint)
	}
	if atChain {
		for _, want := range []string{"--at-chain-id=" + lastApplied, "sluice_cdc_state", "NO broker run started applying", "is no evidence", "silently"} {
			if !strings.Contains(hint, want) {
				t.Errorf("hint lacks %q: %s", want, hint)
			}
		}
	}
	for _, proof := range []string{"refused it with", "before applying any of it", "for example, a v0.156.11"} {
		if strings.Contains(hint, proof) {
			t.Errorf("hint cites a later run's refusal as proof (%q): %s", proof, hint)
		}
	}
	if got := strings.Contains(hint, brokerKeylessHint); got != general {
		t.Errorf("hint carries the general keyless remedy = %v; want %v: %s", got, general, hint)
	}
}

// TestClassicResumeHint_ChoosesByReason pins the hint's selection: a refusal
// naming no BROKER-CLASSIC-RESUME table keeps the general remedy; one whose
// classic tables are blocked by the token alone gets the classic remedy with
// the --at-chain-id route; one where a classic table is ALSO blocked by
// another reason (no identities, a change without one, unusable marks — which
// --at-chain-id cannot cure) gets --reset-target-data only, plus the general
// remedy; and a mixed refusal gets both remedies.
func TestClassicResumeHint_ChoosesByReason(t *testing.T) {
	b := &SyncFromBackup{StreamID: "s", classicSuspect: "INC2", classicAfter: "INC1"}
	classicAlone := keylessBlocker{why: "x", classic: true}
	classicAndMore := keylessBlocker{why: "x", classic: true, beyondClassic: true}
	other := keylessBlocker{why: "x", beyondClassic: true}
	if _, ok := b.classicResumeHint([]keylessBlocker{other}); ok {
		t.Error("a refusal naming no classic-resume table took the classic remedy")
	}
	hint, ok := b.classicResumeHint([]keylessBlocker{classicAlone})
	if !ok {
		t.Fatal("a classic-resume refusal kept the general remedy")
	}
	assertClassicResumeHint(t, hint, "INC1", true, false)
	if !strings.Contains(hint, "INC2") || !strings.Contains(hint, `"s"`) {
		t.Errorf("hint does not name the suspect incremental and the stream: %s", hint)
	}
	hint, _ = b.classicResumeHint([]keylessBlocker{classicAlone, other})
	assertClassicResumeHint(t, hint, "INC1", true, true)
	hint, _ = b.classicResumeHint([]keylessBlocker{classicAlone, classicAndMore})
	assertClassicResumeHint(t, hint, "INC1", false, true)
}
