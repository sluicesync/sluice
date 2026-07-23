// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Injected-fault pins for the control-table preamble + publication-
// ratchet reads (audit 2026-07-23 ARCH-4): phases 2–2.8 run on every
// retry re-establish, so a transient network blip there must carry the
// [connectPhaseError] marker (via connectHint) that lets runWithRetry's
// connect-transient fall-through ride it — exactly like the applier-
// open sites. Pre-fix these sites wrapped with the bare PhaseSchemaApply
// hint, so a blip during a retry attempt's re-establish was terminal at
// precisely the step class Bugs 199/200 fixed one phase earlier.

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// errPreambleDial is the canonical local target-restart shape (Bug 199a
// wording): matched by the shared transient corpus, no ir.RetriableError
// wrapper — only the connect marker can make it retriable.
var errPreambleDial = errors.New("dial tcp 127.0.0.1:5432: connectex: No connection could be made because the target machine actively refused it")

// faultPreambleApplier fails exactly one control-table preamble call
// with the injected error; everything else delegates to the benign
// posControlApplier stub.
type faultPreambleApplier struct {
	posControlApplier

	ensureErr error
	clearErr  error
	listErr   error
}

func (a faultPreambleApplier) EnsureControlTable(context.Context) error { return a.ensureErr }
func (a faultPreambleApplier) ClearStopRequested(context.Context, string) error {
	return a.clearErr
}

func (a faultPreambleApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	if a.listErr != nil {
		return nil, a.listErr
	}
	return nil, nil
}

func TestPhasePrepareControlTable_TransientFaultsCarryConnectMarker(t *testing.T) {
	cases := []struct {
		name    string
		applier faultPreambleApplier
	}{
		{"EnsureControlTable dial failure (phase 2)", faultPreambleApplier{ensureErr: errPreambleDial}},
		{"ClearStopRequested dial failure (phase 2.5)", faultPreambleApplier{clearErr: errPreambleDial}},
		{"ListStreams dial failure (fingerprint check, phase 2.7)", faultPreambleApplier{listErr: errPreambleDial}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			s := &Streamer{
				StreamID: "test-stream",
				// A parseable DSN so fingerprintSourceDSN is non-empty and
				// the phase actually reaches its ListStreams read.
				SourceDSN: "postgres://src.example.com:5432/app",
			}
			err := s.phasePrepareControlTable(context.Background(), c.applier, "test-stream")
			if err == nil {
				t.Fatal("phasePrepareControlTable returned nil; the injected fault must surface")
			}
			if !errors.Is(err, errPreambleDial) {
				t.Fatalf("underlying cause lost from the chain: %v", err)
			}
			if !isRetriableConnectFailure(err) {
				t.Errorf("a transient network blip in the control-table preamble is TERMINAL (missing the connect marker) — the ARCH-4 re-establish gap: %v", err)
			}
		})
	}
}

// TestPhasePrepareControlTable_TerminalFaultsStayTerminal — the marker
// alone must never widen the retry surface: a marked failure without a
// positively-matched transient shape (permission fault, coded refusal)
// stays terminal exactly as before.
func TestPhasePrepareControlTable_TerminalFaultsStayTerminal(t *testing.T) {
	permErr := errors.New(`ERROR: permission denied for table sluice_cdc_state (SQLSTATE 42501)`)
	s := &Streamer{StreamID: "test-stream", SourceDSN: "postgres://src.example.com:5432/app"}
	err := s.phasePrepareControlTable(context.Background(), faultPreambleApplier{ensureErr: permErr}, "test-stream")
	if err == nil {
		t.Fatal("phasePrepareControlTable returned nil; the injected fault must surface")
	}
	if isRetriableConnectFailure(err) {
		t.Errorf("a permission fault classified connect-retriable — the marker must not widen the retry surface: %v", err)
	}
}

// scoperStubEngine makes the stub target engine satisfy
// ir.PublicationScoper so phaseResolvePublicationScope's recorded-state
// read (phase 2.8) is reachable in a unit test.
type scoperStubEngine struct{ posControlTargetEngine }

func (e scoperStubEngine) WithPublicationScope(string, string) ir.Engine { return e }

func TestPhaseResolvePublicationScope_TransientReadCarriesConnectMarker(t *testing.T) {
	s := &Streamer{
		StreamID: "test-stream",
		Source:   scoperStubEngine{},
	}
	err := s.phaseResolvePublicationScope(context.Background(), faultPreambleApplier{listErr: errPreambleDial}, "test-stream")
	if err == nil {
		t.Fatal("phaseResolvePublicationScope returned nil; the injected fault must surface")
	}
	if !errors.Is(err, errPreambleDial) {
		t.Fatalf("underlying cause lost from the chain: %v", err)
	}
	if !isRetriableConnectFailure(err) {
		t.Errorf("a transient network blip in the ratchet's recorded-publication read is TERMINAL (missing the connect marker) — the ARCH-4 re-establish gap: %v", err)
	}
}
