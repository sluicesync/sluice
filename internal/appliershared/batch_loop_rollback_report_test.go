// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// rollbackArm is one place the serial batch loop rolls back and returns an
// error, with the WARN message it logs there.
type rollbackArm struct {
	name, warn string
	batchSize  int
	checkpoint bool
	changes    []ir.Change
	// inject makes cfg fail at this arm with err, and counts the failure.
	inject func(cfg *BatchConfig, err error, fired *int)
}

func rollbackArms() []rollbackArm {
	failDispatchAt := func(token string) func(*BatchConfig, error, *int) {
		return func(cfg *BatchConfig, err error, fired *int) {
			cfg.Dispatch = func(_ context.Context, _ BatchTx, _ string, c ir.Change) (bool, error) {
				if c.Pos().Token == token {
					*fired++
					return false, err
				}
				return false, nil
			}
		}
	}
	failPosition := func(cfg *BatchConfig, err error, fired *int) {
		cfg.WritePosition = func(context.Context, BatchTx, string, string, int64) error { *fired++; return err }
	}
	return []rollbackArm{
		{
			name: "dispatch of the batch's first change", warn: "fake: applier: batch rollback on error",
			batchSize: 10, changes: []ir.Change{insertAt("p1")}, inject: failDispatchAt("p1"),
		},
		{
			name: "dispatch of a later change", warn: "fake: applier: batch rollback on error",
			batchSize: 10, changes: []ir.Change{insertAt("p1"), insertAt("p2")}, inject: failDispatchAt("p2"),
		},
		{
			name: "position write", warn: "fake: applier: batch rollback on position-write error",
			batchSize: 10, changes: []ir.Change{insertAt("p1")}, inject: failPosition,
		},
		{
			name: "commit", warn: "fake: applier: batch commit error",
			batchSize: 10, changes: []ir.Change{insertAt("p1")},
			inject: func(cfg *BatchConfig, err error, fired *int) {
				cfg.Commit = func(BatchTx) error { *fired++; return err }
			},
		},
		{
			// batchSize 1 commits p1 mid-transaction without its position, so
			// the TxCommit arrives on an empty batch: writeBoundaryOnly.
			name: "boundary-only position write", warn: "fake: applier: boundary-position rollback on write error",
			batchSize: 1, checkpoint: true,
			changes: []ir.Change{txBegin("p1"), insertAt("p1"), txCommit("p1")},
			inject:  failPosition,
		},
	}
}

// TestBatchLoopRollback_ReportsATerminalRefusalOnceThroughTheCommand pins Bug
// 309 on every arm where the serial batch loop rolls back (dispatch, position
// write, commit, boundary-only position write):
//
//   - a terminal refusal keeps its sentinel and its terminality through the
//     engine's classifier — the fleet supervisor's do-not-restart roster and
//     chain restore's remedy rewrite both key on them — and is NOT logged at
//     WARN with its text: the command it is returned to reports it, in its own
//     words (chain restore replaces the KEY-COLLATION-MERGE remedy written for
//     `sync start`, Bug 308), and the WARN put the engine's wording in front
//     of the operator first;
//   - a transient (the caller retries it) is still logged at WARN, with the
//     arm's own message — which is also what proves each arm was reached.
//
// Representative terminal: [ir.KeyCollationMergeError]. The loop does not
// dispatch on which sentinel a refusal carries, only on [ir.IsTerminal], so
// one terminal refusal reaches the branch every one takes. The expected
// values are independent of the loop: the injected error itself, and the
// marker written into its remedy.
func TestBatchLoopRollback_ReportsATerminalRefusalOnceThroughTheCommand(t *testing.T) {
	const streamRemedy = "STREAM-ONLY-REMEDY"
	arms := rollbackArms()
	reached := 0
	for _, arm := range arms {
		for _, terminal := range []bool{true, false} {
			kind := "transient"
			if terminal {
				kind = "terminal refusal"
			}
			t.Run(arm.name+"/"+kind, func(t *testing.T) {
				injected := errors.New("transient: connection reset")
				if terminal {
					injected = &ir.KeyCollationMergeError{
						Schema: "s", Table: "t", Remedy: streamRemedy, Cause: errors.New("server: out of range"),
					}
				}
				captured := &logcapture.Buffer{}
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(captured, &slog.HandlerOptions{Level: slog.LevelDebug})))
				t.Cleanup(func() { slog.SetDefault(prev) })

				rec := &recorder{}
				cfg := testConfig(t, rec, false)
				cfg.CheckpointOnlyAtTxBoundary = arm.checkpoint
				fired := 0
				arm.inject(cfg, injected, &fired)
				err := RunBatchLoop(context.Background(), cfg, "stream", feed(true, arm.changes...), arm.batchSize)
				if fired != 1 {
					t.Fatalf("the injected failure fired %d times; want once (the arm was not reached as built)", fired)
				}
				if !errors.Is(err, injected) {
					t.Fatalf("err = %v; want the injected error's chain intact", err)
				}
				logs := captured.String()
				if terminal {
					if !errors.Is(err, ir.ErrKeyCollationMerge) || !ir.IsTerminal(err) {
						t.Fatalf("err = %v; want KEY-COLLATION-MERGE through errors.Is, still terminal", err)
					}
					if strings.Contains(logs, "level=WARN") || strings.Contains(logs, streamRemedy) {
						t.Fatalf("a terminal refusal was logged with its text before the command could report it:\n%s", logs)
					}
					return
				}
				if ir.IsTerminal(err) {
					t.Fatalf("err = %v; a transient became terminal", err)
				}
				if !strings.Contains(logs, "level=WARN msg=\""+arm.warn+"\"") || !strings.Contains(logs, "connection reset") {
					t.Fatalf("a transient at this arm was not logged as %q with its text:\n%s", arm.warn, logs)
				}
				reached++
			})
		}
	}
	// Anti-vacuity: every arm's transient WARN was seen, so every arm ran.
	if reached != len(arms) || len(arms) != 5 {
		t.Fatalf("%d of %d arms reached their WARN; want all 5", reached, len(arms))
	}
}
