// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestBatchLoopRollbackWarn_WithholdsATerminalRefusal pins Bug 309. The
// serial loop logged a WARN carrying the raw applier error before returning
// it; for a KEY-COLLATION-MERGE that WARN repeated the applier's stream
// remedy, which a chain restore then replaces with one `restore` can run —
// so the log held two remedies, the first one wrong. Each arm that fails a
// batch must keep a transient's text (it is retried, and the WARN is its
// only record) and withhold a terminal refusal's (the caller reports it),
// while the returned error still carries the refusal whole.
func TestBatchLoopRollbackWarn_WithholdsATerminalRefusal(t *testing.T) {
	const remedy = "STREAM-ONLY-REMEDY"
	terminal := func() error {
		return &ir.KeyCollationMergeError{Schema: "s", Table: "t", Remedy: remedy, Cause: errors.New("server 1690")}
	}
	transient := func() error { return errors.New("TRANSIENT-TEXT") }

	arms := []struct {
		name   string
		inject func(cfg *BatchConfig, err func() error)
	}{
		{"dispatch, first change", func(cfg *BatchConfig, err func() error) {
			cfg.Dispatch = func(context.Context, BatchTx, string, ir.Change) (bool, error) { return false, err() }
		}},
		{"dispatch, later change", func(cfg *BatchConfig, err func() error) {
			calls := 0
			cfg.Dispatch = func(context.Context, BatchTx, string, ir.Change) (bool, error) {
				calls++
				if calls == 2 {
					return false, err()
				}
				return false, nil
			}
		}},
		{"position write", func(cfg *BatchConfig, err func() error) {
			cfg.WritePosition = func(context.Context, BatchTx, string, string, int64) error { return err() }
		}},
		{"commit", func(cfg *BatchConfig, err func() error) {
			cfg.Commit = func(tx BatchTx) error { _ = tx.Rollback(); return err() }
		}},
	}
	for _, arm := range arms {
		for _, kind := range []struct {
			name     string
			err      func() error
			terminal bool
		}{{"terminal", terminal, true}, {"transient", transient, false}} {
			t.Run(arm.name+"/"+kind.name, func(t *testing.T) {
				buf := &logcapture.Buffer{}
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
				defer slog.SetDefault(prev)

				cfg := testConfig(t, &recorder{}, false)
				arm.inject(cfg, kind.err)
				got := RunBatchLoop(context.Background(), cfg, "s", feed(true, insertAt("1"), insertAt("2")), 10)
				if got == nil {
					t.Fatal("RunBatchLoop = nil; want the injected error")
				}
				logged := buf.String()
				if !strings.Contains(logged, `"level":"WARN"`) {
					t.Fatalf("no WARN logged for the failed batch (anti-vacuity): %q", logged)
				}
				if kind.terminal {
					if !ir.IsTerminal(got) || !strings.Contains(got.Error(), remedy) {
						t.Errorf("returned error lost the refusal: %v", got)
					}
					if strings.Contains(logged, remedy) || strings.Contains(logged, ir.ErrKeyCollationMerge.Error()) {
						t.Errorf("WARN repeats the terminal refusal the caller reports: %s", logged)
					}
				} else if !strings.Contains(logged, "TRANSIENT-TEXT") {
					t.Errorf("WARN dropped a transient's text: %s", logged)
				}
			})
		}
	}
}

// TestBatchLoopRollbackWarn_EveryErrAttrIsGuarded covers the arm the test
// above cannot reach cheaply (the boundary-position write) and any arm added
// later: no WARN in batch_loop.go may log a raw err.Error() as its "err"
// attribute. Floor: the four WARN lines that exist today (the shared
// dispatch helper, which serves both dispatch arms; the position write; the
// commit; the boundary-position write) route through rollbackErrAttr. This
// is a source-text check scoped to this one file, by design.
func TestBatchLoopRollbackWarn_EveryErrAttrIsGuarded(t *testing.T) {
	src, err := os.ReadFile("batch_loop.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if n := strings.Count(s, `slog.String("err", err.Error())`); n != 1 {
		// The one permitted occurrence is rollbackErrAttr's own transient arm.
		t.Errorf("batch_loop.go logs err.Error() directly %d times; want only rollbackErrAttr's own", n)
	}
	if n := strings.Count(s, "rollbackErrAttr(err),"); n < 4 {
		t.Errorf("rollbackErrAttr reaches %d WARN sites; want at least 4", n)
	}
}
