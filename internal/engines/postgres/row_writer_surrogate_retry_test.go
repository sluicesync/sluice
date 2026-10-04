// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit F-E1, the in-run sibling of the replay doors (PG half). The
// chunk-replay gate used to judge only the RECORDED table; a target keyed
// only on a bigserial / identity / defaulted surrogate the rows never carry
// passed it, and a replayed chunk whose first COPY committed before its
// acknowledgement was lost drew fresh surrogate values and doubled at exit
// 0. These unit pins drive copyChunkWithRetry — the retry core of BOTH the
// chunked-COPY path and the idempotent upsert path — with a scripted target
// probe; the real-server committed-but-ack-lost pin is
// TestColdCopyRetry_SurrogateKeyedTarget_AckLostRefuses (integration).

// TestPGCopyChunkRetry_UnsuppliedTargetKeyRefuses: recorded table keyed,
// target not keyed on anything the rows supply (or not found by the probe).
// The second COPY must never be issued.
func TestPGCopyChunkRetry_UnsuppliedTargetKeyRefuses(t *testing.T) {
	for _, c := range []struct {
		name    string
		probe   *scriptedReplayProbe
		verdict irbackup.ReplayKeyVerdict
	}{
		{"surrogate-only key", &scriptedReplayProbe{exists: true, keyed: false}, irbackup.ReplayKeylessTarget},
		{"table not found", &scriptedReplayProbe{exists: false}, irbackup.ReplayTargetAbsent},
	} {
		t.Run(c.name, func(t *testing.T) {
			withFastPGCopyBackoff(t)
			w := &RowWriter{growGate: &recordingGrowGate{}, replayKeyProbeForTest: c.probe}
			attempts := 0
			err := w.copyChunkWithRetry(context.Background(), pgKeyedPinTable("orders"), 5, func(context.Context) error {
				attempts++
				return diskFull53100()
			})
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
				t.Fatalf("got %v (coded=%v); want %s", err, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
			}
			if !strings.Contains(err.Error(), c.verdict.Describe()) {
				t.Errorf("the refusal must say which judgment failed (%q); got: %v", c.verdict.Describe(), err)
			}
			if attempts != 1 {
				t.Errorf("COPY attempts = %d; want 1 — the ambiguous chunk must not be replayed", attempts)
			}
			if got := c.probe.calls.Load(); got != 1 {
				t.Errorf("target probes = %d; want 1 — the refusal must come from the TARGET judgment", got)
			}
		})
	}
}

// TestPGCopyChunkRetry_UnansweredProbeNeverLicensesAReplay: a probe that
// meets the transient is ridden (replay only after an answer); a probe that
// fails terminally ends the chunk loudly, unreplayed and not dressed up as
// a keyless verdict.
func TestPGCopyChunkRetry_UnansweredProbeNeverLicensesAReplay(t *testing.T) {
	t.Run("transient probe failure is ridden", func(t *testing.T) {
		withFastPGCopyBackoff(t)
		probe := &scriptedReplayProbe{exists: true, keyed: true, errs: []error{diskFull53100()}}
		w := &RowWriter{growGate: &recordingGrowGate{}, replayKeyProbeForTest: probe}
		attempts := 0
		err := w.copyChunkWithRetry(context.Background(), pgKeyedPinTable("orders"), 5, func(context.Context) error {
			attempts++
			if attempts == 1 {
				return diskFull53100()
			}
			return nil
		})
		if err != nil {
			t.Fatalf("a keyed chunk whose first probe met the transient must still converge: %v", err)
		}
		if got := probe.calls.Load(); got != 2 {
			t.Errorf("target probes = %d; want 2 (one failed, one answered)", got)
		}
		if attempts != 2 {
			t.Errorf("COPY attempts = %d; want 2 — exactly one replay, only after a probe answered", attempts)
		}
	})
	t.Run("terminal probe failure is loud", func(t *testing.T) {
		withFastPGCopyBackoff(t)
		probeErr := &pgconn.PgError{Code: "42501", Message: "permission denied for table pg_class"}
		probe := &scriptedReplayProbe{errs: []error{probeErr}}
		w := &RowWriter{growGate: &recordingGrowGate{}, replayKeyProbeForTest: probe}
		attempts := 0
		err := w.copyChunkWithRetry(context.Background(), pgKeyedPinTable("orders"), 5, func(context.Context) error {
			attempts++
			return diskFull53100()
		})
		if err == nil || !errors.Is(err, probeErr) || !strings.Contains(err.Error(), "judge whether replaying") {
			t.Fatalf("want the probe's own error, saying what it was for; got: %v", err)
		}
		if _, coded := sluicecode.FromError(err); coded {
			t.Errorf("an unanswered probe must not be reported as a keyless verdict: %v", err)
		}
		if attempts != 1 {
			t.Errorf("COPY attempts = %d; want 1 — nothing replayed unjudged", attempts)
		}
	})
}

// TestPGCopyChunkRetry_ProbesTheTargetOncePerTable: two chunks of one table,
// each meeting a transient, share one probe; a clean chunk never probes.
func TestPGCopyChunkRetry_ProbesTheTargetOncePerTable(t *testing.T) {
	withFastPGCopyBackoff(t)
	probe := keyedReplayProbe()
	w := &RowWriter{growGate: &recordingGrowGate{}, replayKeyProbeForTest: probe}
	ctx := context.Background()

	if err := w.copyChunkWithRetry(ctx, pgKeyedPinTable("orders"), 5, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := probe.calls.Load(); got != 0 {
		t.Errorf("a chunk that met no transient probed the target %d times; want 0", got)
	}
	for chunk := 1; chunk <= 2; chunk++ {
		attempts := 0
		if err := w.copyChunkWithRetry(ctx, pgKeyedPinTable("orders"), 5, func(context.Context) error {
			attempts++
			if attempts == 1 {
				return diskFull53100()
			}
			return nil
		}); err != nil {
			t.Fatalf("chunk %d: %v", chunk, err)
		}
		if attempts != 2 {
			t.Fatalf("chunk %d: attempts = %d; want 2 — the scenario did not happen", chunk, attempts)
		}
	}
	if got := probe.calls.Load(); got != 1 {
		t.Errorf("target probes = %d over two retried chunks of one table; want 1 (memoised)", got)
	}
	// SetSchema moves the probe's scope, so the memo must not survive it.
	w.SetSchema("elsewhere")
	attempts := 0
	if err := w.copyChunkWithRetry(ctx, pgKeyedPinTable("orders"), 5, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return diskFull53100()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := probe.calls.Load(); got != 2 {
		t.Errorf("target probes after SetSchema = %d; want 2 — a memo from the old schema was reused", got)
	}
}
