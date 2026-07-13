// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"bytes"
	"log/slog"
	"testing"
)

// captureLog swaps slog.Default with a text handler that drops the volatile
// `time` attr, so the emitted line is deterministic and can be pinned
// byte-for-byte. It returns the buffer and a restore func.
func captureLog(t *testing.T) (buf *bytes.Buffer, restore func()) {
	t.Helper()
	prev := slog.Default()
	buf = &bytes.Buffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	slog.SetDefault(slog.New(h))
	return buf, func() { slog.SetDefault(prev) }
}

// TestLogSinkGoldenLines pins that LogSink reproduces the EXACT structured
// records the migrate orchestrator emitted before ADR-0155. This is the
// load-bearing contract: --log-format=json and every non-TTY/CI/piped run
// must be byte-identical to before, so a drift here is a broken
// observability contract, not a cosmetic test failure.
//
// The expected strings are the pre-ADR-0155 lines transcribed from the
// call-sites the refactor replaced (migrate.go runBulkCopyPhases /
// runSingleDatabase / reportDegradedFKs), with the `time=` attr stripped.
func TestLogSinkGoldenLines(t *testing.T) {
	cases := []struct {
		name string
		emit func(s LogSink)
		want string
	}{
		{
			"phase complete",
			func(s LogSink) { s.PhaseCompleted(Phase{Key: "bulk_copy", Label: "Bulk copy"}) },
			`level=INFO msg="migration: phase complete" phase=bulk_copy` + "\n",
		},
		{
			"phase complete upfront",
			func(s LogSink) { s.PhaseCompletedEarly(Phase{Key: "indexes", Label: "Indexes"}) },
			`level=INFO msg="migration: phase complete (upfront)" phase=indexes` + "\n",
		},
		{
			"summary",
			func(s LogSink) { s.Summary(Result{Tables: 3}) },
			`level=INFO msg="migration complete" tables=3` + "\n",
		},
		{
			"degraded fk per-constraint warn",
			func(s LogSink) {
				s.Warn("constraint attached degraded (NOT VALID)",
					slog.String("schema", "public"),
					slog.String("table", "orders"),
					slog.String("constraint", "orders_customer_id_fkey"),
					slog.String("referenced_table", "customers"),
					slog.String("reason", "orphan child rows"),
					slog.String("hint", "fix the orphans then VALIDATE"))
			},
			`level=WARN msg="constraint attached degraded (NOT VALID)" schema=public table=orders constraint=orders_customer_id_fkey referenced_table=customers reason="orphan child rows" hint="fix the orphans then VALIDATE"` + "\n",
		},
		{
			"degraded fk summary warn",
			func(s LogSink) {
				s.Warn("constraints phase: degraded FKs",
					slog.Int("count", 2),
					slog.String("action_required",
						"run `ALTER TABLE ... VALIDATE CONSTRAINT <name>` for each after fixing the orphan rows on the child tables"))
			},
			"level=WARN msg=\"constraints phase: degraded FKs\" count=2 action_required=\"run `ALTER TABLE ... VALIDATE CONSTRAINT <name>` for each after fixing the orphan rows on the child tables\"\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, restore := captureLog(t)
			defer restore()
			tc.emit(LogSink{})
			if got := buf.String(); got != tc.want {
				t.Errorf("log line drift:\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// TestLogSinkSilentMethods pins that the methods with NO historical slog
// line emit nothing — PhaseStarted (the orchestrator logged only on
// completion) and TableProgress (the pipeline's own progressTicker still
// emits the rich "bulk copy progress" line on this path; LogSink must not
// double it). Any emission here would be a new record in the structured
// stream, i.e. a contract break.
func TestLogSinkSilentMethods(t *testing.T) {
	buf, restore := captureLog(t)
	defer restore()
	s := LogSink{}
	s.PhaseStarted(Phase{Key: "tables", Label: "Tables"})
	s.TableProgress("orders", 100, 1000)
	if buf.Len() != 0 {
		t.Errorf("expected no output, got: %q", buf.String())
	}
}

// TestFromContextDefaultsToLogSink pins that an un-wired context (every
// sync/test/broker path) yields the LogSink, which is what keeps those
// paths byte-identical.
func TestFromContextDefaultsToLogSink(t *testing.T) {
	if _, ok := FromContext(t.Context()).(LogSink); !ok {
		t.Fatalf("FromContext on a bare context = %T, want progress.LogSink", FromContext(t.Context()))
	}
}

// TestNopSinkEmitsNothing pins that the Nop sink — the non-TTY sink for
// verify/backup/restore — emits NO slog records for ANY method. Those
// commands own their own report/slog output; the sink must add nothing on
// the non-TTY path or it would inject lines into their streams (a broken
// observability contract, the phase-2 analogue of the LogSink golden).
func TestNopSinkEmitsNothing(t *testing.T) {
	buf, restore := captureLog(t)
	defer restore()
	var s Nop
	s.PhaseStarted(Phase{Key: "schema"})
	s.PhaseCompleted(Phase{Key: "schema"})
	s.PhaseCompletedEarly(Phase{Key: "schema"})
	s.TableProgress("orders", 1, 2)
	s.Warn("should not appear", slog.Int("x", 1))
	s.Summary(Result{Tables: 3, Fields: []Field{{Label: "Tables", Value: "3"}}})
	if buf.Len() != 0 {
		t.Errorf("Nop sink must emit nothing, got: %q", buf.String())
	}
}
