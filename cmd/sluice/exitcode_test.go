// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// exitCodeLikeKong mirrors kong's exitCodeFromError (the code
// FatalIfErrorf runs at main's exit boundary): the OUTERMOST error in
// the chain implementing kong.ExitCoder wins; no ExitCoder means the
// traditional 1. Reimplemented here (kong's version is unexported) so
// the mapping the process actually ships with is pinned end to end.
func exitCodeLikeKong(err error) int {
	var ec kong.ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	if err == nil {
		return 0
	}
	return 1
}

// TestExitCodeTaxonomy pins the documented exit-code table
// (docs/operator/error-codes.md) at the kong exit boundary: 0 success,
// 1 generic failure (and runtime-class coded errors), 2 config errors
// and the verify-family's operationalError, 3 refusal-class coded
// errors — with the verify-family wrappers staying outermost so their
// long-documented per-command contract is unchanged even when they
// wrap a coded error.
func TestExitCodeTaxonomy(t *testing.T) {
	refusal := sluicecode.Wrap(
		sluicecode.CodeColdStartTargetNotEmpty, "pass --reset-target-data",
		errors.New("pipeline: cold-start refused"),
	)
	runtimeCoded := sluicecode.Wrap(
		sluicecode.CodeIndexStatementTimeLimit, "use --resume",
		errors.New("Error 3024: maximum statement execution time exceeded"),
	)
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil is success", nil, sluicecode.ExitSuccess},
		{"plain error is generic failure", errors.New("boom"), sluicecode.ExitFailure},
		{
			// The operator declining the destructive-confirm prompt is a
			// deliberate uncoded sentinel: non-zero (the run did not
			// complete) but the generic 1, not the refusal 3 — "refused"
			// names sluice as the decliner, and this decline was the
			// operator's. The --format json envelope renders it as
			// status "aborted" (envelope_test.go).
			"declined destructive confirm exits 1",
			errConfirmDeclined,
			sluicecode.ExitFailure,
		},
		{"runtime-class coded error keeps 1", runtimeCoded, sluicecode.ExitFailure},
		{"refusal-class coded error exits 3", refusal, sluicecode.ExitRefusal},
		{"refusal survives further wrapping", fmt.Errorf("sync: %w", refusal), sluicecode.ExitRefusal},
		{"config error exits 2", &sluicecode.ConfigError{Err: errors.New("config: unmarshal")}, sluicecode.ExitConfig},
		{"verify drift keeps its documented 1", driftError{summary: "1 mismatch"}, 1},
		{"verify operational error keeps its documented 2", operationalError{err: errors.New("dial")}, 2},
		{
			// The verify-family wrapper is outermost, so its exit code
			// wins over an inner coded error — per-command contract
			// stability beats the global taxonomy inside those commands.
			"operationalError wrapping a refusal stays 2",
			operationalError{err: refusal},
			2,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := exitCodeLikeKong(c.err); got != c.want {
				t.Errorf("exit code = %d; want %d", got, c.want)
			}
		})
	}
}

// captureHandler records slog records for assertion.
type captureHandler struct{ records *[]slog.Record }

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// TestLogCodedError pins the exit-boundary slog emission: a coded
// error produces exactly one ERROR record carrying `code` and `hint`
// attributes (the machine-branching surface under --log-format json);
// an uncoded error emits nothing (kong's stderr line covers it).
func TestLogCodedError(t *testing.T) {
	var records []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{records: &records}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	logCodedError(fmt.Errorf("outer: %w", sluicecode.Wrap(
		sluicecode.CodeValueNULByte, "use --type-override COL=bytea",
		errors.New("value contains a NUL byte"),
	)))
	if len(records) != 1 {
		t.Fatalf("coded error emitted %d records; want 1", len(records))
	}
	got := map[string]string{}
	records[0].Attrs(func(a slog.Attr) bool {
		got[a.Key] = a.Value.String()
		return true
	})
	if got["code"] != string(sluicecode.CodeValueNULByte) {
		t.Errorf("code attr = %q; want %q", got["code"], sluicecode.CodeValueNULByte)
	}
	if got["hint"] != "use --type-override COL=bytea" {
		t.Errorf("hint attr = %q; want the remedy", got["hint"])
	}
	if got["err"] == "" {
		t.Error("err attr missing; want the full error text")
	}
	if records[0].Level != slog.LevelError {
		t.Errorf("level = %v; want ERROR", records[0].Level)
	}

	records = records[:0]
	logCodedError(errors.New("plain failure"))
	logCodedError(nil)
	if len(records) != 0 {
		t.Errorf("uncoded/nil errors emitted %d records; want 0", len(records))
	}
}
