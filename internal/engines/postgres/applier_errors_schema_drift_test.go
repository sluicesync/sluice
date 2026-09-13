// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSchemaDriftIsRetriableForCDCAndTerminalForCopy pins Bug 285, and the
// thing it pins is that ONE SQLSTATE has to classify two ways depending on
// which lane asked.
//
// `42P01 undefined_table` / `42703 undefined_column` are RETRIABLE for the CDC
// applier and that is right: a long-running stream can reach a table the target
// does not have yet, sluice does not auto-apply DDL, and an operator creating
// it is the documented recovery. Riding it out beats exiting into a supervisor
// restart loop.
//
// For a COLD COPY the same verdict is wrong in a way that costs the operator
// half an hour. The relation being written into is one THIS RUN created in its
// own schema-apply phase minutes earlier. If it is gone, nobody is about to add
// it mid-migrate — the schema phase failed or targeted a different schema,
// which is exactly what `SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING` says. Measured
// by the v0.152.0 regression cycle: v0.151.1 answered in **67 ms** with that
// code; v0.152.0 retried for **30m24s across 69 attempts** and then reported
// the same code behind a headline about storage growth.
//
// The regression was the RAW lane joining a class the typed lane was already
// in — so this is fixed at the shared classifier and pinned for both, rather
// than patched on the one lane whose behaviour changed.
func TestSchemaDriftIsRetriableForCDCAndTerminalForCopy(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"42P01", "42703"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			// The shape the server actually returns, wrapped the way the copy
			// path wraps it before classifying.
			raw := fmt.Errorf("copy chunk into %q: %w", "public.sparse",
				&pgconn.PgError{Code: code, Message: `relation "public.sparse" does not exist`})

			// --- the CDC applier's verdict must NOT move ---
			applier := classifyApplierError(raw)
			var re ir.RetriableError
			if !errors.As(applier, &re) || !re.Retriable() {
				t.Fatalf("classifyApplierError(%s) is no longer retriable — the CDC applier rides this out "+
					"on purpose, and making it terminal reintroduces the supervisor crash-loop that verdict "+
					"exists to prevent: %v", code, applier)
			}
			if ir.IsTerminal(applier) {
				t.Fatalf("classifyApplierError(%s) is TERMINAL; the CDC lane must keep retrying", code)
			}

			// --- the copy path must refuse ---
			copyErr := classifyCopyError(raw)
			if !ir.IsTerminal(copyErr) {
				t.Fatalf("classifyCopyError(%s) is not TERMINAL. A cold copy will retry a missing relation "+
					"it created itself until the 30-minute wall, turning a 67ms diagnosis into a half-hour "+
					"of WARNs that end with the same verdict: %v", code, copyErr)
			}
			// The operator-facing text must survive the verdict flip — the
			// remedy sentence and the underlying PgError are what make the
			// failure actionable, and only the retry decision should change.
			var pgErr *pgconn.PgError
			if !errors.As(copyErr, &pgErr) || pgErr.Code != code {
				t.Errorf("the underlying *pgconn.PgError is no longer reachable through the copy "+
					"classification; the SQLSTATE is what downstream code and the operator key on: %v", copyErr)
			}
		})
	}
}

// TestCopyClassifierKeepsEveryOtherVerdict is the anti-over-correction half.
//
// `classifyCopyError` reverses exactly ONE verdict. If it reversed more — or if
// a future edit made it refuse anything it did not recognise — it would undo
// the storage-grow ride-out that the same release exists to provide, which is
// the opposite mistake and a worse one: the grow window is transient and
// retrying it is the entire point.
func TestCopyClassifierKeepsEveryOtherVerdict(t *testing.T) {
	t.Parallel()

	// Every code the copy path relies on riding out. Each is classified
	// retriable by the applier classifier and must stay that way here.
	// NOTE the MESSAGES are real, not placeholders. Two of these codes are
	// AND-gated on message text — a bare 25006 or 57014 is NOT transient,
	// because 25006 is only the grow shape when the server says the disk is
	// low, and a 57014 is only the PLATFORM cancelling when the server says so
	// rather than echoing sluice's own refusal back. Writing "simulated" here
	// made this test fail against correct code, which is worth recording: the
	// AND-gate is easy to forget and the failure looks like a regression.
	for _, tc := range []struct {
		code, msg, why string
	}{
		{"53100", "could not extend file: No space left on device", "disk_full while a managed volume grows under the write — the grow-gate ride-out"},
		{"08006", "write: broken pipe", "connection broken mid-COPY; the server aborts the transaction, so a retry is safe"},
		{"57014", "canceling statement due to statement timeout", "a PLATFORM-raised statement cancel (the 30s Neki wall), not sluice's own echoed refusal"},
		{"25006", "cannot execute INSERT in a read-only transaction: disk space low", "a shard gone read-only on low disk; clears when the volume grows"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()
			raw := fmt.Errorf("copy chunk: %w", &pgconn.PgError{Code: tc.code, Message: tc.msg})
			got := classifyCopyError(raw)
			if ir.IsTerminal(got) {
				t.Fatalf("classifyCopyError(%s) became TERMINAL — %s. Refusing it undoes the ride-out this "+
					"release was built to provide", tc.code, tc.why)
			}
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Fatalf("classifyCopyError(%s) is not retriable (%v) — %s", tc.code, got, tc.why)
			}
		})
	}
}
