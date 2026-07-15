// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestClassifyPoolerStripError_Signature pins the item-69e
// classification: the exact Supavisor/pgbouncer strip signature —
// SQLSTATE 42601 on/near CREATE_REPLICATION_SLOT, live-probed against
// Supabase 2026-07-15 — becomes the coded SLUICE-E-CDC-POOLER-ENDPOINT
// error naming the pooler and the direct-endpoint remedy, with the
// original error preserved in the chain.
func TestClassifyPoolerStripError_Signature(t *testing.T) {
	pgErr := &pgconn.PgError{
		Code:     "42601",
		Message:  `syntax error at or near "CREATE_REPLICATION_SLOT"`,
		Severity: "ERROR",
	}
	// The real call site wraps the pgconn error before classification
	// (createSlotRawProtocol's fmt.Errorf) — match that shape so the
	// errors.As traversal is pinned through a wrap.
	wrapped := fmt.Errorf("postgres: create replication slot %q (raw protocol): %w", "sluice_slot", pgErr)

	got := classifyPoolerStripError(wrapped)
	ce, ok := sluicecode.FromError(got)
	if !ok {
		t.Fatalf("classifyPoolerStripError(%v) carries no CodedError", wrapped)
	}
	if ce.Code != sluicecode.CodeCDCPoolerEndpoint {
		t.Errorf("Code = %q; want %q", ce.Code, sluicecode.CodeCDCPoolerEndpoint)
	}
	if ce.Hint == "" {
		t.Error("Hint is empty; want the direct-endpoint remedy")
	}
	msg := got.Error()
	for _, want := range []string{"connection pooler", "replication=database", "DIRECT database endpoint", "IPv6-only", "42601"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message should mention %q; got: %v", want, msg)
		}
	}
	if !errors.Is(got, pgErr) {
		t.Error("original pgconn error must stay traversable in the chain")
	}
}

// TestClassifyPoolerStripError_Passthrough pins the narrow-match
// posture: anything that is not exactly the strip signature — a
// genuine 42601 from user SQL, a different SQLSTATE, a non-pg error,
// nil — passes through UNCHANGED (same error value, no code attached).
func TestClassifyPoolerStripError_Passthrough(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"non-pg error", errors.New("dial tcp: connection refused")},
		{"42601 without the replication command", &pgconn.PgError{
			Code: "42601", Message: `syntax error at or near "SELCT"`,
		}},
		{"replication command with a different SQLSTATE", &pgconn.PgError{
			Code: "55006", Message: `replication slot "sluice_slot" is active; CREATE_REPLICATION_SLOT refused`,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			//nolint:errorlint // pinning identity passthrough, not chain equality
			if got := classifyPoolerStripError(c.err); got != c.err {
				t.Errorf("classifyPoolerStripError(%v) = %v; want the error unchanged", c.err, got)
			}
		})
	}
}
