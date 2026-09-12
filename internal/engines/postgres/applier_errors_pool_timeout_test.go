// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyApplierError_SidecarPoolTimeoutIsTransient pins a fix whose
// absence killed a healthy stream in the field.
//
// A PlanetScale Neki branch runs a sidecar POOL in front of each Postgres
// instance, with a bounded capacity and a pool-max-wait-time (5s default). A
// query that waits out that window fails with `connection pool timed out`
// under SQLSTATE XX000 — the sidecar's error wearing Postgres's generic
// internal_error code. XX000 is default-terminal here, deliberately, because
// it is a catch-all.
//
// The cost of leaving this one terminal, measured 2026-09-12: a concurrent
// cold copy on the same branch briefly asked for more connections than the
// sidecar pool held, and a long-running CDC stream into that database died
// outright mid-apply — permanently, with no recovery but a relaunch, and
// ~40 minutes before anyone noticed. The copy that caused the squeeze had
// already failed and gone away. A pool timeout means "everything was busy
// for longer than the wait window", which clears the moment anything frees;
// it is the textbook shape for bounded retry.
//
// The second cell is the half that keeps this honest: XX000 is a catch-all,
// so an XX000 that is NOT a pool timeout must stay terminal. A gate that
// made all of XX000 retriable would retry genuine internal errors through
// the full ADR-0038 budget.
func TestClassifyApplierError_SidecarPoolTimeoutIsTransient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		code          string
		message       string
		wantRetriable bool
	}{
		{
			// The exact error that killed the stream.
			name:          "sidecar pool timeout",
			code:          "XX000",
			message:       "connection pool timed out",
			wantRetriable: true,
		},
		{
			name:          "sidecar pool timeout, different casing",
			code:          "XX000",
			message:       "Connection Pool Timed Out",
			wantRetriable: true,
		},
		{
			// The read-only AND-gate must keep working — this test must not
			// pass by having broadened XX000.
			name:          "read-only serving transition still transient",
			code:          "XX000",
			message:       "pg_readonly: invalid statement because cluster is read-only",
			wantRetriable: true,
		},
		{
			// THE ANTI-OVER-MATCH ARM. Without this, a fix that simply made
			// XX000 retriable would pass every other cell.
			name:          "a generic XX000 stays terminal",
			code:          "XX000",
			message:       "internal error: something went badly wrong",
			wantRetriable: false,
		},
		{
			// The wording must not be honoured under a code that already
			// means something definite on its own.
			name:          "pool-timeout wording under a terminal code stays terminal",
			code:          "23505",
			message:       "duplicate key value violates unique constraint (connection pool timed out)",
			wantRetriable: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := classifyApplierError(&pgconn.PgError{Code: tc.code, Message: tc.message})

			var retriable ir.RetriableError
			isRetriable := errors.As(got, &retriable) && retriable.Retriable()

			switch {
			case tc.wantRetriable && !isRetriable:
				t.Fatalf(
					"SQLSTATE %s %q classified TERMINAL, want retriable — a transient squeeze that clears "+
						"on its own will permanently kill a running stream",
					tc.code, tc.message,
				)
			case !tc.wantRetriable && isRetriable:
				t.Fatalf(
					"SQLSTATE %s %q classified RETRIABLE, want terminal — this shape does not self-heal, "+
						"so retrying it burns the whole ADR-0038 budget on a deterministic failure",
					tc.code, tc.message,
				)
			}
		})
	}
}

// TestPoolTimeoutIsNotReportedAsGrowEvidence pins the reason the pool-timeout
// wording lives in its own list rather than joining the read-only one.
//
// pgReadOnlyClusterSubstrings is deliberately SHARED between the retriability
// classifier and growEvidenceOf, because a read-only window genuinely IS
// storage-grow evidence. A pool timeout is not — it says a co-tenant was
// busy, which has nothing to do with the volume growing. Folding the two
// lists together would be the cheap refactor and would silently start
// reporting pool contention as a storage-grow claim in the ADR-0110 gate's
// structured log, which is exactly the kind of drift nobody re-reads.
func TestPoolTimeoutIsNotReportedAsGrowEvidence(t *testing.T) {
	t.Parallel()

	poolTimeout := &pgconn.PgError{Code: "XX000", Message: "connection pool timed out"}

	if isPGReadOnlyClusterMessage(poolTimeout.Message) {
		t.Fatal("the pool-timeout wording matches the READ-ONLY predicate — the two substring lists have " +
			"been merged, and pool contention will now be reported as storage-grow evidence")
	}

	// GrowEvidenceNone's own doc names "a contention shape, a timeout" as
	// exactly what must NOT claim a grow face, so this asserts the written
	// contract rather than a preference.
	if ev := growEvidenceOf(poolTimeout); ev != ir.GrowEvidenceNone {
		t.Fatalf("a sidecar pool timeout produced grow evidence %v, want %v — it is co-tenant "+
			"contention, not a volume growing", ev, ir.GrowEvidenceNone)
	}
}

// TestClassifyApplierError_NekiQueryBufferTimeoutIsTransient pins NK205, and
// pins that its Neki siblings stay terminal.
//
// NK205 (`query buffer timeout: request exceeded max wait`) is a Neki router
// telling a client its request queue was saturated. It killed a live 29 GB
// import on 2026-09-12 that had otherwise recovered from every transient it
// met — arriving from connection ACQUISITION rather than the COPY, while the
// same run was already riding out 08006 broken pipes from a saturated primary.
// Unknown SQLSTATEs are terminal by default, correctly, so it needed naming.
//
// The sibling cells are the half that keeps this honest: NK013 and NK213 are
// deliberately terminal, and a fix that made "anything starting NK" retriable
// would retry a missing feature and a workflow cutover forever.
func TestClassifyApplierError_NekiQueryBufferTimeoutIsTransient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		code          string
		message       string
		wantRetriable bool
	}{
		{
			name:          "NK205 query buffer timeout",
			code:          "NK205",
			message:       "query buffer timeout: request exceeded max wait",
			wantRetriable: true,
		},
		{
			// A missing feature never succeeds on retry.
			name:          "NK013 opcode not implemented stays terminal",
			code:          "NK013",
			message:       "not implemented: opcode not implemented: pg_size_bytes",
			wantRetriable: false,
		},
		{
			// A deliberate workflow cutover, not an overload.
			name:          "NK213 blocked table stays terminal",
			code:          "NK213",
			message:       "table is blocked by a workflow",
			wantRetriable: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := classifyApplierError(&pgconn.PgError{Code: tc.code, Message: tc.message})
			var retriable ir.RetriableError
			isRetriable := errors.As(got, &retriable) && retriable.Retriable()

			switch {
			case tc.wantRetriable && !isRetriable:
				t.Fatalf("SQLSTATE %s classified TERMINAL, want retriable — an overload signal that clears "+
					"on its own will kill a run that could have finished", tc.code)
			case !tc.wantRetriable && isRetriable:
				t.Fatalf("SQLSTATE %s classified RETRIABLE, want terminal — this shape does not self-heal, "+
					"so retrying burns the ADR-0038 budget on a deterministic failure", tc.code)
			}
		})
	}
}
