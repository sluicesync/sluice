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

// TestQuiesceAndReportTransient_ReturnsTheClassification pins the defect that
// made a classified transient unreachable to every caller.
//
// quiesceAndReportTransient classifies its error to decide whether to trip the
// grow gate, and until 2026-09-12 returned the RAW error afterwards. So the
// gate learned the error was transient and backed the whole fleet off for it,
// while the caller received a bare error with no ir.RetriableError in its
// chain and could only treat it as terminal.
//
// The cost, measured: a 29 GB import into a fresh Neki branch died on NK205
// from `acquire conn` — a code classified retriable — on a run whose logs show
// the gate tripping four times. The retry could not see what the gate had
// already concluded.
//
// The second cell is what stops a "just wrap everything" fix: a terminal error
// must come back unchanged, because classifyApplierError's pass-through is the
// only reason this is safe for unrecognised shapes.
func TestQuiesceAndReportTransient_ReturnsTheClassification(t *testing.T) {
	t.Parallel()

	t.Run("a transient comes back CLASSIFIED so a caller can retry it", func(t *testing.T) {
		t.Parallel()
		gate := &recordingGrowGate{}
		w := &RowWriter{growGate: gate}

		in := &pgconn.PgError{Code: "NK205", Message: "query buffer timeout: request exceeded max wait"}
		out := w.quiesceAndReportTransient(in, "raw COPY import")

		var re ir.RetriableError
		if !errors.As(out, &re) || !re.Retriable() {
			t.Fatalf("a transient came back UNCLASSIFIED — the gate tripped for it but the caller cannot "+
				"see it is retriable, so it dies as terminal. got %#v", out)
		}
		// The chain must survive: callers match on the underlying PgError.
		var pgErr *pgconn.PgError
		if !errors.As(out, &pgErr) || pgErr.Code != "NK205" {
			t.Errorf("the underlying *pgconn.PgError is no longer reachable through the wrapper")
		}
		if got := gate.trips.Load(); got != 1 {
			t.Errorf("grow-gate trips = %d, want 1 — the fleet backoff must still fire", got)
		}
	})

	t.Run("a TERMINAL error comes back unchanged and does not trip the gate", func(t *testing.T) {
		t.Parallel()
		gate := &recordingGrowGate{}
		w := &RowWriter{growGate: gate}

		in := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}
		out := w.quiesceAndReportTransient(in, "raw COPY import")

		var re ir.RetriableError
		if errors.As(out, &re) && re.Retriable() {
			t.Fatal("a terminal error was wrapped as retriable — a deterministic fault would now burn the " +
				"whole retry budget instead of failing fast")
		}
		if got := gate.trips.Load(); got != 0 {
			t.Errorf("grow-gate trips = %d, want 0 — a terminal fault must not back off the fleet", got)
		}
	})
}

// TestClassifyApplierError_LowDiskReadOnlyIsTransient pins the fifth distinct
// transient PlanetScale Neki produced under bulk load, and pins the standby
// regression that a careless version of this fix would cause.
//
// Neki protects a shard whose disk is running low by making it READ-ONLY
// rather than letting a write hit ENOSPC:
//
//	cannot execute COPY: shard shnvbjnzljqjop is read-only (disk space low)
//	(SQLSTATE 25006)
//
// That is the preventive twin of 53100 and clears when the volume finishes
// growing, so it belongs in the same bounded-retry class.
//
// THE SECOND CELL IS THE POINT. 25006 (read_only_sql_transaction) is
// legitimately terminal in its ordinary meaning — it is what a STANDBY returns
// when asked to write. sluice has a whole preflight devoted to diagnosing that
// (standby_preflight.go, Bug 197). A fix that made all of 25006 retriable
// would turn "you pointed sluice at a replica" from a loud, actionable refusal
// into a thirty-minute stall ending in a timeout.
func TestClassifyApplierError_LowDiskReadOnlyIsTransient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		message       string
		wantRetriable bool
		wantEvidence  ir.GrowEvidence
	}{
		{
			name:          "neki shard read-only because disk is low",
			message:       "cannot execute COPY: shard shnvbjnzljqjop is read-only (disk space low)",
			wantRetriable: true,
			// A storage-driven read-only window IS a grow face — unlike the
			// pool timeout, which is co-tenant contention.
			wantEvidence: ir.GrowEvidenceTargetFace,
		},
		{
			// THE STANDBY ARM. Must stay terminal and must NOT claim a grow.
			name:          "an ordinary read-only transaction stays terminal",
			message:       "cannot execute INSERT in a read-only transaction",
			wantRetriable: false,
			wantEvidence:  ir.GrowEvidenceNone,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := &pgconn.PgError{Code: "25006", Message: tc.message}

			got := classifyApplierError(in)
			var re ir.RetriableError
			isRetriable := errors.As(got, &re) && re.Retriable()

			switch {
			case tc.wantRetriable && !isRetriable:
				t.Fatalf("25006 %q classified TERMINAL, want retriable — a storage guard that lifts on "+
					"its own will fail an import that could have finished", tc.message)
			case !tc.wantRetriable && isRetriable:
				t.Fatalf("25006 %q classified RETRIABLE, want terminal — pointing sluice at a STANDBY "+
					"must stay a loud refusal with a remedy, not a 30-minute stall", tc.message)
			}

			if ev := growEvidenceOf(in); ev != tc.wantEvidence {
				t.Errorf("grow evidence = %v, want %v — the ADR-0110 gate's structured log reports this "+
					"verdict, so an over-claim here becomes a wrong storage-grow claim in the run log",
					ev, tc.wantEvidence)
			}
		})
	}
}
