// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// A PlanetScale Neki MoveTables write cutover blocks the moved table on the
// database it came FROM, because a Postgres client picks its database at
// connect time and the switch cannot redirect a connection that named the old
// one. Measured on a live 3-shard cluster (2026-09-10): a sluice stream
// applying underneath the workflow survived move_tables_create and
// move_tables_switch_reads untouched, then died on the first statement after
// move_tables_switch_writes with a bare
//
//	ERROR: access to table "public.mv_src" is blocked (SQLSTATE NK213)
//
// Halting is right — nothing was lost, the persisted position stopped before
// the block, and reversing the workflow then restarting replayed to exact
// parity. What was missing is any way for an operator to get from that
// sentence to "a workflow just took this table".
func TestNekiBlockedTableClassification(t *testing.T) {
	t.Parallel()

	blocked := func() error {
		return fmt.Errorf("postgres: applier: update public.mv_src: %w", &pgconn.PgError{
			Code:    "NK213",
			Message: `access to table "public.mv_src" is blocked`,
		})
	}

	t.Run("it becomes the coded refusal, carrying the remedy", func(t *testing.T) {
		t.Parallel()
		got := classifyApplierError(blocked())
		ce, ok := sluicecode.FromError(got)
		if !ok {
			t.Fatalf("NK213 did not reach a coded refusal; an operator sees a SQLSTATE and no way to act: %v", got)
		}
		if ce.Code != sluicecode.CodeTargetTableBlockedByWorkflow {
			t.Errorf("got code %s, want %s", ce.Code, sluicecode.CodeTargetTableBlockedByWorkflow)
		}
		// The hint has to name the query that answers "which workflow?" —
		// that is the whole reason this classification exists.
		for _, want := range []string{"list_blocked_tables", "move_tables_status", "move_tables_reverse_traffic"} {
			if !strings.Contains(ce.Hint, want) {
				t.Errorf("the remedy does not name %s, so it does not tell the operator how to find or undo the "+
					"block: %q", want, ce.Hint)
			}
		}
	})

	t.Run("it stays TERMINAL — the block outlives any retry budget", func(t *testing.T) {
		t.Parallel()
		// The block is created with a one-YEAR expiry and clears only when
		// the workflow completes or is reversed. Retrying would burn the
		// ADR-0038 budget against a wall and then fail anyway, having
		// stalled the stream for the whole window.
		var re ir.RetriableError
		if errors.As(classifyApplierError(blocked()), &re) && re.Retriable() {
			t.Error("NK213 classified RETRIABLE; the block lasts a year, so the retry budget would expire " +
				"against it and the operator would learn nothing in the meantime")
		}
	})

	t.Run("the underlying PgError stays reachable", func(t *testing.T) {
		t.Parallel()
		var pgErr *pgconn.PgError
		if !errors.As(classifyApplierError(blocked()), &pgErr) || pgErr.Code != "NK213" {
			t.Error("the wrap hid the pgconn error, so the raw SQLSTATE and message no longer reach the log")
		}
	})

	t.Run("other SQLSTATEs are untouched", func(t *testing.T) {
		t.Parallel()
		// Anti-vacuity for the case above: prove the coded refusal is keyed
		// on NK213 and not applied to every terminal PgError. NK013 is the
		// neighbouring Neki code sluice already handles differently.
		other := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "NK013", Message: "not implemented"})
		if _, ok := sluicecode.FromError(classifyApplierError(other)); ok {
			t.Error("a non-NK213 Neki error was given the blocked-table refusal")
		}
		if _, ok := sluicecode.FromError(classifyApplierError(errors.New("connection reset"))); ok {
			t.Error("a plain error was given the blocked-table refusal")
		}
	})

	t.Run("the predicate keys on the code, not the message text", func(t *testing.T) {
		t.Parallel()
		// A table whose name happens to contain the word "blocked" must not
		// be enough, and an NK213 whose wording changes must still match.
		if isNekiTableBlocked(&pgconn.PgError{Code: "42P01", Message: `access to table "blocked" is blocked`}) {
			t.Error("matched on message text; a wording collision would misclassify an unrelated error")
		}
		if !isNekiTableBlocked(&pgconn.PgError{Code: "NK213", Message: "some future wording"}) {
			t.Error("did not match NK213 with different wording; the classification must key on the SQLSTATE")
		}
	})
}
