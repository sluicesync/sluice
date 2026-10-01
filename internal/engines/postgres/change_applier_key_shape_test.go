// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// TestAnnotateDeferredCheckFailure pins which COMMIT errors get the GC-42
// marker: the two SQLSTATEs a deferred re-check raises, and nothing else —
// with the SQLSTATE still reachable so classification is unchanged.
func TestAnnotateDeferredCheckFailure(t *testing.T) {
	if annotateDeferredCheckFailure(nil) != nil {
		t.Fatal("nil in, non-nil out")
	}
	for _, code := range []string{pgUniqueViolation, pgExclusionViolation} {
		cause := fmt.Errorf("commit: %w", &pgconn.PgError{Code: code, ConstraintName: "t_pk"})
		got := annotateDeferredCheckFailure(cause)
		if !strings.Contains(got.Error(), deferredCheckFailedMarker) || !strings.Contains(got.Error(), "t_pk") {
			t.Errorf("%s: not marked with the constraint: %v", code, got)
		}
		var pgErr *pgconn.PgError
		if !errors.As(got, &pgErr) || pgErr.Code != code {
			t.Errorf("%s: the SQLSTATE is no longer in the chain: %v", code, got)
		}
	}
	for _, other := range []error{
		&pgconn.PgError{Code: "23503"},
		&pgconn.PgError{Code: "40001"},
		errors.New("connection reset"),
	} {
		if got := annotateDeferredCheckFailure(other); got.Error() != other.Error() {
			t.Errorf("%v was annotated: %v", other, got)
		}
	}
}

// TestKeyScopedWriteVerdict pins G1's decision: from the count alone, more
// than one row is refused — a keyed table's key matches one row, and a
// keyless table's whole-row write is addressed to one row, so there is no
// exemption left to grant.
func TestKeyScopedWriteVerdict(t *testing.T) {
	k := guardKeyScopedWrite("delete", ir.Row{"id": int64(2)})
	for _, n := range []int64{0, 1} {
		if err := k.verdict("public", "s", n); err != nil {
			t.Errorf("%d rows refused: %v", n, err)
		}
	}
	if err := k.verdict("public", "s", 2); !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) {
		t.Errorf("2 rows not refused: %v", err)
	}
	var nilCheck *keyScopedWrite
	if err := nilCheck.verdict("public", "s", 5); err != nil {
		t.Errorf("a nil check refused: %v", err)
	}
}

// TestRowTargetSQL pins the one-row address: (tableoid, ctid), not ctid alone
// (a ctid repeats across partitions), wrapping the unchanged whole-row WHERE.
func TestRowTargetSQL(t *testing.T) {
	const ref, where = `"public"."t"`, `"id" = $1 AND "v" IS NULL`
	if got := rowTargetSQL(ref, where, addressEveryMatch); got != where {
		t.Errorf("every-match address rewrote the WHERE: %s", got)
	}
	want := `(tableoid, ctid) = (SELECT tableoid, ctid FROM "public"."t" WHERE "id" = $1 AND "v" IS NULL LIMIT 1)`
	if got := rowTargetSQL(ref, where, addressOneRow); got != want {
		t.Errorf("one-row address = %s\nwant %s", got, want)
	}
}
