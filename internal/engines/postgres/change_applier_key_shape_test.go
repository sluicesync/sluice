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

// TestKeyScopedWriteCheck pins G1's arithmetic and its exemption: only a
// keyless table addressed by its WHOLE row is exempt — a keyless target fed a
// key-narrowed before-image (a keyed source under REPLICA IDENTITY FULL) is
// checked like a keyed one.
func TestKeyScopedWriteCheck(t *testing.T) {
	cols := map[string]*ir.Column{"id": {Name: "id"}, "v": {Name: "v"}}
	keyBefore := ir.Row{"id": int64(2)}
	wholeRow := ir.Row{"id": int64(2), "v": "a"}

	keyed := guardKeyScopedWrite(tableKeyShape{keyed: true}, "delete", wholeRow, cols)
	for _, n := range []int64{0, 1} {
		if err := keyed.check("public", "s", n); err != nil {
			t.Errorf("%d rows refused: %v", n, err)
		}
	}
	if err := keyed.check("public", "s", 2); !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) {
		t.Errorf("2 rows not refused: %v", err)
	}

	exempt := guardKeyScopedWrite(tableKeyShape{}, "delete", wholeRow, cols)
	if exempt != nil {
		t.Fatal("a keyless table addressed by its whole row got a check")
	}
	if err := exempt.check("public", "s", 5); err != nil {
		t.Errorf("a nil check refused: %v", err)
	}

	narrowed := guardKeyScopedWrite(tableKeyShape{}, "delete", keyBefore, cols)
	if err := narrowed.check("public", "s", 2); !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) {
		t.Errorf("a keyless table addressed by a key-only before-image was exempted: %v", err)
	}
	if guardKeyScopedWrite(tableKeyShape{}, "delete", wholeRow, nil) == nil {
		t.Error("an unknown target shape cannot prove a whole-row predicate, yet it was exempted")
	}
}
