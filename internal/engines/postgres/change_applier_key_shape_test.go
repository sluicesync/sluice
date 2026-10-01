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

// TestKeyScopedWriteVerdict pins G1's decision and its laziness: the table's
// key is read ONLY when a write matched more than one row, and then only a
// keyless table addressed by its WHOLE row is exempt — a keyless target fed a
// key-narrowed before-image (a keyed source under REPLICA IDENTITY FULL) is
// refused like a keyed one.
func TestKeyScopedWriteVerdict(t *testing.T) {
	cols := map[string]*ir.Column{"id": {Name: "id"}, "v": {Name: "v"}}
	keyBefore := ir.Row{"id": int64(2)}
	wholeRow := ir.Row{"id": int64(2), "v": "a"}
	lookups := 0
	keyed := func(answer bool) func() (bool, error) {
		return func() (bool, error) { lookups++; return answer, nil }
	}

	for _, n := range []int64{0, 1} {
		if err := guardKeyScopedWrite("delete", keyBefore, cols).verdict("public", "s", n, keyed(true)); err != nil {
			t.Errorf("%d rows refused: %v", n, err)
		}
	}
	if lookups != 0 {
		t.Fatalf("the key was read %d times for writes that matched at most one row; the healthy path must read nothing", lookups)
	}

	refused := []struct {
		name   string
		keyed  bool
		before ir.Row
	}{
		{"keyed table, key-narrowed image", true, keyBefore},
		{"keyed table, whole-row image", true, wholeRow},
		{"keyless table, key-narrowed image", false, keyBefore},
	}
	for _, c := range refused {
		err := guardKeyScopedWrite("delete", c.before, cols).verdict("public", "s", 2, keyed(c.keyed))
		if !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) {
			t.Errorf("%s: 2 rows not refused: %v", c.name, err)
		}
	}
	if err := guardKeyScopedWrite("delete", wholeRow, cols).verdict("public", "s", 5, keyed(false)); err != nil {
		t.Errorf("a keyless table addressed by its whole row was refused: %v", err)
	}
	if err := guardKeyScopedWrite("delete", wholeRow, nil).verdict("public", "s", 2, keyed(false)); err == nil {
		t.Error("an unknown target shape cannot prove a whole-row predicate, yet it was exempted")
	}
	probeErr := errors.New("catalog unreachable")
	if err := guardKeyScopedWrite("delete", wholeRow, cols).verdict("public", "s", 2, func() (bool, error) { return false, probeErr }); !errors.Is(err, probeErr) {
		t.Errorf("a failed key read must surface, not exempt: %v", err)
	}
}
