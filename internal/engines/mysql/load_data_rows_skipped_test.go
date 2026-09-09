// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// A row the server DROPPED is refused in every sql_mode (audit 2026-09-09
// A0909-MYSQL-MEDIUM-2). Before this, --mysql-sql-mode=” WARNed that
// values had been "clamped or truncated" and exited 0 short of rows, and
// strict mode refused with a type-conversion diagnosis and a
// --type-override remedy that cannot address a CHECK constraint.
func TestDecideBulkWriteWarnings_ASkippedRowRefusesInEverySQLMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	relaxed, strict := "", "STRICT_TRANS_TABLES"
	details := []string{"Warning 3819: Check constraint 'ck_chk_1' is violated."}

	for name, mode := range map[string]*string{"relaxed": &relaxed, "strict": &strict, "default": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := &RowWriter{sqlMode: mode}
			err := w.decideBulkWriteWarnings(ctx, "ck", showWarnings{Visible: 1, NonDup: 1, Skipped: 1, Details: details}, 1)
			if err == nil {
				t.Fatalf("%s: a skipped row was accepted (the relaxed WARN-and-continue path, or nothing at all)", name)
			}
			for _, want := range []string{loadDataRowsSkippedMarker, "SKIPPED 1 row", "3819", "CHECK", "local_infile=OFF", "ck_chk_1"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: refusal does not say %q: %v", name, want, err)
				}
			}
			if strings.Contains(err.Error(), "--type-override") {
				t.Errorf("%s: refusal prescribes --type-override, which cannot address a CHECK: %v", name, err)
			}
		})
	}

	t.Run("MariaDB's own code counts as a skip too", func(t *testing.T) {
		t.Parallel()
		// The engines do not share the code: 3819 on MySQL, 4025 on
		// MariaDB (both measured 2026-09-09 — see rowSkippingWarningCodes
		// and TestMariaDB_LoadDataCheckViolationIsWarning4025). Counting
		// only MySQL's left the MariaDB skip invisible wherever the
		// shortfall witness is unavailable.
		w := &RowWriter{sqlMode: &relaxed}
		err := w.decideBulkWriteWarnings(ctx, "ck", showWarnings{
			Visible: 1, NonDup: 1, Skipped: 1,
			Details: []string{"Warning 4025: CONSTRAINT `ck_nonneg` failed for `t`.`ck`"},
		}, 1)
		if err == nil || !strings.Contains(err.Error(), loadDataRowsSkippedMarker) {
			t.Fatalf("a MariaDB CHECK skip was accepted under relaxed mode: %v", err)
		}
	})

	t.Run("the relaxed WARN says when it could not read every warning", func(t *testing.T) {
		// No t.Parallel: swaps the global slog default.
		//
		// The classification only ever sees the warnings SHOW WARNINGS
		// returned, which the server caps at @@max_error_count. Claiming
		// "N values were clamped" when only a few were read would report a
		// row the server DROPPED beyond the cap as a coercion.
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		w := &RowWriter{sqlMode: &relaxed}
		if err := w.decideBulkWriteWarnings(ctx, "capped", showWarnings{
			Visible: 2, NonDup: 2, Details: []string{"Warning 1264: Out of range value", "Warning 1265: Data truncated"},
		}, 900); err != nil {
			t.Fatalf("a capped sample under relaxed mode must WARN, not refuse: %v", err)
		}
		got := buf.String()
		for _, want := range []string{"ONLY 2 of the 900", "NOT classified", "warnings_read=2"} {
			if !strings.Contains(got, want) {
				t.Errorf("the capped-sample WARN does not say %q:\n%s", want, got)
			}
		}

		// And it must NOT say that when the sample is complete.
		buf.Reset()
		w2 := &RowWriter{sqlMode: &relaxed}
		if err := w2.decideBulkWriteWarnings(ctx, "complete", showWarnings{
			Visible: 1, NonDup: 1, Details: []string{"Warning 1264: Out of range value"},
		}, 1); err != nil {
			t.Fatalf("unexpected refusal: %v", err)
		}
		if strings.Contains(buf.String(), "NOT classified") {
			t.Errorf("a complete sample claimed warnings went unclassified:\n%s", buf.String())
		}
	})

	t.Run("a coercion under relaxed mode still WARNs and continues (unchanged)", func(t *testing.T) {
		t.Parallel()
		w := &RowWriter{sqlMode: &relaxed}
		err := w.decideBulkWriteWarnings(ctx, "t", showWarnings{Visible: 1, NonDup: 1, Details: []string{"Warning 1264: Out of range value"}}, 1)
		if err != nil {
			t.Fatalf("a clamp under --mysql-sql-mode='' must WARN, not refuse: %v", err)
		}
	})
}

// The first-attempt witness: rows sluice sent minus rows the server
// reports inserted is a dropped-row count no warning sample can hide —
// not even one the server capped at max_error_count=0.
func TestRowsSkippedError_NamesTheShortfall(t *testing.T) {
	t.Parallel()
	err := rowsSkippedError("orders", 2, nil, "")
	for _, want := range []string{loadDataRowsSkippedMarker, `"orders"`, "SKIPPED 2 row", "every sql_mode"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not say %q: %v", want, err)
		}
	}
}
