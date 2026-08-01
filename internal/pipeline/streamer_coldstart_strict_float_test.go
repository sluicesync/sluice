// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// --strict-float on the SYNC cold-start (2026-08-01). `backup full` has had
// the exact-or-refuse posture since the VStream FLOAT repair landed; the sync
// cold-start — the other consumer of the same display-rounding COPY reader,
// with the same repairable/un-repairable classification — only WARNed and
// proceeded with rounded values. These pin the refusal ACROSS THE WHOLE
// un-repairable class, not one representative: the classification in
// planFloatRepair has three distinct ways to land on !repairable (no PK at
// all, a single-precision FLOAT in the PK, every FLOAT being a PK member), and
// a green test on one says nothing about the others.

// sfFloatCol / sfIntCol build the two column shapes the plan classifies on.
func sfFloatCol(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Float{Precision: ir.FloatSingle}}
}

func sfIntCol(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Integer{Width: 64}}
}

func sfPKTable(name string, pk []string, cols ...*ir.Column) *ir.Table {
	t := &ir.Table{Name: name, Columns: cols}
	if len(pk) > 0 {
		idxCols := make([]ir.IndexColumn, len(pk))
		for i, c := range pk {
			idxCols[i] = ir.IndexColumn{Column: c}
		}
		t.PrimaryKey = &ir.Index{Name: name + "_pkey", Columns: idxCols, Unique: true}
	}
	return t
}

func TestStrictFloatRefusal_UnrepairableClass(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		table     *ir.Table
		wantRefus bool
		wantNames []string
	}{
		{
			// Shape 1 — keyless: nothing to key the PK-targeted UPDATE on.
			name:      "keyless table",
			table:     sfPKTable("events", nil, sfIntCol("id"), sfFloatCol("score")),
			wantRefus: true,
			wantNames: []string{"events.score"},
		},
		{
			// Shape 2 — SL-F1: a single-precision FLOAT inside the PK. The COPY
			// wrote a ROUNDED PK, so the PK-keyed UPDATE matches zero rows and
			// every non-PK FLOAT silently keeps its rounding too.
			name:      "single-precision FLOAT in the PK",
			table:     sfPKTable("readings", []string{"sensor", "raw"}, sfIntCol("sensor"), sfFloatCol("raw"), sfFloatCol("calibrated")),
			wantRefus: true,
			wantNames: []string{"readings.raw", "readings.calibrated"},
		},
		{
			// Shape 3 — every FLOAT is a PK member, so the re-read set is empty
			// even though the table has a usable key.
			name:      "sole FLOAT is the whole PK",
			table:     sfPKTable("grid", []string{"x"}, sfFloatCol("x"), sfIntCol("label")),
			wantRefus: true,
			wantNames: []string{"grid.x"},
		},
		{
			// The repairable control: a real PK, a non-PK FLOAT. --strict-float
			// must NOT refuse — the post-copy exact re-read makes it exact, and
			// refusing here would make the flag unusable on healthy schemas.
			name:      "repairable: int PK + non-PK FLOAT",
			table:     sfPKTable("orders", []string{"id"}, sfIntCol("id"), sfFloatCol("amount")),
			wantRefus: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan := planFloatRepair(&ir.Schema{Tables: []*ir.Table{tc.table}})
			if len(plan) != 1 {
				t.Fatalf("planFloatRepair produced %d entries; want 1 (the fixture must carry a single-precision FLOAT)", len(plan))
			}
			s := &Streamer{StrictFloat: true}
			err := s.strictFloatRefusal(plan)
			if !tc.wantRefus {
				if err != nil {
					t.Fatalf("--strict-float refused a REPAIRABLE table: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("--strict-float must refuse an un-repairable FLOAT table; got nil")
			}
			if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeVStreamFloatLossy {
				t.Errorf("refusal is not coded %s; got %v (coded=%v)", sluicecode.CodeVStreamFloatLossy, err, ok)
			}
			for _, want := range tc.wantNames {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal must name the column %q; got: %v", want, err)
				}
			}
		})
	}
}

// TestStrictFloatRefusal_OffByDefault pins the opt-in posture: the zero-value
// Streamer (every fleet / broker / programmatic construction) keeps the
// pre-2026-08-01 WARN-and-proceed behaviour byte-identically.
func TestStrictFloatRefusal_OffByDefault(t *testing.T) {
	t.Parallel()
	plan := planFloatRepair(&ir.Schema{Tables: []*ir.Table{
		sfPKTable("events", nil, sfIntCol("id"), sfFloatCol("score")),
	}})
	if err := (&Streamer{}).strictFloatRefusal(plan); err != nil {
		t.Fatalf("an unset StrictFloat must never refuse; got: %v", err)
	}
}

// TestStrictFloatRefusal_ContradictsNoReread pins the loud refusal of the
// contradictory pair rather than letting one flag silently win — which is the
// inert-flag class this whole change closes.
func TestStrictFloatRefusal_ContradictsNoReread(t *testing.T) {
	t.Parallel()
	// A fully REPAIRABLE plan, so the only possible refusal is the combination.
	plan := planFloatRepair(&ir.Schema{Tables: []*ir.Table{
		sfPKTable("orders", []string{"id"}, sfIntCol("id"), sfFloatCol("amount")),
	}})
	s := &Streamer{StrictFloat: true, NoFloatExactReread: true}
	err := s.strictFloatRefusal(plan)
	if err == nil {
		t.Fatal("--strict-float with --no-float-exact-reread must refuse; got nil")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeVStreamFloatLossy {
		t.Errorf("refusal is not coded %s; got %v (coded=%v)", sluicecode.CodeVStreamFloatLossy, err, ok)
	}
	if !strings.Contains(err.Error(), "--no-float-exact-reread") {
		t.Errorf("refusal must name both flags; got: %v", err)
	}
}
