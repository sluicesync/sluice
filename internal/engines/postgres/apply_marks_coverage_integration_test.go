//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_MarksCoverReason is the Postgres half of ADR-0191 §3.5's
// target rows, graded against a real server: the broker lifts a keyless
// table's refusal only when this says the marks cover it. Each shape names
// what the applier's own mark subject would do — a target table with NO key
// (table-wide marks: covered), one keyed only on a column the replayed rows do
// not carry (no mark key: refused), one the target does not hold yet (judged
// by the recorded key it is created with), and a mark table this role cannot
// use (APPLY-MARKS-UNAVAILABLE: refused).
func TestChangeApplier_MarksCoverReason(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `
		CREATE TABLE kl (v INT NOT NULL, note TEXT);
		CREATE TABLE sur (sid BIGSERIAL PRIMARY KEY, v INT NOT NULL);
		CREATE TABLE keyed (id INT PRIMARY KEY, v INT);`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	prober := applier.(ir.ApplyMarksCoverageProber)
	col := func(n string) *ir.Column { return &ir.Column{Name: n, Type: ir.Integer{Width: 32}} }
	recordedKeyless := func(name string, cols ...string) *ir.Table {
		t := &ir.Table{Schema: "public", Name: name}
		for _, c := range cols {
			t.Columns = append(t.Columns, col(c))
		}
		return t
	}
	for _, tc := range []struct {
		name  string
		table *ir.Table
		want  string // "" covered, else a substring of the reason
	}{
		{"keyless on the target: table-wide marks", recordedKeyless("kl", "v", "note"), ""},
		{"keyed on an unsupplied surrogate", recordedKeyless("sur", "v"), `"sid"`},
		{"keyed on a supplied column", recordedKeyless("keyed", "id", "v"), ""},
		{"absent: judged by its recorded key (none)", recordedKeyless("absent", "v"), ""},
		{"target level only", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			why, err := prober.MarksCoverReason(ctx, tc.table)
			if err != nil {
				t.Fatal(err)
			}
			if (tc.want == "") != (why == "") || !strings.Contains(why, tc.want) {
				t.Errorf("MarksCoverReason = %q; want %q", why, tc.want)
			}
		})
	}

	t.Run("mark table unusable", func(t *testing.T) {
		applyPGApplier(t, dsn, `DROP TABLE public.sluice_cdc_apply_marks;`)
		why, err := prober.MarksCoverReason(ctx, recordedKeyless("kl", "v", "note"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(why, applymarks.UnavailableMarker) {
			t.Errorf("MarksCoverReason with no mark table = %q; want %s", why, applymarks.UnavailableMarker)
		}
	})
}

// TestChangeApplier_RequireApplyMarks is the engine half of the ADR-0191
// review's apply-time re-check (ir.ApplyMarksRequirer): a mark table that
// became unusable between a replay path's coverage question and the apply
// refuses the apply — before any row is written — while required, on both
// apply entries (the per-change Apply that a batch size of 1 takes, and the
// batch/lane path), and stays the APPLY-MARKS-UNAVAILABLE WARN (rows applied)
// when not. The independent expected value is the target's own row count.
func TestChangeApplier_RequireApplyMarks(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE TABLE kl (v INT NOT NULL);`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	applyPGApplier(t, dsn, `DROP TABLE public.sluice_cdc_apply_marks;`) // after the coverage answer
	req := applier.(ir.ApplyMarksRequirer)
	apply := func(batch int, v int64) error {
		pos := ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, 0x3000000+v*0x100)}
		ch := make(chan ir.Change, 3)
		ch <- ir.TxBegin{Position: pos}
		ch <- ir.Insert{Position: pos, Schema: "public", Table: "kl", Row: ir.Row{"v": v}, ApplyID: ir.ApplyID{TxID: fmt.Sprintf("req-%d", v), Seq: 1}}
		ch <- ir.TxCommit{Position: pos}
		close(ch)
		return applier.(ir.BatchedChangeApplier).ApplyBatch(ctx, "req", ch, batch)
	}
	for _, batch := range []int{1, 100} {
		req.RequireApplyMarks(true)
		if err := apply(batch, int64(batch)); !errors.Is(err, applymarks.ErrMarksRequired) {
			t.Errorf("batch %d, marks required: ApplyBatch = %v; want ErrMarksRequired", batch, err)
		}
		if n := pgCount(t, dsn, `SELECT count(*) FROM kl`); n != 0 {
			t.Errorf("batch %d, marks required: the refused apply wrote %d row(s)", batch, n)
			applyPGApplier(t, dsn, `DELETE FROM kl;`)
		}
	}
	req.RequireApplyMarks(false)
	if err := apply(100, 7); err != nil {
		t.Fatalf("marks not required: ApplyBatch = %v; want the WARN and the row applied", err)
	}
	if n := pgCount(t, dsn, `SELECT count(*) FROM kl`); n != 1 {
		t.Errorf("marks not required: kl holds %d row(s); want 1", n)
	}
}
