// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// guardRaise is the error the guard's invalid integer cast raises.
func guardRaise() error {
	return &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "KEY-COLLATION-MERGE"`}
}

// TestApplyKeyCollationMergeRemedy pins KEY-COLLATION-PG-UNESTABLISHED-REMEDY
// over the plans the guard actually builds (planPGKeyCollationGuard): a key
// armed only because its collation was not established, on a target that
// already carries the source's collation name, is offered the re-copy alone
// — altering it to the source's collation cannot converge — while every
// other armed key names the collation the guard compared it under, with the
// ALTER and the re-copy. Each re-copy invocation is graded against the CLI by
// TestRuntimeMessagesNameRealCLISurface.
func TestApplyKeyCollationMergeRemedy(t *testing.T) {
	const (
		syncRecopy   = "`sluice sync start --restart-from-scratch`"
		brokerRecopy = "`sluice sync from-backup run --reset-target-data --yes`"
	)
	src := func(typ ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "k", Type: typ}}}
	}
	ciTarget := map[string]targetKeyCollation{"k": {name: "ci", deterministic: false}}
	cases := []struct {
		name      string
		source    *ir.Table
		recopy    bool // the re-copy alone
		want      []string
		forbidden []string
	}{
		{
			"not established, the target already has the source's name",
			src(ir.Text{Collation: "ci"}), true,
			[]string{`"k"`, `"s"."kd"`, "already carry the source's collation", "cannot converge", syncRecopy, brokerRecopy},
			[]string{"ALTER TABLE", `COLLATE "C"`, "Give the target's key"},
		},
		{
			"not established, another name",
			src(ir.Text{Collation: "ci_ai"}), false,
			[]string{`"k" COLLATE "C"`, `ALTER TABLE "s"."kd"`, syncRecopy, brokerRecopy},
			[]string{"already carry"},
		},
		{
			"a deterministic source",
			src(ir.Text{Collation: "C", Determinism: ir.CollationDeterministic}), false,
			[]string{`"k" COLLATE "C"`, `ALTER TABLE "s"."kd"`, syncRecopy},
			[]string{"already carry"},
		},
		{
			"another nondeterministic source collation",
			src(ir.Text{Collation: "ci_ai", Determinism: ir.CollationNonDeterministic}), false,
			[]string{`"k" COLLATE "ci_ai"`, `ALTER TABLE "s"."kd"`, syncRecopy},
			[]string{"already carry", `COLLATE "C"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, _ := planPGKeyCollationGuard(tc.source, []string{"k"}, ciTarget)
			if plan == nil {
				t.Fatal("the guard armed nothing — the cell is not exercising a refusal")
			}
			if got := len(plan.targetHasSource) > 0; got != tc.recopy {
				t.Fatalf("targetHasSource = %v; want re-copy-only=%v", plan.targetHasSource, tc.recopy)
			}
			got := applyKeyCollationMergeRemedy(plan, "s", "kd")
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("remedy lacks %q:\n%s", w, got)
				}
			}
			for _, f := range tc.forbidden {
				if strings.Contains(got, f) {
					t.Errorf("remedy carries %q:\n%s", f, got)
				}
			}
		})
	}
}

// TestPipelinedInsert_KeyCollationMergeNamesTheTable pins the wider half: the
// pipelined batch attributes a flushed guard raise to the insert it queued,
// so the refusal names the table and the plan's remedy, and the classifier
// keeps it (a terminal error passes through). Before, only the classifier
// attributed it, with neither, and the generic text named no table. Controls:
// an unguarded insert's raise and a non-guard error are not attributed here.
func TestPipelinedInsert_KeyCollationMergeNamesTheTable(t *testing.T) {
	a := &ChangeApplier{}
	plan, _ := planPGKeyCollationGuard(&ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "k", Type: ir.Text{Collation: "ci"}}}},
		[]string{"k"}, map[string]targetKeyCollation{"k": {name: "ci"}})
	if plan == nil {
		t.Fatal("the guard armed nothing")
	}

	err := classifyApplierError(a.attributeQueuedError(queuedStmt{schema: "s", table: "kd", kind: "insert", guard: plan}, guardRaise()))
	var merge *ir.KeyCollationMergeError
	if !errors.As(err, &merge) {
		t.Fatalf("err = %v; want a KeyCollationMergeError", err)
	}
	if merge.Schema != "s" || merge.Table != "kd" || !strings.Contains(merge.Remedy, "already carry the source's collation") {
		t.Fatalf("merge = %+v; want s.kd with the re-copy remedy", merge)
	}
	if !strings.Contains(err.Error(), "an upsert into s.kd") {
		t.Errorf("err = %v; want the table named", err)
	}

	err = classifyApplierError(a.attributeQueuedError(queuedStmt{schema: "s", table: "kd", kind: "insert"}, guardRaise()))
	if !errors.As(err, &merge) || merge.Table != "" {
		t.Errorf("unguarded raise: err = %v; want the classifier's own (tableless) attribution", err)
	}

	other := errors.New("boom")
	if err := a.attributeQueuedError(queuedStmt{schema: "s", table: "kd", kind: "insert", guard: plan}, other); errors.As(err, &merge) || !errors.Is(err, other) {
		t.Errorf("non-guard error: err = %v; want it unattributed", err)
	}
}
