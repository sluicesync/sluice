// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fakeFKChecker is the test double for ir.PlanetScaleForeignKeyChecker. It
// records call count so the "checker NOT consulted" cases are provable, not
// assumed.
type fakeFKChecker struct {
	status ir.PlanetScaleForeignKeyStatus
	err    error
	calls  int
}

func (f *fakeFKChecker) ForeignKeyStatus(context.Context) (ir.PlanetScaleForeignKeyStatus, error) {
	f.calls++
	return f.status, f.err
}

// captureLogs swaps the default slog logger for one writing to a buffer for the
// duration of fn, returning what was logged. Not parallel-safe (mutates the
// process default), so the tests using it do not call t.Parallel.
func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// TestPreflightPlanetScaleForeignKeys is the family matrix for the FK preflight:
// every combination of (target is PS?, has FKs?, --skip-foreign-keys?, checker
// present?, FK enabled?, safe-migrations?) that changes the verdict, so a green
// run is not one representative case. The confirmable-disabled branch is the
// only refusal; every other branch proceeds (WARN or INFO).
func TestPreflightPlanetScaleForeignKeys(t *testing.T) {
	const fkCount = 3

	cases := []struct {
		name         string
		in           PlanetScaleFKPreflightInput
		wantStatus   PlanetScaleFKStatus
		wantRefusal  bool   // a CodedError with CodePSForeignKeysNotEnabled
		wantConsult  bool   // the checker must have been called
		wantLogMatch string // a substring the emitted logs must contain ("" = don't check)
	}{
		{
			name: "disabled+ps+fks refuses loudly with the code",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: fkCount,
				Checker: &fakeFKChecker{status: ir.PlanetScaleForeignKeyStatus{ForeignKeysEnabled: false}},
			},
			wantStatus: PSFKNotApplicable, wantRefusal: true, wantConsult: true,
		},
		{
			name: "enabled+safe-migrations-off proceeds (INFO), no refusal",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: fkCount,
				Checker: &fakeFKChecker{status: ir.PlanetScaleForeignKeyStatus{ForeignKeysEnabled: true, SafeMigrations: false}},
			},
			wantStatus: PSFKEnabled, wantConsult: true, wantLogMatch: "enabled and safe migrations off",
		},
		{
			name: "enabled+safe-migrations-ON warns (never refuses)",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: fkCount,
				Checker: &fakeFKChecker{status: ir.PlanetScaleForeignKeyStatus{ForeignKeysEnabled: true, SafeMigrations: true}},
			},
			wantStatus: PSFKRiskySafeMigrations, wantConsult: true, wantLogMatch: "safe migrations ON",
		},
		{
			name: "skip-foreign-keys is a silent no-op, checker untouched",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: fkCount, SkipForeignKeys: true,
				Checker: &fakeFKChecker{status: ir.PlanetScaleForeignKeyStatus{ForeignKeysEnabled: false}},
			},
			wantStatus: PSFKNotApplicable, wantConsult: false,
		},
		{
			name: "no foreign keys is a silent no-op, checker untouched",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: 0,
				Checker: &fakeFKChecker{status: ir.PlanetScaleForeignKeyStatus{ForeignKeysEnabled: false}},
			},
			wantStatus: PSFKNotApplicable, wantConsult: false,
		},
		{
			name: "non-planetscale target is a silent no-op, checker untouched",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: false, ForeignKeyCount: fkCount,
				Checker: &fakeFKChecker{status: ir.PlanetScaleForeignKeyStatus{ForeignKeysEnabled: false}},
			},
			wantStatus: PSFKNotApplicable, wantConsult: false,
		},
		{
			name: "no service token (nil checker) warns, still fires",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: fkCount, Checker: nil,
			},
			wantStatus: PSFKUnverified, wantLogMatch: "no PlanetScale service token",
		},
		{
			name: "probe error degrades to a WARN (advisory), never fails the run",
			in: PlanetScaleFKPreflightInput{
				TargetIsPlanetScale: true, ForeignKeyCount: fkCount,
				Checker: &fakeFKChecker{err: errors.New("boom")},
			},
			wantStatus: PSFKUnverified, wantConsult: true, wantLogMatch: "could not probe",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				status PlanetScaleFKStatus
				err    error
			)
			logs := captureLogs(t, func() {
				status, err = PreflightPlanetScaleForeignKeys(context.Background(), tc.in)
			})

			if status != tc.wantStatus {
				t.Errorf("status = %v; want %v", status, tc.wantStatus)
			}

			if tc.wantRefusal {
				var ce *sluicecode.CodedError
				if !errors.As(err, &ce) {
					t.Fatalf("want a CodedError refusal, got %v", err)
				}
				if ce.Code != sluicecode.CodePSForeignKeysNotEnabled {
					t.Errorf("refusal code = %q; want %q", ce.Code, sluicecode.CodePSForeignKeysNotEnabled)
				}
				// The message must name the count and the two escapes.
				msg := ce.Error()
				for _, want := range []string{"foreign-key support disabled", "--skip-foreign-keys", "allow_foreign_key_constraints"} {
					if !strings.Contains(msg, want) {
						t.Errorf("refusal message missing %q\n  got: %s", want, msg)
					}
				}
			} else if err != nil {
				t.Errorf("want no error (proceed), got %v", err)
			}

			if fake, ok := tc.in.Checker.(*fakeFKChecker); ok {
				consulted := fake.calls > 0
				if consulted != tc.wantConsult {
					t.Errorf("checker consulted = %v; want %v (calls=%d)", consulted, tc.wantConsult, fake.calls)
				}
			}

			if tc.wantLogMatch != "" && !strings.Contains(logs, tc.wantLogMatch) {
				t.Errorf("logs missing %q\n  logs: %s", tc.wantLogMatch, logs)
			}
		})
	}
}

// TestCountForeignKeys pins the trigger count across the tables of a schema.
func TestCountForeignKeys(t *testing.T) {
	if got := CountForeignKeys(nil); got != 0 {
		t.Errorf("CountForeignKeys(nil) = %d; want 0", got)
	}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "a", ForeignKeys: []*ir.ForeignKey{{Name: "fk1"}, {Name: "fk2"}}},
		{Name: "b"},
		{Name: "c", ForeignKeys: []*ir.ForeignKey{{Name: "fk3"}}},
	}}
	if got := CountForeignKeys(schema); got != 3 {
		t.Errorf("CountForeignKeys = %d; want 3", got)
	}
}

// TestLogMigrationPlanSummary_GatesTheVerboseParts pins Feature 2's quiet-on-a-
// boring-run contract and its PlanetScale large-table nudge.
func TestLogMigrationPlanSummary_GatesTheVerboseParts(t *testing.T) {
	tests := []struct {
		name       string
		summary    MigrationPlanSummary
		wantSilent bool
		wantSubstr []string
	}{
		{
			name:       "tiny non-PS migration says nothing",
			summary:    MigrationPlanSummary{Command: "migrate", TargetEngine: "postgres", TableCount: 2},
			wantSilent: true,
		},
		{
			name: "FKs present speaks even on a non-PS target",
			summary: MigrationPlanSummary{
				Command: "migrate", TargetEngine: "postgres", TableCount: 4, ForeignKeyCount: 2,
			},
			wantSubstr: []string{"migration plan", "foreign_keys=2"},
		},
		{
			name: "PS target with a large table nudges toward the mitigations",
			summary: MigrationPlanSummary{
				Command: "migrate", TargetEngine: "planetscale", TargetIsPlanetScale: true,
				TableCount: 10, ForeignKeyCount: 1, FKStatus: PSFKEnabled,
				LargestTable: "events", LargestTableRows: 5_000_000, LargestTableKnown: true,
			},
			wantSubstr: []string{"largest_table=events", "fk_support=enabled", "statement-time wall", "--planetscale-raise-query-timeout"},
		},
		{
			name: "PS target with a large table + --upfront-indexes does NOT nag about it",
			summary: MigrationPlanSummary{
				Command: "migrate", TargetEngine: "planetscale", TargetIsPlanetScale: true,
				TableCount: 10, LargestTable: "events", LargestTableRows: 5_000_000, LargestTableKnown: true,
				UpfrontIndexes: true,
			},
			wantSubstr: []string{"build UPFRONT during the copy"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t, func() {
				LogMigrationPlanSummary(context.Background(), tc.summary)
			})
			if tc.wantSilent {
				if strings.Contains(logs, "migration plan") {
					t.Errorf("expected silence, got logs: %s", logs)
				}
				return
			}
			for _, want := range tc.wantSubstr {
				if !strings.Contains(logs, want) {
					t.Errorf("logs missing %q\n  logs: %s", want, logs)
				}
			}
		})
	}
}
