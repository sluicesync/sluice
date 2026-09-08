// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The record-but-never-resume split (2026-09-08 user report).
//
// The sync cold start used to pass a zero-value resumeContext, which was
// the only available way to say "do not resume" — and it also silenced every
// progress write, leaving `sync status` with nothing to report for the whole
// cold start. These cells pin the three properties that make the split safe.
func TestSyncRecordingContext(t *testing.T) {
	t.Parallel()

	t.Run("records: a sync context writes progress", func(t *testing.T) {
		t.Parallel()
		store := newFakeStateStore()
		rc := newSyncRecordingContext(store, "prod-cutover")
		if !rc.writes() {
			t.Fatal("a sync recording context does not write — the cold start would stay invisible to `sync status`, " +
				"which is the whole defect this exists to fix")
		}
		if err := writeTableProgress(context.Background(), rc, "orders", ir.TableProgress{
			State: ir.TableProgressComplete,
		}); err != nil {
			t.Fatalf("writeTableProgress: %v", err)
		}
	})

	t.Run("never resumes: loadOrInitState refuses LOUDLY", func(t *testing.T) {
		t.Parallel()
		// The door held shut ahead of the first caller. It must refuse
		// rather than quietly hand back a fresh state: a silent fresh start
		// is indistinguishable from a successful resume and would re-copy
		// the entire database.
		rc := newSyncRecordingContext(newFakeStateStore(), "prod-cutover")
		_, _, err := loadOrInitState(context.Background(), rc, true, false)
		if err == nil {
			t.Fatal("loadOrInitState accepted a record-only context; a sync cold start's progress rows are not a " +
				"resumable migration and resuming from them would re-copy everything")
		}
		if !strings.Contains(err.Error(), "record-only") {
			t.Errorf("the refusal does not say why: %v", err)
		}
	})

	t.Run("namespaced: sync ids cannot collide with a derived migrate id", func(t *testing.T) {
		t.Parallel()
		// The safety property, not merely tidiness. deriveMigrationID hashes
		// (source, target, schema) behind an "auto-" prefix; if a sync cold
		// start wrote under that same id, a later `migrate --resume` against
		// the same pair would find state describing a copy it never ran.
		syncID := syncMigrationID("prod-cutover")
		migrateID := deriveMigrationID("mysql", "u:p@tcp(a)/d", "postgres", "postgres://b/d", "")
		if syncID == migrateID {
			t.Fatal("a sync id collided with a derived migrate id")
		}
		if !strings.HasPrefix(syncID, "sync-") {
			t.Errorf("sync id %q lacks the sync- prefix that keeps the two populations apart", syncID)
		}
		if strings.HasPrefix(migrateID, "sync-") {
			t.Errorf("deriveMigrationID produced %q, which is inside the sync namespace — the two can now alias", migrateID)
		}
		// Distinct streams against one target stay distinct.
		if syncMigrationID("a") == syncMigrationID("b") {
			t.Error("two stream ids produced the same migration id")
		}
	})

	t.Run("degrades: no store means no recording, not a crash", func(t *testing.T) {
		t.Parallel()
		// A target engine implementing no MigrationStateStore records
		// nothing. That is a real coverage gap rather than a nicety — such a
		// target still reports the stream as absent during a cold start —
		// and it is asserted here so the gap is a stated property.
		for _, tc := range []struct {
			name     string
			store    ir.MigrationStateStore
			streamID string
		}{
			{"no store", nil, "s1"},
			{"no stream id", newFakeStateStore(), ""},
		} {
			rc := newSyncRecordingContext(tc.store, tc.streamID)
			if rc.writes() {
				t.Errorf("%s: context reports it writes", tc.name)
			}
			if err := writeTableProgress(context.Background(), rc, "orders", ir.TableProgress{}); err != nil {
				t.Errorf("%s: writeTableProgress on an inert context returned %v; want a silent no-op", tc.name, err)
			}
		}
	})
}

// The zero value of resumeContext must behave exactly as it did before
// noResume existed, for `migrate` and for every test that constructs one
// inline.
//
// This is the v0.99.51 zero-value trap, and it is pinned rather than
// trusted because the FIRST cut of this change got it wrong in exactly the
// documented way: the field was spelled `recording`, defaulting to false,
// so every pre-existing construction silently stopped writing. It was
// caught by TestMarkFailedJoinsStateError — a test that predates the change
// and constructs a resumeContext by hand, which is precisely the population
// an on-by-name field inverts.
func TestResumeContextZeroValueIsPreSplitBehaviour(t *testing.T) {
	t.Parallel()

	// An inline construction with no knowledge of the new field: writes,
	// and resumes.
	rc := resumeContext{store: newFakeStateStore(), migrationID: "m1", enabled: true}
	if !rc.writes() {
		t.Error("a hand-constructed enabled context does not write; the split inverted existing behaviour")
	}
	if rc.noResume {
		t.Error("the zero value opts OUT of resume; the field's polarity is inverted and every existing " +
			"caller has silently lost the ability to resume")
	}
	if _, _, err := loadOrInitState(context.Background(), rc, false, false); err != nil {
		t.Errorf("loadOrInitState on a pre-split context: %v", err)
	}

	// And the disabled zero value still no-ops rather than panicking on a
	// nil store.
	var inert resumeContext
	if inert.writes() {
		t.Error("the fully-zero context reports it writes; it has no store to write to")
	}
	if err := writeState(context.Background(), inert, ir.MigrationState{}); err != nil {
		t.Errorf("writeState on an inert context: %v", err)
	}
	if err := markFailed(context.Background(), inert, ir.MigrationState{},
		ir.MigrationPhaseTables, errors.New("boom")); err == nil || err.Error() != "boom" {
		t.Errorf("markFailed on an inert context = %v; want the primary error returned untouched", err)
	}
}
