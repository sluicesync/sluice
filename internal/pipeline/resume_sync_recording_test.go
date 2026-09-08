// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

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
		rc := newSyncRecordingContext(context.Background(), store, "prod-cutover")
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
		rc := newSyncRecordingContext(context.Background(), newFakeStateStore(), "prod-cutover")
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
			rc := newSyncRecordingContext(context.Background(), tc.store, tc.streamID)
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

// An over-long stream id costs the STATUS SURFACE, never the migration.
//
// Found by the pre-release persisted-state trigger, and it is a defect the
// cold-start recording feature INTRODUCED. `--stream-id` has no length
// validation anywhere and sluice_cdc_state.stream_id is VARCHAR(255), so a
// 251-to-255-character stream id is legal and worked before this feature
// existed. Prefixing it with "sync-" pushes the migration id past the
// progress table's own VARCHAR(255): strict-mode MySQL (the default)
// refuses with Error 1406, which would have failed the whole cold start on
// a configuration that used to work, and a non-strict server truncates
// silently, collapsing two long stream ids onto one row.
func TestSyncRecordingContextRefusesAnOverlongStreamID(t *testing.T) {
	t.Parallel()

	fits := strings.Repeat("a", migrationIDMaxRunes-len(syncMigrationIDPrefixForTest))
	tooLong := fits + "a"

	if rc := newSyncRecordingContext(context.Background(), newFakeStateStore(), fits); !rc.writes() {
		t.Errorf("a stream id that exactly fills the column was refused; the boundary is off by one and "+
			"operators lose status for ids that are perfectly storable (len=%d)", len(fits))
	}
	if rc := newSyncRecordingContext(context.Background(), newFakeStateStore(), tooLong); rc.writes() {
		t.Errorf("a stream id one rune too long was accepted; on strict-mode MySQL the first progress write "+
			"fails with Error 1406 and takes the cold start with it (len=%d)", len(tooLong))
	}

	// Multi-byte: the column counts CHARACTERS, so a 250-rune id of
	// 4-byte runes fits even though it is 1000 bytes. Counting bytes here
	// would refuse a legal id and silently drop its status surface.
	multibyte := strings.Repeat("🌊", migrationIDMaxRunes-len(syncMigrationIDPrefixForTest))
	if rc := newSyncRecordingContext(context.Background(), newFakeStateStore(), multibyte); !rc.writes() {
		t.Errorf("a 250-RUNE multi-byte stream id was refused — the check is counting bytes, not characters, "+
			"and VARCHAR(255) counts characters on both engines (bytes=%d runes=%d)",
			len(multibyte), utf8.RuneCountInString(multibyte))
	}
}

// syncMigrationIDPrefixForTest is the prefix length the cases above budget
// for, derived rather than hardcoded so a renamed prefix cannot leave the
// boundary cases testing the wrong width.
var syncMigrationIDPrefixForTest = syncMigrationID("")

// The width constant is a claim about DDL that lives in another package.
//
// internal/pipeline cannot import the engines, so migrationIDMaxRunes is a
// copy — and a copy of a number is exactly the kind of invariant that stops
// being true quietly. Widening migration_id without touching the constant
// would leave sluice refusing to record status for ids the column now
// accepts, with no failure anywhere.
func TestMigrationIDWidthMatchesTheEngineDDL(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile(`migration_id\s+VARCHAR\((\d+)\)`)
	checked := 0
	for _, path := range []string{
		"../engines/mysql/migration_state.go",
		"../engines/postgres/migration_state.go",
	} {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		matches := re.FindAllSubmatch(src, -1)
		if len(matches) == 0 {
			t.Fatalf("%s declares no `migration_id VARCHAR(n)` column. If the DDL was reshaped, re-point this "+
				"gate rather than deleting it: migrationIDMaxRunes is only correct because it matches it.", path)
		}
		for _, m := range matches {
			got, err := strconv.Atoi(string(m[1]))
			if err != nil {
				t.Fatalf("%s: unparseable width %q", path, m[1])
			}
			if got != migrationIDMaxRunes {
				t.Errorf("%s declares migration_id VARCHAR(%d) but migrationIDMaxRunes is %d.\n"+
					"  Nothing fails when these diverge: sluice simply refuses to record cold-start progress "+
					"for stream ids the column would have accepted, and `sync status` goes quiet again.",
					path, got, migrationIDMaxRunes)
			}
			checked++
		}
	}
	// Anti-vacuity: both engines declare it on BOTH the header and the
	// per-table progress table, so a passing run that saw fewer than four
	// columns has stopped looking at something.
	if checked < 4 {
		t.Errorf("the walk checked only %d migration_id column(s); expected 4 (header + progress, x2 engines). "+
			"The regex or the DDL shape changed and this gate is now checking less than its name implies.", checked)
	}
}
