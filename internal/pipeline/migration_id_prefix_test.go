// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"
)

// The `sync-` namespace door (audit 2026-09-15 A0915-STATE-MEDIUM-1 (refuter 3)).
//
// The sync cold start records its progress rows under
// SyncMigrationIDPrefix + streamID; `migrate --resume` given the same id
// reads that finished copy as its own and exits 0 having copied nothing
// (measured from a different source engine). The derived id can never
// carry the prefix; an operator-typed one now cannot either.
//
// Mutation-proven (2026-09-15): deleting the prefix from syncMigrationID
// fails the constructor pin; deleting the HasPrefix branch from
// resolveMigrationID fails the refusal pin.

// TestSyncMigrationIDPrefix_ConstructorUsesTheConstant is the
// anti-vacuity half: the exported constant is what the writer actually
// prefixes with, so the CLI readers and the migrate-side refusal that
// import it are keyed to the stored value and not to a mirror.
func TestSyncMigrationIDPrefix_ConstructorUsesTheConstant(t *testing.T) {
	t.Parallel()
	if SyncMigrationIDPrefix == "" {
		t.Fatal("SyncMigrationIDPrefix is empty; a refusal keyed on an empty prefix refuses every id")
	}
	got := syncMigrationID("x")
	if !strings.HasPrefix(got, SyncMigrationIDPrefix) || got != SyncMigrationIDPrefix+"x" {
		t.Fatalf("syncMigrationID(%q) = %q; want %q — the writer must build ids from the exported constant",
			"x", got, SyncMigrationIDPrefix+"x")
	}
	if !strings.HasPrefix(deriveMigrationID("mysql", "s", "postgres", "t", ""), "auto-") {
		t.Fatal("the derived id no longer starts with auto-; the argument that a derived id cannot alias a sync row rests on that")
	}
}

// TestResolveMigrationID_RefusesTheSyncNamespace pins the door at the
// pipeline chokepoint: a typed id under the prefix is refused with a
// message naming the prefix and the harm; a typed id without it passes
// through untouched; an empty one derives.
func TestResolveMigrationID_RefusesTheSyncNamespace(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		id         string
		wantErr    bool
		wantPrefix string
	}{
		{name: "sync namespace is refused", id: SyncMigrationIDPrefix + "x", wantErr: true},
		{name: "the bare prefix is refused too", id: SyncMigrationIDPrefix, wantErr: true},
		// Multi-database migrate hands each per-database Migrator the base id
		// suffixed with the database; the prefix must survive that, or the
		// door would reach the single-database path only.
		{name: "a multi-database per-database id is refused", id: multiDBMigrationID(SyncMigrationIDPrefix+"x", "db"), wantErr: true},
		{name: "a plain operator id passes through", id: "x", wantPrefix: "x"},
		{name: "an id merely CONTAINING the prefix passes", id: "my-" + SyncMigrationIDPrefix + "x", wantPrefix: "my-"},
		{name: "empty derives", id: "", wantPrefix: "auto-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := &Migrator{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "s", TargetDSN: "t", MigrationID: tc.id}
			got, err := m.resolveMigrationID()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveMigrationID(%q) = %q, nil; want a refusal", tc.id, got)
				}
				for _, want := range []string{SyncMigrationIDPrefix, "sync start", "copied nothing"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("refusal does not say %q:\n%s", want, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMigrationID(%q): unexpected refusal: %v", tc.id, err)
			}
			if !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("resolveMigrationID(%q) = %q; want prefix %q", tc.id, got, tc.wantPrefix)
			}
		})
	}
}
