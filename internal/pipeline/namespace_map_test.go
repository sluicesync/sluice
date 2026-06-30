// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"reflect"
	"strings"
	"testing"
)

// TestNewNamespaceRenameMap pins the ADR-0142 construction contract: an
// empty list is the identity map; well-formed pairs parse (with trimming);
// and every loud-refusal class — malformed, empty side, multiple '=',
// duplicate source key, many-to-one — is rejected at construction (before
// any data can move).
func TestNewNamespaceRenameMap(t *testing.T) {
	cases := []struct {
		name    string
		pairs   []string
		wantErr string // "" = expect success
	}{
		{"empty is identity", nil, ""},
		{"single valid", []string{"app=app_prod"}, ""},
		{"multiple valid", []string{"app=app_prod", "billing=billing_prod"}, ""},
		{"trimmed", []string{"  app = app_prod  "}, ""},
		{"self map allowed", []string{"app=app"}, ""},
		{"chain not many-to-one", []string{"app=billing", "billing=archive"}, ""},
		{"malformed no equals", []string{"app"}, "malformed"},
		{"empty source", []string{"=app_prod"}, "empty source or target"},
		{"empty target", []string{"app="}, "empty source or target"},
		{"multiple equals", []string{"app=a=b"}, "multiple '='"},
		{"duplicate source key", []string{"app=x", "app=y"}, "twice"},
		{"many to one", []string{"app=prod", "billing=prod"}, "many-to-one"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := NewNamespaceRenameMap(c.pairs)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("NewNamespaceRenameMap(%v) = %v; want nil", c.pairs, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("NewNamespaceRenameMap(%v) = %v; want error containing %q", c.pairs, err, c.wantErr)
			}
		})
	}
}

// TestNamespaceRenameMapApply pins identity-by-default: a key maps to its
// target, an unmapped namespace returns itself unchanged, and the zero value
// is total identity.
func TestNamespaceRenameMapApply(t *testing.T) {
	m, err := NewNamespaceRenameMap([]string{"app=app_prod", "self=self"})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	if got := m.Apply("app"); got != "app_prod" {
		t.Errorf("Apply(app) = %q; want app_prod", got)
	}
	if got := m.Apply("billing"); got != "billing" {
		t.Errorf("Apply(billing) = %q; want billing (identity)", got)
	}
	if got := m.Apply("self"); got != "self" {
		t.Errorf("Apply(self) = %q; want self", got)
	}
	var zero NamespaceRenameMap
	if !zero.IsEmpty() {
		t.Error("zero value should be empty (identity)")
	}
	if got := zero.Apply("anything"); got != "anything" {
		t.Errorf("zero.Apply(anything) = %q; want identity", got)
	}
	if m.IsEmpty() {
		t.Error("populated map should not be empty")
	}
}

// TestNamespaceRenameMapKeys pins the sorted-keys contract used by both the
// map-only selection and the not-selected cross-check.
func TestNamespaceRenameMapKeys(t *testing.T) {
	m, err := NewNamespaceRenameMap([]string{"zeta=z", "alpha=a", "mu=m"})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	want := []string{"alpha", "mu", "zeta"}
	if got := m.Keys(); !reflect.DeepEqual(got, want) {
		t.Errorf("Keys() = %v; want %v", got, want)
	}
}

// TestSelectNamespaces pins the ADR-0142 selection modes: map-only selects
// exactly the map keys; a filter/all selection renames WITHIN it (the map
// does not change the selection); and the no-map path is byte-identical to
// the filter-only behaviour.
func TestSelectNamespaces(t *testing.T) {
	all := []string{"app", "billing", "scratch", "legacy"}
	mustMap := func(pairs ...string) NamespaceRenameMap {
		m, err := NewNamespaceRenameMap(pairs)
		if err != nil {
			t.Fatalf("construct map %v: %v", pairs, err)
		}
		return m
	}

	cases := []struct {
		name   string
		filter DatabaseFilter
		allDBs bool
		nsMap  NamespaceRenameMap
		want   []string
	}{
		{
			name: "no map, no filter -> all pass (all-databases shape)",
			want: []string{"app", "billing", "legacy", "scratch"},
		},
		{
			name:  "map-only selects exactly the keys",
			nsMap: mustMap("app=app_prod", "billing=billing_prod"),
			want:  []string{"app", "billing"},
		},
		{
			name:   "include filter + map renames within the selection",
			filter: DatabaseFilter{Include: []string{"app", "billing"}},
			nsMap:  mustMap("app=app_prod"),
			want:   []string{"app", "billing"},
		},
		{
			name:   "all-databases + map does NOT shrink to keys",
			allDBs: true,
			nsMap:  mustMap("app=app_prod"),
			want:   []string{"app", "billing", "legacy", "scratch"},
		},
		{
			name:   "exclude filter unaffected by map mode",
			filter: DatabaseFilter{Exclude: []string{"scratch"}},
			want:   []string{"app", "billing", "legacy"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := selectNamespaces(all, c.filter, c.allDBs, c.nsMap)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("selectNamespaces = %v; want %v", got, c.want)
			}
		})
	}
}

// TestCrossCheckMapSelection pins the typo guard: a rename-map key that is
// not in the resolved selection is refused loudly (naming the key); identity
// and fully-selected maps are no-ops.
func TestCrossCheckMapSelection(t *testing.T) {
	m, err := NewNamespaceRenameMap([]string{"app=app_prod", "ghost=ghost_prod"})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	// ghost is not selected -> loud error naming it.
	err = crossCheckMapSelection([]string{"app", "billing"}, m)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("crossCheckMapSelection = %v; want an error naming the unselected key %q", err, "ghost")
	}
	if !strings.Contains(err.Error(), "not in the resolved namespace selection") {
		t.Errorf("error %q should explain the not-selected condition", err)
	}
	// All keys selected -> nil.
	if err := crossCheckMapSelection([]string{"app", "ghost", "billing"}, m); err != nil {
		t.Errorf("crossCheckMapSelection(all selected) = %v; want nil", err)
	}
	// Identity map -> nil regardless of selection.
	if err := crossCheckMapSelection(nil, NamespaceRenameMap{}); err != nil {
		t.Errorf("crossCheckMapSelection(identity) = %v; want nil", err)
	}
}

// TestResolveTargetNamespaces pins the engine-agnostic many-to-one guard: it
// maps each source to its target (identity for unmapped) and refuses when two
// distinct sources resolve to the same target — including the mapped-vs-
// unmapped collision the parse-time check can't see.
func TestResolveTargetNamespaces(t *testing.T) {
	t.Run("identity passthrough", func(t *testing.T) {
		got, err := resolveTargetNamespaces([]string{"app", "billing"}, NamespaceRenameMap{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, []string{"app", "billing"}) {
			t.Errorf("targets = %v; want identity", got)
		}
	})
	t.Run("rename applied", func(t *testing.T) {
		m, _ := NewNamespaceRenameMap([]string{"app=app_prod"})
		got, err := resolveTargetNamespaces([]string{"app", "billing"}, m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, []string{"app_prod", "billing"}) {
			t.Errorf("targets = %v; want [app_prod billing]", got)
		}
	})
	t.Run("mapped collides with unmapped selected -> refuse", func(t *testing.T) {
		// app renamed to billing, while billing is also selected unmapped:
		// both resolve to target "billing" — a silent merge, refused.
		m, _ := NewNamespaceRenameMap([]string{"app=billing"})
		_, err := resolveTargetNamespaces([]string{"app", "billing"}, m)
		if err == nil || !strings.Contains(err.Error(), "many-to-one") {
			t.Fatalf("resolveTargetNamespaces = %v; want a many-to-one refusal", err)
		}
		if !strings.Contains(err.Error(), "billing") {
			t.Errorf("error %q should name the colliding target", err)
		}
	})
}

// TestMultiDatabaseModeEngagedByMap pins that a non-empty NamespaceMap alone
// engages multi-namespace mode on BOTH the Migrator and the Streamer (the
// map-only convenience), while the zero value stays single-database.
func TestMultiDatabaseModeEngagedByMap(t *testing.T) {
	m, err := NewNamespaceRenameMap([]string{"app=app_prod"})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	if !(&Migrator{NamespaceMap: m}).multiDatabaseMode() {
		t.Error("Migrator with a rename map should be in multi-database mode")
	}
	if (&Migrator{}).multiDatabaseMode() {
		t.Error("Migrator with no flags should be single-database")
	}
	if !(&Streamer{NamespaceMap: m}).multiDatabaseMode() {
		t.Error("Streamer with a rename map should be in multi-database mode")
	}
	if (&Streamer{}).multiDatabaseMode() {
		t.Error("Streamer with no flags should be single-database")
	}
}

// TestStreamerNamespaceRenameFunc pins the load-bearing identity default: an
// empty map yields a nil rename (byte-identical pre-ADR-0142 routing), a
// populated map yields a func that renames mapped sources and passes
// unmapped ones through.
func TestStreamerNamespaceRenameFunc(t *testing.T) {
	if fn := (&Streamer{}).namespaceRenameFunc(); fn != nil {
		t.Error("empty NamespaceMap must yield a nil rename func (identity default)")
	}
	m, err := NewNamespaceRenameMap([]string{"app=app_prod"})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	fn := (&Streamer{NamespaceMap: m}).namespaceRenameFunc()
	if fn == nil {
		t.Fatal("populated NamespaceMap must yield a non-nil rename func")
	}
	if got := fn("app"); got != "app_prod" {
		t.Errorf("rename(app) = %q; want app_prod", got)
	}
	if got := fn("billing"); got != "billing" {
		t.Errorf("rename(billing) = %q; want identity", got)
	}
}
