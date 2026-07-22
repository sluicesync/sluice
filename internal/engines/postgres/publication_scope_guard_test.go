// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0175 unit pins for the publication-scope surface: the narrowing
// decision that gates the refusal, and the engine's publication-name
// resolution. The end-to-end behaviour (two concurrent PG streams) is
// pinned by the integration gate in
// internal/pipeline/publication_scope_conflict_pg_integration_test.go.

package postgres

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func keySet(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

// TestRemovedFromPublication covers the whole decision matrix. The
// no-fire cases are the ones with the widest blast radius: the
// ADR-0122 fleet shape (identical scopes) and `schema add-table`
// (additive) must never be classified as narrowing.
func TestRemovedFromPublication(t *testing.T) {
	tests := []struct {
		name     string
		members  map[string]struct{}
		incoming map[string]struct{}
		want     []string
	}{
		{
			name:     "equal scope removes nothing (the ADR-0122 fleet shape)",
			members:  keySet("public.orders", "public.items"),
			incoming: keySet("public.orders", "public.items"),
			want:     nil,
		},
		{
			name:     "widening removes nothing (schema add-table)",
			members:  keySet("public.orders"),
			incoming: keySet("public.orders", "public.users"),
			want:     nil,
		},
		{
			name:     "narrowing reports the dropped table",
			members:  keySet("public.orders", "public.items"),
			incoming: keySet("public.orders"),
			want:     []string{"public.items"},
		},
		{
			name:     "disjoint reports every current member (the wave-migration shape)",
			members:  keySet("public.orders", "public.items"),
			incoming: keySet("public.users", "public.sessions"),
			want:     []string{"public.items", "public.orders"},
		},
		{
			name:     "empty publication removes nothing",
			members:  keySet(),
			incoming: keySet("public.users"),
			want:     nil,
		},
		{
			name:     "empty incoming removes every member",
			members:  keySet("public.orders"),
			incoming: keySet(),
			want:     []string{"public.orders"},
		},
		{
			name:     "results are sorted for a stable refusal message",
			members:  keySet("public.zeta", "public.alpha", "public.mid"),
			incoming: keySet(),
			want:     []string{"public.alpha", "public.mid", "public.zeta"},
		},
		{
			name:     "schema-qualified: same table name in another schema is NOT a match",
			members:  keySet("sales.orders"),
			incoming: keySet("public.orders"),
			want:     []string{"sales.orders"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := removedFromPublication(tc.members, tc.incoming)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("removedFromPublication() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQualifiedTableKeys pins that the guard's incoming-key shape
// matches publicationMemberSet's "schema.table" keying — if these ever
// diverge the guard silently classifies EVERY rescope as narrowing
// (over-fire) or none of them (under-fire, silent loss).
func TestQualifiedTableKeys(t *testing.T) {
	got := qualifiedTableKeys("public", []string{"orders", "items"})
	want := keySet("public.orders", "public.items")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("qualifiedTableKeys() = %v, want %v", got, want)
	}

	// Round-trip against the member-set key shape: a member emitted as
	// nspname+"."+relname must be found by a key built from the same
	// schema and table.
	member := "public" + "." + "orders"
	if _, ok := got[member]; !ok {
		t.Errorf("key shape mismatch: member %q not found in %v", member, got)
	}
}

// TestEngine_PublicationName pins the zero-value default and the
// override, including that WithPublicationScope returns a COPY — the
// engine registry's shared value must stay scope-free for the next
// caller (the same contract WithConnectionLabel has).
func TestEngine_PublicationName(t *testing.T) {
	base := Engine{}
	if got := base.publicationName(); got != defaultPublication {
		t.Errorf("zero-value publicationName() = %q, want %q", got, defaultPublication)
	}

	scoped, ok := base.WithPublicationScope("sluice_wave1", "sluice_wave1_slot").(Engine)
	if !ok {
		t.Fatal("WithPublicationScope did not return a postgres.Engine")
	}
	if got := scoped.publicationName(); got != "sluice_wave1" {
		t.Errorf("scoped publicationName() = %q, want %q", got, "sluice_wave1")
	}
	if got := scoped.ownSlot; got != "sluice_wave1_slot" {
		t.Errorf("scoped ownSlot = %q, want %q", got, "sluice_wave1_slot")
	}

	// The receiver must be untouched (value receiver, copy returned).
	if got := base.publicationName(); got != defaultPublication {
		t.Errorf("WithPublicationScope mutated the receiver: publicationName() = %q, want %q", got, defaultPublication)
	}
	if base.ownSlot != "" {
		t.Errorf("WithPublicationScope mutated the receiver: ownSlot = %q, want empty", base.ownSlot)
	}

	// An empty publication keeps the default, so a stream that never
	// passes --publication-name is byte-identical to pre-ADR-0175.
	unscoped, ok := base.WithPublicationScope("", "sluice_only_slot").(Engine)
	if !ok {
		t.Fatal("WithPublicationScope did not return a postgres.Engine")
	}
	if got := unscoped.publicationName(); got != defaultPublication {
		t.Errorf("empty-publication publicationName() = %q, want %q", got, defaultPublication)
	}
	if unscoped.ownSlot != "sluice_only_slot" {
		t.Errorf("own slot must still be carried when publication is empty; got %q", unscoped.ownSlot)
	}
}

// TestEngine_ImplementsPublicationScoper is the compile-time contract
// the pipeline's type assertion depends on. A silent loss of this
// method would make the streamer's assertion no-op, dropping BOTH the
// per-stream publication and the own-slot exclusion.
func TestEngine_ImplementsPublicationScoper(t *testing.T) {
	var e any = Engine{}
	if _, ok := e.(ir.PublicationScoper); !ok {
		t.Fatal("postgres.Engine no longer implements ir.PublicationScoper")
	}
}
