// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0175 unit pins for the publication-scope surface: the narrowing
// decision that gates the refusal, and the engine's publication-name
// resolution. The end-to-end behaviour (two concurrent PG streams) is
// pinned by the integration gate in
// internal/pipeline/publication_scope_conflict_pg_integration_test.go.

package postgres

import (
	"context"
	"reflect"
	"strings"
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

// defSet builds a definition map with attribute-free members.
func defSet(keys ...string) map[string]publicationMemberAttrs {
	m := make(map[string]publicationMemberAttrs, len(keys))
	for _, k := range keys {
		m[k] = publicationMemberAttrs{}
	}
	return m
}

// bareQuals builds a filter-free incoming definition (every member
// bare) — the shape every pre-ADR-0176 caller sends.
func bareQuals(keys ...string) map[string]string {
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		m[k] = ""
	}
	return m
}

// TestAttributeChangedSurvivors pins the ADR-0176 widening of the
// guard over the full attribute-transition matrix: a row filter /
// column list the incoming definition would CLEAR or ADD is a hard
// (provable) conflict; filter-vs-filter is AMBIGUOUS (raw text vs
// pg_get_expr rendering — only the transactional probe can decide);
// and, just as load-bearing, an attribute-free equal/widening rescope
// must NOT fire (every pre-ADR-0176 shipped shape). Catalog-side
// (potentially PEER) predicate text must never appear in the rendered
// entries — see TestScopeConflictRendering_RedactsPeerPredicates — while
// the caller's own incoming qual stays visible.
func TestAttributeChangedSurvivors(t *testing.T) {
	tests := []struct {
		name          string
		defs          map[string]publicationMemberAttrs
		incoming      map[string]string
		wantHard      []string
		wantAmbiguous []string
	}{
		{
			name:     "same set, no attributes — no fire (every shipped shape)",
			defs:     defSet("public.orders", "public.items"),
			incoming: bareQuals("public.orders", "public.items"),
		},
		{
			name: "bare incoming clears a surviving row filter — hard",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {Qual: "(country = 'US'::text)"},
				"public.items":  {},
			},
			incoming: bareQuals("public.orders", "public.items"),
			wantHard: []string{"public.orders (carries a row filter this rescope would clear)"},
		},
		{
			name: "incoming filter added where none exists — hard (narrows peers)",
			defs: defSet("public.orders"),
			incoming: map[string]string{
				"public.orders": "id < 100",
			},
			wantHard: []string{"public.orders (a row filter WHERE (id < 100) would be added, narrowing the stream)"},
		},
		{
			name: "filter on both sides — ambiguous (only PG can compare)",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {Qual: "(id < 100)"},
			},
			incoming: map[string]string{
				"public.orders": "id < 100",
			},
			wantAmbiguous: []string{"public.orders"},
		},
		{
			name: "column list is always a hard clear, filter or not",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {HasColumnList: true},
			},
			incoming: bareQuals("public.orders"),
			wantHard: []string{"public.orders (a column list would be cleared)"},
		},
		{
			name: "column list + row filter, bare incoming — one entry naming both",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {Qual: "(id > 5)", HasColumnList: true},
			},
			incoming: bareQuals("public.orders"),
			wantHard: []string{"public.orders (a column list would be cleared, and the row filter it carries too)"},
		},
		{
			name: "column list + row filter, incoming filter — hard on the list, not ambiguous",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {Qual: "(id > 5)", HasColumnList: true},
			},
			incoming: map[string]string{"public.orders": "id > 5"},
			wantHard: []string{"public.orders (a column list would be cleared)"},
		},
		{
			name: "attribute on a REMOVED member is not double-reported here",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {Qual: "(id > 5)"},
			},
			incoming: bareQuals("public.users"),
			// removedFromPublication reports public.orders; this arm only
			// guards survivors.
		},
		{
			name: "widening past an attribute-bearing survivor still fires",
			defs: map[string]publicationMemberAttrs{
				"public.orders": {Qual: "(id > 5)"},
			},
			incoming: bareQuals("public.orders", "public.users"),
			wantHard: []string{"public.orders (carries a row filter this rescope would clear)"},
		},
		{
			name:     "results are sorted for a stable refusal message",
			defs:     map[string]publicationMemberAttrs{"public.zeta": {HasColumnList: true}, "public.alpha": {HasColumnList: true}},
			incoming: bareQuals("public.zeta", "public.alpha"),
			wantHard: []string{"public.alpha (a column list would be cleared)", "public.zeta (a column list would be cleared)"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hard, ambiguous := attributeChangedSurvivors(tc.defs, tc.incoming)
			if !reflect.DeepEqual(hard, tc.wantHard) {
				t.Errorf("attributeChangedSurvivors() hard = %v, want %v", hard, tc.wantHard)
			}
			if !reflect.DeepEqual(ambiguous, tc.wantAmbiguous) {
				t.Errorf("attributeChangedSurvivors() ambiguous = %v, want %v", ambiguous, tc.wantAmbiguous)
			}
		})
	}
}

// TestMemberKeySet pins the projection the guard feeds to
// removedFromPublication — a drift here would silently change which
// arm of the definition comparison sees which members.
func TestMemberKeySet(t *testing.T) {
	defs := map[string]publicationMemberAttrs{
		"public.orders": {Qual: "(id > 5)"},
		"public.items":  {},
	}
	if got, want := memberKeySet(defs), keySet("public.orders", "public.items"); !reflect.DeepEqual(got, want) {
		t.Errorf("memberKeySet() = %v, want %v", got, want)
	}
}

// TestQualifiedTableQuals pins that the guard's incoming-key shape
// matches publicationMemberSet's "schema.table" keying — if these ever
// diverge the guard silently classifies EVERY rescope as narrowing
// (over-fire) or none of them (under-fire, silent loss) — and that the
// ADR-0176 filter map (keyed by BARE table name) lands on the right
// qualified member.
func TestQualifiedTableQuals(t *testing.T) {
	got := qualifiedTableQuals("public", []string{"orders", "items"}, map[string]string{"orders": "id < 100"})
	want := map[string]string{
		"public.orders": "id < 100",
		"public.items":  "",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("qualifiedTableQuals() = %v, want %v", got, want)
	}

	// Round-trip against the member-set key shape: a member emitted as
	// nspname+"."+relname must be found by a key built from the same
	// schema and table.
	member := "public" + "." + "orders"
	if _, ok := got[member]; !ok {
		t.Errorf("key shape mismatch: member %q not found in %v", member, got)
	}

	// nil filters — every pre-ADR-0176 caller — leaves every member bare.
	if got := qualifiedTableQuals("public", []string{"orders"}, nil); got["public.orders"] != "" {
		t.Errorf("nil filters must render every member bare; got %v", got)
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
	// ADR-0176: same contract for the row-filter sibling — losing it
	// silently reverts every filtered PG sync to client-side-only
	// (correct but the push-down feature is gone without a trace).
	if _, ok := e.(ir.PublicationRowFilterer); !ok {
		t.Fatal("postgres.Engine no longer implements ir.PublicationRowFilterer")
	}
}

// TestEngine_WithPublicationRowFilters pins the configured-copy
// semantics (registry value stays filter-free) and that the filters
// ride into EnsurePublication's field.
func TestEngine_WithPublicationRowFilters(t *testing.T) {
	base := Engine{}
	filters := map[string]string{"orders": "id < 100"}
	scoped, ok := base.WithPublicationRowFilters(filters).(Engine)
	if !ok {
		t.Fatal("WithPublicationRowFilters did not return a postgres.Engine")
	}
	if got := scoped.publicationRowFilters()["orders"]; got != "id < 100" {
		t.Errorf("scoped publicationRowFilters()[orders] = %q, want %q", got, "id < 100")
	}
	if base.publicationRowFilters() != nil {
		t.Error("WithPublicationRowFilters mutated the receiver; the registry's shared value must stay filter-free")
	}
	// The boxed-pointer wart exists to keep Engine COMPARABLE (interface
	// `==` on ir.Engine values must never panic) — pin that property.
	if scoped == base {
		t.Error("scoped and base compare equal despite differing filters")
	}
	// Empty filters clear the box, restoring the zero value exactly.
	if cleared := scoped.WithPublicationRowFilters(nil).(Engine); cleared != base {
		t.Error("WithPublicationRowFilters(nil) must restore the filter-free zero value")
	}
}

// TestFormatPublicationTableList_RowFilters pins the ADR-0176 DDL
// rendering: a filtered table gets `WHERE (<raw predicate>)` via the
// SAME renderer the snapshot SELECT uses ([rowFilterWhereSQL]), an
// unfiltered sibling stays bare, and nil filters is byte-identical to
// the pre-ADR-0176 output.
func TestFormatPublicationTableList_RowFilters(t *testing.T) {
	tables := []string{"orders", "items"}
	filters := map[string]string{"orders": "country = 'US'"}

	got := formatPublicationTableList("public", tables, filters)
	want := `"public"."orders" WHERE (country = 'US'), "public"."items"`
	if got != want {
		t.Errorf("filtered list:\n got  %s\n want %s", got, want)
	}

	// Single-source pin: the publication's WHERE suffix and the snapshot
	// SELECT's must be the identical rendering of the identical text.
	if snapshot := rowFilterWhereSQL("country = 'US'"); snapshot != ` WHERE (country = 'US')` {
		t.Errorf("rowFilterWhereSQL drifted: %q", snapshot)
	}

	if got := formatPublicationTableList("public", tables, nil); got != `"public"."orders", "public"."items"` {
		t.Errorf("nil filters must render the pre-ADR-0176 bare list; got %s", got)
	}

	// Empty-schema callers render unqualified identifiers, filters intact.
	if got := formatPublicationTableList("", []string{"orders"}, filters); got != `"orders" WHERE (country = 'US')` {
		t.Errorf("empty-schema filtered list = %s", got)
	}
}

// TestPublicationRowFiltersForVersion pins the ADR-0176 version gate:
// below PG 15 ([pgVersionPublicationAttrs]) the filters are DROPPED so
// the emitted DDL is byte-identical to today (client-side-only,
// silently-safe); at/above 15 they pass through untouched.
func TestPublicationRowFiltersForVersion(t *testing.T) {
	ctx := context.Background()
	filters := map[string]string{"orders": "id < 100"}
	if got := publicationRowFiltersForVersion(ctx, 140011, filters); got != nil {
		t.Errorf("PG 14 must drop the filters (client-side-only); got %v", got)
	}
	if got := publicationRowFiltersForVersion(ctx, pgVersionPublicationAttrs, filters); !reflect.DeepEqual(got, filters) {
		t.Errorf("PG 15.0 must pass the filters through; got %v", got)
	}
	if got := publicationRowFiltersForVersion(ctx, 160009, filters); !reflect.DeepEqual(got, filters) {
		t.Errorf("PG 16 must pass the filters through; got %v", got)
	}
}

// TestChangedMemberDefs pins the probe's pure diff: pg_get_expr-
// normalized definitions compare by text equality, and every changed
// member is named for the refusal message — with the BEFORE side's
// predicate text redacted (it is catalog state that may quote a PEER
// stream's filter; audit 2026-07-23 SEC-1) while the caller's own
// incoming (after) qual stays visible.
func TestChangedMemberDefs(t *testing.T) {
	before := map[string]publicationMemberAttrs{
		"public.orders": {Qual: "(id < 100)"},
		"public.items":  {Qual: "(sku <> ''::text)"},
	}
	same := map[string]publicationMemberAttrs{
		"public.orders": {Qual: "(id < 100)"},
		"public.items":  {Qual: "(sku <> ''::text)"},
	}
	if got := changedMemberDefs(before, same); got != nil {
		t.Errorf("identical definitions must diff empty; got %v", got)
	}
	after := map[string]publicationMemberAttrs{
		"public.orders": {Qual: "(id < 50)"},
		"public.items":  {Qual: "(sku <> ''::text)"},
	}
	want := []string{"public.orders (row filter changed: WHERE (<current filter hidden — it may belong to another stream>) → WHERE ((id < 50)))"}
	if got := changedMemberDefs(before, after); !reflect.DeepEqual(got, want) {
		t.Errorf("changedMemberDefs() = %v, want %v", got, want)
	}
}

// TestScopeConflictRendering_RedactsPeerPredicates is the audit
// 2026-07-23 SEC-1 pin: a scope-conflict refusal can land in the
// terminal of an operator who does NOT own the peer stream, and a row
// filter routinely carries data values (customer identifiers, date
// cutoffs) — so no rendered entry may echo catalog-side predicate text.
// The operator's OWN incoming predicate remains visible (they typed it).
func TestScopeConflictRendering_RedactsPeerPredicates(t *testing.T) {
	const peerSecret = "customer_id = 'acme-8842'"

	t.Run("attributeChangedSurvivors never echoes the catalog qual", func(t *testing.T) {
		defs := map[string]publicationMemberAttrs{
			"public.orders": {Qual: "(" + peerSecret + ")"},
			"public.audits": {Qual: "(" + peerSecret + ")", HasColumnList: true},
		}
		hard, _ := attributeChangedSurvivors(defs, bareQuals("public.orders", "public.audits"))
		for _, entry := range hard {
			if strings.Contains(entry, "acme-8842") {
				t.Errorf("peer predicate text leaked into the refusal entry: %s", entry)
			}
		}
		if len(hard) != 2 {
			t.Fatalf("expected both members reported; got %v", hard)
		}
		// The operator's own incoming qual stays visible.
		hard, _ = attributeChangedSurvivors(defSet("public.orders"), map[string]string{"public.orders": "id < 100"})
		if len(hard) != 1 || !strings.Contains(hard[0], "id < 100") {
			t.Errorf("the caller's own incoming qual must stay visible; got %v", hard)
		}
	})

	t.Run("changedMemberDefs redacts the before side only", func(t *testing.T) {
		before := map[string]publicationMemberAttrs{"public.orders": {Qual: "(" + peerSecret + ")"}}
		after := map[string]publicationMemberAttrs{"public.orders": {Qual: "(id < 50)"}}
		changed := changedMemberDefs(before, after)
		if len(changed) != 1 {
			t.Fatalf("changedMemberDefs = %v; want one entry", changed)
		}
		if strings.Contains(changed[0], "acme-8842") {
			t.Errorf("peer predicate text leaked: %s", changed[0])
		}
		if !strings.Contains(changed[0], "(id < 50)") {
			t.Errorf("the caller's own (after) qual must stay visible: %s", changed[0])
		}
	})

	t.Run("verifyMemberDiff never echoes any qual", func(t *testing.T) {
		current := map[string]publicationMemberAttrs{"public.orders": {Qual: "(" + peerSecret + ")"}}
		ensured := map[string]publicationMemberAttrs{"public.orders": {Qual: "(id < 100)"}}
		for _, entry := range verifyMemberDiff(current, ensured) {
			if strings.Contains(entry, "acme-8842") || strings.Contains(entry, "id < 100") {
				t.Errorf("verifyMemberDiff echoed predicate text: %s", entry)
			}
		}
	})
}

// TestVerifyMemberDiff pins the D0-7 post-slot comparison's pure diff
// over its full transition matrix: equal definitions (including
// attribute-free PG<15 member sets) diff empty; a changed filter, a
// peer-added member, and a peer-removed member each render a named,
// sorted, predicate-free entry.
func TestVerifyMemberDiff(t *testing.T) {
	ours := map[string]publicationMemberAttrs{
		"public.orders": {Qual: "(id < 100)"},
		"public.items":  {},
	}
	t.Run("unchanged — empty diff (the happy cold start)", func(t *testing.T) {
		same := map[string]publicationMemberAttrs{
			"public.orders": {Qual: "(id < 100)"},
			"public.items":  {},
		}
		if got := verifyMemberDiff(same, ours); got != nil {
			t.Errorf("verifyMemberDiff(equal) = %v; want empty", got)
		}
	})
	t.Run("peer swapped the filter — reported without the text", func(t *testing.T) {
		current := map[string]publicationMemberAttrs{
			"public.orders": {Qual: "(region = 'EU')"},
			"public.items":  {},
		}
		want := []string{"public.orders (its row filter or column list differs from what this stream ensured)"}
		if got := verifyMemberDiff(current, ours); !reflect.DeepEqual(got, want) {
			t.Errorf("verifyMemberDiff = %v, want %v", got, want)
		}
	})
	t.Run("peer replaced the member set — both directions reported, sorted", func(t *testing.T) {
		current := map[string]publicationMemberAttrs{
			"public.orders": {Qual: "(id < 100)"},
			"public.users":  {},
		}
		want := []string{
			"public.items (removed from the publication)",
			"public.users (present in the publication but not in this stream's definition)",
		}
		if got := verifyMemberDiff(current, ours); !reflect.DeepEqual(got, want) {
			t.Errorf("verifyMemberDiff = %v, want %v", got, want)
		}
	})
	t.Run("PG<15 attribute-free sets compare by membership alone", func(t *testing.T) {
		if got := verifyMemberDiff(defSet("public.a", "public.b"), defSet("public.a", "public.b")); got != nil {
			t.Errorf("attribute-free equal sets must diff empty; got %v", got)
		}
	})
}
