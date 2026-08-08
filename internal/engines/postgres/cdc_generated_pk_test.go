// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// relEntry builds a relation cache entry whose published columns are
// `cols` — i.e. exactly what a RelationMessage would carry.
func relEntry(identity uint8, cols ...string) *relationCacheEntry {
	e := &relationCacheEntry{Schema: "public", Name: "od", ReplicaIdentity: identity}
	for _, c := range cols {
		e.Columns = append(e.Columns, relationColumn{Name: c})
	}
	return e
}

// TestRefuseUnpublishedGeneratedIdentity walks the decision family, not a
// representative: {no generated identity column, part of the key
// generated, the whole key generated, generated BUT PUBLISHED} × the
// replica identities that can carry each. The PUBLISHED cells are the
// over-refusal controls — the PostgreSQL 18
// `publish_generated_columns = stored` shape, verified on a real 18.4
// (see the file comment in cdc_generated_pk.go) — and they are the
// reason the check keys on the wire's own column list rather than on a
// server version.
func TestRefuseUnpublishedGeneratedIdentity(t *testing.T) {
	cases := []struct {
		name     string
		entry    *relationCacheEntry
		identity identityColumns

		wantRefusal bool
		msgHas      []string
		msgOmits    string
	}{
		{
			name:     "an ordinary key: nothing generated",
			entry:    relEntry('d', "a", "b"),
			identity: identityColumns{All: []string{"a"}},
		},
		{
			name:     "no key at all (PK-less FULL)",
			entry:    relEntry('f', "a", "b"),
			identity: identityColumns{},
		},
		{
			// The over-delete shape, DEFAULT: PRIMARY KEY (a, g), g
			// generated and unpublished, so the identity narrows to {a}.
			name:        "DEFAULT, part of a composite key generated and unpublished",
			entry:       relEntry('d', "a", "c", "b"),
			identity:    identityColumns{All: []string{"a", "g"}, Generated: []string{"g"}},
			wantRefusal: true,
			msgHas:      []string{`"g"`, "NON-UNIQUE prefix", "public.od"},
			msgOmits:    "no published column carries",
		},
		{
			// The zero-match shape: the WHOLE key is generated, so no
			// published column carries any part of the identity.
			name:        "DEFAULT, the whole key generated and unpublished",
			entry:       relEntry('d', "a", "b"),
			identity:    identityColumns{All: []string{"g"}, Generated: []string{"g"}},
			wantRefusal: true,
			msgHas:      []string{`"g"`, "no published column carries the row identity at all"},
			msgOmits:    "NON-UNIQUE prefix",
		},
		{
			// USING INDEX is not DEFAULT's alias: the identity is the
			// nominated index, and it loses its generated member the same
			// way. Verified on PG 16 — the RelationMessage for a table
			// with REPLICA IDENTITY USING INDEX over (a, g) flags only a.
			name:        "USING INDEX over a partly generated index",
			entry:       relEntry('i', "a", "c", "b"),
			identity:    identityColumns{All: []string{"a", "g"}, Generated: []string{"g"}},
			wantRefusal: true,
			msgHas:      []string{`"g"`, "NON-UNIQUE prefix"},
		},
		{
			// FULL does NOT rescue it. pgoutput omits the generated column
			// from the old tuple under FULL too, and sluice narrows a FULL
			// table to its catalog PRIMARY KEY — so the identity is
			// incomplete exactly as under DEFAULT. The pre-fix behaviour
			// here was a loud but unactionable "identity column missing
			// from old tuple" out of filterBeforeToKeyCols.
			name:        "FULL, part of the primary key generated and unpublished",
			entry:       relEntry('f', "a", "c", "b"),
			identity:    identityColumns{All: []string{"a", "g"}, Generated: []string{"g"}},
			wantRefusal: true,
			msgHas:      []string{`"g"`, "NON-UNIQUE prefix"},
		},
		{
			// OVER-REFUSAL CONTROL, and the reason the predicate is
			// absence-from-the-wire and not presence-of-a-generated-column:
			// a PG 18 publication created WITH
			// (publish_generated_columns = stored) lists g in the
			// RelationMessage and carries its value in the tuple.
			name:     "generated, but the publication carries it (PG 18 publish_generated_columns)",
			entry:    relEntry('d', "a", "c", "b", "g"),
			identity: identityColumns{All: []string{"a", "g"}, Generated: []string{"g"}},
		},
		{
			// The same control for a wholly-generated key.
			name:     "the whole key generated, but published",
			entry:    relEntry('d', "a", "b", "g"),
			identity: identityColumns{All: []string{"g"}, Generated: []string{"g"}},
		},
		{
			// Two generated members, one published and one not: the
			// refusal must name only the one actually missing.
			name:        "a partly published pair names only the missing column",
			entry:       relEntry('d', "a", "g1"),
			identity:    identityColumns{All: []string{"a", "g1", "g2"}, Generated: []string{"g1", "g2"}},
			wantRefusal: true,
			msgHas:      []string{`"g2"`},
			msgOmits:    `"g1"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseUnpublishedGeneratedIdentity(tc.entry, tc.identity)
			if !tc.wantRefusal {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected a refusal; got nil — this is the silent over-delete / zero-match cell")
			}
			coded, ok := sluicecode.FromError(err)
			if !ok || coded.Code != sluicecode.CodeCDCGeneratedPrimaryKey {
				t.Fatalf("refusal is not %s: %v", sluicecode.CodeCDCGeneratedPrimaryKey, err)
			}
			if coded.Hint == "" {
				t.Error("refusal carries no remedy hint")
			}
			msg := err.Error()
			for _, want := range tc.msgHas {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal does not mention %q:\n%s", want, msg)
				}
			}
			if tc.msgOmits != "" && strings.Contains(msg, tc.msgOmits) {
				t.Errorf("refusal wrongly mentions %q (the other consequence):\n%s", tc.msgOmits, msg)
			}
		})
	}
}

// TestRefuseUnpublishedGeneratedIdentity_RemedyDoesNotRecommendFULL
// pins the half of the message that would otherwise send an operator
// round a loop: REPLICA IDENTITY FULL is the standard cure for a missing
// replica identity and it does NOT fix this one, because pgoutput omits
// a generated column under FULL too (verified on PG 16 and PG 18).
func TestRefuseUnpublishedGeneratedIdentity_RemedyDoesNotRecommendFULL(t *testing.T) {
	err := refuseUnpublishedGeneratedIdentity(
		relEntry('d', "a", "b"),
		identityColumns{All: []string{"a", "g"}, Generated: []string{"g"}},
	)
	if err == nil {
		t.Fatal("expected a refusal")
	}
	coded, _ := sluicecode.FromError(err)
	if strings.Contains(coded.Hint, "REPLICA IDENTITY FULL") {
		t.Errorf("the remedy recommends REPLICA IDENTITY FULL, which does not fix this shape:\n%s", coded.Hint)
	}
	for _, want := range []string{"REPLICA IDENTITY USING INDEX", "--exclude-table", "sluice migrate"} {
		if !strings.Contains(coded.Hint, want) {
			t.Errorf("the remedy omits %q:\n%s", want, coded.Hint)
		}
	}
}
