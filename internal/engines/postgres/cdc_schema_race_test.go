// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDetectIncompatibleRelationChange pins the v0.93.0 CDC schema-race
// detector covering Bug 112 (RENAME silent drop), Bug 119 (DROP COLUMN
// silent drift), Bug 120 (DROP+CREATE silent drop), plus the orthogonal
// ALTER COLUMN TYPE / RENAME COLUMN shapes. Per ADR-0058, ADD COLUMN
// (new columns appended) remains compatible — the existing forwarding
// path handles that shape when --forward-schema-add-column is set.
func TestDetectIncompatibleRelationChange(t *testing.T) {
	base := func() *relationCacheEntry {
		return &relationCacheEntry{
			Schema: "public",
			Name:   "users",
			Columns: []relationColumn{
				{Name: "id", OID: 23},    // int4
				{Name: "email", OID: 25}, // text
			},
		}
	}

	cases := []struct {
		name string
		prev *relationCacheEntry
		curr *relationCacheEntry
		want string // substring expected in the race description; "" = compatible
	}{
		{
			name: "no prior entry (first-touch)",
			prev: nil,
			curr: base(),
			want: "",
		},
		{
			name: "identical re-send (pgoutput reconnect)",
			prev: base(),
			curr: base(),
			want: "",
		},
		{
			name: "ADD COLUMN appended (ADR-0058 compatible)",
			prev: base(),
			curr: &relationCacheEntry{
				Schema: "public", Name: "users",
				Columns: []relationColumn{
					{Name: "id", OID: 23},
					{Name: "email", OID: 25},
					{Name: "created_at", OID: 1184}, // timestamptz
				},
			},
			want: "",
		},
		{
			name: "Bug 112 RENAME (schema.name changed)",
			prev: base(),
			curr: &relationCacheEntry{
				Schema:  "public",
				Name:    "members",
				Columns: base().Columns,
			},
			want: "RENAME public.users → public.members",
		},
		{
			name: "Bug 112 schema-level rename",
			prev: base(),
			curr: &relationCacheEntry{
				Schema:  "archive",
				Name:    "users",
				Columns: base().Columns,
			},
			want: "RENAME public.users → archive.users",
		},
		{
			name: "Bug 119 DROP COLUMN (last column gone)",
			prev: base(),
			curr: &relationCacheEntry{
				Schema: "public", Name: "users",
				Columns: []relationColumn{
					{Name: "id", OID: 23},
				},
			},
			want: "DROP COLUMN",
		},
		{
			name: "Bug 119 DROP COLUMN (middle column gone — surfaces as RENAME COLUMN ordinal mismatch)",
			prev: &relationCacheEntry{
				Schema: "public", Name: "users",
				Columns: []relationColumn{
					{Name: "id", OID: 23},
					{Name: "middle", OID: 25},
					{Name: "email", OID: 25},
				},
			},
			curr: &relationCacheEntry{
				Schema: "public", Name: "users",
				Columns: []relationColumn{
					{Name: "id", OID: 23},
					{Name: "email", OID: 25},
				},
			},
			// Detected as DROP COLUMN (count went down), not the
			// ordinal mismatch we'd see if ordinal-1 was renamed.
			want: "DROP COLUMN",
		},
		{
			name: "ALTER COLUMN TYPE",
			prev: base(),
			curr: &relationCacheEntry{
				Schema: "public", Name: "users",
				Columns: []relationColumn{
					{Name: "id", OID: 23},
					{Name: "email", OID: 1043}, // varchar (was text=25)
				},
			},
			want: "ALTER COLUMN TYPE email",
		},
		{
			name: "RENAME COLUMN at same ordinal",
			prev: base(),
			curr: &relationCacheEntry{
				Schema: "public", Name: "users",
				Columns: []relationColumn{
					{Name: "id", OID: 23},
					{Name: "email_address", OID: 25}, // was "email"
				},
			},
			want: "RENAME COLUMN email → email_address",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := detectIncompatibleRelationChange(c.prev, c.curr)
			if c.want == "" {
				if got != "" {
					t.Errorf("got %q; want empty (compatible)", got)
				}
				return
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("got %q; want substring %q", got, c.want)
			}
		})
	}
}

// TestCheckSchemaRace_DROPCREATESameNameDifferentOID pins Bug 120's
// detection: pgoutput allocates a fresh OID for the recreated relation,
// so the previous entry (same Schema.Name, different OID) is still in
// the relations map. checkSchemaRace scans for that orphan and refuses
// loudly.
func TestCheckSchemaRace_DROPCREATESameNameDifferentOID(t *testing.T) {
	relations := map[uint32]*relationCacheEntry{
		16400: {Schema: "public", Name: "events", Columns: []relationColumn{{Name: "id", OID: 23}}},
	}
	current := &relationCacheEntry{
		Schema: "public", Name: "events",
		Columns: []relationColumn{{Name: "id", OID: 23}, {Name: "payload", OID: 3802}}, // jsonb
	}
	err := checkSchemaRace(relations, 16500, current, false)
	if err == nil {
		t.Fatal("expected schema-race refusal for DROP+CREATE same name different OID; got nil")
	}
	for _, want := range []string{
		"DROP+CREATE",
		"public.events",
		"old OID 16400",
		"new OID 16500",
		"sync stop --wait",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q; got: %v", want, err)
		}
	}
}

// TestCheckSchemaRace_SameOIDReentryIsBenign pins the pgoutput
// reconnect behaviour: the same RelationMessage may arrive multiple
// times for the same OID with no change. The race detector must NOT
// false-positive on those.
func TestCheckSchemaRace_SameOIDReentryIsBenign(t *testing.T) {
	relations := map[uint32]*relationCacheEntry{
		16400: {
			Schema: "public", Name: "users",
			Columns: []relationColumn{{Name: "id", OID: 23}, {Name: "email", OID: 25}},
		},
	}
	current := &relationCacheEntry{
		Schema: "public", Name: "users",
		Columns: []relationColumn{{Name: "id", OID: 23}, {Name: "email", OID: 25}},
	}
	if err := checkSchemaRace(relations, 16400, current, false); err != nil {
		t.Errorf("identical re-send of RelationMessage should be benign; got: %v", err)
	}
}

// TestCheckSchemaRace_ADDColumnIsCompatible pins ADR-0058 compatibility:
// ADD COLUMN at the end is the one shape the live-forwarding path
// supports, so the race detector must NOT refuse it. (Whether the
// forwarding actually fires depends on --forward-schema-add-column,
// which is checked downstream.)
func TestCheckSchemaRace_ADDColumnIsCompatible(t *testing.T) {
	relations := map[uint32]*relationCacheEntry{
		16400: {
			Schema: "public", Name: "users",
			Columns: []relationColumn{{Name: "id", OID: 23}},
		},
	}
	current := &relationCacheEntry{
		Schema: "public", Name: "users",
		Columns: []relationColumn{
			{Name: "id", OID: 23},
			{Name: "created_at", OID: 1184},
		},
	}
	if err := checkSchemaRace(relations, 16400, current, false); err != nil {
		t.Errorf("ADD COLUMN at end should be compatible with ADR-0058 forwarding; got: %v", err)
	}
}

// TestCheckSchemaRace_ForwardMode pins the ADR-0091 F7a GAP #1 policy:
// under schemaForward=true the unambiguous / intercept-routable shapes
// (DROP COLUMN, ALTER COLUMN TYPE, RENAME COLUMN) PASS the reader gate so
// they surface as SchemaSnapshots for the forward intercept, while RENAME
// TABLE and DROP+CREATE-same-name still REFUSE loudly. This is the mirror
// of the refuse-mode tests above; both modes are pinned so neither can
// regress silently.
func TestCheckSchemaRace_ForwardMode(t *testing.T) {
	base := func() *relationCacheEntry {
		return &relationCacheEntry{
			Schema: "public", Name: "users",
			Columns: []relationColumn{
				{Name: "id", OID: 23},    // int4
				{Name: "email", OID: 25}, // text
			},
		}
	}

	t.Run("DROP COLUMN passes under forward", func(t *testing.T) {
		relations := map[uint32]*relationCacheEntry{16400: base()}
		current := &relationCacheEntry{
			Schema: "public", Name: "users",
			Columns: []relationColumn{{Name: "id", OID: 23}},
		}
		if err := checkSchemaRace(relations, 16400, current, true); err != nil {
			t.Errorf("DROP COLUMN must pass under forward mode; got: %v", err)
		}
		// And still refuses under refuse mode (the Bug 119 behavior).
		if err := checkSchemaRace(relations, 16400, current, false); err == nil {
			t.Error("DROP COLUMN must refuse under refuse mode")
		}
	})

	t.Run("ALTER COLUMN TYPE passes under forward", func(t *testing.T) {
		relations := map[uint32]*relationCacheEntry{16400: base()}
		current := &relationCacheEntry{
			Schema: "public", Name: "users",
			Columns: []relationColumn{
				{Name: "id", OID: 23},
				{Name: "email", OID: 1043}, // varchar (was text=25)
			},
		}
		if err := checkSchemaRace(relations, 16400, current, true); err != nil {
			t.Errorf("ALTER COLUMN TYPE must pass under forward mode; got: %v", err)
		}
		if err := checkSchemaRace(relations, 16400, current, false); err == nil {
			t.Error("ALTER COLUMN TYPE must refuse under refuse mode")
		}
	})

	t.Run("RENAME COLUMN passes under forward (intercept refuses with the better message)", func(t *testing.T) {
		relations := map[uint32]*relationCacheEntry{16400: base()}
		current := &relationCacheEntry{
			Schema: "public", Name: "users",
			Columns: []relationColumn{
				{Name: "id", OID: 23},
				{Name: "email_address", OID: 25}, // was "email"
			},
		}
		if err := checkSchemaRace(relations, 16400, current, true); err != nil {
			t.Errorf("RENAME COLUMN must pass the reader gate under forward mode "+
				"(the intercept's ADR-0091 §3 refusal fires downstream); got: %v", err)
		}
		if err := checkSchemaRace(relations, 16400, current, false); err == nil {
			t.Error("RENAME COLUMN must refuse at the reader under refuse mode")
		}
	})

	t.Run("RENAME TABLE refuses even under forward", func(t *testing.T) {
		relations := map[uint32]*relationCacheEntry{16400: base()}
		current := &relationCacheEntry{
			Schema: "public", Name: "members", // table renamed
			Columns: base().Columns,
		}
		if err := checkSchemaRace(relations, 16400, current, true); err == nil {
			t.Error("RENAME TABLE must refuse even under forward mode (genuinely ambiguous)")
		}
	})

	t.Run("DROP+CREATE same name refuses even under forward", func(t *testing.T) {
		relations := map[uint32]*relationCacheEntry{
			16400: {Schema: "public", Name: "events", Columns: []relationColumn{{Name: "id", OID: 23}}},
		}
		current := &relationCacheEntry{
			Schema: "public", Name: "events",
			Columns: []relationColumn{{Name: "id", OID: 23}, {Name: "payload", OID: 3802}},
		}
		err := checkSchemaRace(relations, 16500, current, true)
		if err == nil {
			t.Fatal("DROP+CREATE same name different OID must refuse even under forward mode")
		}
		if !strings.Contains(err.Error(), "DROP+CREATE") {
			t.Errorf("expected DROP+CREATE refusal; got: %v", err)
		}
	})

	t.Run("ADD COLUMN passes under both modes", func(t *testing.T) {
		relations := map[uint32]*relationCacheEntry{16400: base()}
		current := &relationCacheEntry{
			Schema: "public", Name: "users",
			Columns: []relationColumn{
				{Name: "id", OID: 23},
				{Name: "email", OID: 25},
				{Name: "created_at", OID: 1184},
			},
		}
		if err := checkSchemaRace(relations, 16400, current, true); err != nil {
			t.Errorf("ADD COLUMN must pass under forward mode; got: %v", err)
		}
		if err := checkSchemaRace(relations, 16400, current, false); err != nil {
			t.Errorf("ADD COLUMN must pass under refuse mode too; got: %v", err)
		}
	})
}

// TestClassifyRelationChange_TypmodFamilies pins the G3 typmod half of
// the table-rewrite class (capture-completeness sweep 2026-08-26): a
// same-OID RelationMessage whose only delta is the type MODIFIER is an
// ALTER COLUMN TYPE — the shrink members rewrite every stored value
// (numeric(10,4)→(10,1) ROUNDS; observed on the wire with zero decoded
// messages for the rewrite) and the new typmod is the only artifact
// sluice ever sees. Per the Bug 74 discipline the pin covers EVERY
// typmod-carrying family the OID mapper decodes — numeric(p,s),
// varchar(n), timestamp(p) (temporal), bit(n) — × {shrink, widen,
// identical}, not one representative: each family has its own typmod
// ENCODING (numeric packs (p<<16|s)+4, varchar stores n+4, temporal and
// bit store the raw value), so a green pin on one encoding proves
// nothing about the others.
//
// Both directions are pinned: shrink AND widen classify (widen is
// catalog-only on the source — no rewrite — but forwarding it keeps the
// target's declared type converged, and refuse-mode operators asked for
// loud-on-any-DDL), while an IDENTICAL re-send (pgoutput reconnect /
// first-touch) must stay None — the no-false-fire floor.
func TestClassifyRelationChange_TypmodFamilies(t *testing.T) {
	// Per-family typmod encodings, spelled out rather than helper-derived
	// so a bug in the production decode helpers cannot leak into the pin.
	numericTM := func(p, s int32) int32 { return ((p << 16) | s) + 4 } // numeric(p,s)
	varcharTM := func(n int32) int32 { return n + 4 }                  // varchar(n)

	families := []struct {
		name              string
		oid               uint32
		from, to, widened int32
	}{
		{"numeric(p,s) scale shrink", 1700, numericTM(10, 4), numericTM(10, 1), numericTM(12, 6)},
		{"varchar(n) shrink", 1043, varcharTM(20), varcharTM(10), varcharTM(40)},
		{"timestamp(p) precision shrink", 1114, 6, 3, -1},
		{"bit(n) shrink", 1560, 8, 4, 16},
		// The two detected-but-not-forwarded members (VF review
		// 2026-08-26; ADR-0091 impl note): the raw compare fires — so
		// refuse mode is loud, which is what these cells pin — but the
		// projected IR drops these typmods (interval → empty
		// ir.Interval{}; array elements resolve with typmod -1), so
		// under forward mode no boundary is emitted and the rewrite is
		// NOT forwarded. Documented residual, not convergence.
		{"interval(p) precision shrink (refuse-only reach)", 1187, 6, 3, -1},
		{"numeric[] element scale shrink (refuse-only reach)", 1231, numericTM(10, 4), numericTM(10, 1), numericTM(12, 6)},
	}

	entry := func(oid uint32, tm int32) *relationCacheEntry {
		return &relationCacheEntry{
			Schema: "public", Name: "t",
			Columns: []relationColumn{
				{Name: "id", OID: 23},
				{Name: "v", OID: oid, TypeMod: tm},
			},
		}
	}

	for _, f := range families {
		f := f
		t.Run(f.name, func(t *testing.T) {
			shrink := classifyRelationChange(entry(f.oid, f.from), entry(f.oid, f.to))
			if shrink.Kind != relationChangeAlterColumnType {
				t.Errorf("shrink typmod %d → %d classified %v; want AlterColumnType — the value-rewriting shape would bypass every schema-change door",
					f.from, f.to, shrink.Kind)
			}
			if shrink.Kind == relationChangeAlterColumnType && !strings.Contains(shrink.Description, "typmod") {
				t.Errorf("shrink description %q does not name the typmod delta", shrink.Description)
			}
			widen := classifyRelationChange(entry(f.oid, f.from), entry(f.oid, f.widened))
			if widen.Kind != relationChangeAlterColumnType {
				t.Errorf("widen typmod %d → %d classified %v; want AlterColumnType", f.from, f.widened, widen.Kind)
			}
			resend := classifyRelationChange(entry(f.oid, f.from), entry(f.oid, f.from))
			if resend.Kind != relationChangeNone {
				t.Errorf("identical typmod re-send classified %v; want None (pgoutput reconnect must not false-fire)", resend.Kind)
			}
		})
	}
}

// TestCheckSchemaRace_TypmodOnlyChange pins the G3 shape at the policy
// layer, both modes: refuse mode refuses loudly (the door the typmod
// blindness bypassed), forward mode passes the reader gate so the
// boundary reaches the ADR-0091 forward intercept (which applies the
// same USING-less ALTER on the target — convergence pinned end-to-end by
// TestStreamer_SchemaForward_AlterType_TypmodOnly_PG).
func TestCheckSchemaRace_TypmodOnlyChange(t *testing.T) {
	prev := &relationCacheEntry{
		Schema: "public", Name: "n",
		Columns: []relationColumn{
			{Name: "id", OID: 23},
			{Name: "amt", OID: 1700, TypeMod: ((10 << 16) | 4) + 4}, // numeric(10,4)
		},
	}
	curr := &relationCacheEntry{
		Schema: "public", Name: "n",
		Columns: []relationColumn{
			{Name: "id", OID: 23},
			{Name: "amt", OID: 1700, TypeMod: ((10 << 16) | 1) + 4}, // numeric(10,1)
		},
	}
	relations := map[uint32]*relationCacheEntry{16400: prev}

	err := checkSchemaRace(relations, 16400, curr, false)
	if err == nil {
		t.Fatal("typmod-only ALTER must refuse under --schema-changes=refuse (it rewrote every stored value on the source)")
	}
	for _, want := range []string{"ALTER COLUMN TYPE amt", "typmod", "sync stop --wait"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q; got: %v", want, err)
		}
	}
	if err := checkSchemaRace(relations, 16400, curr, true); err != nil {
		t.Errorf("typmod-only ALTER must pass the reader gate under forward mode (the intercept forwards it); got: %v", err)
	}
}

// _ ensures the ir import stays meaningful in this file even if a
// future cleanup removes the only consumer.
var _ ir.Type = (ir.Integer{})
