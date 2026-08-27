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
		// Production-shaped entries carry RESOLVED IR types (the
		// TYPMOD-PROJECTION-GATE compares projections, and two nil types
		// compare equal — the safe-but-wrong-shape fixture would refuse).
		prev := base()
		prev.Columns = []relationColumn{
			typedCol(t, "id", 23, -1),
			typedCol(t, "email", 25, -1), // text
		}
		relations := map[uint32]*relationCacheEntry{16400: prev}
		current := &relationCacheEntry{
			Schema: "public", Name: "users",
			Columns: []relationColumn{
				typedCol(t, "id", 23, -1),
				typedCol(t, "email", 1043, 255+4), // varchar(255) (was text=25)
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
		// The two projection-invisible members (VF review 2026-08-26;
		// ADR-0091 impl note): the raw compare fires — so refuse mode is
		// loud, which is what these cells pin — but the projected IR
		// drops these typmods (interval → empty ir.Interval{}; array
		// elements resolve with typmod -1), so the forward intercept
		// could never see them. Since audit 2026-08-27 A2 they refuse
		// loudly under forward mode too (the TYPMOD-PROJECTION-GATE in
		// checkSchemaRace; policy pinned by
		// TestCheckSchemaRace_UnforwardableTypmod, enumeration by
		// TestTypmodProjectionGate_EveryTypmodFamily).
		{"interval(p) precision shrink (refuses under both modes)", 1186, 6, 3, -1},
		{"numeric[] element scale shrink (refuses under both modes)", 1231, numericTM(10, 4), numericTM(10, 1), numericTM(12, 6)},
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

// typedCol builds a production-shaped relationColumn: the IR type is
// resolved through the REAL wire mapper (oidToType), exactly as
// buildRelationCacheEntry does, so fixtures cannot drift from what the
// projection-comparing paths (TYPMOD-PROJECTION-GATE, maybeSnapshotSchema)
// would see on a live stream.
func typedCol(t *testing.T, name string, oid uint32, typmod int32) relationColumn {
	t.Helper()
	typ, err := oidToType(oid, typmod)
	if err != nil {
		t.Fatalf("oidToType(%d, %d): %v", oid, typmod, err)
	}
	return relationColumn{Name: name, OID: oid, TypeMod: typmod, Type: typ}
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
			typedCol(t, "id", 23, -1),
			typedCol(t, "amt", 1700, ((10<<16)|4)+4), // numeric(10,4)
		},
	}
	curr := &relationCacheEntry{
		Schema: "public", Name: "n",
		Columns: []relationColumn{
			typedCol(t, "id", 23, -1),
			typedCol(t, "amt", 1700, ((10<<16)|1)+4), // numeric(10,1)
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

// TestCheckSchemaRace_UnforwardableTypmod pins the TYPMOD-PROJECTION-GATE
// policy (audit 2026-08-27 A2): an ALTER COLUMN TYPE the classifier caught
// whose PROJECTED IR type is unchanged refuses loudly under BOTH modes —
// under forward, maybeSnapshotSchema could never emit a boundary for it
// (the projected signature is unmoved), so "pass to the intercept" would
// mean no ALTER, no WARN, and silent divergence of pre-existing target
// rows while the source's rewrite rounds every stored value.
//
// The no-false-fire floor rides two facts, both pinned: an identical
// pgoutput re-send classifies None (TestClassifyRelationChange_TypmodFamilies'
// resend cells) so the gate is unreachable for it, and a delta that DOES
// move the projection (numeric/varchar/temporal-precision members —
// including the value-identical bare→(6) collapse-class ALTER, which moves
// the RAW projection) keeps forwarding exactly as before.
func TestCheckSchemaRace_UnforwardableTypmod(t *testing.T) {
	numericTM := func(p, s int32) int32 { return ((p << 16) | s) + 4 }
	intervalTM := func(p int32) int32 { return (0x7FFF << 16) | p } // full-range interval(p)

	table := func(cols ...relationColumn) *relationCacheEntry {
		return &relationCacheEntry{Schema: "public", Name: "t", Columns: cols}
	}
	requireUnforwardableRefusal := func(t *testing.T, err error, col string) {
		t.Helper()
		if err == nil {
			t.Fatal("projection-invisible ALTER passed under forward mode; want the TYPMOD-PROJECTION-GATE refusal (a pass here is the A2 silent-divergence shape)")
		}
		for _, want := range []string{"cannot be forwarded", "%q-col%", "sync stop --wait", "silently diverge"} {
			if want == "%q-col%" {
				want = `column "` + col + `"`
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q; got: %v", want, err)
			}
		}
		// The rewrite shapes must never carry the lossless text — claiming
		// no rewrite happened when one did is the un-honest direction.
		if strings.Contains(err.Error(), "no table rewrite occurred") {
			t.Errorf("rewrite-shape refusal carries the lossless no-rewrite text; got: %v", err)
		}
	}
	// requireLosslessRefusal: the A2-VARCHAR-TEXT-MSG honest variant — the
	// two catalog-only no-rewrite shapes still REFUSE (behavior identical to
	// the rewrite shapes), but the text says no rewrite occurred instead of
	// asserting a divergence that never happened.
	requireLosslessRefusal := func(t *testing.T, err error, col string) {
		t.Helper()
		if err == nil {
			t.Fatal("lossless projection-invisible ALTER passed under forward mode; want the TYPMOD-PROJECTION-GATE refusal (the message changed, the verdict must not)")
		}
		for _, want := range []string{"cannot be forwarded", `column "` + col + `"`, "no table rewrite occurred", "drained model", "sync stop --wait"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("lossless refusal missing %q; got: %v", want, err)
			}
		}
		if strings.Contains(err.Error(), "silently diverge") {
			t.Errorf("lossless refusal carries the divergence text, which is factually wrong for this shape; got: %v", err)
		}
	}

	t.Run("interval(p) precision shrink refuses under forward", func(t *testing.T) {
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(6)))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(3)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
		if err := checkSchemaRace(relations, 16400, curr, false); err == nil {
			t.Error("interval typmod shrink must refuse under refuse mode too")
		}
	})

	t.Run("numeric[] element scale shrink refuses under forward", func(t *testing.T) {
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "amts", 1231, numericTM(10, 4)))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "amts", 1231, numericTM(10, 1)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "amts")
	})

	t.Run("mixed statement: a moved column does not rescue an unmoved one", func(t *testing.T) {
		// One ALTER TABLE, two sub-ALTERs, one RelationMessage: the numeric
		// scale change moves the TABLE signature (a boundary would be
		// emitted), but the interval column's rewrite would still never be
		// forwarded — the per-column gate must refuse, which is why the
		// predicate is per-column rather than the whole-table signature
		// equality the audit sketched.
		prev := table(typedCol(t, "amt", 1700, numericTM(10, 4)), typedCol(t, "iv", 1186, intervalTM(6)))
		curr := table(typedCol(t, "amt", 1700, numericTM(10, 1)), typedCol(t, "iv", 1186, intervalTM(3)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("tail DROP + interval shrink in one delta refuses under forward (VF 2026-08-27)", func(t *testing.T) {
		// The classifier early-returns DropColumn on the shorter list
		// BEFORE its typmod loop, so this combined delta never classifies
		// AlterColumnType — the gate must therefore also run for the
		// DropColumn shape, or the surviving column's rewrite forwards as
		// nothing and diverges silently (the VF review's sibling gap).
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(6)), typedCol(t, "x", 25, -1))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(3)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("middle DROP + interval shrink refuses under forward (ordinal shift)", func(t *testing.T) {
		// A middle-column drop shifts every later ordinal, which is why
		// the predicate is NAME-keyed: an ordinal scan would pair "x"
		// against "iv", hit the name-mismatch skip, and find nothing.
		prev := table(typedCol(t, "x", 25, -1), typedCol(t, "iv", 1186, intervalTM(6)))
		curr := table(typedCol(t, "iv", 1186, intervalTM(3)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("middle DROP + numeric[] element shrink refuses under forward", func(t *testing.T) {
		prev := table(typedCol(t, "x", 25, -1), typedCol(t, "amts", 1231, numericTM(10, 4)))
		curr := table(typedCol(t, "amts", 1231, numericTM(10, 1)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "amts")
	})

	t.Run("plain DROP COLUMN with no surviving-column delta still forwards", func(t *testing.T) {
		// The no-false-fire floor for the DropColumn arm: a drop whose
		// surviving columns are field-identical must keep forwarding
		// (checkSchemaRace nil under forward mode).
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(6)), typedCol(t, "x", 25, -1))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(6)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		if err := checkSchemaRace(relations, 16400, curr, true); err != nil {
			t.Errorf("plain tail DROP with unchanged survivors must forward; got: %v", err)
		}
	})

	t.Run("projection-identical OID swap (time→timetz) refuses under forward", func(t *testing.T) {
		// Not a typmod delta but the same class: both OIDs project to
		// ir.Time{...}, so the signature cannot move and the forward
		// intercept could never see the change (the TIMETZ-PROJECTION
		// filing's forward half).
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "tm", 1083, 3))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "tm", 1266, 3))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "tm")
	})

	t.Run("unbounded varchar→text OID swap refuses with the honest no-rewrite message", func(t *testing.T) {
		// Both sides project ir.Text{TextLong} and PG performs no rewrite
		// (binary-coercible), so the divergence text would be factually
		// wrong — the A2-VARCHAR-TEXT-MSG decision. Still REFUSES under
		// both modes; only the message differs.
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "v", 1043, -1)) // unbounded varchar
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "v", 25, -1))   // text
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireLosslessRefusal(t, checkSchemaRace(relations, 16400, curr, true), "v")
		if err := checkSchemaRace(relations, 16400, curr, false); err == nil {
			t.Error("unbounded varchar→text must refuse under refuse mode too")
		}
	})

	t.Run("text→unbounded varchar OID swap refuses with the honest no-rewrite message", func(t *testing.T) {
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "v", 25, -1))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "v", 1043, -1))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireLosslessRefusal(t, checkSchemaRace(relations, 16400, curr, true), "v")
	})

	t.Run("interval same-range precision WIDENING refuses with the honest no-rewrite message", func(t *testing.T) {
		// interval(3) → interval(6), full range on both sides: every
		// stored value already fits, PG rewrites nothing.
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(3)))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(6)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireLosslessRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("interval(3)→bare interval widens to full precision and gets the honest message", func(t *testing.T) {
		// typmod -1 is the unrestricted declaration (full range, full
		// precision), so this is a widening — the -1 normalization cell.
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(3)))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, -1))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireLosslessRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("interval RANGE-bits change keeps the divergence message even with precision widened", func(t *testing.T) {
		// The lossless predicate is directional AND range-pinned: a field
		// restriction change (e.g. DAY TO SECOND → full range) is not one
		// of the two decided no-rewrite shapes, so it stays on the safe
		// over-claiming divergence text.
		dayToSecond := int32((0x0010|0x0008|0x0004|0x0002)<<16) | 3 // some non-full range bits, precision 3
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, dayToSecond))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "iv", 1186, intervalTM(6)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("mixed lossless + rewrite delta keeps the divergence message", func(t *testing.T) {
		// One RelationMessage carrying a lossless varchar⇄text swap AND an
		// interval precision shrink: the rewrite column wins the message —
		// softening the warning because a sibling column was lossless
		// would hide the real divergence.
		prev := table(typedCol(t, "v", 1043, -1), typedCol(t, "iv", 1186, intervalTM(6)))
		curr := table(typedCol(t, "v", 25, -1), typedCol(t, "iv", 1186, intervalTM(3)))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		requireUnforwardableRefusal(t, checkSchemaRace(relations, 16400, curr, true), "iv")
	})

	t.Run("temporal collapse-class ALTER (bare→(6)) still forwards", func(t *testing.T) {
		// bare and (6) are value-identical on PG, and the RAW projection
		// moves (PrecisionUnspecified flips), so a boundary IS emitted and
		// the gate must not fire — the intended treatment for the
		// collapse-class members: their forwarding posture stays the
		// documented normalizer false-negative downstream, never a reader
		// refusal.
		prev := table(typedCol(t, "id", 23, -1), typedCol(t, "ts", 1114, -1))
		curr := table(typedCol(t, "id", 23, -1), typedCol(t, "ts", 1114, 6))
		relations := map[uint32]*relationCacheEntry{16400: prev}
		if err := checkSchemaRace(relations, 16400, curr, true); err != nil {
			t.Errorf("collapse-class bare→(6) must pass under forward (raw projection moves, boundary emits); got: %v", err)
		}
	})
}

// _ ensures the ir import stays meaningful in this file even if a
// future cleanup removes the only consumer.
var _ ir.Type = (ir.Integer{})
