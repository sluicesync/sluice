// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// The warm-resume zone witness (SLM-1b) at unit level: the family matrix
// of the target read-back, the three fallback rules, and the loader's use
// of the target's own SchemaReader. The end-to-end recovery-loop and
// residual pins run on real containers in
// streamer_session_tz_warm_resume_witness_integration_test.go.

// seedColumnType finds a column's type in a seed, or nil when the table
// or column is absent — "absent" is the seed's way of saying no prior.
func seedColumnType(seed []*ir.Table, table, column string) ir.Type {
	for _, t := range seed {
		if t == nil || t.Name != table {
			continue
		}
		for _, c := range t.Columns {
			if c != nil && c.Name == column {
				return c.Type
			}
		}
	}
	return nil
}

func seedHasTable(seed []*ir.Table, table string) bool {
	for _, t := range seed {
		if t != nil && t.Name == table {
			return true
		}
	}
	return false
}

// TestWarmResumeSeed_ZoneWitnessFamilyMatrix is the Bug-74-shaped pin on
// the witness read: every read-back a sync target can produce for a
// source TIMESTAMP / DATETIME column (Postgres timestamptz / timestamp,
// MySQL TIMESTAMP / DATETIME — identical IR by construction — and the two
// non-members: a SQLite-style DATETIME that collapses the pair to
// ir.Timestamp{WithTimeZone:false}, and an overridden TEXT) × every prior
// the history can hold (each member, or none) × {witness, override}.
//
// The expected value per cell is derived from the rules, not from the
// code: a member read-back with no override is the witness; an override
// on the column falls back to the history; a non-member read-back falls
// back to the history where one exists and yields no prior otherwise. And
// a witnessed table carries ONLY its zone-family columns — the target's
// TEXT for a source JSON never enters the seed (the phantom-delta guard).
func TestWarmResumeSeed_ZoneWitnessFamilyMatrix(t *testing.T) {
	ctx := context.Background()
	readBacks := []struct {
		name   string
		typ    ir.Type
		member bool
	}{
		{"pg-timestamptz", ir.Timestamp{Precision: 6, WithTimeZone: true}, true},
		{"pg-timestamp", ir.DateTime{Precision: 6}, true},
		{"mysql-timestamp", ir.Timestamp{WithTimeZone: true}, true},
		{"mysql-datetime", ir.DateTime{}, true},
		{"sqlite-datetime", ir.Timestamp{}, false},
		{"overridden-text", ir.Text{}, false},
	}
	histories := []struct {
		name string
		typ  ir.Type
	}{
		{"history-timestamp", ir.Timestamp{WithTimeZone: true}},
		{"history-datetime", ir.DateTime{}},
		{"no-history", nil},
	}
	members, nonMembers := 0, 0
	for _, rb := range readBacks {
		for _, h := range histories {
			for _, override := range []bool{false, true} {
				name := rb.name + "/" + h.name
				if override {
					name += "/override"
				}
				t.Run(name, func(t *testing.T) {
					target := &ir.Table{Schema: "public", Name: "events", Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
						{Name: "c", Type: rb.typ},
						// A source JSON the target holds as TEXT: must never
						// enter the witness.
						{Name: "attrs", Type: ir.Text{}},
					}}
					var history []*ir.Table
					var hist *ir.Table
					if h.typ != nil {
						hist = &ir.Table{Schema: "src", Name: "events", Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64}},
							{Name: "c", Type: h.typ},
							{Name: "attrs", Type: ir.JSON{}},
						}}
						history = []*ir.Table{hist}
					}
					var mappings []config.Mapping
					if override {
						mappings = []config.Mapping{{Table: "events", Column: "c", TargetType: "timestamptz"}}
					}
					seed, err := mergeWarmResumeSeed(ctx, "s", map[string]*ir.Table{"events": target}, history, mappings)
					if err != nil {
						t.Fatal(err)
					}

					var want ir.Type
					witnessed := false
					switch {
					case override:
						want = h.typ
					case rb.member:
						want = rb.typ
						witnessed = true
					default:
						want = h.typ
					}
					got := seedColumnType(seed, "events", "c")
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("events.c prior = %#v; want %#v", got, want)
					}
					if witnessed {
						members++
						if seedColumnType(seed, "events", "attrs") != nil || seedColumnType(seed, "events", "id") != nil {
							t.Fatal("witness carried a non-zone-family column into the seed; a target TEXT for a source JSON would read as a phantom delta")
						}
						if seedColumnType(seed, "events", "c") == nil {
							t.Fatal("witnessed table lost its zone-family column")
						}
						return
					}
					nonMembers++
					// Every fallback cell hands the reader the HISTORY table
					// itself, or nothing.
					if hist == nil {
						if seedHasTable(seed, "events") && seedColumnType(seed, "events", "c") != nil {
							t.Fatal("no history and no witness, yet the seed carries a prior for events.c")
						}
						return
					}
					if len(seed) != 1 || seed[0] != hist {
						t.Fatalf("fallback cell seeded %v; want exactly the retained history table", seed)
					}
				})
			}
		}
	}
	if members < 8 || nonMembers < 8 {
		t.Fatalf("anti-vacuity: %d witnessed cells, %d fallback cells; floor 8 each", members, nonMembers)
	}
}

// TestWarmResumeSeed_TargetLacksTheTable pins the first fallback rule
// and the merge's table universe: a table the target does not hold
// resumes on its history version; a table only the target holds is
// witnessed; the seed is ordered by name.
func TestWarmResumeSeed_TargetLacksTheTable(t *testing.T) {
	ctx := context.Background()
	witness := map[string]*ir.Table{
		"held": {Schema: "public", Name: "held", Columns: []*ir.Column{{Name: "c", Type: ir.DateTime{}}}},
	}
	gone := &ir.Table{Schema: "src", Name: "gone", Columns: []*ir.Column{{Name: "c", Type: ir.Timestamp{WithTimeZone: true}}}}
	seed, err := mergeWarmResumeSeed(ctx, "s", witness, []*ir.Table{gone}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(seed) != 2 || seed[0].Name != "gone" || seed[1].Name != "held" {
		t.Fatalf("seed = %v; want [gone held]", seed)
	}
	if seed[0] != gone {
		t.Error("a table the target lacks must resume on the retained history table itself")
	}
	if seed[1].Schema != "" {
		t.Errorf("witness table carries Schema %q; want empty so the reader keys it under its own database", seed[1].Schema)
	}
	if _, ok := seedColumnType(seed, "held", "c").(ir.DateTime); !ok {
		t.Errorf("held.c = %#v; want the target's DateTime read-back", seedColumnType(seed, "held", "c"))
	}
}

// TestWarmResumeSeed_OverrideTouchesTemporal_ThreeArms pins each arm of
// the override rule independently, and the negative: an override on a
// non-temporal column leaves the witness in force.
func TestWarmResumeSeed_OverrideTouchesTemporal_ThreeArms(t *testing.T) {
	ctx := context.Background()
	hist := &ir.Table{Schema: "src", Name: "events", Columns: []*ir.Column{
		{Name: "c", Type: ir.DateTime{}},
		{Name: "note", Type: ir.Varchar{Length: 32}},
		{Name: "attrs", Type: ir.JSON{}},
	}}
	cases := []struct {
		name     string
		target   *ir.Table
		mapping  config.Mapping
		fallback bool
	}{
		{
			// Arm (a): the override's own target type is temporal — a source
			// VARCHAR forced to timestamptz; the target's family is the
			// override's, not the source's.
			name:     "override-target-type-temporal",
			target:   &ir.Table{Name: "events", Columns: []*ir.Column{{Name: "c", Type: ir.DateTime{}}, {Name: "note", Type: ir.Timestamp{WithTimeZone: true}}}},
			mapping:  config.Mapping{Table: "events", Column: "note", TargetType: "timestamptz"},
			fallback: true,
		},
		{
			// Arm (b): the target reads the overridden column back as
			// temporal — reached without history through the read-back.
			name:     "target-reads-back-temporal",
			target:   &ir.Table{Name: "events", Columns: []*ir.Column{{Name: "c", Type: ir.DateTime{}}, {Name: "note", Type: ir.Date{}}}},
			mapping:  config.Mapping{Table: "events", Column: "note", TargetType: "text"},
			fallback: true,
		},
		{
			// Arm (c): the history types the overridden column as temporal
			// while the target reads it as TEXT (a source TIMESTAMP forced
			// to text).
			name:     "history-types-it-temporal",
			target:   &ir.Table{Name: "events", Columns: []*ir.Column{{Name: "c", Type: ir.Text{}}, {Name: "note", Type: ir.Text{}}}},
			mapping:  config.Mapping{Table: "events", Column: "c", TargetType: "text"},
			fallback: true,
		},
		{
			// Negative: an override on a non-temporal column (attrs → jsonb)
			// does not touch the zone family; the witness stays in force.
			name:     "non-temporal-override-keeps-the-witness",
			target:   &ir.Table{Name: "events", Columns: []*ir.Column{{Name: "c", Type: ir.Timestamp{WithTimeZone: true}}, {Name: "attrs", Type: ir.JSON{}}}},
			mapping:  config.Mapping{Table: "events", Column: "attrs", TargetType: "jsonb"},
			fallback: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed, err := mergeWarmResumeSeed(ctx, "s", map[string]*ir.Table{"events": tc.target}, []*ir.Table{hist}, []config.Mapping{tc.mapping})
			if err != nil {
				t.Fatal(err)
			}
			if len(seed) != 1 {
				t.Fatalf("seeded %d tables; want 1", len(seed))
			}
			if tc.fallback && seed[0] != hist {
				t.Fatal("override touches a temporal column but the target witness was used")
			}
			if !tc.fallback && seed[0] == hist {
				t.Fatal("override on a non-temporal column fell back to history; the witness should stand")
			}
		})
	}

	t.Run("an unresolvable override is loud", func(t *testing.T) {
		_, err := mergeWarmResumeSeed(ctx, "s", map[string]*ir.Table{}, nil, []config.Mapping{{Table: "events", Column: "c", TargetType: "no-such-type"}})
		if err == nil {
			t.Fatal("unknown override type degraded to a seed; the cold-start path refuses it")
		}
	})
}

// TestLoadWarmResumeSchemaSeed_ReadsTheTargetThroughItsSchemaReader pins
// the loader end to end against a fake target engine: the witness wins
// over a stale history version, a table only the target holds is
// witnessed, a table only the history holds falls back, and a target
// read error is loud rather than a silent fallback.
func TestLoadWarmResumeSchemaSeed_ReadsTheTargetThroughItsSchemaReader(t *testing.T) {
	ctx := context.Background()
	persisted := ir.Position{Engine: "mysql", Token: "50"}
	target := &recordingEngine{name: "fake-target", schema: &ir.Schema{Tables: []*ir.Table{
		{Schema: "public", Name: "events", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "c", Type: ir.Timestamp{WithTimeZone: true}}}},
		{Schema: "public", Name: "only_target", Columns: []*ir.Column{{Name: "c", Type: ir.DateTime{}}}},
	}}}
	// The history says DATETIME for events.c — the operator's drained-model
	// ALTER on the target has since made it timestamptz, which is the
	// recovery-loop shape: the witness must win.
	applier := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{
		historyRow("src", "events", "10", zoneSwapPost()),
		historyRow("src", "only_history", "10", &ir.Table{Name: "only_history", Columns: []*ir.Column{{Name: "c", Type: ir.DateTime{}}}}),
	}}
	s := &Streamer{Source: orderedStubEngine{}, Target: target, TargetDSN: "fake://target"}

	seed, err := s.loadWarmResumeSchemaSeed(ctx, applier, "s", persisted)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := seedColumnType(seed, "events", "c").(ir.Timestamp); !ok || !got.WithTimeZone {
		t.Errorf("events.c = %#v; want the target's timestamptz witness over the stale DATETIME history", seedColumnType(seed, "events", "c"))
	}
	if _, ok := seedColumnType(seed, "only_target", "c").(ir.DateTime); !ok {
		t.Errorf("only_target.c = %#v; want the target's witness for a table with no history row (the SLM-1b residual closed)", seedColumnType(seed, "only_target", "c"))
	}
	if _, ok := seedColumnType(seed, "only_history", "c").(ir.DateTime); !ok {
		t.Errorf("only_history.c = %#v; want the history fallback for a table the target lacks", seedColumnType(seed, "only_history", "c"))
	}

	t.Run("a target read error is loud", func(t *testing.T) {
		broken := &recordingEngine{name: "fake-target", readSchemaErr: errors.New("catalog unreachable")}
		s := &Streamer{Source: orderedStubEngine{}, Target: broken, TargetDSN: "fake://target"}
		if _, err := s.loadWarmResumeSchemaSeed(ctx, applier, "s", persisted); err == nil {
			t.Fatal("target read error degraded to the history seed; the armed refusal would silently narrow its prior")
		}
	})
}

// TestZoneFamilyMember_IsExactlyTheReaderProjection pins the membership
// predicate to the two IR types the MySQL source reader produces for the
// pair, and specifically EXCLUDES ir.Timestamp{WithTimeZone:false}: the
// pipeline's own zoneFamily classifies that as zone-naive, so admitting it
// would let a SQLite-style read-back witness a source TIMESTAMP as its
// own sibling.
func TestZoneFamilyMember_IsExactlyTheReaderProjection(t *testing.T) {
	cases := []struct {
		typ  ir.Type
		want bool
	}{
		{ir.Timestamp{WithTimeZone: true}, true},
		{ir.Timestamp{Precision: 3, WithTimeZone: true, PrecisionUnspecified: false}, true},
		{ir.DateTime{}, true},
		{ir.DateTime{Precision: 6}, true},
		{ir.Timestamp{}, false},
		{ir.Timestamp{Precision: 6, PrecisionUnspecified: true}, false},
		{ir.Time{WithTimeZone: true}, false},
		{ir.Date{}, false},
		{ir.Text{}, false},
		{ir.Array{Element: ir.Timestamp{WithTimeZone: true}}, false},
	}
	for _, tc := range cases {
		if got := zoneFamilyMember(tc.typ); got != tc.want {
			t.Errorf("zoneFamilyMember(%#v) = %v; want %v", tc.typ, got, tc.want)
		}
	}
	// And every member is something the pipeline predicate also classes
	// as a zone-family member, so the two agree on the members they share.
	for _, tc := range cases {
		if !tc.want {
			continue
		}
		if _, _, ok := zoneFamily(tc.typ); !ok {
			t.Errorf("zoneFamilyMember admits %#v but zoneFamily does not classify it", tc.typ)
		}
	}
}
