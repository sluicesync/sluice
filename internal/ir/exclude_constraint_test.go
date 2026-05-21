// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// ADR-0053 — round-trip pins for ir.ExcludeConstraint. The new IR
// shape is a plain concrete struct (no sealed-interface envelope
// work), so standard json.Marshal / json.Unmarshal round-trips it for
// free — but a regression here would silently drop EXCLUDE
// constraints from the schema-history store (ADR-0049). The pin is
// load-bearing.

// TestExcludeConstraint_TableJSONRoundTrip pins MarshalTable /
// UnmarshalTable for a Table carrying EXCLUDE constraints in the
// four observed real-world shapes from the GitLab corpus.
func TestExcludeConstraint_TableJSONRoundTrip(t *testing.T) {
	in := &Table{
		Schema: "public",
		Name:   "schedule_slots",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64, AutoIncrement: true}},
		},
		ExcludeConstraints: []*ExcludeConstraint{
			{
				Name:       "simple_overlap",
				Definition: "EXCLUDE USING gist (builds_id_range WITH &&)",
			},
			{
				Name:       "predicated_overlap",
				Definition: "EXCLUDE USING gist (builds_id_range WITH &&) WHERE ((builds_id_range IS NOT NULL))",
			},
			{
				Name:       "multikey_overlap",
				Definition: "EXCLUDE USING gist (rotation_id WITH =, tstzrange(starts_at, ends_at, '[)'::text) WITH &&)",
			},
			{
				Name:       "deferrable_overlap",
				Definition: "EXCLUDE USING gist (iterations_cadence_id WITH =, daterange(start_date, due_date, '[]'::text) WITH &&) WHERE ((group_id IS NOT NULL)) DEFERRABLE INITIALLY DEFERRED",
			},
		},
	}

	b, err := MarshalTable(in)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	out, err := UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	if !reflect.DeepEqual(in.ExcludeConstraints, out.ExcludeConstraints) {
		t.Errorf("EXCLUDE round-trip diverged:\ngot:  %#v\nwant: %#v",
			out.ExcludeConstraints, in.ExcludeConstraints)
	}
}

// TestExcludeConstraint_OmittedWhenEmpty pins that a Table with no
// EXCLUDE constraints emits an empty or absent slice (not the
// unmarshaller's implicit zero value masquerading as a populated
// shape). Belt-and-braces against silent shape drift in the JSON
// codec.
func TestExcludeConstraint_OmittedWhenEmpty(t *testing.T) {
	in := &Table{Schema: "public", Name: "no_excludes"}
	b, err := MarshalTable(in)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	// JSON shape: either "ExcludeConstraints": null OR field absent
	// (struct's zero-value slice). Both deserialise to a nil/empty
	// slice — both acceptable. Reject only a populated array shape.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("re-decode: %v", err)
	}
	if ex, ok := raw["ExcludeConstraints"]; ok {
		s := strings.TrimSpace(string(ex))
		if s != "null" && s != "[]" {
			t.Errorf("ExcludeConstraints encoded as %q; want null/[] for empty input", s)
		}
	}
}
