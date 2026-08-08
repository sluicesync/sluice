// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestUnwrapDomain covers the four shapes every caller depends on. The
// last two are the ones a per-site hand-rolled unwrap kept getting
// differently: before this function existed, one site returned the
// value unchanged for a nil BaseType, another returned nil, and none of
// them looped.
func TestUnwrapDomain(t *testing.T) {
	nested := Domain{Name: "outer", BaseType: Domain{Name: "inner", BaseType: Array{Element: JSON{}}}}

	for _, tc := range []struct {
		name string
		in   Type
		want string
	}{
		{"identity for a non-domain", Array{Element: JSON{}}, Array{Element: JSON{}}.String()},
		{"one wrapper", Domain{Name: "d", BaseType: Text{}}, Text{}.String()},
		{"nested wrappers resolve in one call", nested, Array{Element: JSON{}}.String()},
		{
			// Malformed. The wrapper comes back so the sites that refuse
			// a nil BaseType BY NAME still reach their refusal, instead of
			// dispatching on a nil Type.
			"nil BaseType returns the wrapper",
			Domain{Name: "broken"},
			Domain{Name: "broken"}.String(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := UnwrapDomain(tc.in)
			if got == nil {
				t.Fatalf("UnwrapDomain(%s) = nil; it must never hand a caller a nil Type", tc.in.String())
			}
			if got.String() != tc.want {
				t.Errorf("UnwrapDomain(%s) = %s; want %s", tc.in.String(), got.String(), tc.want)
			}
		})
	}

	// A nil Type in, a nil Type out — the SourceColumnType spelling is
	// nil on every column no override or retarget touched, and callers
	// pass it straight in.
	if got := UnwrapDomain(nil); got != nil {
		t.Errorf("UnwrapDomain(nil) = %#v; want nil so a nil SourceColumnType stays nil", got)
	}
}

// TestDomainOf holds the complement: it reads the DECLARED type only,
// and it does not fall back to SourceColumnType. Both halves matter —
// the first is what makes it the identity reading, and the second is a
// deliberate scope the doc states and nothing else enforces.
func TestDomainOf(t *testing.T) {
	dom := Domain{Name: "email", BaseType: Text{}, Checks: []DomainCheck{{Body: "VALUE <> ''"}}}

	if got, ok := DomainOf(&Column{Name: "c", Type: dom}); !ok || got.Name != "email" || len(got.Checks) != 1 {
		t.Errorf("DomainOf(domain column) = %#v, %v; want the wrapper with its CHECK", got, ok)
	}
	if _, ok := DomainOf(&Column{Name: "c", Type: Text{}}); ok {
		t.Error("DomainOf reported a wrapper on a plain text column")
	}
	if _, ok := DomainOf(&Column{Name: "c", Type: Text{}, SourceColumnType: dom}); ok {
		t.Error("DomainOf read the PARKED type: a column whose Type is no longer a domain no longer " +
			"declares one, and re-emitting its CHECKs from provenance is a different decision than the " +
			"two consumers of this function make")
	}
	if _, ok := DomainOf(nil); ok {
		t.Error("DomainOf(nil) reported a wrapper")
	}
}
