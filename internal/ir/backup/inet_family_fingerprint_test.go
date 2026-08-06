// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ir.Inet.Family / ir.Cidr.Family fingerprint-exclusion gate
// (roadmap item 133, guarding against a repeat of item 104).
//
// The families exist so a `--where` on a MariaDB INET6 column knows the column
// widens `10.0.0.1` to `::ffff:10.0.0.1`. They RIDE THE WIRE so a manifest
// records which network type the column was — and the type envelope is reached
// by ComputeSchemaHash through ir.Column.MarshalJSON, so without an exclusion
// they land in the fingerprint of every chain carrying an inet or cidr column.
//
// `omitempty` does NOT save these, for exactly the reason it did not save
// ir.Macaddr.Width or ir.Enum.TypeName: it drops only the ZERO value, and the
// Postgres reader sets InetFamilyAny on EVERY inet and cidr column it reads —
// deliberately, because a declared "any" is what makes an UNSPECIFIED family
// mean "a new producer appeared without deciding". So the key would be present
// on every real Postgres chain with a network column, the fingerprint would
// move, and a new epoch would be minted that makes chains an operator already
// holds unrestorable.
//
// Written as an EQUALITY between a schema WITH the field and the same schema
// WITHOUT it, plus a byte-level absence check — never as a frozen golden built
// from post-change values, which is what made item 104's own golden
// self-referential.

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// schemaWithInetFamily builds the fixture at a given family, in the shapes a
// network column actually takes: the scalar inet, the scalar cidr, the ARRAY
// (`inet[]` is a real Postgres column, and an exclusion reaching only the
// scalar would still have moved the fingerprint of every chain carrying one),
// and a DOMAIN over the scalar — the other type that nests another.
func schemaWithInetFamily(f ir.InetFamily) *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Schema: "app",
			Name:   "hosts",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Default: ir.DefaultNone{}},
				{Name: "ip", Type: ir.Inet{Family: f}, Default: ir.DefaultNone{}},
				{Name: "net", Type: ir.Cidr{Family: f}, Default: ir.DefaultNone{}},
				{Name: "ips", Type: ir.Array{Element: ir.Inet{Family: f}}, Default: ir.DefaultNone{}},
				{Name: "nets", Type: ir.Array{Element: ir.Cidr{Family: f}}, Default: ir.DefaultNone{}},
				{
					Name:    "ip_dom",
					Type:    ir.Domain{Name: "hostaddr", BaseType: ir.Inet{Family: f}},
					Default: ir.DefaultNone{},
				},
			},
		}},
	}
}

// TestSchemaHash_ExcludesInetFamily is the no-new-epoch gate.
func TestSchemaHash_ExcludesInetFamily(t *testing.T) {
	without, err := ComputeSchemaHash(schemaWithInetFamily(ir.InetFamilyUnspecified))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(unspecified): %v", err)
	}
	// EVERY declared family, not just the interesting one. InetFamilyAny is
	// the value the Postgres reader sets for the ordinary inet/cidr column
	// that every existing chain already carries, so it is the family whose
	// inclusion would break the field; the two MariaDB families are the new
	// shapes. A gate that pinned only one of them would certify part of the
	// exclusion and imply the rest.
	for _, f := range []ir.InetFamily{ir.InetFamilyAny, ir.InetFamilyIPv4, ir.InetFamilyIPv6} {
		got, err := ComputeSchemaHash(schemaWithInetFamily(f))
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%d): %v", f, err)
		}
		if got != without {
			t.Errorf("ir.Inet/Cidr.Family=%d moved the schema fingerprint:\n"+
				"  with the family:    %s\n"+
				"  without the family: %s\n\n"+
				"That mints a NEW backup-chain epoch for every Postgres schema carrying an inet or cidr "+
				"column — the reader sets InetFamilyAny on the ORDINARY one, so `omitempty` does not hold "+
				"it back. Chains written by this build would not restore on any earlier release, reported "+
				"to the operator as `your backup is corrupt`. This is roadmap item 104 repeating. Exclude "+
				"it in fingerprintType (chain.go).", f, got, without)
		}
	}
}

// TestSchemaHash_InetFamilyAbsentFromTheHashedBytes is the byte-level half, and
// it is what makes "the fingerprint did not move" a statement about EARLIER
// RELEASES rather than about this binary agreeing with itself. The equality
// above proves each family hashes like the unspecified one; this proves the
// canonical bytes carry no inet_family key at all — exactly what a pre-item-133
// binary, whose envelope had no such field, emitted.
func TestSchemaHash_InetFamilyAbsentFromTheHashedBytes(t *testing.T) {
	for _, f := range []ir.InetFamily{
		ir.InetFamilyUnspecified, ir.InetFamilyAny, ir.InetFamilyIPv4, ir.InetFamilyIPv6,
	} {
		b, err := json.Marshal(canonicalSchemaForHash(schemaWithInetFamily(f)))
		if err != nil {
			t.Fatalf("marshal canonical (family %d): %v", f, err)
		}
		if strings.Contains(string(b), "inet_family") {
			t.Errorf("the bytes hashed for a family-%d schema carry an inet_family key, so they are NOT "+
				"the bytes an older binary produced for the same schema:\n%s", f, b)
		}
	}
}

// TestSchemaHash_InetExclusionDoesNotMutateTheCaller: the exclusion must be a
// COPY. The manifest records the schema verbatim, so a fingerprint helper that
// stripped the family in place would erase from the recorded schema the very
// fact the wire half exists to carry.
func TestSchemaHash_InetExclusionDoesNotMutateTheCaller(t *testing.T) {
	s := schemaWithInetFamily(ir.InetFamilyIPv6)
	if _, err := ComputeSchemaHash(s); err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	cols := s.Tables[0].Columns
	checks := []struct {
		name string
		got  ir.InetFamily
	}{
		{"scalar inet", cols[1].Type.(ir.Inet).Family},
		{"scalar cidr", cols[2].Type.(ir.Cidr).Family},
		{"inet array element", cols[3].Type.(ir.Array).Element.(ir.Inet).Family},
		{"cidr array element", cols[4].Type.(ir.Array).Element.(ir.Cidr).Family},
		{"domain base type", cols[5].Type.(ir.Domain).BaseType.(ir.Inet).Family},
	}
	for _, c := range checks {
		if c.got != ir.InetFamilyIPv6 {
			t.Errorf("hashing CLEARED the %s's address family (got %d)", c.name, c.got)
		}
	}
}

// TestInetFamily_RidesTheWire is the other half of the trade, and the one that
// makes the exclusion safe rather than merely convenient: the family is kept
// OUT of the hash precisely because it is IN the recorded schema. Delete the
// codec half and a restored schema reads back unspecified, where a `--where`
// on a network column refuses.
func TestInetFamily_RidesTheWire(t *testing.T) {
	s := schemaWithInetFamily(ir.InetFamilyIPv6)
	b, err := ir.MarshalTable(s.Tables[0])
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	if !strings.Contains(string(b), `"inet_family":3`) {
		t.Fatalf("the inet family did not reach the manifest wire. wire was: %s", b)
	}
	back, err := ir.UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	for i, col := range back.Columns {
		if i == 0 {
			continue // the id column
		}
		var got ir.InetFamily
		switch v := col.Type.(type) {
		case ir.Inet:
			got = v.Family
		case ir.Cidr:
			got = v.Family
		case ir.Array:
			switch e := v.Element.(type) {
			case ir.Inet:
				got = e.Family
			case ir.Cidr:
				got = e.Family
			}
		case ir.Domain:
			got = v.BaseType.(ir.Inet).Family
		}
		if got != ir.InetFamilyIPv6 {
			t.Errorf("column %q decoded with address family %d, want %d", col.Name, got, ir.InetFamilyIPv6)
		}
	}
}

// TestSchemaHash_StillSeesInetColumnChanges is the anti-vacuity floor. A
// canonicaliser that zeroed too much — or that dropped network columns from the
// hash outright — would satisfy the exclusion trivially. A real change to one
// of these columns must still move the fingerprint.
//
// Note what this pair does NOT claim, because the sibling of it on the MAC axis
// DOES claim it: [ir.Macaddr.String] renders its two widths differently, so the
// schema diff picks up what the fingerprint gives away there. [ir.Inet.String]
// deliberately does NOT — every family renders "Inet", because the family
// changes no target DDL — so an inet4 ⇄ inet6 retype is seen by NEITHER. That
// is the stated cost in fingerprintType's comment, asserted here rather than
// left to be discovered.
func TestSchemaHash_StillSeesInetColumnChanges(t *testing.T) {
	base, err := ComputeSchemaHash(schemaWithInetFamily(ir.InetFamilyAny))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(base): %v", err)
	}

	renamed := schemaWithInetFamily(ir.InetFamilyAny)
	renamed.Tables[0].Columns[1].Name = "addr"
	retyped := schemaWithInetFamily(ir.InetFamilyAny)
	retyped.Tables[0].Columns[1].Type = ir.Text{Size: ir.TextLong}
	inetToCidr := schemaWithInetFamily(ir.InetFamilyAny)
	inetToCidr.Tables[0].Columns[1].Type = ir.Cidr{Family: ir.InetFamilyAny}

	for _, tc := range []struct {
		name string
		s    *ir.Schema
	}{
		{"a renamed inet column", renamed},
		{"an inet column retyped to text", retyped},
		{"an inet column retyped to cidr", inetToCidr},
	} {
		got, err := ComputeSchemaHash(tc.s)
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%s): %v", tc.name, err)
		}
		if got == base {
			t.Errorf("%s did NOT move the fingerprint — the hash is not seeing these columns at all, "+
				"so the exclusion gate above is vacuous", tc.name)
		}
	}

	// The stated cost, pinned so it cannot quietly become untrue in either
	// direction: String() renders every family identically, so the diff does
	// not see a family retype either.
	if a, b := (ir.Inet{Family: ir.InetFamilyIPv4}).String(), (ir.Inet{Family: ir.InetFamilyIPv6}).String(); a != b {
		t.Errorf("ir.Inet.String() now distinguishes the families (%q vs %q). That is a fine change to "+
			"make, but it must be made deliberately: it makes every MariaDB-source inet column report "+
			"schema drift against the Postgres `inet` sluice emits for it. Update the cost note in "+
			"fingerprintType and this gate together.", a, b)
	}
	if got := (ir.Inet{}).String(); got != "Inet" {
		t.Errorf("ir.Inet{}.String() = %q, want %q — a legacy manifest would read as a retype", got, "Inet")
	}
	if got := (ir.Cidr{}).String(); got != "Cidr" {
		t.Errorf("ir.Cidr{}.String() = %q, want %q — a legacy manifest would read as a retype", got, "Cidr")
	}
}
