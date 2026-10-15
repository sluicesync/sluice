// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestSeededShapes_NamespaceRule pins the lookup [GuardSourceShapeSeeder]
// relies on: an exact (namespace, table) shape wins; a shape recorded with
// no namespace (a MySQL-family manifest, Bug 306) answers for its table
// under any namespace; a later seed replaces an earlier one; and nothing
// answers for a table never seeded.
func TestSeededShapes_NamespaceRule(t *testing.T) {
	var s SeededShapes
	if s.Lookup("public", "kd") != nil {
		t.Fatal("the zero value answered a lookup")
	}
	bare := &Table{Name: "kd"}
	exact := &Table{Schema: "public", Name: "kd"}
	other := &Table{Schema: "public", Name: "other"}
	s.Seed([]*Table{bare, other, nil})
	if got := s.Lookup("appdb", "kd"); got != bare {
		t.Errorf("a namespace-less shape did not answer for appdb.kd: %v", got)
	}
	if got := s.Lookup("public", "other"); got != other {
		t.Errorf("public.other = %v; want the seeded shape", got)
	}
	if got := s.Lookup("appdb", "other"); got != nil {
		t.Errorf("a namespaced shape answered for another namespace: %v", got)
	}
	s.Seed([]*Table{exact})
	if got := s.Lookup("public", "kd"); got != exact {
		t.Errorf("public.kd = %v; want the exact shape over the namespace-less one", got)
	}
	replaced := &Table{Name: "kd"}
	s.Seed([]*Table{replaced})
	if got := s.Lookup("appdb", "kd"); got != replaced {
		t.Errorf("a later seed did not replace the earlier shape: %v", got)
	}
	if s.Lookup("public", "missing") != nil {
		t.Error("an unseeded table answered")
	}
}
