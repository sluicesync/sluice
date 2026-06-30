// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestOrmEngineFamily(t *testing.T) {
	cases := map[string]string{
		"mysql": "mysql", "planetscale": "mysql", "vitess": "mysql",
		"postgres": "postgres", "postgres-trigger": "postgres",
		"sqlite": "sqlite", "d1": "sqlite", "sqlite-trigger": "sqlite", "d1-trigger": "sqlite",
		"MySQL":   "mysql",   // case-insensitive
		"unknown": "unknown", // unknown maps to itself
	}
	for in, want := range cases {
		if got := ormEngineFamily(in); got != want {
			t.Errorf("ormEngineFamily(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestResolveSkipORMTables(t *testing.T) {
	tests := []struct {
		name          string
		src, dst      string
		include, skip bool
		want          bool
	}{
		{"cross d1->pg default skip", "d1", "postgres", false, false, true},
		{"cross mysql->pg default skip", "mysql", "postgres", false, false, true},
		{"cross sqlite->mysql default skip", "sqlite", "mysql", false, false, true},
		{"same pg->pg default keep", "postgres", "postgres", false, false, false},
		{"same-family mysql->planetscale default keep", "mysql", "planetscale", false, false, false},
		{"same-family sqlite->d1 default keep", "sqlite", "d1", false, false, false},
		{"same-family pg->pg-trigger default keep", "postgres", "postgres-trigger", false, false, false},
		{"include forces keep on cross", "d1", "postgres", true, false, false},
		{"skip forces skip on same", "postgres", "postgres", false, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveSkipORMTables(tc.src, tc.dst, tc.include, tc.skip); got != tc.want {
				t.Errorf("resolveSkipORMTables(%q,%q,include=%v,skip=%v) = %v; want %v",
					tc.src, tc.dst, tc.include, tc.skip, got, tc.want)
			}
		})
	}
}
