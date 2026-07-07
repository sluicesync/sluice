// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
)

// mysqlSpecZeroDate is a MySQL-source spec with the per-sync zero-date key set.
func mysqlSpecZeroDate(id, zeroDate string) SyncSpec {
	s := mysqlSpec(id)
	s.ZeroDate = zeroDate
	return s
}

// TestFleetValidate_ZeroDate pins the ADR-0127 config-load contract for the
// per-sync `zero-date` key: empty defers to the global, a set value must be one
// of error|null|epoch, and it is refused on a non-MySQL source where it has no
// meaning (loud, not silently ignored).
func TestFleetValidate_ZeroDate(t *testing.T) {
	cases := []struct {
		name        string
		spec        SyncSpec
		wantErr     bool
		wantSubstrs []string
	}{
		{name: "unset → ok", spec: mysqlSpec("a"), wantErr: false},
		{name: "error → ok", spec: mysqlSpecZeroDate("a", "error"), wantErr: false},
		{name: "null → ok", spec: mysqlSpecZeroDate("a", "null"), wantErr: false},
		{name: "epoch → ok", spec: mysqlSpecZeroDate("a", "epoch"), wantErr: false},
		{
			name:        "bogus value → refused",
			spec:        mysqlSpecZeroDate("a", "bogus"),
			wantErr:     true,
			wantSubstrs: []string{"invalid zero-date", "bogus", "error, null, epoch"},
		},
		{
			name: "set on a postgres source → refused",
			spec: func() SyncSpec {
				s := pgSpec("a", "slot_a")
				s.ZeroDate = "null"
				return s
			}(),
			wantErr:     true,
			wantSubstrs: []string{"MySQL-source", "postgres"},
		},
		{name: "planetscale source + epoch → ok", spec: func() SyncSpec {
			s := mysqlSpecZeroDate("a", "epoch")
			s.SourceDriver = "planetscale"
			return s
		}(), wantErr: false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := fleetFromSpecs(c.spec).validate()
			if c.wantErr != (err != nil) {
				t.Fatalf("validate() err = %v; wantErr = %v", err, c.wantErr)
			}
			if err != nil {
				for _, sub := range c.wantSubstrs {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q missing substring %q", err.Error(), sub)
					}
				}
			}
		})
	}
}

// TestApplyZeroDateToSourceDSN pins the DSN merge: append with the right
// separator, leave a hand-set zero_date alone (the DSN param is foundational
// and wins), and detect the query separator after the last '@' so a '?'/'@' in
// the password never confuses it.
func TestApplyZeroDateToSourceDSN(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		mode string
		want string
	}{
		{"empty mode → unchanged", "u:p@tcp(h:3306)/db", "", "u:p@tcp(h:3306)/db"},
		{"no query → ?param", "u:p@tcp(h:3306)/db", "null", "u:p@tcp(h:3306)/db?zero_date=null"},
		{"existing query → &param", "u:p@tcp(h:3306)/db?parseTime=true", "epoch", "u:p@tcp(h:3306)/db?parseTime=true&zero_date=epoch"},
		{"already set → DSN wins", "u:p@tcp(h:3306)/db?zero_date=epoch", "null", "u:p@tcp(h:3306)/db?zero_date=epoch"},
		// A '?' inside the password must not be mistaken for the query start:
		// the separator is found after the last '@'.
		{"question-mark in password → ?param", "u:pa?ss@tcp(h:3306)/db", "error", "u:pa?ss@tcp(h:3306)/db?zero_date=error"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := applyZeroDateToSourceDSN(c.dsn, c.mode); got != c.want {
				t.Errorf("applyZeroDateToSourceDSN(%q, %q) = %q; want %q", c.dsn, c.mode, got, c.want)
			}
		})
	}
}

// TestIsMySQLSourceDriver pins the family membership used to gate the merge.
func TestIsMySQLSourceDriver(t *testing.T) {
	for _, n := range []string{"mysql", "MySQL", "planetscale", "vitess"} {
		if !isMySQLSourceDriver(n) {
			t.Errorf("isMySQLSourceDriver(%q) = false; want true", n)
		}
	}
	for _, n := range []string{"postgres", "postgres-trigger", ""} {
		if isMySQLSourceDriver(n) {
			t.Errorf("isMySQLSourceDriver(%q) = true; want false", n)
		}
	}
}

// TestBuildStreamerFromSpec_ZeroDate pins that the fleet `zero-date` key lands
// as the source DSN's zero_date param for a MySQL source, and is inert for a
// non-MySQL source (SourceDSN stays byte-identical to spec.Source).
func TestBuildStreamerFromSpec_ZeroDate(t *testing.T) {
	t.Run("mysql source → param folded into SourceDSN", func(t *testing.T) {
		spec := mysqlSpecZeroDate("a", "epoch")
		streamer, err := buildStreamerFromSpec(context.Background(), &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if !strings.Contains(streamer.SourceDSN, "zero_date=epoch") {
			t.Errorf("SourceDSN = %q; want it to carry zero_date=epoch", streamer.SourceDSN)
		}
	})
	t.Run("mysql source, unset → SourceDSN unchanged", func(t *testing.T) {
		spec := mysqlSpec("a")
		streamer, err := buildStreamerFromSpec(context.Background(), &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if streamer.SourceDSN != spec.Source {
			t.Errorf("SourceDSN = %q; want unchanged %q", streamer.SourceDSN, spec.Source)
		}
	})
	t.Run("postgres source → SourceDSN unchanged", func(t *testing.T) {
		spec := pgSpec("a", "slot_a")
		streamer, err := buildStreamerFromSpec(context.Background(), &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if streamer.SourceDSN != spec.Source {
			t.Errorf("SourceDSN = %q; want unchanged %q", streamer.SourceDSN, spec.Source)
		}
	})
}

// TestLoadFleetConfig_ParsesZeroDate pins that the koanf loader decodes the
// `zero-date` YAML key into SyncSpec.ZeroDate.
func TestLoadFleetConfig_ParsesZeroDate(t *testing.T) {
	path := writeFleetYAML(t, `
syncs:
  - stream-id: legacy
    source-driver: mysql
    source: mysql://u:p@src:3306/app
    target-driver: postgres
    target: postgres://u:p@dst:5432/app
    zero-date: "null"
`)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	if got := fleet.Syncs[0].ZeroDate; got != "null" {
		t.Errorf("ZeroDate = %q; want \"null\"", got)
	}
}
