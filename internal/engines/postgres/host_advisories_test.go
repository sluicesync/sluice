// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSourceHostAdvisories_PoolerPatterns pins the item-69a pooler
// WARN across the whole named pattern table × both DSN forms — pin
// the class, not one representative: each table entry is its own
// match family (Supavisor suffix, `-pooler.` label, pgbouncer
// substring) and the URI vs keyword DSN forms take different parse
// paths.
func TestSourceHostAdvisories_PoolerPatterns(t *testing.T) {
	var _ ir.SourceHostAdvisor = Engine{}

	cases := []struct {
		name      string
		dsn       string
		wantLabel string
	}{
		{
			"Supabase Supavisor URI",
			"postgres://u:p@aws-0-us-east-1.pooler.supabase.com:6543/postgres?sslmode=require",
			"Supavisor",
		},
		{
			"Supabase Supavisor keyword form",
			"host=aws-0-us-east-1.pooler.supabase.com port=5432 user=u password=p dbname=postgres",
			"Supavisor",
		},
		{
			"Neon -pooler URI",
			"postgres://u:p@ep-cool-cloud-123456-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require",
			"-pooler",
		},
		{
			"Neon -pooler keyword form",
			"host=ep-cool-cloud-123456-pooler.us-east-2.aws.neon.tech user=u dbname=neondb",
			"-pooler",
		},
		{
			"generic pgbouncer host",
			"postgres://u:p@pgbouncer.internal.example.com:6432/app",
			"pgbouncer",
		},
		{
			"mixed-case host still matches",
			"postgres://u:p@AWS-0-EU-WEST-1.Pooler.Supabase.com:5432/postgres",
			"Supavisor",
		},
		{
			"sluice schema param stripped before parse",
			"postgres://u:p@ep-x-pooler.eu-central-1.aws.neon.tech/db?schema=app",
			"-pooler",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, cdc := range []bool{false, true} {
				got := Engine{}.SourceHostAdvisories(c.dsn, cdc)
				if len(got) != 1 {
					t.Fatalf("cdc=%v: got %d advisories; want 1", cdc, len(got))
				}
				a := got[0]
				if !strings.Contains(a.Message, c.wantLabel) {
					t.Errorf("cdc=%v: message should name the matched pattern %q; got: %s", cdc, c.wantLabel, a.Message)
				}
				if !strings.Contains(a.Message, "direct") {
					t.Errorf("cdc=%v: message should recommend the direct endpoint; got: %s", cdc, a.Message)
				}
				if a.Hint == "" {
					t.Errorf("cdc=%v: advisory carries no hint", cdc)
				}
			}
			// The CDC variant names the harder consequence (slot
			// creation will fail); the migrate variant names the
			// pool-exhaustion caveat.
			cdcMsg := Engine{}.SourceHostAdvisories(c.dsn, true)[0].Message
			if !strings.Contains(cdcMsg, "slot creation") {
				t.Errorf("cdc message should name the slot-creation failure; got: %s", cdcMsg)
			}
			migMsg := Engine{}.SourceHostAdvisories(c.dsn, false)[0].Message
			if !strings.Contains(migMsg, "exhaust the pool") {
				t.Errorf("migrate message should name the pool-exhaustion caveat; got: %s", migMsg)
			}
		})
	}
}

// TestSourceHostAdvisories_NoMatch pins the silent no-op for direct
// endpoints, socket paths, and unparseable DSNs — a WARN that fires
// on ordinary hosts would train operators to ignore it.
func TestSourceHostAdvisories_NoMatch(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{"plain local", "postgres://u:p@localhost:5432/db"},
		{"Neon DIRECT endpoint", "postgres://u:p@ep-cool-cloud-123456.us-east-2.aws.neon.tech/neondb"},
		{"Supabase DIRECT endpoint", "postgres://u:p@db.abcdefghijkl.supabase.co:5432/postgres"},
		{"unix socket", "host=/var/run/postgresql dbname=db user=u"},
		{"empty", ""},
		{"garbage", "not a dsn at all \x00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := (Engine{}).SourceHostAdvisories(c.dsn, true); len(got) != 0 {
				t.Errorf("got %d advisories (%v); want none", len(got), got)
			}
		})
	}
}
