// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The PG RowReader must satisfy the decline surface, or the pipeline's
// type-assert in asRawCopyEndpoints silently never fires and a Neki source
// goes back to failing at the COPY. A compile-time pin, per the project's
// `var _ ir.X = …` convention.
var _ ir.RawCopyDecliner = (*RowReader)(nil)

// The version marker is an environmental fact about someone else's product,
// so it is pinned to the string a real server returned rather than to an
// assumption about it (the premise-naming rule). Measured 2026-09-10 against
// a live PlanetScale Neki PS-10 cluster.
const measuredNekiVersionString = "PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) (Neki)"

func TestNekiDetectionMatchesTheMeasuredVersionString(t *testing.T) {
	t.Parallel()
	if !isNekiVersion(measuredNekiVersionString) {
		t.Fatalf("isNekiVersion(%q) = false; the marker %q no longer matches the version string a real "+
			"Neki router returns, so every Neki source would silently go back onto the raw-copy lane and "+
			"fail at the COPY", measuredNekiVersionString, nekiVersionMarker)
	}

	// The discriminating half: ordinary PostgreSQL must NOT match, or every
	// PostgreSQL user quietly loses the fast lane. A detector that says yes
	// to everything is worse than none.
	for _, v := range []string{
		"PostgreSQL 16.15 (Debian 16.15-1.pgdg120+1) on x86_64-pc-linux-gnu",
		"PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)",
		"PostgreSQL 15.4 on aarch64-unknown-linux-gnu, compiled by gcc",
		"",
	} {
		if isNekiVersion(v) {
			t.Errorf("isNekiVersion(%q) = true; ordinary PostgreSQL must keep the raw-copy lane", v)
		}
	}
}

func TestRowReaderDeclinesRawCopyOnlyWhenNeki(t *testing.T) {
	t.Parallel()
	plain := &RowReader{}
	if declined, _ := plain.DeclinesRawCopy(); declined {
		t.Error("a non-Neki reader declined the raw-copy lane; the zero value must keep today's fast path")
	}

	neki := &RowReader{isNeki: true}
	declined, why := neki.DeclinesRawCopy()
	if !declined {
		t.Fatal("a Neki reader did not decline the raw-copy lane")
	}
	if why == "" {
		t.Error("the decline carried no reason; the reason is what an operator sees in the log")
	}
}

// serverKey is the memo's key, and the memo caches a per-SERVER verdict. Two
// DSNs for one server must share a key (or the probe re-runs per door), two
// different servers must not (or one server's verdict is served for another),
// and no key may carry a credential.
func TestServerKeyIdentifiesTheServerWithoutCredentials(t *testing.T) {
	t.Parallel()
	same := []struct{ a, b string }{
		{
			"postgres://alice:secret1@db.example.com:5432/app?sslmode=require",
			"postgres://bob:secret2@db.example.com:5432/app?sslmode=disable&schema=other",
		},
		{
			"host=db.example.com port=5432 dbname=app user=alice password=secret1",
			"host=db.example.com port=5432 dbname=app user=bob password=secret2",
		},
	}
	for _, c := range same {
		ka := (&pgConfig{dsn: c.a}).serverKey()
		kb := (&pgConfig{dsn: c.b}).serverKey()
		if ka != kb {
			t.Errorf("same server keyed differently:\n  %q -> %q\n  %q -> %q", c.a, ka, c.b, kb)
		}
		for _, cred := range []string{"secret1", "secret2", "alice", "bob"} {
			if strings.Contains(ka, cred) || strings.Contains(kb, cred) {
				t.Errorf("server key carries a credential (%q): %q / %q", cred, ka, kb)
			}
		}
	}

	differ := []struct{ a, b string }{
		{"postgres://u@a.example.com:5432/app", "postgres://u@b.example.com:5432/app"},
		{"postgres://u@a.example.com:5432/app", "postgres://u@a.example.com:5433/app"},
		{"host=a.example.com port=5432 dbname=app", "host=b.example.com port=5432 dbname=app"},
	}
	for _, c := range differ {
		if (&pgConfig{dsn: c.a}).serverKey() == (&pgConfig{dsn: c.b}).serverKey() {
			t.Errorf("different servers share a memo key: %q and %q", c.a, c.b)
		}
	}
}
