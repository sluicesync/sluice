// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// libpqQueryValues decodes a URI query string the way libpq (and pgx from
// v5.11) does: `+` is a literal plus, only %XX escapes are decoded. It is
// the INDEPENDENT reader for the premise test below — deliberately not
// url.ParseQuery, which shares Encode's form-encoding convention.
func libpqQueryValues(t *testing.T, raw string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, pair := range strings.Split(raw, "&") {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		dk, err := url.PathUnescape(k)
		if err != nil {
			t.Fatalf("unescape key %q: %v", k, err)
		}
		dv, err := url.PathUnescape(v)
		if err != nil {
			t.Fatalf("unescape value %q: %v", v, err)
		}
		out[dk] = dv
	}
	return out
}

// TestEncodeURIQuery_PlusIsOnlyEverASpace pins the premise EncodeURIQuery
// rests on: url.Values.Encode escapes a real `+` to %2B, so every `+` it
// leaves behind stands for a space and may be rewritten to %20. The
// expected values are the inputs themselves, read back by a libpq-style
// decoder that treats `+` literally.
func TestEncodeURIQuery_PlusIsOnlyEverASpace(t *testing.T) {
	values := []string{
		"-c a.b=1",
		"a+b",
		"+",
		" ",
		"+ +",
		"%2B literally",
		"-csearch_path=app -cstatement_timeout=5",
		"-c __neki.shard=0a1b+2c",
		"tab\tand newline\n",
		"ünïcödé + spaces",
	}
	for _, v := range values {
		q := url.Values{"k": {v}, "with space key": {v}}
		enc := EncodeURIQuery(q)
		if strings.Contains(enc, "+") {
			t.Errorf("EncodeURIQuery(%q) = %q: contains a '+', which libpq reads as a literal plus", v, enc)
		}
		got := libpqQueryValues(t, enc)
		if got["k"] != v || got["with space key"] != v {
			t.Errorf("EncodeURIQuery(%q) = %q: a libpq-style reader decodes %q / %q", v, enc, got["k"], got["with space key"])
		}
		// And the form-encoding reader (pgx before 5.11) agrees too.
		back, err := url.ParseQuery(enc)
		if err != nil || back.Get("k") != v {
			t.Errorf("EncodeURIQuery(%q) = %q: url.ParseQuery decodes %q (err %v)", v, enc, back.Get("k"), err)
		}
	}
}

// uriRewriteCase is one DSN put through every sluice rewriter of a
// `postgres://` URI, with the `options` value the driver must end up with.
type uriRewriteCase struct {
	name        string
	dsn         string
	wantOptions string
}

var uriRewriteCases = []uriRewriteCase{
	{
		name:        "one option with a space",
		dsn:         "postgres://u:p@h/db?options=-c%20a.b%3D1&schema=s",
		wantOptions: "-c a.b=1",
	},
	{
		name:        "a real plus survives",
		dsn:         "postgres://u:p@h/db?options=-c%20a.b%3D1%2B2&schema=s",
		wantOptions: "-c a.b=1+2",
	},
	{
		name:        "two options (the silent shape)",
		dsn:         "postgres://u:p@h/db?options=-csearch_path%3Dapp%20-cstatement_timeout%3D5&schema=s",
		wantOptions: "-csearch_path=app -cstatement_timeout=5",
	},
	{
		// sluice reads an operator-written bare `+` as a space (Go's and
		// pgx 5.10's reading), not as libpq/pgx 5.11 would. Pinned so the
		// choice is visible; the CHANGELOG states it.
		name:        "operator-written plus is read as a space",
		dsn:         "postgres://u:p@h/db?options=-c+a.b%3D1&schema=s",
		wantOptions: "-c a.b=1",
	},
	{
		name:        "the Neki shard recipe",
		dsn:         "postgresql://u:p@h/db?options=-c%20__neki.shard%3D0a1b&schema=s",
		wantOptions: "-c __neki.shard=0a1b",
	},
}

// TestURIRewriters_KeepOptionsSpacesForPgx is the pin for the pgx 5.11
// `+`-is-literal change: every function that re-encodes a `postgres://`
// query must hand pgx an `options` value it parses back exactly. Reached:
// parseURIDSN (the *sql.DB / pool path), withApplicationName, the
// replication rewriter withReplicationParam, withSchemaURI (WithDatabase),
// and the composition the replication connect actually performs. The
// pgtrigger engine's own parseURIDSN copy is pinned in that package.
func TestURIRewriters_KeepOptionsSpacesForPgx(t *testing.T) {
	rewriters := map[string]func(string) (string, error){
		"parseURIDSN": func(dsn string) (string, error) {
			cfg, err := parseURIDSN(dsn)
			if err != nil {
				return "", err
			}
			return cfg.dsn, nil
		},
		"withApplicationName": func(dsn string) (string, error) {
			return withApplicationName(dsn, roleCDCReader, "stream"), nil
		},
		"withReplicationParam": withReplicationParam,
		"WithDatabase": func(dsn string) (string, error) {
			return Engine{}.WithDatabase(dsn, "other")
		},
		"replication connect chain": func(dsn string) (string, error) {
			withRepl, err := withReplicationParam(dsn)
			if err != nil {
				return "", err
			}
			return withApplicationName(withRepl, roleCDCReader, "stream"), nil
		},
		"pool connect chain": func(dsn string) (string, error) {
			cfg, err := parseURIDSN(dsn)
			if err != nil {
				return "", err
			}
			return withApplicationName(cfg.dsn, roleCDCReader, "stream"), nil
		},
	}
	for _, tc := range uriRewriteCases {
		for name, rewrite := range rewriters {
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				out, err := rewrite(tc.dsn)
				if err != nil {
					t.Fatalf("rewrite: %v", err)
				}
				if strings.Contains(out, "+") {
					t.Errorf("rewritten DSN %q contains '+', which pgx >= 5.11 reads as a literal plus", out)
				}
				if name == "WithDatabase" {
					// WithDatabase's output is a sluice DSN (schema kept);
					// what the driver sees is its parseURIDSN.
					cfg, err := parseURIDSN(out)
					if err != nil {
						t.Fatalf("parseURIDSN(WithDatabase output): %v", err)
					}
					if cfg.schema != "other" {
						t.Errorf("schema = %q, want other", cfg.schema)
					}
					out = cfg.dsn
				}
				pc, err := pgx.ParseConfig(out)
				if err != nil {
					t.Fatalf("pgx.ParseConfig(%q): %v", out, err)
				}
				if got := pc.RuntimeParams["options"]; got != tc.wantOptions {
					t.Errorf("pgx options = %q, want %q (DSN handed to pgx: %q)", got, tc.wantOptions, out)
				}
			})
		}
	}
}
