// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// defaultSchema is the namespace a Postgres DSN carrying no `schema`
// parameter operates on. Shared by [parseURIDSN], [parseKVDSN] and
// [Engine.SourceIdentity] so the three cannot drift: two spellings of
// one source — `?schema=public` and an absent parameter — must render
// the SAME resume identity, or a legitimate `migrate --resume` is
// refused. Before this constant the pipeline held a hand-kept mirror of
// the literal, held to the engine by a source-reading test; the
// describer below retired the mirror, so the agreement is a build fact.
const defaultSchema = "public"

// SourceIdentity implements [ir.SourceIdentityDescriber]: the database
// and schema a Postgres DSN names, with the host, the port and every
// credential deliberately left out (ADR-0015 — a DNS move or a failover
// must still resume).
//
// Both accepted forms are read, and the KEY/VALUE form is read the way
// libpq reads it rather than the way a whitespace split does. That
// distinction is the finding (audit 2026-09-15 F-3): a `strings.Fields`
// walk taking the FIRST `dbname=` rendered `host=h dbname='a b'` and
// `host=h dbname='a c'` identically, took `a` from `dbname=a dbname=b`
// where libpq and pgx take `b`, and read `x` out of
// `password='s dbname=x' dbname=real1` — each of which lets a resume
// adopt a copy made from a different database.
func (Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	if strings.TrimSpace(dsn) == "" {
		return ir.SourceIdentity{}
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			// Refused loudly at open, by parseURIDSN, with its own
			// message; the door says "no discriminator" instead of
			// diagnosing a connection here.
			return ir.SourceIdentity{}
		}
		q := u.Query()
		database := strings.TrimPrefix(u.Path, "/")
		// A `dbname` parameter overrides the path, which is what pgconn
		// does when a URI carries both — so the two spellings cannot
		// name different databases and render one identity.
		if v := q.Get("dbname"); v != "" {
			database = v
		}
		schema := q.Get("schema")
		if schema == "" {
			schema = defaultSchema
		}
		return ir.SourceIdentity{Database: database, Schema: schema}
	}
	kv := parseKVFields(dsn)
	schema := kv["schema"]
	if schema == "" {
		schema = defaultSchema
	}
	return ir.SourceIdentity{Database: kv["dbname"], Schema: schema}
}

// parseKVFields tokenises a libpq key/value connection string the way
// libpq's own conninfo parser does, and returns the settings it carries
// with LAST occurrence winning — the rule libpq and pgx both apply.
//
// Three things a whitespace split gets wrong, all of them in the
// direction that makes two different sources look like one:
//
//   - a SINGLE-QUOTED value may contain spaces and `=`, so
//     `dbname='a b'` is one setting, not the two tokens `dbname='a` and
//     `b'`;
//   - a backslash escapes the next character, inside quotes and out, so
//     `dbname=a\ b` is one value;
//   - a repeated keyword takes the LAST value, so `dbname=a dbname=b`
//     is `b`.
//
// Whitespace is permitted around the `=`. A malformed tail (a keyword
// with no `=`) stops the walk and keeps what was parsed: this is an
// identity extractor, not a validator — the connection path refuses a
// DSN the driver cannot read, with the driver's message.
func parseKVFields(dsn string) map[string]string {
	out := map[string]string{}
	i, n := 0, len(dsn)
	skipSpace := func() {
		for i < n && isConnInfoSpace(dsn[i]) {
			i++
		}
	}
	for i < n {
		skipSpace()
		if i >= n {
			break
		}
		start := i
		for i < n && !isConnInfoSpace(dsn[i]) && dsn[i] != '=' {
			i++
		}
		key := dsn[start:i]
		skipSpace()
		if i >= n || dsn[i] != '=' {
			break
		}
		i++
		skipSpace()
		var val strings.Builder
		if i < n && dsn[i] == '\'' {
			i++
			for i < n {
				if dsn[i] == '\\' && i+1 < n {
					val.WriteByte(dsn[i+1])
					i += 2
					continue
				}
				if dsn[i] == '\'' {
					i++
					break
				}
				val.WriteByte(dsn[i])
				i++
			}
		} else {
			for i < n && !isConnInfoSpace(dsn[i]) {
				if dsn[i] == '\\' && i+1 < n {
					val.WriteByte(dsn[i+1])
					i += 2
					continue
				}
				val.WriteByte(dsn[i])
				i++
			}
		}
		if key != "" {
			out[strings.ToLower(key)] = val.String()
		}
	}
	return out
}

// isConnInfoSpace reports whether b is whitespace by libpq's conninfo
// rules (the C `isspace` set).
func isConnInfoSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	}
	return false
}

// Pinned beside the method: the orchestrator discovers this surface by
// runtime type-assertion off the registry, so a method-set break would
// silently downgrade the resume door to "no discriminator" instead of
// failing the build. The registry-derived roster
// (docsync.TestEverySourceEngineDescribesItsIdentity) enforces that
// every registered engine carries one.
var _ ir.SourceIdentityDescriber = Engine{}
