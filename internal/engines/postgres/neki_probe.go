// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"net/url"
	"strings"
	"sync"
)

// A per-server memo for "is the endpoint behind this DSN a PlanetScale Neki
// router?", used to decline the ADR-0078 raw-copy passthrough lane.
//
// # What Neki is, for this file's purposes
//
// Neki is PlanetScale's sharded PostgreSQL. It speaks the PostgreSQL wire
// protocol, serves the ordinary catalog, and is driven by THIS engine — see
// docs/adr/adr-0186-neki-is-a-postgres-flavor.md for why it is a flavor of
// postgres rather than an engine of its own. What it is not is a PostgreSQL
// server: a router sits in front of real Postgres clusters, plans each
// statement, and some constructs are not implemented there.
//
// One of those is `COPY (SELECT …) TO`, which is exactly what
// [RowReader.ExportRawCopy] must emit — the exporter's CRITICAL projection
// invariant ([ir.RawCopyExporter]) requires the SELECT form so generated
// columns are excluded and the two sides' column lists line up. There is no
// version of the raw lane that works here.
//
// Measured 2026-09-10 on a live PS-10 cluster: a Neki source to a PostgreSQL
// target failed with
//
//	ExportRawCopy: COPY TO STDOUT: ERROR: not implemented:
//	COPY (SELECT …) TO is not supported (SQLSTATE NK013)
//
// while the SAME source to a MySQL target copied 100/100 rows, because
// cross-engine already takes the IR path. The fast lane was the only thing in
// the way, and `--raw-copy-format` selects text-vs-binary with no off value —
// so without this, a Neki source could not be migrated to a PostgreSQL target
// at all.
//
// # Why detection rather than a flag
//
// Asking the operator to pass a flag would make a correctness property depend
// on them knowing an implementation detail of their provider's router. The
// server volunteers the answer: `version()` returns
// `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) (Neki)`. This is the same shape as
// the MySQL engine's MariaDB detection, and the same reason.
//
// # Why a memo
//
// The row doors open per worker on the parallel copy path, so probing
// unmemoised would add a `SELECT version()` per worker. The answer cannot
// change under a running process for a given server. Mirrors
// mysql/flavor_memo.go, including its two rules: key on the server's NETWORK
// IDENTITY rather than the whole DSN (different paths legitimately pass
// differently-scoped DSNs to one server), and store no credentials.
//
// # What is cached, and what is not
//
// Only a REACHED verdict. A probe that could not run is not a verdict and is
// never cached — the caller then treats the endpoint as ordinary PostgreSQL,
// which is the pre-existing behaviour and fails loudly at the COPY rather
// than silently. That is the safe direction for a fast-lane decision: a
// missed decline costs a clear error, while a fabricated decline would
// quietly drop every PostgreSQL user onto the slower path.
var nekiMemo = struct {
	mu       sync.Mutex
	byServer map[string]bool
}{}

// serverKey identifies the SERVER this config points at, for the memo above,
// carrying NO credentials. Two DSNs differing only in user, password, schema
// or query parameters share a key, which is the point — the doors
// legitimately pass differently-scoped DSNs to one server, and "is this
// endpoint Neki" is a property of the endpoint.
//
// Both DSN forms [Engine.parseDSN] accepts are handled. An unparseable DSN
// falls back to a constant key rather than the raw string, because the raw
// string is exactly what must not be stored; the cost is that such DSNs share
// one memo entry, and the probe's answer is a property of the server either
// way — a wrong share would at worst decline the fast lane, never enable it
// against a server that cannot serve it.
func (c *pgConfig) serverKey() string {
	if u, err := url.Parse(c.dsn); err == nil && u.Host != "" {
		return "uri|" + u.Host + "|" + strings.TrimPrefix(u.Path, "/")
	}
	var host, port, dbname string
	for _, f := range strings.Fields(c.dsn) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		switch k {
		case "host":
			host = v
		case "port":
			port = v
		case "dbname":
			dbname = v
		}
	}
	if host == "" {
		return "unparsed"
	}
	return "kv|" + host + ":" + port + "|" + dbname
}

// nekiVersionMarker is the token Neki appends to its version string. Measured
// 2026-09-10: `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) (Neki)`.
const nekiVersionMarker = "(Neki)"

// isNekiVersion reports whether a `version()` string identifies a Neki
// router. Split out from the probe so it can be tested without a server.
func isNekiVersion(version string) bool {
	return strings.Contains(version, nekiVersionMarker)
}

// probeIsNeki asks the server whether it is a Neki router, memoised per
// server. serverKey identifies the endpoint; q runs the query.
//
// Returns (isNeki, reached). reached=false means the probe could not run and
// the caller must NOT treat the answer as authoritative.
func probeIsNeki(ctx context.Context, serverKey string, q querier) (isNeki, reached bool) {
	nekiMemo.mu.Lock()
	if v, ok := nekiMemo.byServer[serverKey]; ok {
		nekiMemo.mu.Unlock()
		return v, true
	}
	nekiMemo.mu.Unlock()

	rows, err := q.QueryContext(ctx, "SELECT pg_catalog.version()")
	if err != nil {
		return false, false
	}
	defer func() { _ = rows.Close() }()
	var version string
	if !rows.Next() {
		return false, false
	}
	if err := rows.Scan(&version); err != nil {
		return false, false
	}
	if err := rows.Err(); err != nil {
		return false, false
	}
	v := isNekiVersion(version)

	nekiMemo.mu.Lock()
	if nekiMemo.byServer == nil {
		nekiMemo.byServer = map[string]bool{}
	}
	nekiMemo.byServer[serverKey] = v
	nekiMemo.mu.Unlock()
	return v, true
}

// nekiRawCopyDeclineReason is the operator-facing reason logged when the lane
// is declined. It names the construct and the consequence, because an
// operator who notices the fast lane is off should not have to guess why.
const nekiRawCopyDeclineReason = "PlanetScale Neki router does not implement `COPY (SELECT …) TO`, " +
	"which the raw-copy passthrough lane requires; using the IR copy path instead"

// DeclinesRawCopy implements [ir.RawCopyDecliner]. A Neki router keeps this
// reader off the ADR-0078 raw-copy passthrough lane; every other PostgreSQL
// endpoint answers false and the lane behaves exactly as before.
func (r *RowReader) DeclinesRawCopy() (declined bool, reason string) {
	if !r.isNeki {
		return false, ""
	}
	return true, nekiRawCopyDeclineReason
}
