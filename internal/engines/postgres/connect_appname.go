// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
)

// connRole names the sluice subsystem a Postgres connection belongs to.
// It is the trailing segment of the application_name sluice stamps on
// every connection it opens (see [withApplicationName]) so operators can
// tell a snapshot pool apart from the CDC reader or the control-table
// session in pg_stat_activity. The set is deliberately small and stable:
// each value maps to one of the engine's Open* entry points.
type connRole string

const (
	// roleSnapshot is the bulk-copy / row-reader / row-writer family —
	// the connections that move table data during cold-start.
	roleSnapshot connRole = "snapshot"
	// roleApplier is the CDC change-applier's pool.
	roleApplier connRole = "applier"
	// roleCDCReader is the logical-replication reader (both its catalog
	// *sql.DB pool and its replication-protocol streaming connection).
	roleCDCReader connRole = "cdc-reader"
	// roleSchema is the schema reader/writer used for DDL and catalog
	// reads.
	roleSchema connRole = "schema"
	// roleControl is the control/state surface: migration-state store,
	// slot manager, publication ensure, position probes, and the
	// public [OpenPgxDB] funnel callers (e.g. the postgres-trigger
	// poller) that don't name a more specific role.
	roleControl connRole = "control"
)

// applicationID is the stream- or migration-id segment sluice embeds in
// the application_name it stamps on every Postgres connection. It is
// process-global state set once at startup by [SetApplicationID] (called
// from main.go with the resolved --stream-id / --migration-id), mirroring
// the [SetSessionSQLMode] pattern the mysql engine uses to thread a
// CLI-level value into the connection layer without routing it through
// every ir.Engine.Open* signature.
//
// Empty (the default) is the stable fallback: connections are labelled
// `sluice/-/<role>`. Paths that never go through main() (a bare
// `go test ./...`, direct Go-API callers) get that fallback rather than
// no label at all.
var applicationID = "-"

// SetApplicationID sets the stream/migration-id segment of the
// application_name sluice stamps on every Postgres connection. main.go
// calls this once at startup, before any engine opens a connection, with
// the operator's resolved --stream-id (sync) or --migration-id (migrate).
//
// An empty id is normalised to "-" so the application_name format
// (`sluice/<id>/<role>`) stays well-formed and greppable even when no id
// is available.
//
// Concurrency: this is process-wide global state set once at startup,
// before any engine opens a connection. Don't call it from long-lived
// goroutines.
func SetApplicationID(id string) {
	if id == "" {
		id = "-"
	}
	applicationID = id
}

// withApplicationName returns dsn with sluice's application_name added
// for the given role, unless the operator already set application_name
// in the DSN — an operator-supplied value is never clobbered (they may
// be coordinating with their own pg_stat_activity tooling).
//
// The stamped value is `sluice/<applicationID>/<role>`. Both DSN forms
// are handled, mirroring [parseDSN]:
//
//   - URI: postgres://…?application_name=sluice/mystream/snapshot
//   - libpq KV: host=… application_name=sluice/mystream/snapshot
//
// A DSN that fails to parse as a URI is returned unchanged; the driver's
// own parse step will surface the malformed-DSN error with a better
// message than anything we could synthesise here.
func withApplicationName(dsn string, role connRole) string {
	appName := "sluice/" + applicationID + "/" + string(role)

	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return dsn
		}
		q := u.Query()
		if q.Get("application_name") != "" {
			return dsn // operator-supplied; don't clobber
		}
		q.Set("application_name", appName)
		u.RawQuery = q.Encode()
		return u.String()
	}

	// libpq KV form. Scan for an existing application_name token; leave
	// it untouched if present.
	for _, tok := range strings.Fields(dsn) {
		if k, _, ok := strings.Cut(tok, "="); ok && strings.EqualFold(k, "application_name") {
			return dsn // operator-supplied; don't clobber
		}
	}
	// pgx/libpq KV values with embedded `/` don't need quoting, so the
	// bare append is safe for our slash-separated value.
	if strings.TrimSpace(dsn) == "" {
		return "application_name=" + appName
	}
	return dsn + " application_name=" + appName
}
