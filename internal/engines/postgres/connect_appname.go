// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
	"unicode/utf8"
)

// connRole names the sluice subsystem a Postgres connection belongs to.
// It is the role segment of the application_name sluice stamps on every
// connection it opens — `sluice/<role>/<id>`, see [withApplicationName] —
// so operators can tell a snapshot pool apart from the CDC reader or the
// control-table session in pg_stat_activity. The set is deliberately small
// and stable: each value maps to one of the engine's Open* entry points.
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

// maxAppNameBytes is PostgreSQL's effective application_name limit:
// pg_stat_activity holds it in a NAMEDATALEN (64) byte buffer and silently
// truncates anything past NAMEDATALEN-1 = 63 bytes (replacing non-printable
// bytes with '?'). sluice truncates proactively so the stored value is
// deterministic, and — because role precedes the id — so the truncation
// eats the *id tail* rather than letting PG silently chop the role (the
// discriminator the budget probe + Phase-2 reaping match on) off the end.
const maxAppNameBytes = 63

// buildApplicationName assembles sluice's application_name for a role.
//
// Format: `sluice/<role>/<id>`. The role comes *before* the id so the
// machine-readable parts (the `sluice/` prefix and the role) always
// survive truncation — only a long operator stream-/migration-id loses its
// tail. The whole value is clamped to [maxAppNameBytes] on a rune boundary.
func buildApplicationName(role connRole, id string) string {
	return clampUTF8("sluice/"+string(role)+"/"+id, maxAppNameBytes)
}

// clampUTF8 truncates s to at most maxBytes bytes without splitting a
// multibyte rune. (PG sanitises non-ASCII application_name bytes to '?'
// regardless, but we keep the DSN handed to the driver valid UTF-8.)
func clampUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	b := s[:maxBytes]
	for b != "" && !utf8.ValidString(b) {
		b = b[:len(b)-1]
	}
	return b
}

// withApplicationName returns dsn with sluice's application_name added
// for the given role and stream-/migration-id, unless the operator
// already set application_name in the DSN — an operator-supplied value
// is never clobbered (they may be coordinating with their own
// pg_stat_activity tooling).
//
// The stamped value is `sluice/<role>/<id>` (clamped to 63 bytes, see
// [buildApplicationName]). The id is threaded down from
// [Engine.WithConnectionLabel] via [pgConfig.appID]; an empty id (a
// bare Go-API caller on the zero-value Engine, a path with no id to
// carry) is normalised here — the single choke point every connection
// funnels through — to the stable "-" fallback, so the label format
// stays well-formed and greppable. Both DSN forms are handled,
// mirroring [Engine.parseDSN]:
//
//   - URI: postgres://…?application_name=sluice/snapshot/mystream
//   - libpq KV: host=… application_name=sluice/snapshot/mystream
//
// A DSN that fails to parse as a URI is returned unchanged; the driver's
// own parse step will surface the malformed-DSN error with a better
// message than anything we could synthesise here.
func withApplicationName(dsn string, role connRole, id string) string {
	if id == "" {
		id = "-"
	}
	appName := buildApplicationName(role, id)

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
