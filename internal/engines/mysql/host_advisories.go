// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// DigitalOcean Managed MySQL lying-retention advisory (roadmap item
// 70a, live-probed 2026-07-15 on a fresh db-s-1vcpu-1gb / MySQL 8.4).
//
// On DO Managed MySQL defaults, an out-of-band platform reaper purges
// every binlog file ~13-16 minutes after creation — while
// @@binlog_expire_logs_seconds reads 259200 (3 days) and the DO config
// API shows no retention field until first set. The variable LIES, so
// no SQL-level preflight can catch this host class; the DSN host
// pattern is the only reliable signal. Without the advisory the
// hazard is loud only AFTER loss (ErrPositionInvalid "binlog purged"),
// and with auto-resnapshot a >15-minute cold copy becomes a resnapshot
// livelock (each retry re-copies, exceeds the window again).
//
// The confirmed remedy is DO's config-API knob:
//
//	PATCH /v2/databases/{id}/config {"config":{"binlog_retention_period":86400}}
//
// (seconds, range 600-86400; effective immediately, no restart, and
// pre-existing binlogs stop being purged). Aiven-hosted MySQL likely
// shares the behavior (same platform lineage) — noted in the doc, not
// pattern-matched here until probed.

// doMySQLHostSuffix is the DigitalOcean Managed MySQL endpoint suffix
// (both public and `private-` VPC hostnames end with it).
const doMySQLHostSuffix = ".db.ondigitalocean.com"

// SourceHostAdvisories implements [ir.SourceHostAdvisor]: on a
// CDC-anchoring run (sync, backup) whose source host is a DigitalOcean
// Managed MySQL endpoint, WARN that effective binlog retention may be
// ~10-15 minutes regardless of what @@binlog_expire_logs_seconds
// reports, naming the config-API knob. A plain migrate never returns
// to the binlog, so cdc=false is a no-op; so are non-DO hosts and
// unparseable DSNs.
func (e Engine) SourceHostAdvisories(dsn string, cdc bool) []ir.SourceHostAdvisory {
	if !cdc {
		return nil
	}
	host, ok := doManagedMySQLHost(dsn)
	if !ok {
		return nil
	}
	return []ir.SourceHostAdvisory{{
		Message: fmt.Sprintf(
			"source host %q is a DigitalOcean Managed MySQL endpoint: on defaults the platform purges "+
				"binlogs out-of-band ~10-15 minutes after creation REGARDLESS of what "+
				"@@binlog_expire_logs_seconds reports (the variable reads 3 days but does not govern the "+
				"purger) — a CDC position older than that window is unrecoverable, and a cold copy longer "+
				"than it can livelock auto-resnapshot. Before relying on this stream, set the retention "+
				"knob via the DO config API: PATCH /v2/databases/{id}/config "+
				`{"config":{"binlog_retention_period":86400}} (seconds, 600-86400; 86400 recommended for `+
				"migrations — effective immediately, no restart)",
			host,
		),
		Hint: "set binlog_retention_period=86400 via the DigitalOcean database config API; see docs/managed-services.md",
	}}
}

// doManagedMySQLHost reports whether dsn parses to a hostname under
// the DigitalOcean Managed MySQL suffix, returning the matched
// (lowercased) host so callers can name it. Mirrors
// [planetScaleMySQLHost]: ("", false) on parse failure or non-host DSN
// forms.
func doManagedMySQLHost(dsn string) (string, bool) {
	if dsn == "" {
		return "", false
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return "", false
	}
	host, _, err := hostPortFromAddr(cfg.Addr)
	if err != nil {
		return "", false
	}
	host = strings.ToLower(host)
	if strings.HasSuffix(host, doMySQLHostSuffix) {
		return host, true
	}
	return "", false
}
