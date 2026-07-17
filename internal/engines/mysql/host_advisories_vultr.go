// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Vultr Managed MySQL no-remedy retention advisory (the Vultr leg of
// the managed-MySQL retention story; live-probed 2026-07-17 on a fresh
// hobbyist single-node / MySQL 8.4.8).
//
// Vultr's DBaaS is the same Aiven-derived platform as DigitalOcean's
// and shares DO's headline hazard: an out-of-band platform reaper
// purges every binlog file ~10-16 minutes after creation while
// @@binlog_expire_logs_seconds reads 259200 (3 days) — the variable
// LIES and does not govern the purger. An attached, caught-up
// binlog-dump connection does NOT hold the reaper back (files behind a
// live stream purged on schedule while sluice streamed; a caught-up
// stream survives only because it sits on the never-purged active
// file).
//
// Unlike every other probed platform, Vultr exposes NO retention knob
// whatsoever — all three surfaces verified live: the advanced-options
// API rejects `binlog_retention_period` by name, the database-update
// API silently ignores it, and SET GLOBAL / SET PERSIST /
// PURGE BINARY LOGS are all denied to vultradmin (ERROR 1227). The
// ~10-minute floor is permanent, so where DO's WARN can point at a
// remedy, this one can only shape expectations: CDC on Vultr MySQL is
// migrate-and-cut-over-shaped.
//
// Like DO, @@binlog_expire_logs_seconds is not ground truth and
// @@version_comment is a bare "Source distribution", so the DSN host
// pattern is the only reliable in-band signal — an unconditional
// [ir.SourceHostAdvisor] WARN, not a probe.

// vultrMySQLHostSuffix is the Vultr Managed Database endpoint suffix
// (e.g. vultr-prod-<uuid>-vultr-prod-<hex>.vultrdb.com).
const vultrMySQLHostSuffix = ".vultrdb.com"

// vultrMySQLAdvisories is the Vultr no-remedy retention WARN —
// deliberately STRONGER than the DigitalOcean message, because there
// is no knob to recommend. Pure so the message pins run without a DSN
// parse.
func vultrMySQLAdvisories(host string) []ir.SourceHostAdvisory {
	return []ir.SourceHostAdvisory{{
		Message: fmt.Sprintf(
			"source host %q is a Vultr Managed MySQL endpoint: the platform purges each binlog out-of-band "+
				"~10-16 minutes after creation REGARDLESS of what @@binlog_expire_logs_seconds reports (the "+
				"variable reads 3 days but does not govern the purger), and Vultr exposes NO retention setting "+
				"— the API, CLI, and SQL paths were all verified to refuse or ignore one — so the window cannot "+
				"be extended. A CDC position older than ~10 minutes is unrecoverable, an attached caught-up "+
				"stream does NOT hold the purger back (it is safe only while it stays on the active binlog "+
				"file), and a cold copy or pause longer than the window can livelock auto-resnapshot with no "+
				"remedy. Treat this source as migrate-and-cut-over: keep the sync stream attached and caught "+
				"up from snapshot to cutover, and keep any planned pause under ~10 minutes",
			host,
		),
		Hint: "no binlog-retention knob exists on Vultr Managed MySQL; keep the stream attached and any pause under ~10 minutes; see docs/managed-services.md",
	}}
}

// vultrManagedMySQLHost reports whether dsn parses to a hostname under
// the Vultr Managed Database suffix, returning the matched
// (lowercased) host so callers can name it. Mirrors
// [doManagedMySQLHost]: ("", false) on parse failure or non-host DSN
// forms.
func vultrManagedMySQLHost(dsn string) (string, bool) {
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
	if strings.HasSuffix(host, vultrMySQLHostSuffix) {
		return host, true
	}
	return "", false
}
