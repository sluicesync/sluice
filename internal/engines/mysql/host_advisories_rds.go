// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// AWS RDS MySQL detect-first retention advisory (roadmap item 70's RDS
// sibling, live-probed 2026-07-16 on a fresh db.t4g.micro / MySQL
// 8.4.9).
//
// On RDS MySQL defaults ('binlog retention hours' = NULL), the platform
// purges each binlog file on a ~5-minute sweep once automated backups
// upload it — observed lifetime ~5-11 minutes per file — while
// @@binlog_expire_logs_seconds reads 30 days (the variable LIES, same
// class as DigitalOcean). Live-proven: an ATTACHED, caught-up
// binlog-dump connection does NOT hold the purger back (files the
// stream had already read were purged on schedule while it ran), so a
// stop/lag longer than the window is fatal at defaults, and a cold
// copy longer than it livelocks auto-resnapshot — a window TIGHTER
// than DO's ~13-16 min.
//
// Unlike DO, the truth is SQL-visible and the remedy is plain SQL:
//
//	CALL mysql.rds_set_configuration('binlog retention hours', 24);  -- 1..168; effective immediately
//
// So this advisory is DETECT-FIRST — strictly better than the DO
// advisory's blind host-pattern WARN, which is the best DO allows (its
// purger setting is only visible via an authenticated cloud API): the
// host pattern gates WHETHER to probe, one query reads the REAL
// setting, and a correctly configured source (>= 24h) stays silent
// instead of collecting a boilerplate WARN on every run.

// rdsMySQLHostSuffix matches RDS MySQL and Aurora MySQL endpoints
// (Aurora shares the mysql.rds_set_configuration procedure). RDS
// Postgres shares the suffix, but this advisory lives in the mysql
// engine, so there is no cross-engine false positive.
const rdsMySQLHostSuffix = ".rds.amazonaws.com"

// rdsRetentionRecommendedHours is the WARN threshold and the value the
// advisory recommends: 24h mirrors the DO 86400s recommendation and
// comfortably covers cold-copy + restart gaps. (RDS caps the knob at
// 168h / 7 days.)
const rdsRetentionRecommendedHours = 24

// rdsRetentionProbeTimeout bounds the advisory's one probe connection
// so a black-holed host can't stall sync/backup start; on timeout the
// advisory degrades to the conservative pattern WARN and the run's own
// source connect surfaces the real failure.
const rdsRetentionProbeTimeout = 15 * time.Second

// SourceProbedAdvisories implements [ir.SourceProbedAdvisor]: on a
// CDC-anchoring run (sync, backup), read the source's REAL retention
// truth and advise accordingly. Two probe legs, each gated so a
// non-matching host never pays a connection:
//
//   - AWS RDS / Aurora MySQL endpoints (host suffix): read the
//     retention setting from mysql.rds_configuration.
//   - Google Cloud SQL candidates (an IP-literal or localhost host —
//     Cloud SQL has NO host pattern, see host_advisories_cloudsql.go):
//     fingerprint via @@version and, only when it IS Cloud SQL, read
//     @@binlog_expire_logs_seconds (honest on that platform).
//
// A plain migrate never returns to the binlog, so cdc=false is a
// no-op; so are named non-RDS hosts (no connection is even attempted)
// and unparseable DSNs.
func (e Engine) SourceProbedAdvisories(ctx context.Context, dsn string, cdc bool) []ir.SourceHostAdvisory {
	if !cdc {
		return nil
	}
	if host, ok := rdsMySQLHost(dsn); ok {
		hours, err := e.probeRDSBinlogRetentionHours(ctx, dsn)
		return rdsRetentionAdvisories(host, hours, err)
	}
	return e.probeCloudSQLAdvisories(ctx, dsn)
}

// probeRDSBinlogRetentionHours opens a short-lived, timeout-bounded
// connection to the source and reads the retention setting.
func (e Engine) probeRDSBinlogRetentionHours(ctx context.Context, dsn string) (sql.NullFloat64, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return sql.NullFloat64{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, rdsRetentionProbeTimeout)
	defer cancel()
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return sql.NullFloat64{}, err
	}
	defer func() { _ = db.Close() }()
	return readRDSBinlogRetentionHours(ctx, db)
}

// readRDSBinlogRetentionHours reads 'binlog retention hours' from
// mysql.rds_configuration. NULL value (the RDS default, "purge as soon
// as possible") returns Valid=false with a nil error; a missing row or
// an unparseable value is an error (the caller degrades to the
// conservative pattern WARN — on a genuine RDS instance the row always
// exists).
func readRDSBinlogRetentionHours(ctx context.Context, db *sql.DB) (sql.NullFloat64, error) {
	const q = `SELECT value FROM mysql.rds_configuration WHERE name = 'binlog retention hours'`
	var raw sql.NullString
	if err := db.QueryRowContext(ctx, q).Scan(&raw); err != nil {
		return sql.NullFloat64{}, err
	}
	if !raw.Valid {
		return sql.NullFloat64{}, nil
	}
	hours, err := strconv.ParseFloat(strings.TrimSpace(raw.String), 64)
	if err != nil {
		return sql.NullFloat64{}, fmt.Errorf("mysql: rds_configuration 'binlog retention hours' has unexpected value %q: %w", raw.String, err)
	}
	return sql.NullFloat64{Float64: hours, Valid: true}, nil
}

// rdsRetentionAdvisories classifies the probe result into advisories.
// Pure so the (NULL / <24 / >=24 / probe-error) matrix is unit-pinned
// without a server.
func rdsRetentionAdvisories(host string, hours sql.NullFloat64, probeErr error) []ir.SourceHostAdvisory {
	const remedyHint = "run CALL mysql.rds_set_configuration('binlog retention hours', 24) on the source; see docs/managed-services.md"
	switch {
	case probeErr != nil:
		// Host matched the RDS pattern but the truth couldn't be read
		// (connect failure, privilege, or not actually RDS behind a
		// vanity CNAME). Degrade to the DO-style unconditional pattern
		// WARN rather than staying silent: at RDS defaults silence
		// costs a livelock, a spurious WARN costs a sentence.
		return []ir.SourceHostAdvisory{{
			Message: fmt.Sprintf(
				"source host %q is an AWS RDS MySQL endpoint but its binlog retention setting could not be read "+
					"(SELECT value FROM mysql.rds_configuration failed: %v): on RDS defaults each binlog is purged "+
					"~5-11 minutes after creation REGARDLESS of what @@binlog_expire_logs_seconds reports — a CDC "+
					"position older than that window is unrecoverable, and a cold copy longer than it can livelock "+
					"auto-resnapshot. Verify retention with CALL mysql.rds_show_configuration and set it with "+
					"CALL mysql.rds_set_configuration('binlog retention hours', 24) (max 168; effective immediately, no restart)",
				host, probeErr,
			),
			Hint: remedyHint,
		}}
	case !hours.Valid:
		// The live-probed default: purge-ASAP.
		return []ir.SourceHostAdvisory{{
			Message: fmt.Sprintf(
				"source host %q is an AWS RDS MySQL endpoint with no binlog retention configured "+
					"('binlog retention hours' is NULL): RDS purges each binlog ~5-11 minutes after creation once "+
					"automated backups upload it, REGARDLESS of what @@binlog_expire_logs_seconds reports — a CDC "+
					"position older than that window is unrecoverable, and a cold copy longer than it can livelock "+
					"auto-resnapshot. An attached, caught-up stream does NOT hold the purger back. Before relying on "+
					"this stream run: CALL mysql.rds_set_configuration('binlog retention hours', 24); "+
					"(max 168; effective immediately, no restart)",
				host,
			),
			Hint: remedyHint,
		}}
	case hours.Float64 < rdsRetentionRecommendedHours:
		return []ir.SourceHostAdvisory{{
			Message: fmt.Sprintf(
				"source host %q is an AWS RDS MySQL endpoint with binlog retention configured at %g hours — below "+
					"the 24 recommended for migrations: a CDC position older than that window is unrecoverable, and a "+
					"stop/lag or cold copy longer than it forces a full re-copy. Raise it with "+
					"CALL mysql.rds_set_configuration('binlog retention hours', 24) (max 168; effective immediately, no restart)",
				host, hours.Float64,
			),
			Hint: remedyHint,
		}}
	default:
		// >= 24h: correctly configured — stay silent. This branch is
		// the whole point of detect-first.
		return nil
	}
}

// rdsMySQLHost reports whether dsn parses to a hostname under the AWS
// RDS suffix, returning the matched (lowercased) host so callers can
// name it. Mirrors [doManagedMySQLHost].
func rdsMySQLHost(dsn string) (string, bool) {
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
	if strings.HasSuffix(host, rdsMySQLHostSuffix) {
		return host, true
	}
	return "", false
}

// isRDSMySQLAddr reports whether a parsed DSN's Addr ("host:port" or
// bare host) is an AWS RDS endpoint. Used by the FTWRL fallback WARNs
// to swap the "Grant RELOAD" remedy — a dead end on RDS, where the
// platform blocks FLUSH TABLES WITH READ LOCK even though the master
// user holds RELOAD (live-probed 2026-07-16: SHOW GRANTS lists RELOAD
// + FLUSH_TABLES via rds_superuser_role, FTWRL still returns 1045) —
// for the platform reality.
func isRDSMySQLAddr(addr string) bool {
	host, _, err := hostPortFromAddr(addr)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.ToLower(host), rdsMySQLHostSuffix)
}
