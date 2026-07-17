// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Google Cloud SQL for MySQL detection + retention advisory (the GCP leg
// of the managed-MySQL retention story; live-probed 2026-07-16 on a
// fresh db-f1-micro / MySQL 8.0.45).
//
// Cloud SQL is the first of the three probed managed-MySQL platforms
// whose binlog-retention story is BOTH truthful and safe by default:
// @@binlog_expire_logs_seconds reads 86400 (1 day), it is the real
// governing knob (no DO/RDS-style out-of-band reaper), and the platform
// refuses to set it below 86400 (allowed: 0 = never expire, or
// 86400..4294967295). So unlike the DO and RDS advisories there is
// nothing to warn about at defaults — the classifier below is a guard
// that only fires if the window is somehow under 24h, which the
// platform's own floor makes nearly unreachable.
//
// The detection exists primarily to power the position-loss recovery
// text ([cloudSQLPositionLossHint]): Cloud SQL's one genuinely dangerous
// knob is the PITR toggle itself — `--no-enable-bin-log` destroys every
// binlog, and re-enabling resets numbering to mysql-bin.000001, so every
// position persisted before the round-trip is permanently invalid
// (observed live: warm resume → "binlog file ... no longer available
// (purged)" → auto-resnapshot, the correct recovery).
//
// Unlike DO (host suffix) and RDS (host suffix gating a probe), Cloud
// SQL has NO host pattern: instances connect via bare public IP, or via
// the cloud-sql-proxy / connector at 127.0.0.1 — which defeats any host
// heuristic anyway. The reliable fingerprint is in-band: @@version ends
// in "-google" / @@version_comment contains "(Google)" — one SELECT, no
// privileges, proxy-transparent. The probe is therefore gated on the
// host SHAPE (an IP literal or localhost, the only shapes a Cloud SQL
// DSN takes) so named-host DSNs never pay a probe connection.

// cloudSQLVersionSuffix is the @@version suffix Cloud SQL for MySQL
// stamps (e.g. "8.0.45-google"); cloudSQLVersionCommentMark is the
// @@version_comment fingerprint ("(Google)"). Either alone identifies
// the platform; matching both forms keeps the detection robust to one
// of them changing shape.
const (
	cloudSQLVersionSuffix      = "-google"
	cloudSQLVersionCommentMark = "(google)"
)

// cloudSQLSafeRetentionFloorSeconds is the platform's own enforced
// floor for binlog_expire_logs_seconds (1 day) and the advisory's WARN
// threshold: at or above it (or 0 = never expire) the source is safe
// for any cold-copy + reattach gap under a day and the advisory stays
// silent.
const cloudSQLSafeRetentionFloorSeconds = 86400

// probeCloudSQLAdvisories is the Cloud SQL leg of
// [Engine.SourceProbedAdvisories]: for a candidate host shape it opens
// a short-lived, timeout-bounded connection, fingerprints the server,
// and — only when it IS Cloud SQL — reads the real retention window.
// Every failure degrades to SILENCE, not a WARN: the fingerprint is
// unknown until the probe succeeds (so a conservative WARN would spam
// every unreachable IP-addressed source), and a confirmed Cloud SQL
// whose variable can't be read still has the platform's ≥1-day floor —
// the safe-defaults asymmetry with the RDS probe, whose defaults are
// dangerous and therefore degrade to a WARN.
func (e Engine) probeCloudSQLAdvisories(ctx context.Context, dsn string) []ir.SourceHostAdvisory {
	host, ok := cloudSQLCandidateHost(dsn)
	if !ok {
		return nil
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, rdsRetentionProbeTimeout)
	defer cancel()
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil
	}
	defer func() { _ = db.Close() }()
	return cloudSQLAdvisoriesFromDB(ctx, host, db)
}

// cloudSQLAdvisoriesFromDB runs the fingerprint + retention read on an
// existing handle. Split from the connection plumbing so the scripted-
// driver unit pins exercise the real query layer.
func cloudSQLAdvisoriesFromDB(ctx context.Context, host string, db *sql.DB) []ir.SourceHostAdvisory {
	version, comment, err := readServerVersionStrings(ctx, db)
	if err != nil || !isCloudSQLServer(version, comment) {
		return nil
	}
	seconds, err := readBinlogExpireLogsSeconds(ctx, db)
	if err != nil {
		// Confirmed Cloud SQL but the variable was unreadable: silence
		// (see the function doc for the safe-defaults asymmetry with RDS).
		return nil
	}
	return cloudSQLRetentionAdvisories(host, seconds)
}

// cloudSQLRetentionAdvisories classifies the probed retention window.
// Pure so the (0 / <86400 / >=86400) matrix is unit-pinned without a
// server. 0 means "never expire" — safe. The <86400 WARN is the no-op
// guard branch: Cloud SQL's API refuses values below the 1-day floor,
// so it fires only if the platform contract ever changes (or something
// Cloud-SQL-fingerprinted is not actually Cloud SQL).
func cloudSQLRetentionAdvisories(host string, seconds int64) []ir.SourceHostAdvisory {
	if seconds == 0 || seconds >= cloudSQLSafeRetentionFloorSeconds {
		// The platform default (86400) and everything above it — stay
		// silent. This branch is the whole point of detect-first.
		return nil
	}
	return []ir.SourceHostAdvisory{{
		Message: fmt.Sprintf(
			"source %q fingerprints as Google Cloud SQL for MySQL with binlog_expire_logs_seconds = %d — below "+
				"the 86400 (1 day) the platform normally enforces as its floor: a CDC position older than that "+
				"window is unrecoverable, and a stop/lag or cold copy longer than it forces a full re-copy. Raise "+
				"it with: gcloud sql instances patch INSTANCE --database-flags=binlog_expire_logs_seconds=86400 "+
				"(applied live, no restart; careful — --database-flags replaces the ENTIRE flag set, include any "+
				"existing flags)",
			host, seconds,
		),
		Hint: "raise binlog_expire_logs_seconds via gcloud database flags; see docs/managed-services.md",
	}}
}

// isCloudSQLServer reports whether the server's version strings carry
// the Google Cloud SQL fingerprint: @@version ending in "-google"
// (observed live: "8.0.45-google") or @@version_comment containing
// "(Google)". Case-insensitive; either signal alone matches.
func isCloudSQLServer(version, versionComment string) bool {
	return strings.HasSuffix(strings.ToLower(version), cloudSQLVersionSuffix) ||
		strings.Contains(strings.ToLower(versionComment), cloudSQLVersionCommentMark)
}

// readServerVersionStrings reads the two identity variables the Cloud
// SQL fingerprint keys on. One round trip, no privileges required.
func readServerVersionStrings(ctx context.Context, db *sql.DB) (version, versionComment string, err error) {
	if err := db.QueryRowContext(ctx, "SELECT @@version, @@version_comment").Scan(&version, &versionComment); err != nil {
		return "", "", fmt.Errorf("mysql: read @@version/@@version_comment: %w", err)
	}
	return version, versionComment, nil
}

// readBinlogExpireLogsSeconds reads the on-disk binlog retention window
// — on Cloud SQL the variable is honest (contrast DO and RDS, where it
// lies), which is what makes the detect-first advisory possible.
func readBinlogExpireLogsSeconds(ctx context.Context, db *sql.DB) (int64, error) {
	var seconds int64
	if err := db.QueryRowContext(ctx, "SELECT @@global.binlog_expire_logs_seconds").Scan(&seconds); err != nil {
		return 0, fmt.Errorf("mysql: read @@binlog_expire_logs_seconds: %w", err)
	}
	return seconds, nil
}

// cloudSQLCandidateHost reports whether dsn's host has a shape a Cloud
// SQL connection can take — an IP literal (bare public IP, the platform
// norm) or localhost (the cloud-sql-proxy / connector) — returning the
// host so advisories can name it. Named hostnames are NOT candidates:
// Cloud SQL publishes no DNS endpoint in its default mode, and gating
// on shape keeps the probe connection free for every named-host source
// (an operator-managed CNAME in front of Cloud SQL is the accepted,
// documented miss). Mirrors [rdsMySQLHost]'s ("", false)-on-parse-
// failure contract.
func cloudSQLCandidateHost(dsn string) (string, bool) {
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
	if host == "localhost" || net.ParseIP(host) != nil {
		return host, true
	}
	return "", false
}

// cloudSQLPositionLossHint returns the Cloud-SQL-specific recovery
// sentence folded into the [ir.ErrPositionInvalid] wraps when the
// source fingerprints as Google Cloud SQL, and "" otherwise (including
// on any probe failure — the generic message stands on its own). The
// dominant Cloud SQL position-loss cause is not retention expiry but
// the PITR toggle: disabling binary logging destroys every binlog, and
// re-enabling resets numbering to mysql-bin.000001, so positions from
// before the round-trip are permanently invalid on BOTH the file/pos
// and GTID paths (observed live 2026-07-16). Called only on the
// already-failing branch, so the extra SELECT costs nothing on healthy
// resumes.
func cloudSQLPositionLossHint(ctx context.Context, db *sql.DB) string {
	version, comment, err := readServerVersionStrings(ctx, db)
	if err != nil || !isCloudSQLServer(version, comment) {
		return ""
	}
	return " — note for Google Cloud SQL sources: besides ordinary retention expiry " +
		"(binlog_expire_logs_seconds, default 1 day), a binary-log disable/re-enable round-trip " +
		"(the PITR/automated-backup toggle) deletes every binlog and resets numbering to " +
		"mysql-bin.000001, permanently invalidating every position persisted before the toggle; " +
		"the auto-resnapshot (cold-start re-copy) this triggers is the correct recovery"
}
