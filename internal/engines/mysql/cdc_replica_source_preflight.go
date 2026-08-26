// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// M2 capture-completeness sweep G5 — replica-source preflight.
//
// A server that is ITSELF a replica applies replicated writes through
// its SQL thread, and with log_replica_updates=OFF those writes never
// enter its own binlog. Ground-truthed on a real linked mysql:8.0.46
// pair (2026-08-26, capture-completeness matrix §binlog): replicated
// rows are SQL-visible — a cold copy sees them — while the replica's
// binlog position stays frozen; a direct local write IS logged, so the
// stream looks alive while every replicated transaction is silently
// absent. Sharper still, the replicated GTIDs land in gtid_executed AND
// gtid_purged (purged = executed − binlogged), so on every GTID-mode
// restart verifyGTIDSetReachable sees an "unreachable" position and the
// ADR-0093 auto-resnapshot fires: a perpetual silent-window/resnapshot
// churn loop misdiagnosed as retention loss. In file/pos mode there is
// no tripwire at all — fully silent forever.
//
// [preflightReplicaSource] therefore refuses the CONJUNCTION — the
// server is a configured replica (SHOW REPLICA STATUS returns a row)
// AND log updates are off — at the same three CDC-open chokepoints as
// preflightBinlogFormat (StreamChanges + both snapshot openers, roster
// TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights). A
// replica WITH log_replica_updates=ON is a legitimate chained-
// replication source and must keep passing; a non-replica with the
// variable off (the MariaDB default) is likewise fine — its own writes
// are all local and all logged. This is the MySQL twin of the Postgres
// standby door (CodeCDCStandbySource, pg_is_in_recovery()).
//
// Spellings: SHOW REPLICA STATUS is MySQL 8.0.22+ / MariaDB 10.5.1+;
// SHOW SLAVE STATUS is the fallback for older servers. The variable is
// read as @@GLOBAL.log_replica_updates first (MySQL 8.0.26+; the only
// spelling guaranteed on a future MySQL that drops the alias) falling
// back to @@GLOBAL.log_slave_updates (readable on BOTH mysql:8.0 and
// mariadb:11.4 — verified live; MariaDB has NO log_replica_updates and
// errors 1193 on it). Scope: binlog lane only — the VStream lane's
// replica-tablet stream is a different mechanism whose tablet mysqld
// config is Vitess-owned (matrix: CAPTURED); vtgate never reaches these
// paths.
//
// Failure posture, deliberately ASYMMETRIC (unlike the format
// preflight's read-must-succeed rule): SHOW REPLICA STATUS is
// privilege-gated (REPLICATION CLIENT on MySQL; the split-out REPLICA
// MONITOR on MariaDB 10.5+, which a minimally-granted CDC user may
// lack even while SHOW MASTER STATUS works), so a failed status probe
// WARNs — naming the blind spot — and passes rather than refusing a
// working configuration; the refusal requires successful evidence
// (mirrors the PG replication-headroom census posture). The VARIABLE
// read, only attempted once the server is a PROVEN replica, has no such
// excuse (globals are readable by every account) and fails loudly.
//
// Accepted residue (the format door's session-override class, one shape
// over): a source rewired into a replica AFTER the preflight passes is
// a TOCTOU this start-time gate cannot see; the GTID-mode resnapshot
// churn then surfaces it loudly, file/pos mode does not.

// replicaSourceRemedyHint is the machine-readable remedy carried on the
// coded refusal, mirroring the prose in the error message.
const replicaSourceRemedyHint = "point the sync at the primary, or restart mysqld with " +
	"log_replica_updates=ON (the variable is read-only at runtime), then re-run"

// dbQuerier is the query surface the M2 preflights need — both the
// single-row form ([rowQuerier]) and the multi-row/any-column form —
// satisfied by *sql.DB and *sql.Conn alike.
type dbQuerier interface {
	rowQuerier
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// preflightReplicaSource refuses (coded,
// [sluicecode.CodeCDCReplicaNoLogUpdates]) when the source is a
// configured replica whose log_replica_updates is OFF. See the file
// comment for the mechanism, spellings, and failure posture.
func preflightReplicaSource(ctx context.Context, q dbQuerier) error {
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()

	isReplica, err := sourceIsConfiguredReplica(pctx, q)
	if err != nil {
		// Privilege-gated probe: WARN and pass, never refuse a working
		// configuration on a failed read (see file comment). The gap this
		// leaves open is stated rather than implied.
		slog.WarnContext(ctx, "mysql: cdc: could not read SHOW REPLICA STATUS (nor SHOW SLAVE STATUS); "+
			"the replica-source preflight is degraded — if this source is itself a replica with "+
			"log_replica_updates=OFF, replicated writes are absent from its binlog and the CDC tail "+
			"would be silently empty for them. Grant REPLICATION CLIENT (MariaDB 10.5+: REPLICA MONITOR) "+
			"to restore the check",
			slog.String("error", err.Error()))
		return nil
	}
	if !isReplica {
		return nil
	}

	spelling := "log_replica_updates"
	var raw string
	if err := q.QueryRowContext(pctx, "SELECT @@GLOBAL.log_replica_updates").Scan(&raw); err != nil {
		// MariaDB (and MySQL < 8.0.26) has only the log_slave_updates
		// spelling; error 1193 routes here. A failure on BOTH spellings is
		// a broken connection or an unrecognizable server — loud, uncoded
		// (a failed read is not evidence of blindness, and the refusal's
		// remedy would be wrong advice).
		spelling = "log_slave_updates"
		if err2 := q.QueryRowContext(pctx, "SELECT @@GLOBAL.log_slave_updates").Scan(&raw); err2 != nil {
			return fmt.Errorf("mysql: cdc: source is a configured replica but neither "+
				"@@GLOBAL.log_replica_updates nor @@GLOBAL.log_slave_updates is readable: %w", err2)
		}
	}
	if v := strings.TrimSpace(raw); v != "0" && !strings.EqualFold(v, "OFF") {
		// Log updates ON: a chained replica is a legitimate CDC source —
		// its binlog carries the replicated writes too.
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCReplicaNoLogUpdates,
		replicaSourceRemedyHint,
		fmt.Errorf(
			"mysql: cdc: the source is itself a replica with @@GLOBAL.%s=0: writes replicated from its "+
				"primary are applied by the SQL thread but never enter THIS server's binlog, so sluice's CDC "+
				"would stream only local writes — the replicated traffic is silently absent while the stream "+
				"stays green (and on GTID resume the advanced gtid_purged forces a perpetual resnapshot loop "+
				"misdiagnosed as retention loss; ground-truthed on a real linked mysql:8.0 pair, 2026-08-26). "+
				"Point the sync at the primary instead, or restart mysqld with log_replica_updates=ON — the "+
				"variable is read-only at runtime, so SET GLOBAL cannot fix it. A replica WITH "+
				"log_replica_updates=ON is a legitimate chained source and passes this check. Then re-run",
			spelling,
		),
	)
}

// sourceIsConfiguredReplica reports whether the server has at least one
// configured replication channel — SHOW REPLICA STATUS (SHOW SLAVE
// STATUS on pre-8.0.22 / pre-10.5.1 servers) returns one row per
// channel and an empty set on a non-replica. A configured-but-stopped
// channel counts: the blindness begins the moment its threads start,
// and a channel's presence is the operator's stated intent.
func sourceIsConfiguredReplica(ctx context.Context, q dbQuerier) (bool, error) {
	var lastErr error
	for _, stmt := range []string{"SHOW REPLICA STATUS", "SHOW SLAVE STATUS"} {
		has, err := queryReturnsRows(ctx, q, stmt)
		if err != nil {
			lastErr = err
			continue
		}
		return has, nil
	}
	return false, lastErr
}

// queryReturnsRows reports whether stmt returns at least one row,
// discarding the row content.
func queryReturnsRows(ctx context.Context, q dbQuerier, stmt string) (bool, error) {
	rows, err := q.QueryContext(ctx, stmt)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	has := rows.Next()
	if err := rows.Err(); err != nil {
		return false, err
	}
	return has, nil
}
