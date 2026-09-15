// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
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
// AND log updates are off AND at least one channel could still be
// applying replicated writes — at the same three CDC-open chokepoints as
// preflightBinlogFormat (StreamChanges + both snapshot openers, roster
// TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights). A
// replica WITH log_replica_updates=ON is a legitimate chained-
// replication source and must keep passing; a non-replica with the
// variable off (the MariaDB default) is likewise fine — its own writes
// are all local and all logged. This is the MySQL twin of the Postgres
// standby door (CodeCDCStandbySource, pg_is_in_recovery()).
//
// A server whose EVERY configured channel has both its IO and SQL
// threads stopped is ACCEPTED, with an INFO carrying
// [replicaChannelsStoppedMarker] (operator decision 2026-09-15, audit
// 2026-09-15 A0915-CLI-MEDIUM-1). That is the ordinary state of a primary
// promoted after a failover that ran STOP REPLICA but not RESET REPLICA
// ALL: no replicated write can arrive, local writes are the whole
// workload, and every one of them is binlogged. Until then the door keyed
// on the channel RECORD and refused it, with remedies that could not run
// on that server. A thread that runs, is Connecting/Preparing, or whose
// state cannot be read keeps the refusal — only positive evidence that
// nothing is being applied releases it.
//
// Spellings: SHOW REPLICA STATUS is MySQL 8.0.22+ / MariaDB 10.5.1+;
// SHOW SLAVE STATUS is the fallback for older servers. On MariaDB the
// bare forms list ONLY the default connection — a `CHANGE MASTER
// 'name' TO …` named multi-source connection returns ZERO rows there
// (observed on mariadb:11.4, audit 2026-08-27 A1), so the probe also
// runs the MariaDB-only SHOW ALL REPLICAS STATUS / SHOW ALL SLAVES
// STATUS spellings; see sourceReplicaChannels for the posture that
// keeps MySQL's syntax error on the ALL forms from degrading the door.
// The variable is
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
// excuse (globals are readable by every account) and fails loudly. The
// thread-state columns ride the SAME rows that proved the server a
// replica, so "a channel exists but its state is unreadable" is evidence
// of a replica with nothing proving it idle — it refuses.
//
// Accepted residue (the format door's session-override class, one shape
// over): a source rewired into a replica AFTER the preflight passes —
// and, since the stopped-channel acceptance, a stopped channel STARTED
// after it passes — is a TOCTOU this start-time gate cannot see until the
// next CDC (re)open re-runs it; the GTID-mode resnapshot churn then
// surfaces it loudly, file/pos mode does not. The acceptance INFO says
// so, so the operator who restarts replication on that server has read it.

// replicaChannelsStoppedMarker is the grep-stable marker on the INFO a
// server with only stopped channels is accepted under.
const replicaChannelsStoppedMarker = "REPLICA-CHANNELS-STOPPED"

// replicaSourceRemedyHint is the machine-readable remedy carried on the
// coded refusal, mirroring the prose in the error message.
//
// The refused server has a channel that could be applying replicated
// writes (see [replicaChannel.stopped]), so it names what makes each kind
// of server pass: the primary is the right endpoint for a replica; a
// promoted primary whose channel is still running passes once the channel
// is stopped (STOP REPLICA) or cleared (RESET REPLICA ALL — through
// v0.153.2 that remedy appeared only in the door's own integration test,
// audit 2026-09-15 A0915-CLI-MEDIUM-1); a deliberate chained replica passes
// with log_replica_updates=ON.
const replicaSourceRemedyHint = "point the sync at the primary; if THIS server is the primary (promoted after a " +
	"failover, its replication channel still running or connecting), stop replication on it (STOP REPLICA — a " +
	"server whose channels are all stopped is accepted) or clear the channel with RESET REPLICA ALL (MariaDB: " +
	"RESET REPLICA 'connection_name' ALL) — its direct writes are binlogged; or restart mysqld with " +
	"log_replica_updates=ON (the variable is read-only at runtime); then re-run"

// dbQuerier is the query surface the M2 preflights need — both the
// single-row form ([rowQuerier]) and the multi-row/any-column form —
// satisfied by *sql.DB and *sql.Conn alike.
type dbQuerier interface {
	rowQuerier
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// replicaChannel is one configured replication channel (MySQL) or
// connection (MariaDB) as a status probe reported it.
type replicaChannel struct {
	// name is Channel_Name (MySQL) / Connection_name (MariaDB); "" is
	// the default channel, and also a server that reports no name column.
	name string
	// ioRunning / sqlRunning are the raw Replica_IO_Running /
	// Replica_SQL_Running values (Slave_* on older servers and MariaDB);
	// "" when the column is absent or NULL.
	ioRunning, sqlRunning string
}

// stopped reports whether both threads are reported "No". Every other
// value — "Yes", "Connecting", MariaDB's "Preparing", an absent or NULL
// column — is not-stopped, the refusing direction: only a server's
// positive statement that neither thread runs releases the door.
func (c replicaChannel) stopped() bool {
	return strings.EqualFold(strings.TrimSpace(c.ioRunning), "No") &&
		strings.EqualFold(strings.TrimSpace(c.sqlRunning), "No")
}

// label renders the channel for a log line or a refusal.
func (c replicaChannel) label() string {
	name := c.name
	if name == "" {
		name = "(default)"
	}
	return fmt.Sprintf("%s [IO=%s SQL=%s]", name, orUnreadable(c.ioRunning), orUnreadable(c.sqlRunning))
}

func orUnreadable(v string) string {
	if strings.TrimSpace(v) == "" {
		return "unreadable"
	}
	return v
}

// preflightReplicaSource refuses (coded,
// [sluicecode.CodeCDCReplicaNoLogUpdates]) when the source is a
// configured replica whose log_replica_updates is OFF and at least one
// channel is not stopped. See the file comment for the mechanism,
// spellings, and failure posture.
func preflightReplicaSource(ctx context.Context, q dbQuerier) error {
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()

	channels, err := sourceReplicaChannels(pctx, q)
	if err != nil {
		// Privilege-gated probe: WARN and pass, never refuse a working
		// configuration on a failed read (see file comment). The gap this
		// leaves open is stated rather than implied.
		slog.WarnContext(ctx, "mysql: cdc: could not read SHOW REPLICA STATUS in any spelling "+
			"(SLAVE fallback and MariaDB ALL forms included); "+
			"the replica-source preflight is degraded — if this source is itself a replica with "+
			"log_replica_updates=OFF, replicated writes are absent from its binlog and the CDC tail "+
			"would be silently empty for them. Grant REPLICATION CLIENT (MariaDB 10.5+: REPLICA MONITOR) "+
			"to restore the check",
			slog.String("error", err.Error()))
		return nil
	}
	if len(channels) == 0 {
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

	var stopped, live []string
	for _, c := range channels {
		if c.stopped() {
			stopped = append(stopped, c.label())
		} else {
			live = append(live, c.label())
		}
	}
	sort.Strings(stopped)
	sort.Strings(live)
	if len(live) == 0 {
		// Every channel's IO and SQL threads are stopped: nothing can be
		// replicated INTO this server, so its binlog is complete for the
		// writes it takes (see the file comment for the decision and the
		// TOCTOU it accepts).
		slog.InfoContext(ctx, "mysql: cdc: "+replicaChannelsStoppedMarker+": the source has replication channel(s) "+
			"configured but every one has its IO and SQL threads stopped, so no replicated write can arrive and "+
			"every write this server takes is its own and binlogged — accepted as a CDC source despite @@GLOBAL."+
			spelling+"=0 (the usual state of a primary promoted after a failover). The channel record is "+
			"bookkeeping only; RESET REPLICA ALL (MariaDB: RESET REPLICA 'connection_name' ALL) clears it. If "+
			"replication is STARTED on this server again, its replicated writes will not reach its binlog and "+
			"this stream will not see them — the check re-runs only when the stream is next opened",
			slog.String("stopped_channels", strings.Join(stopped, "; ")))
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCReplicaNoLogUpdates,
		replicaSourceRemedyHint,
		fmt.Errorf(
			"mysql: cdc: the source is itself a replica with @@GLOBAL.%s=0 and a replication channel that is "+
				"not stopped (%s): writes replicated from its primary are applied by the SQL thread but never "+
				"enter THIS server's binlog, so sluice's CDC would stream only local writes — the replicated "+
				"traffic is silently absent while the stream stays green (and on GTID resume the advanced "+
				"gtid_purged forces a perpetual resnapshot loop misdiagnosed as retention loss; ground-truthed "+
				"on a real linked mysql:8.0 pair, 2026-08-26). Point the sync at the primary instead. If THIS "+
				"server IS the primary — promoted after a failover, its channel still running or connecting — "+
				"stop replication on it with STOP REPLICA (a server whose channels all have both threads stopped "+
				"is accepted), or clear the channel with RESET REPLICA ALL (MariaDB: RESET REPLICA "+
				"'connection_name' ALL); the direct writes it takes are binlogged. Otherwise restart mysqld with "+
				"log_replica_updates=ON — the variable is read-only at runtime, so SET GLOBAL cannot fix it. A "+
				"replica WITH log_replica_updates=ON is a legitimate chained source and passes this check. "+
				"Then re-run",
			spelling, strings.Join(live, "; "),
		),
	)
}

// sourceReplicaChannels returns every configured replication channel
// the server reports, with its thread state. Two spelling families are
// probed, because they see DIFFERENT channel sets:
//
//   - SHOW REPLICA STATUS (SHOW SLAVE STATUS on pre-8.0.22 /
//     pre-10.5.1 servers): on MySQL one row per channel, FOR CHANNEL
//     multi-source included; on MariaDB only the DEFAULT connection —
//     a `CHANGE MASTER 'name' TO …` named connection returns ZERO
//     rows here (observed on mariadb:11.4, audit 2026-08-27 A1),
//     exactly the multi-source replica the G5 door exists to catch.
//   - SHOW ALL REPLICAS STATUS (SHOW ALL SLAVES STATUS on
//     pre-10.5.1): MariaDB-only syntax listing EVERY connection,
//     named ones included. MySQL rejects both with a 1064 syntax
//     error.
//
// Both families are ALWAYS probed and their rows returned together.
// Until the stopped-channel acceptance a bare-form row answered the
// only question (is it a replica?) on its own; now every channel's state
// matters, and a MariaDB with a stopped default connection and a RUNNING
// named one shows only the stopped one to the bare form. A channel seen
// by both families appears twice, which the caller's "every channel is
// stopped" question does not mind; a disagreement between the two reads
// counts as not-stopped.
//
// Posture: an ALL-form error after a bare form SUCCEEDED is tolerated
// WITHOUT degrading the door — on every server where the ALL syntax
// exists (MariaDB) it is readable under the same privilege as the bare
// form, so bare-success + ALL-error identifies a MySQL, whose bare form
// already enumerates every channel: there is no blind spot to WARN about.
// Only when NO spelling succeeds does the caller take the
// privilege-blocked WARN-and-pass posture.
func sourceReplicaChannels(ctx context.Context, q dbQuerier) ([]replicaChannel, error) {
	var (
		lastErr       error
		channels      []replicaChannel
		bareSucceeded bool
	)
	for _, stmt := range []string{"SHOW REPLICA STATUS", "SHOW SLAVE STATUS"} {
		chans, err := readReplicaChannels(ctx, q, stmt)
		if err != nil {
			lastErr = err
			continue
		}
		channels, bareSucceeded = chans, true
		break
	}
	for _, stmt := range []string{"SHOW ALL REPLICAS STATUS", "SHOW ALL SLAVES STATUS"} {
		chans, err := readReplicaChannels(ctx, q, stmt)
		if err != nil {
			lastErr = err
			continue
		}
		return append(channels, chans...), nil
	}
	if bareSucceeded {
		// Neither ALL spelling exists (MySQL's 1064): the bare probe's
		// channel view was already complete there, so its answer stands,
		// undegraded (see the posture note above).
		return channels, nil
	}
	return nil, lastErr
}

// readReplicaChannels runs one status spelling and reads each row's
// channel name and thread state by COLUMN NAME — the row is wide, its
// order is a server detail, and the thread-state columns are spelled
// Replica_* on MySQL 8.0.22+ and Slave_* on older MySQL and on MariaDB.
// A row missing either column, or one that fails to scan, is returned
// with that state "" (not stopped), never skipped: the row itself is the
// evidence of a channel, and a failed read of it must not turn into the
// no-spelling-succeeded WARN-and-pass.
func readReplicaChannels(ctx context.Context, q dbQuerier, stmt string) ([]replicaChannel, error) {
	rows, err := q.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var chans []replicaChannel
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		var c replicaChannel
		if err := rows.Scan(ptrs...); err != nil {
			chans = append(chans, c)
			continue
		}
		for i, col := range cols {
			switch strings.ToLower(col) {
			case "channel_name", "connection_name":
				c.name = vals[i].String
			case "replica_io_running", "slave_io_running":
				c.ioRunning = vals[i].String
			case "replica_sql_running", "slave_sql_running":
				c.sqlRunning = vals[i].String
			}
		}
		chans = append(chans, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return chans, nil
}
