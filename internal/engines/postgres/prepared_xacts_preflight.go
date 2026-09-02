// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// M2 capture-completeness sweep, P3 residual — pending prepared
// transactions (2PC) block logical-slot creation, silently.
//
// Observed on real PG 16 (2026-08-26, capture-completeness matrix
// §pgoutput 2PC row): a transaction sitting in PREPARE TRANSACTION
// state makes CREATE_REPLICATION_SLOT hang — the server's
// consistent-point builder treats the prepared tx as in-progress and
// waits for it to resolve, and sluice sets no statement timeout on
// that path — so a cold start (or a backup anchor) stalls indefinitely
// with NO message. Healthy 2PC usage resolves its transactions in
// milliseconds; the classic stuck-2PC hazard is an ORPHANED prepared
// tx whose coordinator died, which blocks forever. Once the tx
// resolves, slot creation proceeds and capture is CORRECT (the
// tx's commit lands ahead of the consistent point and is carried by
// the exported cold-copy snapshot) — this is purely a loud-failure
// fix: the WARN makes the hang legible, it does not refuse.
//
// [warnPreparedTransactions] runs immediately before slot creation at
// the single chokepoint [createLogicalReplicationSlot], which every
// slot-creating caller reaches (the enumeration lives on that
// function). Failure posture: a probe error WARNs ("cannot rule the
// block out") and passes — the SL-1 discipline; the slot-create right
// after will surface a genuinely broken connection loudly.

// preparedXactMarker is the grep-stable marker both P3 WARNs (the
// firing census and the degraded probe-error shape) carry; the tests
// and mutation runs key on it.
const preparedXactMarker = "PREPARED-XACT-BLOCKS-SLOT-CREATE"

// preparedXactProbeTimeout bounds the census query so a half-dead
// pooled connection cannot itself hang the open this WARN exists to
// make legible. pg_prepared_xacts is a tiny in-memory view; a healthy
// server answers in milliseconds.
const preparedXactProbeTimeout = 15 * time.Second

// preparedXactWarnListCap bounds how many gids one WARN line
// enumerates; the count in the message always names the full total.
const preparedXactWarnListCap = 5

// preparedXactQuery lists every pending prepared transaction on the
// CLUSTER, oldest first — the consistent-point builder waits on the
// global xmin horizon, so a prepared tx in ANOTHER database blocks
// this database's slot creation just the same (which is why the
// database column is carried into the WARN).
const preparedXactQuery = `
	SELECT gid, database, owner,
	       GREATEST(EXTRACT(EPOCH FROM (pg_catalog.now() - prepared)), 0)::bigint
	FROM   pg_prepared_xacts
	ORDER  BY prepared`

// warnPreparedTransactions WARNs (never fails) when pg_prepared_xacts
// has rows: the CREATE_REPLICATION_SLOT the caller is about to send
// will block until every listed transaction is resolved. See the file
// comment for the mechanism and posture.
func warnPreparedTransactions(ctx context.Context, db *sql.DB, slotName string) {
	pctx, cancel := context.WithTimeout(ctx, preparedXactProbeTimeout)
	defer cancel()

	rows, err := db.QueryContext(pctx, preparedXactQuery)
	if err != nil {
		slog.WarnContext(ctx, "postgres: cdc: "+preparedXactMarker+": could not read pg_prepared_xacts before "+
			"creating the replication slot; if a prepared transaction (2PC) is pending anywhere on the cluster, "+
			"CREATE_REPLICATION_SLOT will block until it is resolved and this probe could not rule that out",
			slog.String("slot", slotName),
			slog.String("error", err.Error()))
		return
	}
	defer func() { _ = rows.Close() }()

	type preparedXact struct {
		gid, database, owner string
		ageSeconds           int64
	}
	var xacts []preparedXact
	for rows.Next() {
		var x preparedXact
		if err := rows.Scan(&x.gid, &x.database, &x.owner, &x.ageSeconds); err != nil {
			slog.WarnContext(ctx, "postgres: cdc: "+preparedXactMarker+": could not read the pg_prepared_xacts "+
				"census rows; the census is incomplete and this probe could not rule a pending prepared "+
				"transaction out",
				slog.String("slot", slotName),
				slog.String("error", err.Error()))
			return
		}
		xacts = append(xacts, x)
	}
	if err := rows.Err(); err != nil {
		slog.WarnContext(ctx, "postgres: cdc: "+preparedXactMarker+": could not read the pg_prepared_xacts "+
			"census rows; the census is incomplete and this probe could not rule a pending prepared "+
			"transaction out",
			slog.String("slot", slotName),
			slog.String("error", err.Error()))
		return
	}
	if len(xacts) == 0 {
		return
	}

	descs := make([]string, 0, min(len(xacts), preparedXactWarnListCap))
	for i, x := range xacts {
		if i == preparedXactWarnListCap {
			descs = append(descs, fmt.Sprintf("… and %d more", len(xacts)-preparedXactWarnListCap))
			break
		}
		descs = append(descs, fmt.Sprintf("gid=%q database=%s owner=%s age=%s",
			x.gid, x.database, x.owner, (time.Duration(x.ageSeconds)*time.Second).String()))
	}
	slog.WarnContext(ctx, "postgres: cdc: "+preparedXactMarker+": the cluster has pending prepared "+
		"transactions (2PC), and creating replication slot "+fmt.Sprintf("%q", slotName)+" will BLOCK with no "+
		"further output until every one of them is resolved — the server's consistent-point builder waits on "+
		"them, wherever their database. If a listed transaction is orphaned (its coordinator died), resolve it "+
		"on the source: COMMIT PREPARED '<gid>' or ROLLBACK PREPARED '<gid>'. Pending: "+strings.Join(descs, "; "),
		slog.Int("prepared_xact_count", len(xacts)))
}
