// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// utcSessionConnector is the independent half of GC-39 item 1: every new
// physical connection [openDB] hands to database/sql reads back its own
// `@@session.time_zone` and is refused unless it names UTC.
//
// [refuseNonUTCSessionTimeZone] parses the DSN's parameter KEYS, and a key
// it cannot parse reaches the server anyway — the driver sends every
// parameter raw as `SET <key> = <val>`, alongside the injected
// `time_zone='+00:00'`, in Go's random map order. The pre-tag review
// measured a `@@local.time_zone` key slipping the first cut and 1–7 of 30
// fresh connections per spelling ending on +09:00: a random fraction of
// the pool silently shifting every TIMESTAMP. This check asks the session
// what it ended up with, so it reaches every spelling without parsing any
// of them. Cost: one single-row query per new physical connection (a
// pooled connection is checked once, when it is opened).
//
// Reach: every connection opened through [openDB], which is every MySQL,
// MariaDB, PlanetScale and Vitess engine connection. Exempt, stated:
// internal/planetscale/expandcontract opens its own connector to a
// just-minted branch credential to run operator DDL only — no operator
// DSN parameters and no TIMESTAMP values cross it.
//
// A server that cannot answer the query (an error, not a verdict) is let
// through with one WARN rather than refused: the DSN parser is the primary
// door, and a proxy that rejects the read must not take every connection
// down with it. UNVERIFIED PREMISE: vtgate is expected to serve the read
// (time_zone is in its system-variable list), but no pin runs this probe
// through vtgate.
type utcSessionConnector struct{ driver.Connector }

var sessionTZProbeWarnOnce sync.Once

func (c utcSessionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	zone, err := readSessionTimeZone(ctx, conn)
	if err != nil {
		sessionTZProbeWarnOnce.Do(func() {
			slog.Warn("mysql: could not read @@session.time_zone on a new connection; relying on the DSN check alone",
				slog.String("err", err.Error()))
		})
		return conn, nil //nolint:nilerr // a probe that cannot answer is not a verdict; see the type doc
	}
	if !sessionTimeZoneIsUTC(zone) {
		_ = conn.Close()
		return nil, fmt.Errorf("mysql: DSN-TIME-ZONE-NOT-UTC: a new connection's session time_zone is %q, not UTC: "+
			"sluice reads and writes MySQL TIMESTAMP values as UTC instants, so this session would shift every one "+
			"by the zone's offset, silently. A DSN parameter set it; remove every time_zone setting from the DSN "+
			"(in any spelling), or set it to '+00:00'", zone)
	}
	return conn, nil
}

func readSessionTimeZone(ctx context.Context, conn driver.Conn) (string, error) {
	q, ok := conn.(driver.QueryerContext)
	if !ok {
		return "", errors.New("driver connection cannot run a query")
	}
	rows, err := q.QueryContext(ctx, "SELECT @@session.time_zone", nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return "", errors.New("no row")
		}
		return "", err
	}
	switch v := dest[0].(type) {
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	}
	return "", fmt.Errorf("unexpected %T", dest[0])
}
