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

// sessionInvariantsConnector holds every new physical connection [openDB]
// hands to database/sql to the two session invariants sluice's value
// fidelity depends on, by asking the session what it ended up with — one
// query, `SELECT @@session.time_zone, @@session.sql_mode`:
//
//   - time_zone must name UTC, or the connection is refused (GC-39 item 1,
//     below);
//   - sql_mode must carry NO_AUTO_VALUE_ON_ZERO, or the flag is added to the
//     live session and read back ([ensureNoAutoValueOnZero]).
//
// The time_zone half is the independent half of GC-39 item 1.
// [refuseNonUTCSessionTimeZone] parses the DSN's parameter KEYS, and a key
// it cannot parse reaches the server anyway — the driver sends every
// parameter raw as `SET <key> = <val>`, alongside the injected
// `time_zone='+00:00'`, in Go's random map order. The pre-tag review
// measured a `@@local.time_zone` key slipping the first cut and 1–7 of 30
// fresh connections per spelling ending on +09:00: a random fraction of
// the pool silently shifting every TIMESTAMP. This check asks the session
// what it ended up with, so it reaches every spelling without parsing any
// of them. The sql_mode half reaches every spelling the same way. Cost: one
// single-row query per new physical connection (a pooled connection is
// checked once, when it is opened), plus one SET and one read-back only on
// a connection whose sql_mode sluice did not inject itself.
//
// Reach: every connection opened through [openDB], which is every MySQL,
// MariaDB, PlanetScale and Vitess engine connection — and
// TestMySQLConnectionRoster_EveryPoolGoesThroughOpenDB holds every MySQL
// pool constructor in the tree to that one site. Exempt, stated:
// internal/planetscale/expandcontract opens its own connector to a
// just-minted branch credential to run operator DDL only — no operator
// DSN parameters and no row values cross it.
//
// A server that cannot answer the query (an error, not a verdict) is let
// through with one WARN rather than refused: the DSN parser and the
// injected sql_mode are the primary doors, and a proxy that rejects the
// read must not take every connection down with it. vtgate serves the read
// and the sql_mode repair: measured on vttestserver by
// TestVStream_SessionInvariants_ThroughVTGate.
type sessionInvariantsConnector struct{ driver.Connector }

var sessionInvariantsProbeWarnOnce sync.Once

func (c sessionInvariantsConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	zone, sqlMode, err := readSessionInvariants(ctx, conn)
	if err != nil {
		sessionInvariantsProbeWarnOnce.Do(func() {
			slog.Warn("mysql: could not read @@session.time_zone / @@session.sql_mode on a new connection; "+
				"relying on the DSN checks and the injected sql_mode alone",
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
	if err := ensureNoAutoValueOnZero(ctx, conn, sqlMode); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// readSessionInvariants reads the session's time_zone and sql_mode in one
// round trip.
func readSessionInvariants(ctx context.Context, conn driver.Conn) (zone, sqlMode string, err error) {
	vals, err := querySessionStrings(ctx, conn, "SELECT @@session.time_zone, @@session.sql_mode", 2)
	if err != nil {
		return "", "", err
	}
	return vals[0], vals[1], nil
}

// querySessionStrings runs q on the raw driver connection and returns the
// first row's n columns as strings.
func querySessionStrings(ctx context.Context, conn driver.Conn, q string, n int) ([]string, error) {
	qc, ok := conn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New("driver connection cannot run a query")
	}
	rows, err := qc.QueryContext(ctx, q, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	dest := make([]driver.Value, n)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("no row")
		}
		return nil, err
	}
	out := make([]string, n)
	for i, d := range dest {
		switch v := d.(type) {
		case []byte:
			out[i] = string(v)
		case string:
			out[i] = v
		default:
			return nil, fmt.Errorf("unexpected %T", d)
		}
	}
	return out, nil
}

// addNoAutoValueOnZeroSQL adds the flag to whatever the session's sql_mode
// already is — the operator's DSN value or the server default — rather
// than replacing it: the strictness choice stays the operator's, only the
// fidelity flag is sluice's. CONCAT_WS skips the NULL an empty mode
// becomes, so an empty server mode yields exactly the one flag.
const addNoAutoValueOnZeroSQL = "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), '" +
	noAutoValueOnZero + "')"

// ensureNoAutoValueOnZero makes the session carry [noAutoValueOnZero]. mode
// is the session's sql_mode as just read. When sluice injected the mode
// itself it already carries the flag ([withRequiredSQLModes]) and this costs
// nothing; on the two tiers where sluice injects nothing — a DSN `sql_mode=`
// in any key spelling, and the empty --mysql-sql-mode escape hatch — it adds the
// flag to the live session and reads the result back. A server that will
// not take it is refused, with marker NO-AUTO-VALUE-ON-ZERO-UNSET: every
// write into an AUTO_INCREMENT column through this session would turn a
// carried 0 into a generated value, silently.
func ensureNoAutoValueOnZero(ctx context.Context, conn driver.Conn, mode string) error {
	if sqlModeHas(mode, noAutoValueOnZero) {
		return nil
	}
	refuse := func(cause error) error {
		return fmt.Errorf("mysql: NO-AUTO-VALUE-ON-ZERO-UNSET: a new connection's session sql_mode %q lacks %s and "+
			"sluice could not add it: without it MySQL turns every 0 written into an AUTO_INCREMENT column into the "+
			"next generated value, so a source row keyed 0 would land under a different key, silently. Make the "+
			"server accept `SET SESSION sql_mode` (or include %s in any DSN sql_mode value): %w",
			mode, noAutoValueOnZero, noAutoValueOnZero, cause)
	}
	ec, ok := conn.(driver.ExecerContext)
	if !ok {
		return refuse(errors.New("driver connection cannot execute a statement"))
	}
	if _, err := ec.ExecContext(ctx, addNoAutoValueOnZeroSQL, nil); err != nil {
		return refuse(err)
	}
	got, err := querySessionStrings(ctx, conn, "SELECT @@session.sql_mode", 1)
	if err != nil {
		return refuse(err)
	}
	if !sqlModeHas(got[0], noAutoValueOnZero) {
		return refuse(fmt.Errorf("the session reads back sql_mode %q after the SET", got[0]))
	}
	return nil
}
