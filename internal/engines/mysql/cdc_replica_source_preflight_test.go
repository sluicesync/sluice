// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// replicaSourceDriver is a minimal fake driver whose DSN encodes the
// scenario as "status=<mode>;lru=<mode>". Mirrors the binlogFormat /
// rowImage fixtures; kept separate so each preflight's fixture stays
// single-purpose.
//
//	status: replica          SHOW REPLICA STATUS returns one row
//	        replica_slaveonly  SHOW REPLICA STATUS errors (old server);
//	                           SHOW SLAVE STATUS returns one row
//	        none             SHOW REPLICA STATUS returns zero rows
//	        err              both spellings error (privilege sim)
//	lru:    on / off         @@GLOBAL.log_replica_updates = 1 / 0
//	        maria_on/maria_off  log_replica_updates errors 1193 (MariaDB);
//	                            log_slave_updates = 1 / 0
//	        botherr          both variable spellings error
type replicaSourceDriver struct{}

type replicaSourceConn struct{ status, lru string }

func (replicaSourceDriver) Open(dsn string) (driver.Conn, error) {
	c := &replicaSourceConn{}
	for _, kv := range strings.Split(dsn, ";") {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "status":
			c.status = v
		case "lru":
			c.lru = v
		}
	}
	return c, nil
}

func (c *replicaSourceConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *replicaSourceConn) Close() error { return nil }
func (c *replicaSourceConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

func (c *replicaSourceConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch query {
	case "SHOW REPLICA STATUS":
		switch c.status {
		case "replica":
			return &replicaStatusRows{rows: 1}, nil
		case "none":
			return &replicaStatusRows{rows: 0}, nil
		default:
			return nil, errors.New("access denied; you need (at least one of) the REPLICA MONITOR privilege(s)")
		}
	case "SHOW SLAVE STATUS":
		switch c.status {
		case "replica_slaveonly":
			return &replicaStatusRows{rows: 1}, nil
		default:
			return nil, errors.New("access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s)")
		}
	case "SELECT @@GLOBAL.log_replica_updates":
		switch c.lru {
		case "on":
			return &oneFormatRow{val: "1"}, nil
		case "off":
			return &oneFormatRow{val: "0"}, nil
		default:
			return nil, errors.New("Unknown system variable 'log_replica_updates'")
		}
	case "SELECT @@GLOBAL.log_slave_updates":
		switch c.lru {
		case "maria_on":
			return &oneFormatRow{val: "1"}, nil
		case "maria_off":
			return &oneFormatRow{val: "0"}, nil
		default:
			return nil, errors.New("Unknown system variable 'log_slave_updates'")
		}
	}
	return nil, errors.New("unexpected query: " + query)
}

// replicaStatusRows fakes SHOW REPLICA STATUS's wide row; the preflight
// only cares about row EXISTENCE, so two token columns suffice.
type replicaStatusRows struct {
	rows int
	sent int
}

func (r *replicaStatusRows) Columns() []string { return []string{"Source_Host", "Replica_IO_Running"} }
func (r *replicaStatusRows) Close() error      { return nil }
func (r *replicaStatusRows) Next(dest []driver.Value) error {
	if r.sent >= r.rows {
		return io.EOF
	}
	r.sent++
	dest[0] = "primary.example"
	dest[1] = "No"
	return nil
}

var registerReplicaSourceOnce sync.Once

func newReplicaSourceDB(t *testing.T, scenario string) *sql.DB {
	t.Helper()
	registerReplicaSourceOnce.Do(func() { sql.Register("sluice-replicasource-test", replicaSourceDriver{}) })
	db, err := sql.Open("sluice-replicasource-test", scenario)
	if err != nil {
		t.Fatalf("open replica-source db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestPreflightReplicaSource pins the G5 door in BOTH directions: the
// refusal fires only on the CONJUNCTION (configured replica AND log
// updates off), on both variable spellings (MySQL log_replica_updates,
// MariaDB's log_slave_updates-only) and both status spellings
// (SHOW REPLICA STATUS, the SHOW SLAVE STATUS fallback).
func TestPreflightReplicaSource(t *testing.T) {
	t.Parallel()

	pass := map[string]string{
		"not_a_replica_lru_off":      "status=none;lru=off", // MariaDB default posture: OFF but not a replica
		"replica_with_log_updates":   "status=replica;lru=on",
		"mariadb_replica_updates_on": "status=replica;lru=maria_on",
	}
	for name, scenario := range pass {
		if err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, scenario)); err != nil {
			t.Errorf("%s (%q) = %v; want nil", name, scenario, err)
		}
	}

	refuse := map[string]struct {
		scenario string
		spelling string
	}{
		"mysql_replica_off":       {"status=replica;lru=off", "log_replica_updates"},
		"mariadb_replica_off":     {"status=replica;lru=maria_off", "log_slave_updates"},
		"old_server_slave_status": {"status=replica_slaveonly;lru=maria_off", "log_slave_updates"},
	}
	for name, tc := range refuse {
		err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, tc.scenario))
		if err == nil {
			t.Errorf("%s (%q) = nil; want the coded refusal", name, tc.scenario)
			continue
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCReplicaNoLogUpdates {
			t.Errorf("%s: want %s; got %T: %v", name, sluicecode.CodeCDCReplicaNoLogUpdates, err, err)
			continue
		}
		for _, phrase := range []string{
			"@@GLOBAL." + tc.spelling + "=0", // names the evidence
			"silently absent",                // names the consequence
			"log_replica_updates=ON",         // the remedy
			"read-only at runtime",           // why SET GLOBAL is not the remedy
			"legitimate chained source",      // the passing sibling, so the operator doesn't over-correct
			"point the sync at the primary,", // hint mirror check below
		} {
			if !strings.Contains(err.Error(), phrase) && !strings.Contains(ce.Hint, phrase) {
				t.Errorf("%s: message+hint missing %q; got: %v (hint %q)", name, phrase, err, ce.Hint)
			}
		}
	}
}

// TestPreflightReplicaSource_ProbeFailureWarnsAndPasses: the status
// probe is privilege-gated (MariaDB 10.5+ REPLICA MONITOR split), so a
// failed probe must NOT refuse a working configuration — it degrades
// with a WARN. The refusal requires successful evidence (the PG
// replication-headroom census posture).
func TestPreflightReplicaSource_ProbeFailureWarnsAndPasses(t *testing.T) {
	t.Parallel()
	if err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, "status=err;lru=off")); err != nil {
		t.Fatalf("preflight with an unreadable replica status = %v; want nil (WARN-and-pass)", err)
	}
}

// TestPreflightReplicaSource_VariableUnreadableIsPlainError: once the
// server is a PROVEN replica, an unreadable log-updates variable (both
// spellings) is a loud plain error — sluice cannot prove the invariant
// it is about to depend on, and globals are readable by every account,
// so the connection itself is broken.
func TestPreflightReplicaSource_VariableUnreadableIsPlainError(t *testing.T) {
	t.Parallel()
	err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, "status=replica;lru=botherr"))
	if err == nil {
		t.Fatal("preflight with an unreadable log-updates variable on a proven replica = nil; want a loud error")
	}
	if _, ok := sluicecode.FromError(err); ok {
		t.Fatalf("a failed variable read must not carry the refusal code: %v", err)
	}
}
