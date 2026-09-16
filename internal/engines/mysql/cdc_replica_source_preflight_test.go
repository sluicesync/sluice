// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// replicaSourceDriver is a minimal fake driver whose DSN encodes the
// scenario as "status=<mode>;lru=<mode>". Mirrors the binlogFormat /
// rowImage fixtures; kept separate so each preflight's fixture stays
// single-purpose.
//
//	status: replica            SHOW REPLICA STATUS: one RUNNING row (Yes/Yes)
//	        replica_slaveonly  SHOW REPLICA STATUS errors (old server);
//	                           SHOW SLAVE STATUS: one RUNNING row (Slave_* columns)
//	        stopped            SHOW REPLICA STATUS: one row, No/No; ALL forms 1064
//	        never_started      as stopped — a channel configured and never started
//	                           reports No/No too
//	        io_connecting      one row, IO=Connecting SQL=Yes (START REPLICA against
//	                           an unreachable source)
//	        sql_only           one row, IO=No SQL=Yes
//	        two_one_running    two rows (FOR CHANNEL): ch_a No/No, ch_b Yes/Yes
//	        two_stopped        two rows, both No/No
//	        unreadable         one row carrying NO thread-state columns
//	        null_state         one row whose thread-state columns are NULL
//	        none               SHOW REPLICA STATUS returns zero rows;
//	                           ALL forms error 1064 (the MySQL shape)
//	        maria_none         bare + ALL REPLICAS forms both zero rows
//	        maria_named        SHOW REPLICA STATUS returns ZERO rows;
//	                           SHOW ALL REPLICAS STATUS: one RUNNING row
//	                           (a CHANGE MASTER 'name' TO named
//	                           connection — audit 2026-08-27 A1)
//	        maria_named_stopped  as maria_named, the row No/No
//	        maria_default_stopped_named_running
//	                           bare: the default connection No/No; ALL: the
//	                           default No/No AND conn1 Yes/Yes — the shape
//	                           only the ALL probe can refuse
//	        maria_named_old    bare REPLICA + ALL REPLICAS error (old
//	                           MariaDB); SHOW SLAVE STATUS zero rows;
//	                           SHOW ALL SLAVES STATUS: one RUNNING row
//	        err                all four spellings error (privilege sim)
//	lru:    on / off           @@GLOBAL.log_replica_updates = 1 / 0
//	        maria_on/maria_off log_replica_updates errors 1193 (MariaDB);
//	                           log_slave_updates = 1 / 0
//	        botherr            both variable spellings error
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

// Status-row shapes, by column family.
var (
	mysqlStatusCols = []string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running", "Channel_Name"}
	slaveStatusCols = []string{"Master_Host", "Slave_IO_Running", "Slave_SQL_Running"}
	mariaStatusCols = []string{"Connection_name", "Master_Host", "Slave_IO_Running", "Slave_SQL_Running"}
)

func mysqlRow(channel, ioThread, sqlThread string) []driver.Value {
	return []driver.Value{"primary.example", ioThread, sqlThread, channel}
}

func mariaRow(conn, ioThread, sqlThread string) []driver.Value {
	return []driver.Value{conn, "primary.example", ioThread, sqlThread}
}

func (c *replicaSourceConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	denied := errors.New("access denied; you need (at least one of) the REPLICA MONITOR privilege(s)")
	switch query {
	case "SHOW REPLICA STATUS":
		switch c.status {
		case "replica":
			return statusRows(mysqlStatusCols, mysqlRow("", "Yes", "Yes")), nil
		case "stopped", "never_started":
			return statusRows(mysqlStatusCols, mysqlRow("", "No", "No")), nil
		case "io_connecting":
			return statusRows(mysqlStatusCols, mysqlRow("", "Connecting", "Yes")), nil
		case "sql_only":
			return statusRows(mysqlStatusCols, mysqlRow("", "No", "Yes")), nil
		case "two_one_running":
			return statusRows(mysqlStatusCols, mysqlRow("ch_a", "No", "No"), mysqlRow("ch_b", "Yes", "Yes")), nil
		case "two_stopped":
			return statusRows(mysqlStatusCols, mysqlRow("ch_a", "No", "No"), mysqlRow("ch_b", "No", "No")), nil
		case "unreadable":
			return statusRows([]string{"Source_Host"}, []driver.Value{"primary.example"}), nil
		case "null_state":
			return statusRows(mysqlStatusCols, []driver.Value{"primary.example", nil, nil, ""}), nil
		case "none", "maria_none", "maria_named", "maria_named_stopped":
			return statusRows(mysqlStatusCols), nil
		case "maria_default_stopped_named_running":
			return statusRows(mariaStatusCols, mariaRow("", "No", "No")), nil
		default:
			return nil, denied
		}
	case "SHOW SLAVE STATUS":
		switch c.status {
		case "replica_slaveonly":
			return statusRows(slaveStatusCols, []driver.Value{"primary.example", "Yes", "Yes"}), nil
		case "maria_named_old":
			return statusRows(slaveStatusCols), nil
		default:
			return nil, errors.New("access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s)")
		}
	case "SHOW ALL REPLICAS STATUS":
		switch c.status {
		case "maria_none":
			return statusRows(mariaStatusCols), nil
		case "maria_named":
			return statusRows(mariaStatusCols, mariaRow("conn1", "Yes", "Yes")), nil
		case "maria_named_stopped":
			return statusRows(mariaStatusCols, mariaRow("conn1", "No", "No")), nil
		case "maria_default_stopped_named_running":
			return statusRows(mariaStatusCols, mariaRow("", "No", "No"), mariaRow("conn1", "Yes", "Yes")), nil
		default:
			// MySQL (and pre-10.5.1 MariaDB): the ALL REPLICAS syntax
			// does not exist.
			return nil, errors.New("You have an error in your SQL syntax; check the manual ... near 'ALL REPLICAS STATUS'")
		}
	case "SHOW ALL SLAVES STATUS":
		switch c.status {
		case "maria_named_old":
			return statusRows(mariaStatusCols, mariaRow("conn1", "Yes", "Yes")), nil
		default:
			return nil, errors.New("You have an error in your SQL syntax; check the manual ... near 'ALL SLAVES STATUS'")
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

// replicaStatusRows fakes a status probe's wide rows: the columns the
// preflight reads by name, in a server-chosen order.
type replicaStatusRows struct {
	cols []string
	rows [][]driver.Value
	sent int
}

func statusRows(cols []string, rows ...[]driver.Value) *replicaStatusRows {
	return &replicaStatusRows{cols: cols, rows: rows}
}

func (r *replicaStatusRows) Columns() []string { return r.cols }
func (r *replicaStatusRows) Close() error      { return nil }
func (r *replicaStatusRows) Next(dest []driver.Value) error {
	if r.sent >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.sent])
	r.sent++
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
// updates off AND a channel that is not stopped), on both variable
// spellings (MySQL log_replica_updates, MariaDB's log_slave_updates-only)
// and all four status spellings (SHOW REPLICA STATUS, the SHOW SLAVE
// STATUS fallback, and MariaDB's SHOW ALL REPLICAS/SLAVES STATUS — the
// only view that lists a `CHANGE MASTER 'name' TO …` named connection;
// audit 2026-08-27 A1).
//
// The stopped-channel acceptance (operator decision 2026-09-15, audit
// 2026-09-15 A0915-CLI-MEDIUM-1) is graded as a family, not one cell:
// every thread-state shape a server can report — both stopped, IO
// connecting, only SQL running, one channel of two running, the state
// column absent, the state NULL — on each column spelling (Replica_*,
// Slave_*, MariaDB's Connection_name rows), because "accepted" is only
// safe when EVERY channel's BOTH threads positively read "No".
func TestPreflightReplicaSource(t *testing.T) {
	t.Parallel()

	pass := map[string]string{
		"not_a_replica_lru_off":      "status=none;lru=off", // MySQL shape: bare empty, ALL forms 1064 — tolerated, no refusal
		"mariadb_not_a_replica":      "status=maria_none;lru=maria_off",
		"replica_with_log_updates":   "status=replica;lru=on",
		"mariadb_replica_updates_on": "status=replica;lru=maria_on",
		"mariadb_named_conn_lru_on":  "status=maria_named;lru=maria_on",
		// The promoted-primary shape: every channel's threads stopped.
		"stopped_channel_lru_off":         "status=stopped;lru=off",
		"never_started_channel_lru_off":   "status=never_started;lru=off",
		"two_stopped_channels_lru_off":    "status=two_stopped;lru=off",
		"mariadb_named_conn_stopped_off":  "status=maria_named_stopped;lru=maria_off",
		"mariadb_default_stopped_lru_off": "status=stopped;lru=maria_off",
	}
	for name, scenario := range pass {
		if err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, scenario)); err != nil {
			t.Errorf("%s (%q) = %v; want nil", name, scenario, err)
		}
	}

	refuse := map[string]struct {
		scenario string
		spelling string
		evidence string // the not-stopped channel the message must name
	}{
		"mysql_replica_off":       {"status=replica;lru=off", "log_replica_updates", "(default) [IO=Yes SQL=Yes]"},
		"mariadb_replica_off":     {"status=replica;lru=maria_off", "log_slave_updates", "(default) [IO=Yes SQL=Yes]"},
		"old_server_slave_status": {"status=replica_slaveonly;lru=maria_off", "log_slave_updates", "(default) [IO=Yes SQL=Yes]"},
		// The A1 shape: a named connection invisible to the bare
		// spellings, caught only by the ALL forms.
		"mariadb_named_conn_off":     {"status=maria_named;lru=maria_off", "log_slave_updates", "conn1 [IO=Yes SQL=Yes]"},
		"mariadb_named_conn_old_off": {"status=maria_named_old;lru=maria_off", "log_slave_updates", "conn1 [IO=Yes SQL=Yes]"},
		// Every not-stopped thread shape keeps the refusal.
		"io_connecting":   {"status=io_connecting;lru=off", "log_replica_updates", "(default) [IO=Connecting SQL=Yes]"},
		"sql_thread_only": {"status=sql_only;lru=off", "log_replica_updates", "(default) [IO=No SQL=Yes]"},
		// One of two channels running: the stopped one must not vouch
		// for the other.
		"one_of_two_channels_running": {"status=two_one_running;lru=off", "log_replica_updates", "ch_b [IO=Yes SQL=Yes]"},
		// The status row exists (the server IS a replica) but its thread
		// state cannot be read: nothing proves it idle.
		"status_unreadable":  {"status=unreadable;lru=off", "log_replica_updates", "(default) [IO=unreadable SQL=unreadable]"},
		"thread_state_null":  {"status=null_state;lru=off", "log_replica_updates", "(default) [IO=unreadable SQL=unreadable]"},
		"mariadb_named_live": {"status=maria_default_stopped_named_running;lru=maria_off", "log_slave_updates", "conn1 [IO=Yes SQL=Yes]"},
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
		// The diagnosis lives in the message.
		for _, phrase := range []string{
			"@@GLOBAL." + tc.spelling + "=0", // names the evidence
			tc.evidence,                      // names the channel that keeps it refused
			"silently absent",                // names the consequence
			"legitimate chained source",      // the passing sibling, so the operator doesn't over-correct
		} {
			if !strings.Contains(err.Error(), phrase) {
				t.Errorf("%s: message missing %q; got: %v", name, phrase, err)
			}
		}
		// The remedies live in BOTH homes — message and hint — or an
		// operator who reads only the hint line misses one the message
		// carries. Audit 2026-09-15 A0915-CLI-MEDIUM-1 was a remedy that lived in
		// neither: RESET REPLICA ALL. Both spellings, because MariaDB's
		// takes the connection name. Case-insensitive, because the message
		// opens a sentence with the re-point remedy.
		for _, phrase := range []string{
			"point the sync at the primary",
			"STOP REPLICA",
			"RESET REPLICA ALL",
			"RESET REPLICA 'connection_name' ALL",
			"log_replica_updates=ON",
			"read-only at runtime", // why SET GLOBAL is not the remedy
		} {
			for _, home := range []struct{ name, text string }{{"message", err.Error()}, {"hint", ce.Hint}} {
				if !strings.Contains(strings.ToLower(home.text), strings.ToLower(phrase)) {
					t.Errorf("%s: %s missing remedy %q; got: %q", name, home.name, phrase, home.text)
				}
			}
		}
	}
}

// TestPreflightReplicaSource_StoppedChannelsAcceptedWithInfo pins the
// acceptance's other half: the pass is not silent. The INFO carries the
// grep-stable marker, names every stopped channel, names RESET REPLICA
// ALL as the bookkeeping clear, and states the TOCTOU it accepts. Not
// parallel: it swaps the default slog handler.
func TestPreflightReplicaSource_StoppedChannelsAcceptedWithInfo(t *testing.T) {
	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	if err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, "status=two_stopped;lru=off")); err != nil {
		t.Fatalf("all channels stopped = %v; want nil", err)
	}
	out := buf.String()
	for _, want := range []string{
		"level=INFO",
		replicaChannelsStoppedMarker,
		"ch_a [IO=No SQL=No]",
		"ch_b [IO=No SQL=No]",
		"RESET REPLICA ALL",
		"STARTED on this server again",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("acceptance log missing %q:\n%s", want, out)
		}
	}

	// And a refused server does NOT log the acceptance.
	buf.Reset()
	if err := preflightReplicaSource(context.Background(), newReplicaSourceDB(t, "status=two_one_running;lru=off")); err == nil {
		t.Fatal("one of two channels running = nil; want the refusal")
	}
	if strings.Contains(buf.String(), replicaChannelsStoppedMarker) {
		t.Fatalf("a refused server logged the acceptance marker:\n%s", buf.String())
	}
}

// TestSourceReplicaChannels_MySQLAllFormsErrorTolerated pins the A1
// posture at the probe itself: a MySQL rejecting the MariaDB-only ALL
// spellings with a syntax error, after a bare spelling SUCCEEDED, is its
// bare answer with no error — not an error, which would degrade the door
// into the privilege-blocked WARN-and-pass posture on every healthy MySQL
// source. The bare form is channel-complete on MySQL, so there is no
// blind spot to warn about.
func TestSourceReplicaChannels_MySQLAllFormsErrorTolerated(t *testing.T) {
	t.Parallel()
	for scenario, want := range map[string]int{"status=none;lru=off": 0, "status=two_stopped;lru=off": 2} {
		channels, err := sourceReplicaChannels(context.Background(), newReplicaSourceDB(t, scenario))
		if err != nil {
			t.Fatalf("%s: MySQL shape (bare answered, ALL forms 1064) returned err = %v; want nil (the syntax error must not degrade the door)", scenario, err)
		}
		if len(channels) != want {
			t.Fatalf("%s: %d channels; want %d: %+v", scenario, len(channels), want, channels)
		}
	}
}

// TestPreflightReplicaSource_ProbeFailureWarnsAndPasses: the status
// probe is privilege-gated (MariaDB 10.5+ REPLICA MONITOR split), so a
// failed probe must NOT refuse a working configuration — it degrades
// with a WARN. The refusal requires successful evidence (the PG
// replication-headroom census posture). Distinct from the
// status_unreadable refusal cell above: there the probe SUCCEEDED and
// proved a replica; here nothing was learned at all.
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
