// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the AWS RDS MySQL detect-first retention advisory
// (roadmap item 70's RDS sibling, live-probed 2026-07-16). The matrix:
// scripted mysql.rds_configuration responses (NULL / 12 / 48 /
// query-error) through the real query layer, × host-pattern and cdc
// gating, plus the FTWRL provider-awareness predicate. The sibling DO
// advisory's own pins live in host_advisories_test.go and are
// unchanged — the two advisories are independent surfaces.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// rdsConfScript scripts the one query the retention probe issues.
// value=nil serves SQL NULL; queryErr fails the query; noRow serves an
// empty result set (ErrNoRows at the Scan).
type rdsConfScript struct {
	value    *string
	noRow    bool
	queryErr error
}

type rdsConfDriver struct{ script *rdsConfScript }

type rdsConfConn struct{ script *rdsConfScript }

func (d rdsConfDriver) Open(string) (driver.Conn, error) { return rdsConfConn(d), nil }

func (rdsConfConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (rdsConfConn) Close() error                        { return nil }
func (rdsConfConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c rdsConfConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "mysql.rds_configuration") {
		return nil, errors.New("unexpected query: " + query)
	}
	if c.script.queryErr != nil {
		return nil, c.script.queryErr
	}
	return &rdsConfRows{script: c.script}, nil
}

// rdsConfRows serves the single-column `value` result with zero or one
// row (a nil value slot is the SQL NULL the RDS default reports).
type rdsConfRows struct {
	script *rdsConfScript
	done   bool
}

func (*rdsConfRows) Columns() []string { return []string{"value"} }
func (*rdsConfRows) Close() error      { return nil }

func (r *rdsConfRows) Next(dest []driver.Value) error {
	if r.done || r.script.noRow {
		return io.EOF
	}
	if r.script.value == nil {
		dest[0] = nil
	} else {
		dest[0] = []byte(*r.script.value)
	}
	r.done = true
	return nil
}

// rdsConfDriverSeq disambiguates driver names across the multiple
// scripted DBs one test opens (sql.Register panics on a duplicate).
var rdsConfDriverSeq atomic.Int64

func newRDSConfDB(t *testing.T, script *rdsConfScript) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("sluice-rdsconf-test-%s-%d", t.Name(), rdsConfDriverSeq.Add(1))
	sql.Register(name, rdsConfDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open scripted db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func strPtr(s string) *string { return &s }

// TestReadRDSBinlogRetentionHours pins the query layer against the
// scripted driver: NULL value → (invalid, nil); numeric text → parsed
// hours; missing row / failed query / garbage value → error (the
// classification layer degrades those to the conservative WARN).
func TestReadRDSBinlogRetentionHours(t *testing.T) {
	ctx := context.Background()

	hours, err := readRDSBinlogRetentionHours(ctx, newRDSConfDB(t, &rdsConfScript{value: nil}))
	if err != nil || hours.Valid {
		t.Errorf("NULL value: got (%+v, %v); want (invalid, nil)", hours, err)
	}

	hours, err = readRDSBinlogRetentionHours(ctx, newRDSConfDB(t, &rdsConfScript{value: strPtr("12")}))
	if err != nil || !hours.Valid || hours.Float64 != 12 {
		t.Errorf("value 12: got (%+v, %v); want (12, nil)", hours, err)
	}

	hours, err = readRDSBinlogRetentionHours(ctx, newRDSConfDB(t, &rdsConfScript{value: strPtr("48")}))
	if err != nil || !hours.Valid || hours.Float64 != 48 {
		t.Errorf("value 48: got (%+v, %v); want (48, nil)", hours, err)
	}

	if _, err := readRDSBinlogRetentionHours(ctx, newRDSConfDB(t, &rdsConfScript{noRow: true})); err == nil {
		t.Error("missing row: want error (degrades to conservative WARN); got nil")
	}
	if _, err := readRDSBinlogRetentionHours(ctx, newRDSConfDB(t, &rdsConfScript{queryErr: errors.New("Table 'mysql.rds_configuration' doesn't exist")})); err == nil {
		t.Error("query error: want error; got nil")
	}
	if _, err := readRDSBinlogRetentionHours(ctx, newRDSConfDB(t, &rdsConfScript{value: strPtr("soon")})); err == nil {
		t.Error("unparseable value: want error; got nil")
	}
}

// TestRDSRetentionAdvisories pins the classification matrix: NULL →
// the full purge-ASAP WARN (naming the remedy procedure, the observed
// ~5-11-minute window, and that an attached stream does NOT hold the
// purger back); <24 → the milder WARN naming the configured window;
// >=24 → SILENT (the detect-first payoff); probe error → the
// conservative DO-style pattern WARN.
func TestRDSRetentionAdvisories(t *testing.T) {
	const host = "mydb.abc123.us-east-1.rds.amazonaws.com"

	nullCase := rdsRetentionAdvisories(host, sql.NullFloat64{}, nil)
	if len(nullCase) != 1 {
		t.Fatalf("NULL: got %d advisories; want 1", len(nullCase))
	}
	for _, want := range []string{
		host,
		"binlog retention hours' is NULL",
		"5-11 minutes",
		"binlog_expire_logs_seconds",
		"does NOT hold the purger back",
		"mysql.rds_set_configuration('binlog retention hours', 24)",
		"max 168",
	} {
		if !strings.Contains(nullCase[0].Message, want) {
			t.Errorf("NULL message should mention %q; got: %s", want, nullCase[0].Message)
		}
	}
	if nullCase[0].Hint == "" {
		t.Error("NULL advisory carries no hint")
	}

	low := rdsRetentionAdvisories(host, sql.NullFloat64{Float64: 12, Valid: true}, nil)
	if len(low) != 1 {
		t.Fatalf("12h: got %d advisories; want 1", len(low))
	}
	for _, want := range []string{host, "12 hours", "mysql.rds_set_configuration"} {
		if !strings.Contains(low[0].Message, want) {
			t.Errorf("12h message should mention %q; got: %s", want, low[0].Message)
		}
	}

	if got := rdsRetentionAdvisories(host, sql.NullFloat64{Float64: 48, Valid: true}, nil); len(got) != 0 {
		t.Errorf("48h: got %d advisories (%v); want SILENT — a correctly configured host must not collect a WARN", len(got), got)
	}
	if got := rdsRetentionAdvisories(host, sql.NullFloat64{Float64: 24, Valid: true}, nil); len(got) != 0 {
		t.Errorf("24h (the boundary): got %d advisories; want silent", len(got))
	}

	fallback := rdsRetentionAdvisories(host, sql.NullFloat64{}, errors.New("dial tcp: i/o timeout"))
	if len(fallback) != 1 {
		t.Fatalf("probe error: got %d advisories; want 1 (the conservative pattern WARN)", len(fallback))
	}
	for _, want := range []string{host, "could not be read", "dial tcp: i/o timeout", "rds_show_configuration"} {
		if !strings.Contains(fallback[0].Message, want) {
			t.Errorf("fallback message should mention %q; got: %s", want, fallback[0].Message)
		}
	}
}

// TestSourceProbedAdvisories_Gates pins the two no-connection gates:
// cdc=false (a plain migrate never returns to the binlog) and non-RDS
// hosts must return nil WITHOUT probing — the host pattern is what
// keeps the probe free for everyone else. Both DSNs would fail any
// real dial, so a non-nil return or a hang here would itself signal a
// gate regression.
func TestSourceProbedAdvisories_Gates(t *testing.T) {
	var _ ir.SourceProbedAdvisor = Engine{}
	ctx := context.Background()

	const rdsDSN = "admin:pw@tcp(mydb.abc123.us-east-1.rds.amazonaws.com:3306)/app"
	if got := (Engine{Flavor: FlavorVanilla}).SourceProbedAdvisories(ctx, rdsDSN, false); len(got) != 0 {
		t.Errorf("cdc=false: got %d advisories (%v); want none", len(got), got)
	}

	for _, c := range []struct {
		name string
		dsn  string
	}{
		{"local", "root:pw@tcp(localhost:3306)/app"},
		{"digitalocean", "doadmin:pw@tcp(db-mysql-nyc3-1.b.db.ondigitalocean.com:25060)/defaultdb"},
		{"suffix embedded mid-host does not match", "u:p@tcp(x.rds.amazonaws.com.evil.example:3306)/app"},
		{"empty", ""},
		{"garbage", "::::"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := (Engine{Flavor: FlavorVanilla}).SourceProbedAdvisories(ctx, c.dsn, true); len(got) != 0 {
				t.Errorf("got %d advisories (%v); want none", len(got), got)
			}
		})
	}
}

// TestRDSMySQLHostAndAddr pins the host predicates: DSN-level matching
// (rdsMySQLHost, case-insensitive) and Addr-level matching
// (isRDSMySQLAddr, the FTWRL remedy selector).
func TestRDSMySQLHostAndAddr(t *testing.T) {
	host, ok := rdsMySQLHost("admin:pw@tcp(MyDB.ABC123.us-east-1.RDS.amazonaws.com:3306)/app")
	if !ok || host != "mydb.abc123.us-east-1.rds.amazonaws.com" {
		t.Errorf("rdsMySQLHost = (%q, %v); want the lowercased RDS host, true", host, ok)
	}
	if _, ok := rdsMySQLHost("u:p@tcp(aws.connect.psdb.cloud:3306)/app"); ok {
		t.Error("rdsMySQLHost matched a PlanetScale host")
	}

	if !isRDSMySQLAddr("mydb.abc123.us-east-1.rds.amazonaws.com:3306") {
		t.Error("isRDSMySQLAddr = false for an RDS host:port")
	}
	if isRDSMySQLAddr("localhost:3306") || isRDSMySQLAddr("") {
		t.Error("isRDSMySQLAddr matched a non-RDS addr")
	}
}
