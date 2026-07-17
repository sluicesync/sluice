// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the Google Cloud SQL for MySQL detection + retention
// advisory and the PITR-toggle position-loss hint (the GCP leg of the
// managed-MySQL retention story, live-probed 2026-07-16). The matrix:
// fake-driver version-string fingerprints × retention-variable values
// through the real query layer, the candidate-host shape gate (no
// dial), the pure retention classifier, and the hint folded into both
// ErrPositionInvalid wraps (file/pos and GTID). The sibling DO / RDS
// advisories' own pins are unchanged — independent surfaces.

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

// csqlScript scripts the queries the Cloud SQL probe + position-verify
// paths issue: the @@version fingerprint, the retention variable, SHOW
// BINARY LOGS, and GTID_SUBSET. Unscripted queries error, so a test
// exercises exactly the query set it declares.
type csqlScript struct {
	version    string
	comment    string
	versionErr error

	seconds    int64
	secondsErr error

	// binlogFiles serves SHOW BINARY LOGS (Log_name, File_size).
	binlogFiles []string

	// gtidSubset serves SELECT GTID_SUBSET(...). nil = unscripted.
	gtidSubset *int64

	// retentionQueried records whether the retention variable was read
	// — pins that a non-Google server never pays the second query.
	retentionQueried atomic.Bool
}

type csqlDriver struct{ script *csqlScript }

type csqlConn struct{ script *csqlScript }

func (d csqlDriver) Open(string) (driver.Conn, error) { return csqlConn(d), nil }

func (csqlConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (csqlConn) Close() error                        { return nil }
func (csqlConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c csqlConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	s := c.script
	switch {
	case strings.Contains(query, "@@version"):
		if s.versionErr != nil {
			return nil, s.versionErr
		}
		return &csqlRows{cols: []string{"@@version", "@@version_comment"}, rows: [][]driver.Value{{[]byte(s.version), []byte(s.comment)}}}, nil
	case strings.Contains(query, "binlog_expire_logs_seconds"):
		s.retentionQueried.Store(true)
		if s.secondsErr != nil {
			return nil, s.secondsErr
		}
		return &csqlRows{cols: []string{"@@global.binlog_expire_logs_seconds"}, rows: [][]driver.Value{{s.seconds}}}, nil
	case strings.Contains(query, "SHOW BINARY LOGS"):
		out := make([][]driver.Value, 0, len(s.binlogFiles))
		for _, f := range s.binlogFiles {
			out = append(out, []driver.Value{[]byte(f), int64(1024)})
		}
		return &csqlRows{cols: []string{"Log_name", "File_size"}, rows: out}, nil
	case strings.Contains(query, "GTID_SUBSET"):
		if s.gtidSubset == nil {
			return nil, errors.New("GTID_SUBSET not scripted")
		}
		return &csqlRows{cols: []string{"subset"}, rows: [][]driver.Value{{*s.gtidSubset}}}, nil
	default:
		return nil, errors.New("unexpected query: " + query)
	}
}

// csqlRows serves a fixed row set.
type csqlRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

func (r *csqlRows) Columns() []string { return r.cols }
func (*csqlRows) Close() error        { return nil }

func (r *csqlRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// csqlDriverSeq disambiguates driver names across scripted DBs
// (sql.Register panics on a duplicate).
var csqlDriverSeq atomic.Int64

func newCSQLDB(t *testing.T, script *csqlScript) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("sluice-csql-test-%s-%d", t.Name(), csqlDriverSeq.Add(1))
	sql.Register(name, csqlDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open scripted db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestIsCloudSQLServer pins the fingerprint matrix: the live-observed
// pair, each signal alone, case variants, and the non-Google negatives
// (stock MySQL, Vitess, MariaDB, and a non-suffix "-google" infix).
func TestIsCloudSQLServer(t *testing.T) {
	cases := []struct {
		version, comment string
		want             bool
	}{
		{"8.0.45-google", "(Google)", true}, // live-observed shape
		{"8.0.45-google", "", true},         // version suffix alone
		{"8.0.45", "(Google)", true},        // comment mark alone
		{"8.0.45-GOOGLE", "", true},         // case-insensitive suffix
		{"8.0.45", "(google)", true},        // case-insensitive comment
		{"8.4.5", "MySQL Community Server - GPL", false},
		{"8.0.30-Vitess", "Version: 8.0.30-Vitess", false},
		{"11.4.2-MariaDB-log", "mariadb.org binary distribution", false},
		{"8.0.45-googley", "", false}, // suffix must be exact
		{"", "", false},
	}
	for _, c := range cases {
		if got := isCloudSQLServer(c.version, c.comment); got != c.want {
			t.Errorf("isCloudSQLServer(%q, %q) = %v; want %v", c.version, c.comment, got, c.want)
		}
	}
}

// TestCloudSQLCandidateHost pins the shape gate: IP literals (Cloud
// SQL's public-IP norm) and localhost (the auth-proxy / connector) are
// candidates; named hostnames — including the DO/RDS/PlanetScale
// managed suffixes — and unparseable DSNs are not, so they never pay a
// probe connection.
func TestCloudSQLCandidateHost(t *testing.T) {
	candidates := []struct {
		dsn, wantHost string
	}{
		{"u:p@tcp(34.23.5.10:3306)/app", "34.23.5.10"},
		{"u:p@tcp(127.0.0.1:3307)/app", "127.0.0.1"},
		{"u:p@tcp(localhost:3306)/app", "localhost"},
		{"u:p@tcp(LOCALHOST:3306)/app", "localhost"},
	}
	for _, c := range candidates {
		host, ok := cloudSQLCandidateHost(c.dsn)
		if !ok || host != c.wantHost {
			t.Errorf("cloudSQLCandidateHost(%q) = (%q, %v); want (%q, true)", c.dsn, host, ok, c.wantHost)
		}
	}

	nonCandidates := []struct {
		name, dsn string
	}{
		{"named internal host", "root:pw@tcp(mysql.internal.example:3306)/app"},
		{"digitalocean", "doadmin:pw@tcp(db-mysql-nyc3-1.b.db.ondigitalocean.com:25060)/defaultdb"},
		{"rds", "admin:pw@tcp(mydb.abc123.us-east-1.rds.amazonaws.com:3306)/app"},
		{"planetscale", "u:p@tcp(aws.connect.psdb.cloud:3306)/app"},
		{"empty", ""},
		{"garbage", "::::"},
	}
	for _, c := range nonCandidates {
		if host, ok := cloudSQLCandidateHost(c.dsn); ok {
			t.Errorf("%s: cloudSQLCandidateHost(%q) matched %q; want no match", c.name, c.dsn, host)
		}
	}
}

// TestCloudSQLRetentionAdvisories pins the pure classifier: the
// platform default (86400) and everything at/above it, plus 0 (never
// expire), are SILENT; a sub-day window — nearly unreachable, the
// platform floor refuses it — WARNs naming the gcloud remedy and the
// replace-the-whole-flag-set caveat.
func TestCloudSQLRetentionAdvisories(t *testing.T) {
	const host = "34.23.5.10"

	for _, seconds := range []int64{0, 86400, 172800, 4294967295} {
		if got := cloudSQLRetentionAdvisories(host, seconds); len(got) != 0 {
			t.Errorf("%d seconds: got %d advisories (%v); want SILENT — safe windows must not collect a WARN", seconds, len(got), got)
		}
	}

	for _, seconds := range []int64{1, 3600, 86399} {
		got := cloudSQLRetentionAdvisories(host, seconds)
		if len(got) != 1 {
			t.Fatalf("%d seconds: got %d advisories; want 1", seconds, len(got))
		}
		for _, want := range []string{
			host,
			"Google Cloud SQL",
			fmt.Sprintf("binlog_expire_logs_seconds = %d", seconds),
			"gcloud sql instances patch",
			"--database-flags=binlog_expire_logs_seconds=86400",
			"replaces the ENTIRE flag set",
		} {
			if !strings.Contains(got[0].Message, want) {
				t.Errorf("%d seconds: message should mention %q; got: %s", seconds, want, got[0].Message)
			}
		}
		if got[0].Hint == "" {
			t.Errorf("%d seconds: advisory carries no hint", seconds)
		}
	}
}

// TestCloudSQLAdvisoriesFromDB pins the fingerprint × variable matrix
// through the real query layer: only a Google-fingerprinted server
// with a sub-day window WARNs; a non-Google server never even pays the
// retention query; and every probe failure — fingerprint unreadable,
// or confirmed-Google with an unreadable variable — degrades to
// SILENCE (the safe-defaults asymmetry with the RDS probe, whose
// dangerous defaults degrade to a WARN).
func TestCloudSQLAdvisoriesFromDB(t *testing.T) {
	ctx := context.Background()
	const host = "34.23.5.10"

	google := func(seconds int64) *csqlScript {
		return &csqlScript{version: "8.0.45-google", comment: "(Google)", seconds: seconds}
	}

	if got := cloudSQLAdvisoriesFromDB(ctx, host, newCSQLDB(t, google(3600))); len(got) != 1 {
		t.Errorf("google + 3600s: got %d advisories; want 1 WARN", len(got))
	}
	if got := cloudSQLAdvisoriesFromDB(ctx, host, newCSQLDB(t, google(86400))); len(got) != 0 {
		t.Errorf("google + 86400s (the platform default): got %v; want silent", got)
	}
	if got := cloudSQLAdvisoriesFromDB(ctx, host, newCSQLDB(t, google(0))); len(got) != 0 {
		t.Errorf("google + 0 (never expire): got %v; want silent", got)
	}

	stock := &csqlScript{version: "8.4.5", comment: "MySQL Community Server - GPL", seconds: 3600}
	if got := cloudSQLAdvisoriesFromDB(ctx, host, newCSQLDB(t, stock)); len(got) != 0 {
		t.Errorf("non-google: got %v; want silent", got)
	}
	if stock.retentionQueried.Load() {
		t.Error("non-google server paid the retention query; the fingerprint must gate it")
	}

	verr := &csqlScript{versionErr: errors.New("dial tcp: i/o timeout")}
	if got := cloudSQLAdvisoriesFromDB(ctx, host, newCSQLDB(t, verr)); len(got) != 0 {
		t.Errorf("fingerprint error: got %v; want silent (fingerprint unknown — no blind WARN)", got)
	}

	serr := &csqlScript{version: "8.0.45-google", comment: "(Google)", secondsErr: errors.New("variable gone")}
	if got := cloudSQLAdvisoriesFromDB(ctx, host, newCSQLDB(t, serr)); len(got) != 0 {
		t.Errorf("google + variable error: got %v; want silent (platform floor makes silence safe)", got)
	}
}

// TestCloudSQLPositionLossHint pins the PITR-toggle recovery hint: a
// Google-fingerprinted source gets the numbering-reset explanation
// naming auto-resnapshot as the correct recovery; everything else —
// including a fingerprint failure — gets "".
func TestCloudSQLPositionLossHint(t *testing.T) {
	ctx := context.Background()

	hint := cloudSQLPositionLossHint(ctx, newCSQLDB(t, &csqlScript{version: "8.0.45-google", comment: "(Google)"}))
	for _, want := range []string{"Google Cloud SQL", "mysql-bin.000001", "auto-resnapshot", "PITR"} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint should mention %q; got: %q", want, hint)
		}
	}

	if h := cloudSQLPositionLossHint(ctx, newCSQLDB(t, &csqlScript{version: "8.4.5", comment: "MySQL Community Server - GPL"})); h != "" {
		t.Errorf("non-google hint = %q; want empty", h)
	}
	if h := cloudSQLPositionLossHint(ctx, newCSQLDB(t, &csqlScript{versionErr: errors.New("boom")})); h != "" {
		t.Errorf("fingerprint-error hint = %q; want empty", h)
	}
}

// TestVerifyPositionInvalid_CloudSQLHint pins the hint folded into BOTH
// ErrPositionInvalid wraps — the file/pos "binlog purged" path (the
// exact shape the live PITR-toggle probe produced) and the GTID
// purged-set path — and that non-Google sources keep the pre-existing
// message with no Cloud SQL mention. The errors stay ErrPositionInvalid
// either way (the hint is text, never routing).
func TestVerifyPositionInvalid_CloudSQLHint(t *testing.T) {
	ctx := context.Background()
	zero := int64(0)

	googleFilePos := &csqlScript{
		version: "8.0.45-google", comment: "(Google)",
		binlogFiles: []string{"mysql-bin.000001", "mysql-bin.000002"},
	}
	err := verifyBinlogFilePresent(ctx, newCSQLDB(t, googleFilePos), "mysql-bin.000012")
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("file/pos purged: want ErrPositionInvalid; got %v", err)
	}
	for _, want := range []string{`"mysql-bin.000012"`, "purged", "Google Cloud SQL", "mysql-bin.000001", "auto-resnapshot"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("file/pos purged (google) message should mention %q; got: %v", want, err)
		}
	}

	stockFilePos := &csqlScript{
		version: "8.4.5", comment: "MySQL Community Server - GPL",
		binlogFiles: []string{"mysql-bin.000001"},
	}
	err = verifyBinlogFilePresent(ctx, newCSQLDB(t, stockFilePos), "mysql-bin.000012")
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("file/pos purged (stock): want ErrPositionInvalid; got %v", err)
	}
	if strings.Contains(err.Error(), "Cloud SQL") {
		t.Errorf("non-google source must not carry the Cloud SQL hint; got: %v", err)
	}

	googleGTID := &csqlScript{version: "8.0.45-google", comment: "(Google)", gtidSubset: &zero}
	err = verifyGTIDSetReachable(ctx, newCSQLDB(t, googleGTID), "uuid:1-100")
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("gtid purged: want ErrPositionInvalid; got %v", err)
	}
	for _, want := range []string{"purged GTIDs", "Google Cloud SQL", "mysql-bin.000001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("gtid purged (google) message should mention %q; got: %v", want, err)
		}
	}

	stockGTID := &csqlScript{version: "8.4.5", comment: "MySQL Community Server - GPL", gtidSubset: &zero}
	err = verifyGTIDSetReachable(ctx, newCSQLDB(t, stockGTID), "uuid:1-100")
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("gtid purged (stock): want ErrPositionInvalid; got %v", err)
	}
	if strings.Contains(err.Error(), "Cloud SQL") {
		t.Errorf("non-google gtid source must not carry the Cloud SQL hint; got: %v", err)
	}
}
