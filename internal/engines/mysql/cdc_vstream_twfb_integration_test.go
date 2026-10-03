//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 on the VStream lane: the target-witnessed first boundary
// (pipeline/schema_forward_witness.go) on a real vttestserver source.
//
// VStream hands the pipeline a FIELD event — a schema boundary — for every
// table at its first row of every resumed stream, so this lane is both the
// one where the witness does the most work and the one a phantom would
// hurt most. One stream, two arms:
//
//   - FORWARD: a DATETIME → DATETIME(6) widen made while the stream is
//     stopped reaches the target, and the row after it keeps its
//     microseconds (before the fix the restart cached the boundary as the
//     baseline and the target rounded every following value at exit 0).
//   - ANTI-PHANTOM: a table carrying every MySQL family resumes onto a
//     MATCH — no forward, no refusal — with a row in every column.
//
// The independent expected values are the target's catalog (COLUMN_TYPE)
// and the rows read back from it.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline"
)

const vstreamTWFBFamilyDDL = `CREATE TABLE fam (
	id            INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	c_bool        TINYINT(1),
	c_tinyint_u   TINYINT UNSIGNED,
	c_smallint    SMALLINT,
	c_mediumint_u MEDIUMINT UNSIGNED,
	c_int         INT,
	c_int_u       INT UNSIGNED,
	c_int_zf      INT(6) ZEROFILL,
	c_bigint_u    BIGINT UNSIGNED,
	c_year        YEAR,
	c_decimal     DECIMAL(10,2),
	c_decimal_u   DECIMAL(8,3) UNSIGNED,
	c_float       FLOAT,
	c_double      DOUBLE,
	c_bit8        BIT(8),
	c_char        CHAR(10),
	c_varchar     VARCHAR(50),
	c_vc_bin      VARCHAR(20) COLLATE utf8mb4_bin,
	c_vc_latin1   VARCHAR(20) CHARACTER SET latin1,
	c_text        TEXT,
	c_longtext    LONGTEXT,
	c_binary      BINARY(16),
	c_varbinary   VARBINARY(64),
	c_blob        BLOB,
	c_date        DATE,
	c_time3       TIME(3),
	c_datetime    DATETIME,
	c_datetime6   DATETIME(6),
	c_ts          TIMESTAMP NULL,
	c_ts6         TIMESTAMP(6) NULL,
	c_enum        ENUM('red','green'),
	c_set         SET('a','b','c'),
	c_json        JSON,
	g_stored      INT AS (c_int + 1) STORED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

const vstreamTWFBFamilyProbe = `INSERT INTO fam (id, c_bool, c_tinyint_u, c_smallint, c_mediumint_u, c_int, c_int_u,
	c_int_zf, c_bigint_u, c_year, c_decimal, c_decimal_u, c_float, c_double, c_bit8, c_char, c_varchar, c_vc_bin,
	c_vc_latin1, c_text, c_longtext, c_binary, c_varbinary, c_blob, c_date, c_time3, c_datetime, c_datetime6,
	c_ts, c_ts6, c_enum, c_set, c_json)
VALUES (2, 1, 200, -300, 16000000, -5, 4000000000, 42, 18000000000000000000, 2026, 12.34, 1.5, 1.5, 2.25,
	b'10101010', 'ch', 'vc', 'Bin', 'lat', 't', 't', 0x0102, 0x0102, 0x0102, '2026-10-02', '10:11:12.345',
	'2026-10-02 10:11:12', '2026-10-02 10:11:12.345678', '2026-10-02 10:11:12', '2026-10-02 10:11:12.345678',
	'green', 'a,c', '{"k": 1}')`

func TestVStream_TWFB_StoppedWidenForwardsAndFamilyResumesClean(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startVTTestServer(t)
	defer cleanupSrc()
	tgtDSN, cleanupTgt := startMySQLForApplier(t)
	defer cleanupTgt()

	exec := func(dsn, stmt string) {
		t.Helper()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%v\n%s", err, stmt)
		}
	}
	scalar := func(dsn, q string, args ...any) string {
		t.Helper()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return "<" + err.Error() + ">"
		}
		defer func() { _ = db.Close() }()
		var v sql.NullString
		if err := db.QueryRow(q, args...).Scan(&v); err != nil {
			return "<" + err.Error() + ">"
		}
		return v.String
	}
	waitFor := func(what string, done <-chan error, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(180 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			select {
			case err := <-done:
				t.Fatalf("%s: the stream exited: %v", what, err)
			default:
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("%s: timed out", what)
	}
	hasRow := func(table string, id int) func() bool {
		return func() bool {
			return scalar(tgtDSN, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = %d", table, id)) == "1"
		}
	}

	exec(mysqlDSN, vstreamTWFBFamilyDDL)
	exec(mysqlDSN, "INSERT INTO fam (id) VALUES (1)")
	exec(mysqlDSN, "CREATE TABLE t_fsp (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL) ENGINE=InnoDB")
	exec(mysqlDSN, "INSERT INTO t_fsp VALUES (1, '2026-10-02 10:00:00')")
	time.Sleep(3 * time.Second) // the schema tracker

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}
	streamer := func() *pipeline.Streamer {
		return &pipeline.Streamer{
			Source: srcEng, Target: Engine{Flavor: FlavorVanilla},
			SourceDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
				mysqlDSN, grpcEndpoint),
			TargetDSN: tgtDSN,
			StreamID:  "gc44-vstream",
		}
	}
	start := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		s := streamer()
		go func() { done <- s.Run(ctx) }()
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done chan error) {
		t.Helper()
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("the stream returned %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("Streamer.Run did not return after cancel")
		}
	}
	persisted := func() bool {
		return !strings.HasPrefix(scalar(tgtDSN, "SELECT source_position FROM sluice_cdc_state WHERE stream_id = ?", "gc44-vstream"), "<")
	}

	cancel, done := start()
	waitFor("cold start", done, hasRow("fam", 1))
	waitFor("cold start", done, hasRow("t_fsp", 1))
	waitFor("CDC handoff", done, persisted)
	time.Sleep(2 * time.Second)
	stop(cancel, done)

	exec(mysqlDSN, "ALTER TABLE t_fsp MODIFY ts DATETIME(6) NOT NULL")
	time.Sleep(3 * time.Second)

	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)
	cancel, done = start()
	exec(mysqlDSN, vstreamTWFBFamilyProbe)
	exec(mysqlDSN, "INSERT INTO t_fsp VALUES (2, '2026-10-02 10:11:12.345678')")
	waitFor("probe", done, hasRow("fam", 2))
	waitFor("probe", done, hasRow("t_fsp", 2))
	stop(cancel, done)

	if got := scalar(tgtDSN, "SELECT COLUMN_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 't_fsp' AND COLUMN_NAME = 'ts'"); got != "datetime(6)" {
		t.Errorf("GC-44 (VStream): target t_fsp.ts is %q, want datetime(6) — the widen made while stopped was not forwarded", got)
	}
	if got := scalar(tgtDSN, "SELECT DATE_FORMAT(ts, '%H:%i:%s.%f') FROM t_fsp WHERE id = 2"); got != "10:11:12.345678" {
		t.Errorf("GC-44 (VStream): target t_fsp row 2 = %q, want 10:11:12.345678", got)
	}
	out := logs.String()
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "RESUME-SCHEMA-DIVERGENCE") ||
			(strings.Contains(line, "differs from the target") && strings.Contains(line, "fam")) {
			t.Errorf("PHANTOM on the VStream family table: %s", line)
		}
	}
	if !strings.Contains(out, "first boundary matches the target") {
		t.Error("VACUOUS: no first-boundary match was logged — the witness never checked the family table")
	}
}
