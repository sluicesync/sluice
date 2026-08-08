//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The vtgate half of [binlogEnabledQuery]'s premise, per PR.
//
// binlogEnabled() runs on EVERY MySQL-family flavor and RETURNS AN ERROR
// when the read fails, and both backup-position capturers propagate that
// error — so a flavor whose server declines `@@GLOBAL.log_bin` cannot take
// a `backup full` at all. The premise was ground-truthed on MySQL 8.0.46
// and MariaDB 11.4; NEITHER is vtgate, where a planetscale/vitess
// connection actually terminates and where the system-variable surface is
// vtgate's own partial one rather than mysqld's.
//
// This is the cheap vehicle (vtcombo, in the per-PR `Integration
// (vstream)` job). Its sibling
// TestVitessCluster_BackupPositionProbesAnswerThroughVtgate runs the same
// assertions against a real multi-process vtgate across the weekly
// version matrix (v21..v24) — the axis on which a claim about someone
// else's proxy is likeliest to rot. Both are needed: this one catches a
// regression on every PR, that one catches a vtgate version that stops
// answering.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_BackupPositionProbesAnswerThroughVtgate measures, rather
// than assumes, that a vtgate connection serves the system variables the
// backup-position preflight and the table-name-fold probe read.
func TestVStream_BackupPositionProbesAnswerThroughVtgate(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Seed real committed transactions so the GTID arm has a non-empty
	// set to find. Without them a green "the capture recorded a token"
	// would be indistinguishable from the empty-set defect the preflight
	// exists to close.
	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE premise_probe (
			id BIGINT NOT NULL AUTO_INCREMENT,
			v  VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", `
		INSERT INTO premise_probe (v) VALUES ('a');
		INSERT INTO premise_probe (v) VALUES ('b');
	`)

	assertVtgateServesBackupPositionProbes(t, ctx, mysqlDSN, grpcEndpoint)
}

// assertVtgateServesBackupPositionProbes is the shared body of the two
// vtgate premise pins. It is DUPLICATED verbatim in the vitesscluster
// sibling rather than shared from one file: the two suites' build tags are
// mutually exclusive (a `vstream || vitesscluster` expression is refused by
// scripts/vet-tags.sh, which only handles conjunctions), and the package
// already carries same-named helpers across that seam for the same reason.
func assertVtgateServesBackupPositionProbes(t *testing.T, ctx context.Context, mysqlDSN, grpcEndpoint string) {
	t.Helper()

	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open vtgate mysql: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Ground truth first, as TEXT. "binlogEnabled returned an error"
	// cannot distinguish "vtgate declined the variable" from "the driver
	// could not scan what vtgate sent", and those want different fixes.
	var raw string
	if err := db.QueryRowContext(ctx, binlogEnabledQuery).Scan(&raw); err != nil {
		t.Fatalf("vtgate declined %q: %v\n"+
			"The premise on [binlogEnabledQuery] has broken for this vtgate. Consequence: every "+
			"`backup full` on a planetscale/vitess source that degrades off the VStream snapshot "+
			"path now FAILS OUTRIGHT at the preflight instead of falling back.",
			binlogEnabledQuery, err)
	}
	t.Logf("vtgate answered %q with %q", binlogEnabledQuery, raw)

	on, err := binlogEnabled(ctx, db)
	if err != nil {
		t.Fatalf("binlogEnabled through vtgate: %v (vtgate answered %q, which the bool scan "+
			"could not take — the variable is served but not in a shape sluice reads)", err, raw)
	}
	if !on {
		t.Fatalf("binlogEnabled through vtgate = false (raw %q). A Vitess tablet runs mysqld WITH "+
			"binary logging — VReplication and VStream both ride it — so false here means the "+
			"probe is reading something other than a serving tablet's mysqld, and the capture "+
			"would silently record NO end position on a perfectly healthy source", raw)
	}

	// The sibling vtgate-served probe (item 149). It is read on EVERY
	// MySQL-family target including these two, and its doc recorded the
	// vtgate answer as a measured-but-unpinned premise. Same surface, same
	// failure direction, one boot — so it is graded here rather than left
	// as prose.
	lct, err := readLowerCaseTableNames(ctx, db)
	if err != nil {
		t.Fatalf("vtgate declined @@global.lower_case_table_names: %v — the table-name-fold "+
			"preflight would now fail the create-tables phase on every VStream target "+
			"(see table_name_fold.go)", err)
	}
	t.Logf("vtgate answered @@global.lower_case_table_names with %d", lct)

	// Bind the probe to the thing that depends on it. Two facts can each
	// be pinned and still leave the ARGUMENT unpinned; what actually
	// matters is that the preflight does not turn a healthy VStream
	// source's capture into a refusal or an empty position.
	//
	// The recorded position is BINLOG-shaped here, which VStream
	// chain-resume cannot decode (GitHub #16) — a pre-existing property of
	// this degraded fallback, not something this pin blesses. What is
	// asserted is only that the capture succeeds and is non-empty.
	exercised := 0
	for _, f := range registeredFlavors() {
		if !f.usesVStream() {
			continue
		}
		exercised++
		t.Run(f.String(), func(t *testing.T) {
			// The DSN a real `--source-driver=<flavor>` invocation
			// produces, vstream_* extensions and all — the strip logic is
			// part of the path under test (Bug 126).
			sluiceDSN := mysqlDSN +
				"&vstream_endpoint=" + grpcEndpoint +
				"&vstream_transport=plaintext" +
				"&vstream_auth=none" +
				"&vstream_shards=0"

			sr, err := Engine{Flavor: f}.OpenSchemaReader(ctx, sluiceDSN)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer func() {
				if c, ok := sr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()

			capturer, ok := sr.(interface {
				CaptureBackupPosition(context.Context, string) (ir.Position, error)
			})
			if !ok {
				t.Fatal("mysql SchemaReader no longer implements the backup PositionCapturer")
			}
			pos, err := capturer.CaptureBackupPosition(ctx, "")
			if err != nil {
				t.Fatalf("CaptureBackupPosition through vtgate: %v — a `backup full` degrading off "+
					"the VStream snapshot path would fail here", err)
			}
			if pos.Token == "" {
				t.Fatal("CaptureBackupPosition recorded NO position on a healthy VStream source; " +
					"the log_bin preflight is refusing the healthy case")
			}
			decoded, ok, err := decodeBinlogPos(pos)
			if err != nil || !ok {
				t.Fatalf("decodeBinlogPos(%q) = ok %v, err %v", pos.Token, ok, err)
			}
			if decoded.Mode == positionModeGTID && decoded.GTIDSet == "" {
				t.Fatal("the capture recorded an EMPTY GTID set on a source with committed " +
					"transactions — exactly the well-formed-looking, resumes-nothing cursor the " +
					"log_bin preflight exists to prevent")
			}
			t.Logf("%s: captured %s", f, pos.Token)
		})
	}
	if exercised < 2 {
		t.Fatalf("only %d VStream flavor(s) exercised; both planetscale and vitess reach this "+
			"capturer door, so a one-flavor run is the representative, not the class", exercised)
	}
}
