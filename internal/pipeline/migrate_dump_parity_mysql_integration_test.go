//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Restore-parity oracle harness, MySQL dialect (roadmap item 51,
// Phase 2): migrate the same MySQL source through sluice AND through
// `mysqldump | mysql`, then diff `mysqldump --no-data` of the two
// targets element-by-element. Any divergence not covered by
// DumpParityAllowlistMySQL (every entry cited, TRIAGE-marked when
// undocumented) fails the test.
//
// One container hosts all three databases (source_db, parity_sluice,
// parity_mysqldump) so the mysqldump/mysql client and server versions
// always match — the dump/restore leg runs *inside* the container via
// Exec. The comparator itself is pure (dumpparity_mysql.go) and
// unit-pinned without Docker.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"

	// Register the mysql engine so engines.Get("mysql") works.
	_ "sluicesync.dev/sluice/internal/engines/mysql"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// Vacuous-pass floors (roadmap item 51, gotcha 2). Derived from the
// feature checklist in testdata/dump_parity_seed_mysql.sql — if the
// parser/normalizer yields fewer elements than the seed declares, the
// comparator is eating CREATE TABLE elements and an empty diff must NOT
// read as parity.
//
// Seed columns: customers 10 + orders 9 + line_items 6 + blobs 5 = 30,
// across 4 tables. The oracle leg is a lossless mysqldump→mysql restore
// so it must yield exactly those. The sluice leg carries the 4 tables
// (plus its own 2 migrate-state tables, which only add to the count) —
// floored a touch below 30 columns so a genuine column-fidelity gap
// surfaces as a DIFF rather than tripping the guard, while a parser that
// eats half the element list still trips it. The 4-table floor is hard:
// all four must land or the migrate is badly broken.
const (
	dumpParityMySQLOracleColumnFloor = 30
	dumpParityMySQLOracleTableFloor  = 4
	dumpParityMySQLSluiceColumnFloor = 28
	dumpParityMySQLSluiceTableFloor  = 4
)

// startDumpParityMySQL boots one MySQL container with the source
// database plus the two parity target databases, returning the container
// (for in-container mysqldump/mysql execs) and the source DSN.
func startDumpParityMySQL(t *testing.T) (ctr *mysqltc.MySQLContainer, sourceDSN string, cleanup func()) {
	t.Helper()

	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, name := range []string{"parity_sluice", "parity_mysqldump"} {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name+" CHARACTER SET utf8mb4"); err != nil {
			terminate()
			t.Fatalf("create %s: %v", name, err)
		}
	}

	return container, srcConn, terminate
}

// dumpParityMySQLExec runs a bash script inside the container with
// pipefail, draining combined output and failing loudly on a nonzero
// exit so a broken mysqldump/mysql leg can't silently produce an empty
// dump. MYSQL_PWD (not -p on the command line) keeps the "insecure
// password" warning off stderr.
func dumpParityMySQLExec(t *testing.T, ctx context.Context, ctr *mysqltc.MySQLContainer, script string) {
	t.Helper()
	full := "export MYSQL_PWD=rootpw; " + script
	code, reader, err := ctr.Exec(ctx, []string{"bash", "-o", "pipefail", "-c", full}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec %q: %v", script, err)
	}
	out, rerr := io.ReadAll(reader)
	if rerr != nil {
		t.Fatalf("exec %q: drain output: %v", script, rerr)
	}
	if code != 0 {
		t.Fatalf("exec %q: exit=%d\n%s", script, code, out)
	}
}

// dumpParityMySQLSchemaDump produces the schema-only dump of dbName by
// running mysqldump inside the container to a file (keeping stderr out
// of the captured text) and copying the file back out.
//
// --no-data: schema only (sequence/AUTO_INCREMENT counters and row data
// carry no schema-fidelity signal and would only add ledger noise).
// --skip-comments keeps the standard header FOREIGN_KEY_CHECKS=0 guard
// (so the oracle restore tolerates FK-before-referent ordering) while
// dropping the descriptive `-- ` comments; the comparator unwraps the
// executable `/*!… */` guards itself.
func dumpParityMySQLSchemaDump(t *testing.T, ctx context.Context, ctr *mysqltc.MySQLContainer, dbName string) string {
	t.Helper()
	path := "/tmp/parity_" + dbName + ".sql"
	dumpParityMySQLExec(t, ctx, ctr, fmt.Sprintf(
		"mysqldump -uroot --no-data --skip-comments %s > %s", dbName, path,
	))
	rc, err := ctr.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatalf("copy %s from container: %v", path, err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(data) == 0 {
		t.Fatalf("dump of %s is empty", dbName)
	}
	return string(data)
}

// TestMigrate_DumpParity_MySQLKitchenSink is the item-51 Phase-2 oracle
// run over the MySQL kitchen-sink seed. sluice migrates source_db into
// parity_sluice; `mysqldump | mysql` restores the same source into
// parity_mysqldump; the two targets' schema dumps are decomposed into
// element-identity statement sets and diffed. Every divergence is either
// allowlisted-with-citation or fails the test.
func TestMigrate_DumpParity_MySQLKitchenSink(t *testing.T) {
	ctr, sourceDSN, cleanup := startDumpParityMySQL(t)
	defer cleanup()

	seed, err := os.ReadFile(filepath.Join("testdata", "dump_parity_seed_mysql.sql"))
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	applyMySQLDDL(t, sourceDSN, string(seed))

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	sluiceDSN, err := buildMySQLDSN(sourceDSN, "parity_sluice")
	if err != nil {
		t.Fatalf("build sluice-target DSN: %v", err)
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: sluiceDSN,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Oracle leg: mysqldump | mysql entirely inside the container, so
	// client and server versions match by construction. The header
	// FOREIGN_KEY_CHECKS=0 guard lets a FK-before-referent CREATE TABLE
	// restore cleanly.
	dumpParityMySQLExec(t, ctx, ctr,
		"mysqldump -uroot --no-data --skip-comments source_db | mysql -uroot parity_mysqldump")

	sluiceStmts := backup.ParseMySQLSchemaDump(dumpParityMySQLSchemaDump(t, ctx, ctr, "parity_sluice"))
	oracleStmts := backup.ParseMySQLSchemaDump(dumpParityMySQLSchemaDump(t, ctx, ctr, "parity_mysqldump"))

	// Vacuous-pass guard BEFORE diffing: an empty diff because the
	// comparator ate everything must not read as parity.
	if n := backup.CountMySQLColumns(oracleStmts); n < dumpParityMySQLOracleColumnFloor {
		t.Fatalf("vacuous-pass guard: oracle dump yielded %d columns; seed declares >= %d — the comparator is eating statements", n, dumpParityMySQLOracleColumnFloor)
	}
	if n := backup.CountMySQLTables(oracleStmts); n < dumpParityMySQLOracleTableFloor {
		t.Fatalf("vacuous-pass guard: oracle dump yielded %d tables; seed declares >= %d — the comparator is eating statements", n, dumpParityMySQLOracleTableFloor)
	}
	if n := backup.CountMySQLColumns(sluiceStmts); n < dumpParityMySQLSluiceColumnFloor {
		t.Fatalf("vacuous-pass guard: sluice dump yielded %d columns; seed declares >= %d — the comparator is eating statements", n, dumpParityMySQLSluiceColumnFloor)
	}
	if n := backup.CountMySQLTables(sluiceStmts); n < dumpParityMySQLSluiceTableFloor {
		t.Fatalf("vacuous-pass guard: sluice dump yielded %d tables; seed declares >= %d — the comparator is eating statements", n, dumpParityMySQLSluiceTableFloor)
	}

	diff := backup.DiffDumpStatements(sluiceStmts, oracleStmts)
	if diff.Empty() {
		t.Log("dump parity: FULL PARITY (no divergences)")
		return
	}

	// Walk the ledger: every divergence is either allowlisted (logged,
	// TRIAGE entries banner-marked) or a failure.
	var unlisted int
	report := func(side, key, detail string) {
		e := backup.MatchDumpParityAllowlist(key, backup.DumpParityAllowlistMySQL)
		if e == nil {
			unlisted++
			t.Errorf("UNLISTED PARITY DIVERGENCE [%s] %s\n  %s", side, key, detail)
			return
		}
		marker := "ALLOWLISTED"
		if e.Citation == backup.DumpParityTriageCitation {
			marker = "TRIAGE (latent gap under investigation)"
		}
		t.Logf("%s [%s] %s\n  reason: %s\n  citation: %s\n  %s", marker, side, key, e.Reason, e.Citation, detail)
	}
	for _, s := range diff.OnlyInSluice {
		report("only-in-sluice", s.Key, "stmt: "+s.Body)
	}
	for _, s := range diff.OnlyInOracle {
		report("only-in-oracle", s.Key, "stmt: "+s.Body)
	}
	for _, m := range diff.Mismatched {
		report("mismatch", m.Key, "sluice: "+m.Sluice+"\n  oracle: "+m.Oracle)
	}
	t.Logf("dump parity ledger: %d only-in-sluice, %d only-in-oracle, %d mismatched, %d unlisted",
		len(diff.OnlyInSluice), len(diff.OnlyInOracle), len(diff.Mismatched), unlisted)

	if unlisted > 0 {
		t.Errorf("dump parity: %d divergence(s) not covered by DumpParityAllowlistMySQL — each is either a missing documented-degradation entry (cite it) or a latent bug (TRIAGE it and file the finding)", unlisted)
	}
}
