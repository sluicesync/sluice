//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Restore-parity oracle harness (roadmap item 51): migrate the same
// PG source through sluice AND through `pg_dump | psql`, then diff
// `pg_dump --schema-only` of the two targets statement-by-statement.
// Any divergence not covered by dumpParityAllowlist (every entry
// cited, TRIAGE-marked when undocumented) fails the test.
//
// One container hosts all three databases (source_db, parity_sluice,
// parity_pgdump) so pg_dump/psql client and server versions always
// match — the dump/restore leg runs *inside* the container via Exec.
// The comparator itself is pure (dumpparity.go) and unit-pinned
// without Docker.

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
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	// Register the postgres engine so engines.Get("postgres") works.
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Vacuous-pass floors (roadmap item 51, gotcha 2). Derived from the
// feature checklist in testdata/dump_parity_seed.sql — if the parser/
// normalizer yields fewer CREATE statements than the seed declares,
// the comparator is eating statements and an empty diff must NOT read
// as parity.
//
// Oracle side (full-fidelity pg_dump restore): 1 enum type + 1 domain
// + 1 standalone sequence + 1 serial backing sequence
// (legacy_counters_id_seq) + 8 tables (customers, orders, shipments,
// bookings, legacy_counters, events + 2 partitions) + 4 secondary
// indexes (incl. the plain unique index shipments_order_carrier_uidx;
// the *_unique UNIQUE constraints dump as ALTERs, not CREATEs) = 16.
// Sluice side: the partition family (3 tables) is knowingly absent
// and the serial backing sequence is modernized into an identity
// column; the standalone sequence IS carried (item-51 finding #1
// fix), and identity/PK/unique objects dump as ALTERs, not CREATEs.
// Floor: 5 carried tables + enum + domain + 1 standalone sequence +
// the 2 sluice state tables = 10 (indexes excluded from the floor so
// an index-fidelity regression surfaces as a DIFF, not a guard trip).
const (
	dumpParityOracleCreateFloor = 16
	dumpParitySluiceCreateFloor = 10
)

// startDumpParityPostgres boots one PG container with the source
// database plus the two parity target databases, returning the
// container (for in-container pg_dump/psql execs) and the source DSN.
func startDumpParityPostgres(t *testing.T) (ctr *pgtc.PostgresContainer, sourceDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", srcConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, name := range []string{"parity_sluice", "parity_pgdump"} {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
			terminate()
			t.Fatalf("create %s: %v", name, err)
		}
	}

	return container, srcConn, terminate
}

// dumpParityExec runs a bash script inside the container with
// pipefail, draining combined output and failing loudly on a nonzero
// exit so a broken pg_dump/psql leg can't silently produce an empty
// dump.
func dumpParityExec(t *testing.T, ctx context.Context, ctr *pgtc.PostgresContainer, script string) {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"bash", "-o", "pipefail", "-c", script}, tcexec.Multiplexed())
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

// dumpParitySchemaDump produces the schema-only dump of dbName by
// running pg_dump inside the container to a file (keeping stderr out
// of the captured text) and copying the file back out.
//
// --no-owner/--no-privileges: both targets are owned by the same
// role, so ownership/ACL statements carry no fidelity signal — they
// would only add ledger noise.
func dumpParitySchemaDump(t *testing.T, ctx context.Context, ctr *pgtc.PostgresContainer, dbName string) string {
	t.Helper()
	path := "/tmp/parity_" + dbName + ".sql"
	dumpParityExec(t, ctx, ctr, fmt.Sprintf(
		"pg_dump -U test --schema-only --no-owner --no-privileges -f %s %s", path, dbName,
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

// TestMigrate_DumpParity_PGKitchenSink is the item-51 oracle run over
// the kitchen-sink seed. The partitioned `events` family is excluded
// from the sluice leg (Bug 100 refuses partitioned parents loudly);
// the oracle leg carries it, and the resulting oracle-side surplus is
// an allowlisted, cited degradation — the harness working as
// designed, not a comparator gap.
func TestMigrate_DumpParity_PGKitchenSink(t *testing.T) {
	ctr, sourceDSN, cleanup := startDumpParityPostgres(t)
	defer cleanup()

	seed, err := os.ReadFile(filepath.Join("testdata", "dump_parity_seed.sql"))
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	applyPGDDL(t, sourceDSN, string(seed))

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sluiceDSN, err := buildPGDSN(sourceDSN, "parity_sluice")
	if err != nil {
		t.Fatalf("build sluice-target DSN: %v", err)
	}

	filter, err := migcore.NewTableFilter(nil, []string{"events", "events_*"})
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: sluiceDSN,
		Filter:    filter,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Oracle leg: pg_dump | psql entirely inside the container, so
	// client and server versions match by construction.
	dumpParityExec(t, ctx, ctr,
		"pg_dump -U test --schema-only source_db | psql -q -U test -v ON_ERROR_STOP=1 -d parity_pgdump")

	sluiceStmts := parseSchemaDump(dumpParitySchemaDump(t, ctx, ctr, "parity_sluice"))
	oracleStmts := parseSchemaDump(dumpParitySchemaDump(t, ctx, ctr, "parity_pgdump"))

	// Vacuous-pass guard BEFORE diffing: an empty diff because the
	// comparator ate everything must not read as parity.
	if n := countCreateStatements(oracleStmts); n < dumpParityOracleCreateFloor {
		t.Fatalf("vacuous-pass guard: oracle dump yielded %d CREATE statements; seed declares >= %d — the comparator is eating statements", n, dumpParityOracleCreateFloor)
	}
	if n := countCreateStatements(sluiceStmts); n < dumpParitySluiceCreateFloor {
		t.Fatalf("vacuous-pass guard: sluice dump yielded %d CREATE statements; seed declares >= %d — the comparator is eating statements", n, dumpParitySluiceCreateFloor)
	}

	diff := diffDumpStatements(sluiceStmts, oracleStmts)
	if diff.Empty() {
		t.Log("dump parity: FULL PARITY (no divergences)")
		return
	}

	// Walk the ledger: every divergence is either allowlisted (logged,
	// TRIAGE entries banner-marked) or a failure.
	var unlisted int
	report := func(side, key, detail string) {
		e := matchDumpParityAllowlist(key, dumpParityAllowlist)
		if e == nil {
			unlisted++
			t.Errorf("UNLISTED PARITY DIVERGENCE [%s] %s\n  %s", side, key, detail)
			return
		}
		marker := "ALLOWLISTED"
		if e.Citation == dumpParityTriageCitation {
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

	// Belt-and-braces: the report() Errorf already failed the test for
	// each unlisted divergence; this summary makes the count explicit.
	if unlisted > 0 {
		t.Errorf("dump parity: %d divergence(s) not covered by dumpParityAllowlist — each is either a missing documented-degradation entry (cite it) or a latent bug (TRIAGE it and file the finding)", unlisted)
	}
}
