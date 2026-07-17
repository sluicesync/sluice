//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// F1 (Bug 194 review finding) — TRANSACTION-MODE pooler cell. The
// extra_float_digits pins must be SET statements (poolers strip the GUC
// from startup packets), but a bare autocommit SET followed by the
// COPY/hash query is not pooler-proof either: under transaction-mode
// pooling (Supabase's RECOMMENDED :6543 endpoint) consecutive
// autocommit statements may land on DIFFERENT backends, leaving the
// data statement silently unpinned (rc=0 — Bug 194 alive behind the
// exact endpoint the docs steer users to). The fix wraps the pins +
// data statement in ONE explicit transaction (BEGIN; SET LOCAL …;
// <stmt>; COMMIT) — a transaction is what pins a server backend in
// transaction-mode pooling, and SET LOCAL scopes the pin to it.
//
// This test is the honest rig: a REAL pgbouncer 1.25.x in
// pool_mode=transaction with the Supavisor-shaped
// ignore_startup_parameters, fronting a PG whose database default is
// the Supabase shape (extra_float_digits=0). It drives the two
// pooler-crossing pinned surfaces — ExportRawCopy (text) and
// SampleRowHashes — through the pooler and asserts exactness.
//
// Ground-truth gap, stated honestly: with a single client and an idle
// pool, pgbouncer tends to REUSE the same backend, so a revert to
// autocommit SET would not deterministically fail this cell (backend
// rotation under contention is what makes the unpinned bug fire in
// production). The deterministic guards are elsewhere: removing the
// BEGIN turns SET LOCAL into a warned no-op, which the direct-conn efd
// matrix (migrate_raw_copy_float_efd_pg_integration_test.go) fails
// loudly; this cell proves the transaction-scoped shape FUNCTIONS and
// stays exact through real transaction-mode pooling (hand-validated
// against pgbouncer 1.25.2: unpinned session renders π rounded, the
// BEGIN/SET LOCAL/COPY/COMMIT block renders it exact, nothing leaks).

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPGBouncerTxnRig boots a dedicated PG 17 and a pgbouncer in
// transaction mode fronting it, returning the direct admin DSN and the
// pooler DSN (both pointed at the same database). trust auth keeps the
// rig free of scram plumbing; ignore_startup_parameters mirrors
// Supavisor's (which is what forces the pins to be statements).
func startPGBouncerTxnRig(t *testing.T) (directDSN, poolerDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pgC, err := pgtc.Run(
		ctx,
		"postgres:17",
		pgtc.WithDatabase("pooled_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	terminatePG := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = pgC.Terminate(shutdown)
	}

	directDSN, err = pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminatePG()
		t.Fatalf("pg connection string: %v", err)
	}
	pgIP, err := pgC.ContainerIP(ctx)
	if err != nil {
		terminatePG()
		t.Fatalf("pg container IP: %v", err)
	}

	// pgbouncer config: transaction mode, Supavisor-shaped startup-param
	// stripping, prepared-statement support (sluice's pgx pools prepare).
	dir := t.TempDir()
	ini := fmt.Sprintf(`[databases]
* = host=%s port=5432 user=test password=test
[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
auth_type = trust
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = transaction
max_prepared_statements = 200
ignore_startup_parameters = extra_float_digits,options
`, pgIP)
	iniPath := filepath.Join(dir, "pgbouncer.ini")
	userPath := filepath.Join(dir, "userlist.txt")
	if err := os.WriteFile(iniPath, []byte(ini), 0o644); err != nil {
		terminatePG()
		t.Fatalf("write ini: %v", err)
	}
	if err := os.WriteFile(userPath, []byte("\"test\" \"test\"\n"), 0o644); err != nil {
		terminatePG()
		t.Fatalf("write userlist: %v", err)
	}

	pgbC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			// Pinned tag (v265 review nit): `latest` made the rig's pgbouncer
			// version drift silently under the test — v1.25.2-p0 is the exact
			// build the transaction-scoped pin shape was hand-validated
			// against (see the file header). Bump deliberately, re-validating
			// the unpinned-vs-pinned rendering by hand.
			Image:        "edoburu/pgbouncer:v1.25.2-p0",
			ExposedPorts: []string{"6432/tcp"},
			Files: []testcontainers.ContainerFile{
				{HostFilePath: iniPath, ContainerFilePath: "/etc/pgbouncer/pgbouncer.ini", FileMode: 0o644},
				{HostFilePath: userPath, ContainerFilePath: "/etc/pgbouncer/userlist.txt", FileMode: 0o644},
			},
			WaitingFor: wait.ForListeningPort("6432/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		terminatePG()
		t.Fatalf("start pgbouncer: %v", err)
	}
	cleanup = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = pgbC.Terminate(shutdown)
		terminatePG()
	}

	host, err := pgbC.Host(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("pgbouncer host: %v", err)
	}
	port, err := pgbC.MappedPort(ctx, "6432/tcp")
	if err != nil {
		cleanup()
		t.Fatalf("pgbouncer port: %v", err)
	}
	poolerDSN = fmt.Sprintf("postgres://test:test@%s:%s/pooled_db?sslmode=disable", host, port.Port())
	return directDSN, poolerDSN, cleanup
}

func TestRawCopyAndSampleHashes_PinnedThroughTxnModePooler(t *testing.T) {
	directDSN, poolerDSN, cleanup := startPGBouncerTxnRig(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The Supabase shape on the pooled database, plus the float corpus.
	applyPGSQL(t, directDSN, `
		CREATE TABLE pfloat (id BIGINT PRIMARY KEY, f8 float8, f4 float4);
		INSERT INTO pfloat VALUES (1, pi(), 16777215.0), (2, 2.718281828459045, NULL);
	`)
	applyPGSQL(t, directDSN, "ALTER DATABASE pooled_db SET extra_float_digits = 0")

	// --- Surface 1: the raw-copy TEXT export through the pooler. ---
	srdr, err := Engine{}.OpenSchemaReader(ctx, poolerDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader via pooler: %v", err)
	}
	defer func() {
		if c, ok := srdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := srdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema via pooler: %v", err)
	}
	var table *ir.Table
	for _, tb := range schema.Tables {
		if tb.Name == "pfloat" {
			table = tb
		}
	}
	if table == nil {
		t.Fatal("table pfloat not found via pooler")
	}

	rrdr, err := Engine{}.OpenRowReader(ctx, poolerDSN)
	if err != nil {
		t.Fatalf("OpenRowReader via pooler: %v", err)
	}
	defer func() {
		if c, ok := rrdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	exp, ok := rrdr.(ir.RawCopyExporter)
	if !ok {
		t.Fatalf("RowReader %T does not implement ir.RawCopyExporter", rrdr)
	}
	var buf bytes.Buffer
	if err := exp.ExportRawCopy(ctx, table, nil, ir.RawCopyText, &buf); err != nil {
		t.Fatalf("ExportRawCopy via txn-mode pooler: %v", err)
	}
	out := buf.String()
	// float4out renders large float4 in scientific notation:
	// "1.6777215e+07" is the 8-significant-digit shortest-exact form of
	// 16777215 (the unpinned efd=0 rendering is "1.67772e+07" — 6
	// digits, rounded to 16777200).
	for _, want := range []string{"3.141592653589793", "1.6777215e+07", "2.718281828459045"} {
		if !strings.Contains(out, want) {
			t.Errorf("text COPY through the txn-mode pooler is missing the shortest-exact rendering %q — the extra_float_digits pin did not reach the COPY's backend; stream:\n%s", want, out)
		}
	}

	// --- Surface 2: SampleRowHashes through the pooler vs a direct
	// endpoint whose database default differs (efd=1). Identical values
	// must hash identically — unpinned, the differing defaults render
	// (and hash) them differently. ---
	adminDB, err := sql.Open("pgx", directDSN)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer func() { _ = adminDB.Close() }()
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE ref_db"); err != nil {
		t.Fatalf("create ref_db: %v", err)
	}
	refDSN := strings.Replace(directDSN, "/pooled_db", "/ref_db", 1)
	applyPGSQL(t, refDSN, `
		CREATE TABLE pfloat (id BIGINT PRIMARY KEY, f8 float8, f4 float4);
		INSERT INTO pfloat VALUES (1, pi(), 16777215.0), (2, 2.718281828459045, NULL);
	`)
	applyPGSQL(t, refDSN, "ALTER DATABASE ref_db SET extra_float_digits = 1")

	sv, ok := srdr.(ir.SampleVerifier)
	if !ok {
		t.Fatalf("SchemaReader %T does not implement ir.SampleVerifier", srdr)
	}
	pooled, err := sv.SampleRowHashes(ctx, table, 10, 7, ir.HashMD5)
	if err != nil {
		t.Fatalf("SampleRowHashes via txn-mode pooler: %v", err)
	}

	refRdr, err := Engine{}.OpenSchemaReader(ctx, refDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader ref: %v", err)
	}
	defer func() {
		if c, ok := refRdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	refSV := refRdr.(ir.SampleVerifier)
	ref, err := refSV.SampleRowHashes(ctx, table, 10, 7, ir.HashMD5)
	if err != nil {
		t.Fatalf("SampleRowHashes ref: %v", err)
	}
	if len(pooled) != 2 || len(ref) != 2 {
		t.Fatalf("sample sizes = %d/%d; want 2 each", len(pooled), len(ref))
	}
	for i := range pooled {
		if pooled[i] != ref[i] {
			t.Errorf("row %s: hash via txn-mode pooler (efd-0 db) %s != direct efd-1 db %s — the SET LOCAL pin did not reach the hash query's backend",
				pooled[i].PrimaryKey, pooled[i].Hash, ref[i].Hash)
		}
	}
}
