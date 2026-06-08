//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Full Vitess cluster harness (ADR-0073 track item (b2)). Unlike the
// `vstream`-tagged vttestserver tests — which run vtcombo, whose
// online-DDL scheduler is stubbed ("not implemented in vtcombo") — this
// harness boots a REAL multi-process Vitess cluster:
//
//	etcd + vtctld + a PRIMARY vttablet + a REPLICA vttablet
//	(each with its own MySQL) + vtgate
//
// so the genuine online-DDL scheduler runs the VReplication copy + the
// atomic rename cutover that vtcombo cannot. This is the vehicle the
// ADR-0073 tiering decision calls "tier 2" — the one that proves the
// full cutover-survival of ADR-0073 (c) that vttestserver could not.
//
// It is deliberately gated behind its OWN build tag (`vitesscluster`,
// distinct from `vstream`) so this heavy harness (5 long-lived
// containers, ~30-60s boot, ~2.6 GB image) is NOT pulled into the
// per-PR `Integration (vstream)` gate. Run it manually / in a separate
// gated job:
//
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessCluster' ./internal/engines/mysql/...
//
// Resource needs: Docker with ~2 GB free RAM for the stack and the
// vitess/lite:v24.0.1 + quay.io/coreos/etcd images pulled (~2.7 GB
// disk). On Windows/Rancher Desktop, docker.exe must be reachable (the
// harness probes the Rancher install path) and TESTCONTAINERS_RYUK is
// irrelevant here because the harness drives `docker compose` directly
// rather than via testcontainers' container API.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Fixed host ports the harness publishes vtgate on. They are
// env-substitutable in the compose file (VTGATE_MYSQL_PORT /
// VTGATE_GRPC_PORT) but the harness uses the proven defaults; because
// the cluster boot is heavy this suite is not meant to run in parallel
// with itself, so a fixed port is acceptable and keeps the harness
// simple.
const (
	clusterMySQLPort = 15306
	clusterGRPCPort  = 15991
	clusterKeyspace  = "test"
)

// defaultVitessLiteImage mirrors the compose file's default. The harness
// boots on this unless VITESS_LITE_IMAGE overrides it (the multi-version
// matrix in scripts/vitess-version-matrix.{ps1,sh} drives that override).
// Keep this MAJOR in lockstep with the vendored vitess.io/vitess client.
const defaultVitessLiteImage = "vitess/lite:v24.0.1"

// vitessClusterImage reports which Vitess image the cluster will boot on,
// honoring the VITESS_LITE_IMAGE override the compose file reads.
func vitessClusterImage() string {
	if img := os.Getenv("VITESS_LITE_IMAGE"); img != "" {
		return img
	}
	return defaultVitessLiteImage
}

// vitessClusterMajor extracts the Vitess MAJOR version from the booted
// image's tag (e.g. "vitess/lite:v21.0.6" -> 21). It returns ok=false for
// `latest`, untagged, or any tag that isn't the `vNN.x.y` shape — those are
// treated as MODERN (no legacy-flag override layered). This gates the
// legacy-underscore-flag compose override: v21/v22 server binaries only
// accept underscore flags, so the override is layered when major <= 22.
func vitessClusterMajor() (int, bool) {
	img := vitessClusterImage()
	tag := img[strings.LastIndex(img, ":")+1:]
	if !strings.HasPrefix(tag, "v") {
		return 0, false
	}
	rest := tag[1:]
	if dot := strings.IndexByte(rest, '.'); dot >= 0 {
		rest = rest[:dot]
	}
	major, err := strconv.Atoi(rest)
	if err != nil {
		return 0, false
	}
	return major, true
}

// startVitessCluster boots the full Vitess cluster defined in
// testdata/vitesscluster/docker-compose.yml and returns the vtgate
// MySQL DSN (for SQL setup), the vtgate gRPC endpoint (for VStream),
// the keyspace, and a cleanup that tears the stack down.
//
// Mirrors the ergonomics of startVTTestServer (cdc_vstream_integration_test.go)
// so the engine tests can drive it identically; the only difference is
// the underlying vehicle (a real cluster vs vtcombo).
//
// The harness shells out to `docker compose` rather than adding the
// testcontainers compose module as a dependency: the dependency surface
// stays zero, and `docker compose` already handles the multi-service
// dependency graph + healthchecks the cluster needs.
func startVitessCluster(t *testing.T) (mysqlDSN, grpcEndpoint, keyspace string, cleanup func()) {
	t.Helper()

	dockerBin := findDocker(t)
	composeFile := composeFilePath(t)
	project := fmt.Sprintf("sluice-vitesscluster-%d", os.Getpid())

	// Log the Vitess image up front so a matrix run's per-version output is
	// self-identifying (which server version this boot exercised). The
	// vendored client stays v24.0.1; older servers are reached via
	// newer-client->older-server skew (the rolling-upgrade direction).
	t.Logf("vitesscluster: booting on %s (override via VITESS_LITE_IMAGE; vendored client = vitess.io/vitess v0.24.1)", vitessClusterImage())

	// The base compose uses the modern HYPHENATED server flags (canonical
	// from Vitess v23). v21/v22 server binaries only accept the LEGACY
	// UNDERSCORE form, so for major <= 22 we layer the legacy-flags override
	// onto the base. This MUST be in the `-f` set of EVERY runCompose call
	// (up/down/logs) or docker compose computes a different project view for
	// teardown/log than for up. v23+ / latest / unparseable tags skip it and
	// stay on the clean hyphen form (no deprecated flags on the modern path).
	composeFiles := []string{"-f", composeFile}
	if major, ok := vitessClusterMajor(); ok && major <= 22 {
		composeFiles = append(composeFiles, "-f", legacyFlagsOverridePath(t))
		t.Logf("vitesscluster: layering legacy-underscore-flag override for Vitess major %d (<=22)", major)
	}

	// Inherit the env and pin the project name + ports so a stale stack
	// from a crashed run doesn't collide and teardown is unambiguous.
	baseEnv := append(
		os.Environ(),
		"COMPOSE_PROJECT="+project,
		fmt.Sprintf("VTGATE_MYSQL_PORT=%d", clusterMySQLPort),
		fmt.Sprintf("VTGATE_GRPC_PORT=%d", clusterGRPCPort),
	)

	runCompose := func(ctx context.Context, args ...string) ([]byte, error) {
		full := append(append([]string{"compose"}, composeFiles...), "-p", project)
		full = append(full, args...)
		cmd := exec.CommandContext(ctx, dockerBin, full...)
		cmd.Env = baseEnv
		return cmd.CombinedOutput()
	}

	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if out, err := runCompose(ctx, "down", "-v", "--remove-orphans"); err != nil {
			t.Logf("cluster teardown: %v\n%s", err, out)
		}
	}

	// Bring the stack up detached. `up -d` returns once containers are
	// created/started and healthcheck-gated dependencies satisfied; the
	// `init` one-shot performs the primary reparent.
	upCtx, upCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer upCancel()
	if out, err := runCompose(upCtx, "up", "-d"); err != nil {
		cleanup()
		t.Fatalf("docker compose up: %v\n%s", err, out)
	}

	mysqlDSN = fmt.Sprintf(
		"root@tcp(127.0.0.1:%d)/%s?parseTime=true&interpolateParams=true",
		clusterMySQLPort, clusterKeyspace,
	)
	grpcEndpoint = fmt.Sprintf("127.0.0.1:%d", clusterGRPCPort)

	// THE load-bearing readiness gate: after the replica joins and the
	// primary is reparented, vtgate needs a few seconds before it
	// advertises a healthy PRIMARY. Until then a write through vtgate
	// fails with "no healthy tablet available ... tablet_type:PRIMARY".
	// Seeding before this point is the race that bit the Phase-A bring-up.
	// We poll a trivial write through the vtgate MySQL port until it
	// succeeds, which proves the whole topo->vtgate->primary path is live.
	if err := waitForWritablePrimary(t, mysqlDSN, 4*time.Minute); err != nil {
		out, _ := runCompose(context.Background(), "logs", "--tail", "40")
		cleanup()
		t.Fatalf("cluster never reached writable PRIMARY: %v\nrecent logs:\n%s", err, out)
	}

	return mysqlDSN, grpcEndpoint, clusterKeyspace, cleanup
}

// waitForWritablePrimary polls a CREATE/DROP through the vtgate MySQL
// port until it succeeds, confirming vtgate is routing to a healthy
// serving PRIMARY. This is the readiness signal the rest of the test
// depends on; without it the seed DDL races vtgate's healthcheck.
func waitForWritablePrimary(t *testing.T, dsn string, timeout time.Duration) error {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		return fmt.Errorf("open vtgate mysql: %w", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		_, lastErr = db.ExecContext(ctx,
			"CREATE TABLE IF NOT EXISTS _sluice_readiness (id INT PRIMARY KEY); DROP TABLE _sluice_readiness")
		cancel()
		if lastErr == nil {
			return nil
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("timed out after %v; last error: %w", timeout, lastErr)
}

// composeFilePath resolves the absolute path to the harness's compose
// file relative to this source file (the test runs with the package dir
// as cwd, but an absolute path is robust to that).
func composeFilePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed; cannot locate compose file")
	}
	return filepath.Join(filepath.Dir(thisFile), "testdata", "vitesscluster", "docker-compose.yml")
}

// legacyFlagsOverridePath returns the absolute path to the
// docker-compose.legacy-flags.yml override (layered for Vitess major <= 22).
func legacyFlagsOverridePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed; cannot locate legacy-flags override")
	}
	return filepath.Join(filepath.Dir(thisFile), "testdata", "vitesscluster", "docker-compose.legacy-flags.yml")
}

// findDocker returns a usable `docker` binary path. It prefers a
// docker on PATH; on Windows it falls back to the Rancher Desktop
// install location (which is frequently missing from PATH per
// docs/dev/development.md). Skips the test if none is found.
func findDocker(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("docker"); err == nil {
		return p
	}
	if runtime.GOOS == "windows" {
		rancher := `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\docker.exe`
		if _, err := os.Stat(rancher); err == nil {
			return rancher
		}
	}
	t.Skip("docker not found on PATH (and no Rancher Desktop docker.exe); skipping Vitess cluster harness")
	return ""
}

// applyClusterSQL runs DDL/DML against the cluster's vtgate MySQL port.
// Mirrors applyVTTestSQL but for the cluster DSN. multiStatements is
// caller-controlled via the DSN.
func applyClusterSQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

// launchOnlineDDL fires an online (ddl_strategy='vitess') statement
// through vtgate and returns the migration UUID vtgate hands back.
func launchOnlineDDL(t *testing.T, dsn, alter string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The session var and the ALTER must share one connection; the
	// driver returns the UUID as the single result row of the ALTER.
	var uuid string
	row := db.QueryRowContext(ctx, "SET @@ddl_strategy='vitess'; "+alter)
	if err := row.Scan(&uuid); err != nil {
		t.Fatalf("launch online DDL: %v", err)
	}
	return strings.TrimSpace(uuid)
}

// waitMigrationComplete polls SHOW VITESS_MIGRATIONS until the named
// migration reaches migration_status='complete' (the real cutover the
// scheduler performs) or fails the test. This is the signal vtcombo
// could never reach.
func waitMigrationComplete(t *testing.T, dsn, uuid string, timeout time.Duration) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(timeout)
	var lastStatus string
	for time.Now().Before(deadline) {
		status, msg := migrationStatus(t, db, uuid)
		lastStatus = status
		switch status {
		case "complete":
			return
		case "failed", "cancelled":
			t.Fatalf("migration %s reached terminal status %q (message: %s); want complete", uuid, status, msg)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("migration %s did not reach complete within %v (last status: %q)", uuid, timeout, lastStatus)
}

// migrationStatus reads the (status, message) for a migration UUID from
// SHOW VITESS_MIGRATIONS. Returns empty status if the row isn't found
// yet (the scheduler may not have recorded it on the first poll).
func migrationStatus(t *testing.T, db *sql.DB, uuid string) (status, message string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SHOW VITESS_MIGRATIONS")
	if err != nil {
		t.Fatalf("SHOW VITESS_MIGRATIONS: %v", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("migration columns: %v", err)
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan migration row: %v", err)
		}
		row := map[string]string{}
		for i, c := range cols {
			row[c] = cellString(vals[i])
		}
		if row["migration_uuid"] == uuid {
			status, message = row["migration_status"], row["message"]
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migration rows: %v", err)
	}
	return status, message
}

// cellString coerces a driver cell value (often []byte for text
// columns) to a string.
func cellString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// targetRowCount returns COUNT(*) of the named table through vtgate —
// the source of truth for the zero-loss assertion.
func targetRowCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	//nolint:gosec // table is a test-controlled literal, not user input.
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
