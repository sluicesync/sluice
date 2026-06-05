//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0073 (b2) primary-only CDC-tail bug pins — the loud-failure-tenet
// fix + the vstream_tablet_type usability knob.
//
// The full-cluster compose (docker-compose.yml) boots a primary AND a
// replica vttablet, so vtgate can always serve the default REPLICA
// VStream. The PRIMARY-ONLY variant (docker-compose.primary-only.yml)
// boots ONLY the primary, so vtgate has NO REPLICA tablet — the topology
// of a PlanetScale dev branch and minimal self-hosted Vitess. Against it:
//
//	(i)  the DEFAULT (REPLICA) CDC tail must fail LOUDLY via the liveness
//	     timeout (NOT hang silently with Err()==nil) — the tenet fix;
//	(ii) vstream_tablet_type=primary makes the CDC tail WORK (DML events
//	     flow, zero loss) — the usability fix.
//
// Run (heavy — own build tag, NOT in the per-PR gate):
//
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessClusterPrimaryOnly' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Distinct host ports from the full-cluster harness so the two suites
// (and a stale stack from a crashed run) don't collide.
const (
	primaryOnlyMySQLPort = 15406
	primaryOnlyGRPCPort  = 15891
)

// startVitessClusterPrimaryOnly boots the primary-only Vitess cluster
// (docker-compose.primary-only.yml) and returns the vtgate MySQL DSN, the
// vtgate gRPC endpoint, the keyspace, and a teardown. Mirrors
// startVitessCluster's ergonomics; the only differences are the compose
// file (no replica tablet) and the published ports.
func startVitessClusterPrimaryOnly(t *testing.T) (mysqlDSN, grpcEndpoint, keyspace string, cleanup func()) {
	t.Helper()

	dockerBin := findDocker(t)
	composeFile := primaryOnlyComposeFilePath(t)
	project := fmt.Sprintf("sluice-vitesscluster-po-%d", os.Getpid())

	baseEnv := append(
		os.Environ(),
		"COMPOSE_PROJECT="+project,
		fmt.Sprintf("VTGATE_MYSQL_PORT=%d", primaryOnlyMySQLPort),
		fmt.Sprintf("VTGATE_GRPC_PORT=%d", primaryOnlyGRPCPort),
	)

	runCompose := func(ctx context.Context, args ...string) ([]byte, error) {
		full := append([]string{"compose", "-f", composeFile, "-p", project}, args...)
		cmd := exec.CommandContext(ctx, dockerBin, full...)
		cmd.Env = baseEnv
		return cmd.CombinedOutput()
	}

	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if out, err := runCompose(ctx, "down", "-v", "--remove-orphans"); err != nil {
			t.Logf("primary-only cluster teardown: %v\n%s", err, out)
		}
	}

	upCtx, upCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer upCancel()
	if out, err := runCompose(upCtx, "up", "-d"); err != nil {
		cleanup()
		t.Fatalf("docker compose up (primary-only): %v\n%s", err, out)
	}

	mysqlDSN = fmt.Sprintf(
		"root@tcp(127.0.0.1:%d)/%s?parseTime=true&interpolateParams=true",
		primaryOnlyMySQLPort, clusterKeyspace,
	)
	grpcEndpoint = fmt.Sprintf("127.0.0.1:%d", primaryOnlyGRPCPort)

	if err := waitForWritablePrimary(t, mysqlDSN, 4*time.Minute); err != nil {
		out, _ := runCompose(context.Background(), "logs", "--tail", "40")
		cleanup()
		t.Fatalf("primary-only cluster never reached writable PRIMARY: %v\nrecent logs:\n%s", err, out)
	}

	return mysqlDSN, grpcEndpoint, clusterKeyspace, cleanup
}

// primaryOnlyComposeFilePath resolves the primary-only compose file next
// to the full-cluster one. Reuses composeFilePath's resolution then swaps
// the filename so both stay anchored to this package's testdata dir.
func primaryOnlyComposeFilePath(t *testing.T) string {
	t.Helper()
	full := composeFilePath(t)
	return strings.Replace(full, "docker-compose.yml", "docker-compose.primary-only.yml", 1)
}

// TestVitessClusterPrimaryOnly_DefaultReplicaTailFailsLoudly is the
// loud-failure-tenet pin: against a primary-only cluster, the DEFAULT
// (REPLICA) CDC tail must surface a LOUD, actionable error within the
// liveness window instead of hanging silently with Err()==nil.
//
// It seeds a tiny table, opens a pure CDC tail (no snapshot, no cursor)
// with a SHORT vstream_liveness_timeout so the test is fast, and asserts:
//   - the changes channel closes (the pump terminated), and
//   - Err() is non-nil and names vstream_tablet_type (the remediation).
func TestVitessClusterPrimaryOnly_DefaultReplicaTailFailsLoudly(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessClusterPrimaryOnly(t)
	defer cleanup()

	applyClusterSQL(t, mysqlDSN, `
		CREATE TABLE gadgets (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	// Let the schema tracker settle.
	time.Sleep(3 * time.Second)

	// Default tablet type (REPLICA) + a short liveness window so the loud
	// timeout fires quickly. No vstream_tablet_type set ⇒ replica default.
	const livenessWindow = 12 * time.Second
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_liveness_timeout=%s",
		mysqlDSN, grpcEndpoint, livenessWindow,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc, ok := rdr.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *vstreamCDCReader", rdr)
	}
	defer func() { _ = cdc.Close() }()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The watchdog should close the channel within ~livenessWindow. Give
	// it generous slack but FAIL if it never closes (that would BE the
	// silent-hang bug).
	deadline := time.After(livenessWindow + 30*time.Second)
	for {
		select {
		case _, ok := <-changes:
			if ok {
				// An unexpected event flowed (a primary-only cluster
				// should yield nothing on a REPLICA request); keep
				// draining until close.
				continue
			}
			// Channel closed — the pump terminated. Assert it did so LOUDLY.
			streamErr := cdc.Err()
			if streamErr == nil {
				t.Fatal("CDC tail closed with Err()==nil against a primary-only cluster — this is the SILENT-WEDGE bug (the stream must fail loudly)")
			}
			if !strings.Contains(streamErr.Error(), "vstream_tablet_type") {
				t.Fatalf("loud error does not mention the remediation: %v", streamErr)
			}
			if !strings.Contains(streamErr.Error(), "no events within") {
				t.Fatalf("loud error is not the liveness-timeout shape: %v", streamErr)
			}
			t.Logf("loud-failure PASS: default REPLICA tail failed loudly within the window: %v", streamErr)
			return
		case <-deadline:
			t.Fatal("CDC tail neither delivered events nor closed within the liveness window + slack — SILENT HANG (the bug is NOT fixed)")
		}
	}
}

// TestVitessClusterPrimaryOnly_PrimaryTabletTypeTailWorks is the
// usability pin: with vstream_tablet_type=primary the pure CDC tail
// WORKS against a primary-only cluster — DML events flow with zero loss.
func TestVitessClusterPrimaryOnly_PrimaryTabletTypeTailWorks(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessClusterPrimaryOnly(t)
	defer cleanup()

	applyClusterSQL(t, mysqlDSN, `
		CREATE TABLE gizmos (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc, ok := rdr.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *vstreamCDCReader", rdr)
	}
	defer func() { _ = cdc.Close() }()

	// Start the tail from current — pure CDC, no cursor.
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Give the stream a moment to attach, then write rows. (vtgate buffers
	// from the requested position, so writes after attach are captured;
	// the PRIMARY tablet serves the stream so events flow.)
	time.Sleep(3 * time.Second)
	const wantRows = 10
	for i := 1; i <= wantRows; i++ {
		applyClusterSQL(t, mysqlDSN, fmt.Sprintf("INSERT INTO gizmos (name) VALUES ('row-%d')", i))
	}

	got := 0
	seen := map[string]bool{}
	deadline := time.After(2 * time.Minute)
	for got < wantRows {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("CDC channel closed before all rows arrived (got %d/%d); stream err=%v", got, wantRows, cdc.Err())
			}
			ins, ok := c.(ir.Insert)
			if !ok {
				continue
			}
			if ins.Table != "gizmos" {
				continue
			}
			name, _ := ins.Row["name"].(string)
			if strings.HasPrefix(name, "row-") && !seen[name] {
				seen[name] = true
				got++
			}
		case <-deadline:
			t.Fatalf("timed out: got %d/%d rows via vstream_tablet_type=primary; stream err=%v", got, wantRows, cdc.Err())
		}
	}

	if err := cdc.Err(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("stream errored despite working primary tail: %v", err)
	}

	// Zero loss: every distinct row-N landed exactly once.
	if got != wantRows {
		t.Fatalf("delivered %d distinct rows; want %d", got, wantRows)
	}
	if n := targetRowCount(t, mysqlDSN, "gizmos"); n != wantRows {
		t.Fatalf("source gizmos COUNT(*) = %d; want %d", n, wantRows)
	}
	t.Logf("usability PASS: vstream_tablet_type=primary delivered %d/%d rows from a primary-only cluster, zero loss", got, wantRows)
}
