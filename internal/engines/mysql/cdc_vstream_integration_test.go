//go:build integration && vstream

// VStream integration tests using vitess/vttestserver — Vitess's
// all-in-one test harness that runs vtcombo (vtgate + vttablet +
// etcd in a single binary) plus an embedded MySQL with binlogs
// enabled. Gated behind a separate `vstream` build tag because the
// vttestserver image is heavier (~700 MB) than the plain mysql:8.0
// the default integration suite uses, so the standard `make
// test-it` shouldn't pull it on every run.
//
// These tests prove the FlavorPlanetScale engine works against
// vanilla Vitess deployments — not just PlanetScale's hosted
// service. Real PlanetScale verification lives in
// cdc_vstream_psverify_test.go (psverify build tag).
//
// Usage from a shell:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_VTTestServer' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/orware/sluice/internal/ir"
)

// startVTTestServer boots a vitess/vttestserver container with one
// keyspace, one shard, returns the MySQL DSN (for SQL setup) and
// the gRPC endpoint (for VStream).
//
// The container takes a noticeably long time to start — ~20–40
// seconds for vtcombo to wire up the underlying MySQL, register
// the keyspace, and start serving on gRPC. The wait strategy looks
// for the canonical "ready" log line vttestserver emits.
func startVTTestServer(t *testing.T) (mysqlDSN, grpcEndpoint, keyspace string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const (
		basePort      = 33574 // vttestserver default base; MySQL is base+3, gRPC is base+1
		mysqlPortBase = "33577/tcp"
		grpcPortBase  = "33575/tcp"
	)

	keyspace = "test"

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "vitess/vttestserver:mysql80",
		ExposedPorts: []string{mysqlPortBase, grpcPortBase},
		Env: map[string]string{
			"PORT":       fmt.Sprintf("%d", basePort),
			"KEYSPACES":  keyspace,
			"NUM_SHARDS": "1",
			// Without an override, vttestserver binds the MySQL
			// listener to 127.0.0.1 (container-local), which makes
			// the host-side port mapping useless. 0.0.0.0 binds on
			// all interfaces so the published port reaches us.
			"MYSQL_BIND_HOST": "0.0.0.0",
		},
		// vttestserver logs "Local cluster started." once vtcombo
		// has finished bringing up the embedded MySQL + vtgate +
		// vttablet stack and the gRPC listener is up. Pair with a
		// port check so the test doesn't race against the listener
		// finishing its bind.
		WaitingFor: wait.ForAll(
			wait.ForLog("Local cluster started."),
			wait.ForListeningPort(grpcPortBase),
			wait.ForListeningPort(mysqlPortBase),
		).WithStartupTimeoutDefault(3 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start vttestserver: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("container host: %v", err)
	}
	mysqlPort, err := container.MappedPort(ctx, mysqlPortBase)
	if err != nil {
		terminate()
		t.Fatalf("mapped mysql port: %v", err)
	}
	grpcPort, err := container.MappedPort(ctx, grpcPortBase)
	if err != nil {
		terminate()
		t.Fatalf("mapped grpc port: %v", err)
	}

	mysqlDSN = fmt.Sprintf(
		// vttestserver doesn't require auth on its embedded MySQL;
		// the user/passwd are arbitrary. parseTime=true keeps the
		// driver decoding TIMESTAMP into time.Time so the IR
		// contract holds.
		"root@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true",
		host, mysqlPort.Num(), keyspace,
	)
	grpcEndpoint = fmt.Sprintf("%s:%d", host, grpcPort.Num())
	return mysqlDSN, grpcEndpoint, keyspace, terminate
}

// TestVStream_VTTestServer_BasicChangeStream is the spine
// integration test for the VStream reader against vanilla Vitess:
// open against vttestserver with vstream_transport=plaintext +
// vstream_auth=none, perform DML on the SQL side, and assert the
// resulting ir.Insert / ir.Update / ir.Delete events arrive on the
// CDC channel.
//
// Mirrors the binlog reader's TestCDCReader_BasicChangeStream
// shape so the diff between the two CDC paths is purely the
// transport.
func TestVStream_VTTestServer_BasicChangeStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, keyspace, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			email  VARCHAR(255) NOT NULL,
			active TINYINT(1)   NOT NULL DEFAULT 1,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	// Build a sluice DSN with the VStream knobs set for
	// vttestserver: gRPC endpoint override, plaintext transport,
	// no auth, and the vttestserver shard convention ("0" instead
	// of PlanetScale's "-" default).
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Stream from "current" — head of the binlog at request time.
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle window: vtgate's stream takes a moment to register at
	// "current"; events generated too quickly can land just before
	// the boundary and get dropped.
	time.Sleep(2 * time.Second)

	const dml = `
		INSERT INTO users (email, active) VALUES ('alice@example.com', 1);
		INSERT INTO users (email, active) VALUES ('bob@example.com', 0);
		UPDATE users SET active = 0 WHERE email = 'alice@example.com';
		DELETE FROM users WHERE email = 'bob@example.com';
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", dml)

	got := drainVTTestChanges(t, ctx, changes, 4, 60*time.Second)
	if len(got) != 4 {
		if cdcRdr, ok := rdr.(*vstreamCDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 4 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 4", len(got))
	}

	// Verify the four-event sequence: insert alice, insert bob,
	// update alice, delete bob.
	insAlice, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("got[0] = %T; want ir.Insert", got[0])
	}
	if email, _ := insAlice.Row["email"].(string); email != "alice@example.com" {
		t.Errorf("got[0].Row[email] = %#v; want alice@example.com", insAlice.Row["email"])
	}

	insBob, ok := got[1].(ir.Insert)
	if !ok {
		t.Fatalf("got[1] = %T; want ir.Insert", got[1])
	}
	if email, _ := insBob.Row["email"].(string); email != "bob@example.com" {
		t.Errorf("got[1].Row[email] = %#v; want bob@example.com", insBob.Row["email"])
	}

	upd, ok := got[2].(ir.Update)
	if !ok {
		t.Fatalf("got[2] = %T; want ir.Update", got[2])
	}
	if email, _ := upd.After["email"].(string); email != "alice@example.com" {
		t.Errorf("got[2].After[email] = %#v; want alice@example.com", upd.After["email"])
	}

	del, ok := got[3].(ir.Delete)
	if !ok {
		t.Fatalf("got[3] = %T; want ir.Delete", got[3])
	}
	if email, _ := del.Before["email"].(string); email != "bob@example.com" {
		t.Errorf("got[3].Before[email] = %#v; want bob@example.com", del.Before["email"])
	}

	// Position bookkeeping: every emitted change must carry a
	// non-empty position the engine can decode.
	for i, c := range got {
		if c.Pos().Engine != engineNameVStream {
			t.Errorf("got[%d].Pos.Engine = %q; want %q", i, c.Pos().Engine, engineNameVStream)
		}
		if c.Pos().Token == "" {
			t.Errorf("got[%d].Pos.Token is empty", i)
		}
		if shards, ok, err := decodeVStreamPos(c.Pos()); err != nil || !ok || len(shards) == 0 {
			t.Errorf("got[%d].Pos failed to decode: ok=%v err=%v shards=%v", i, ok, err, shards)
		}
	}

	// Quiet the unused-import warning for keyspace; surfacing it
	// in a log line keeps the value useful when the test logs are
	// inspected.
	t.Logf("keyspace = %q", keyspace)
}

// applyVTTestSQL runs DDL/DML against vttestserver's embedded
// MySQL. The connection is short-lived; multiStatements=true is
// caller-controlled (DML batches typically need it; the seed DDL
// is one statement and doesn't).
func applyVTTestSQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

// drainVTTestChanges drains up to want events with a timeout.
// Returned slice may be shorter than want if the stream closed
// early; caller asserts.
func drainVTTestChanges(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	want int,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	got := make([]ir.Change, 0, want)
	for len(got) < want {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			got = append(got, c)
		case <-deadline.C:
			t.Logf("timed out after %v with %d/%d changes", timeout, len(got), want)
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}
